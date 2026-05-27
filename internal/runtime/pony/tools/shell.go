package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

// cappedWriter is an io.Writer that stops writing after limit bytes.
type cappedWriter struct {
	w     io.Writer
	limit int
	written int
}

func (c *cappedWriter) Write(p []byte) (int, error) {
	remaining := c.limit - c.written
	if remaining <= 0 {
		return len(p), nil // silently discard
	}
	if len(p) > remaining {
		p = p[:remaining]
	}
	n, err := c.w.Write(p)
	c.written += n
	return len(p), err // report full length to avoid short-write errors upstream
}

var allowedShellCommands = []string{
	"go", "git", "make", "mise", "npm", "bun", "node",
	"ls", "cat", "echo", "grep", "find", "head", "tail", "wc",
	"sort", "uniq", "diff", "mkdir", "rmdir", "cp", "mv", "rm",
	"chmod", "date", "pwd", "which", "test", "python", "python3",
}

// maxShellOutput is the maximum bytes read from command stdout/stderr.
const maxShellOutput = 256 << 10 // 256 KB

func isCommandAllowed(cmd string) bool {
	for _, allowed := range allowedShellCommands {
		if cmd == allowed {
			return true
		}
	}
	return false
}

func NewShellTool() Tool {
	return &ShellTool{}
}

type ShellTool struct{}

type shellInput struct {
	Command string `json:"command"`
}

func (t *ShellTool) Name() string { return "shell" }

func (t *ShellTool) Description() string {
	return "Run a command and return its output. Commands are executed directly (no shell interpreter), so pipes and redirects do not work. Use git, go, and other tools individually. 30-second timeout, output capped at 256KB."
}

func (t *ShellTool) Schema() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {
			"command": {
				"type": "string",
				"description": "Shell command to run (e.g. go build ./... or git status)"
			}
		},
		"required": ["command"]
	}`)
}

func (t *ShellTool) Execute(ctx context.Context, workingDir string, args json.RawMessage) (string, error) {
	var input shellInput
	if err := json.Unmarshal(args, &input); err != nil {
		return "", fmt.Errorf("shell: invalid args: %w", err)
	}
	if input.Command == "" {
		return "", fmt.Errorf("shell: command is required")
	}

	// Split into command + args. No shell interpreter is involved — the binary
	// is executed directly. This prevents shell injection entirely.
	parts := strings.Fields(input.Command)
	if len(parts) == 0 {
		return "", fmt.Errorf("shell: command is required")
	}
	if !isCommandAllowed(parts[0]) {
		return "", fmt.Errorf("shell: command %q is not in the allowed list", parts[0])
	}

	cmdCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, parts[0], parts[1:]...)
	cmd.Dir = workingDir

	// Cap output to prevent runaway output
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &cappedWriter{w: &stdout, limit: maxShellOutput}
	cmd.Stderr = &cappedWriter{w: &stderr, limit: maxShellOutput}

	if err := cmd.Run(); err != nil {
		output := strings.TrimSpace(stdout.String())
		errOutput := strings.TrimSpace(stderr.String())
		var parts []string
		if output != "" {
			parts = append(parts, output)
		}
		if errOutput != "" {
			parts = append(parts, errOutput)
		}
		parts = append(parts, fmt.Sprintf("error: %v", err))
		return strings.Join(parts, "\n"), nil
	}

	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("shell: %w", err)
	}

	return strings.TrimSpace(stdout.String()), nil
}
