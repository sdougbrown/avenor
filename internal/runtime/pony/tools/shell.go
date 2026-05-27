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

// ShellConfig allows overriding shell tool defaults per-profile.
type ShellConfig struct {
	AllowedCommands []string `json:"allowed_commands,omitempty"`
	TimeoutSeconds  int      `json:"timeout_seconds,omitempty"`
	MaxOutputBytes  int      `json:"max_output_bytes,omitempty"`
}

// DefaultShellConfig returns a ShellConfig with built-in defaults.
func DefaultShellConfig() ShellConfig {
	return ShellConfig{
		AllowedCommands: allowedShellCommands,
		TimeoutSeconds:  30,
		MaxOutputBytes:  256 << 10,
	}
}

var allowedShellCommands = []string{
	"go", "git", "make", "mise", "npm", "bun", "node",
	"ls", "cat", "echo", "grep", "find", "head", "tail", "wc",
	"sort", "uniq", "diff", "mkdir", "rmdir", "cp", "mv", "rm",
	"chmod", "date", "pwd", "which", "test", "python", "python3",
}

func NewShellTool() Tool {
	return NewShellToolWithConfig(nil)
}

// NewShellToolWithConfig creates a ShellTool with the given config overrides.
// Nil fields fall back to DefaultShellConfig values. Partial configs are merged
// with defaults so that unset fields keep sensible values.
func NewShellToolWithConfig(cfg *ShellConfig) Tool {
	t := &ShellTool{cfg: &ShellConfig{}}
	if cfg == nil {
		d := DefaultShellConfig()
		t.cfg = &d
		return t
	}
	// Merge user config with defaults
	d := DefaultShellConfig()
	if cfg.AllowedCommands != nil {
		t.cfg.AllowedCommands = cfg.AllowedCommands
	} else {
		t.cfg.AllowedCommands = d.AllowedCommands
	}
	if cfg.TimeoutSeconds > 0 {
		t.cfg.TimeoutSeconds = cfg.TimeoutSeconds
	} else {
		t.cfg.TimeoutSeconds = d.TimeoutSeconds
	}
	if cfg.MaxOutputBytes > 0 {
		t.cfg.MaxOutputBytes = cfg.MaxOutputBytes
	} else {
		t.cfg.MaxOutputBytes = d.MaxOutputBytes
	}
	return t
}

type ShellTool struct {
	cfg *ShellConfig
}

type shellInput struct {
	Command string `json:"command"`
}

func (t *ShellTool) Name() string { return "shell" }

func (t *ShellTool) Description() string {
	return fmt.Sprintf("Run a command and return its output. Commands are executed directly (no shell interpreter), so pipes and redirects do not work. Timeout: %ds, output cap: %dKB.",
		t.cfg.TimeoutSeconds, t.cfg.MaxOutputBytes/1024)
}

func (t *ShellTool) Schema() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {
			"command": {
				"type": "string",
				"description": "Command to run (e.g. go build ./... or git status)"
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
	if !t.isCommandAllowed(parts[0]) {
		return "", fmt.Errorf("shell: command %q is not in the allowed list", parts[0])
	}

	timeout := time.Duration(t.cfg.TimeoutSeconds) * time.Second
	cmdCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, parts[0], parts[1:]...)
	cmd.Dir = workingDir

	// Cap output to prevent runaway output
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &cappedWriter{w: &stdout, limit: t.cfg.MaxOutputBytes}
	cmd.Stderr = &cappedWriter{w: &stderr, limit: t.cfg.MaxOutputBytes}

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

func (t *ShellTool) isCommandAllowed(cmd string) bool {
	for _, allowed := range t.cfg.AllowedCommands {
		if cmd == allowed {
			return true
		}
	}
	return false
}
