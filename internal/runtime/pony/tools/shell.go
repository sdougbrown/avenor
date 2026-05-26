package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
)

func NewShellTool() Tool {
	return &ShellTool{}
}

type ShellTool struct{}

type shellInput struct {
	Command string `json:"command"`
}

func (t *ShellTool) Name() string { return "shell" }

func (t *ShellTool) Description() string {
	return "Run a shell command and return its output. Commands are executed in the working directory with a 30-second timeout. Use this for git operations, build commands, and other CLI tools."
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

	cmdCtx, cancel := context.WithTimeout(ctx, 30*1000*1000*1000)
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, "sh", "-c", input.Command)
	cmd.Dir = workingDir

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

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
