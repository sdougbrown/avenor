package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

func NewFileWriteTool() Tool {
	return &FileWriteTool{}
}

type FileWriteTool struct{}

type fileWriteInput struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

func (t *FileWriteTool) Name() string { return "file_write" }

func (t *FileWriteTool) Description() string {
	return "Write content to a file at the given path. Creates parent directories if they don't exist. Overwrites existing files."
}

func (t *FileWriteTool) Schema() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {
			"path": {
				"type": "string",
				"description": "File path relative to the working directory"
			},
			"content": {
				"type": "string",
				"description": "Content to write to the file"
			}
		},
		"required": ["path", "content"]
	}`)
}

func (t *FileWriteTool) Execute(ctx context.Context, workingDir string, args json.RawMessage) (string, error) {
	var input fileWriteInput
	if err := json.Unmarshal(args, &input); err != nil {
		return "", fmt.Errorf("file_write: invalid args: %w", err)
	}
	if input.Path == "" {
		return "", fmt.Errorf("file_write: path is required")
	}

	safePath, err := safeResolvePath(workingDir, AllowedWriteDirsFromContext(ctx), input.Path)
	if err != nil {
		return "", fmt.Errorf("file_write: %w", err)
	}

	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("file_write: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(safePath), 0755); err != nil {
		return "", fmt.Errorf("file_write: create directories: %w", err)
	}

	if err := os.WriteFile(safePath, []byte(input.Content), 0644); err != nil {
		return "", fmt.Errorf("file_write: write file: %w", err)
	}

	return "ok", nil
}
