package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
)

func NewFileReadTool() Tool {
	return &FileReadTool{}
}

type FileReadTool struct{}

type fileReadInput struct {
	Path string `json:"path"`
}

func (t *FileReadTool) Name() string { return "file_read" }

func (t *FileReadTool) Description() string {
	return "Read the contents of a file at the given path. Use this to read files from the working directory."
}

func (t *FileReadTool) Schema() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {
			"path": {
				"type": "string",
				"description": "Path to the file to read, relative to the working directory"
			}
		},
		"required": ["path"]
	}`)
}

func (t *FileReadTool) Execute(ctx context.Context, workingDir string, args json.RawMessage) (string, error) {
	var input fileReadInput
	if err := json.Unmarshal(args, &input); err != nil {
		return "", fmt.Errorf("file_read: invalid args: %w", err)
	}
	if input.Path == "" {
		return "", fmt.Errorf("file_read: path is required")
	}

	safePath, err := safeResolvePath(workingDir, input.Path)
	if err != nil {
		return "", fmt.Errorf("file_read: %w", err)
	}

	// Check context before IO
	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("file_read: %w", err)
	}

	data, err := os.ReadFile(safePath)
	if err != nil {
		return "", fmt.Errorf("file_read: %w", err)
	}

	return string(data), nil
}
