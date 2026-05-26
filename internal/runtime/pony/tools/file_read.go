package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

	// Resolve path and validate it's within working directory
	absPath := input.Path
	if !filepath.IsAbs(input.Path) {
		absPath = filepath.Join(workingDir, input.Path)
	}
	absPath, err := filepath.Abs(absPath)
	if err != nil {
		return "", fmt.Errorf("file_read: resolve path: %w", err)
	}

	wdAbs, err := filepath.Abs(workingDir)
	if err != nil {
		return "", fmt.Errorf("file_read: resolve working dir: %w", err)
	}

	// Prevent path traversal outside working directory
	if !strings.HasPrefix(absPath, wdAbs) {
		return "", fmt.Errorf("file_read: path %q is outside the working directory %q", absPath, workingDir)
	}

	// Check context before IO
	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("file_read: %w", err)
	}

	data, err := os.ReadFile(absPath)
	if err != nil {
		return "", fmt.Errorf("file_read: %w", err)
	}

	return string(data), nil
}
