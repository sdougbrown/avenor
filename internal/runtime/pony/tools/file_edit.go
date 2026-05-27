package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

func NewFileEditTool() Tool {
	return &FileEditTool{}
}

type FileEditTool struct{}

type fileEditInput struct {
	Path      string `json:"path"`
	OldString string `json:"old_string"`
	NewString string `json:"new_string"`
}

func (t *FileEditTool) Name() string { return "file_edit" }

func (t *FileEditTool) Description() string {
	return "Edit a file by performing a string replacement. Reads the file, replaces the first occurrence of old_string with new_string, and writes it back."
}

func (t *FileEditTool) Schema() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {
			"path": {
				"type": "string",
				"description": "File path relative to the working directory"
			},
			"old_string": {
				"type": "string",
				"description": "Text to search for"
			},
			"new_string": {
				"type": "string",
				"description": "Text to replace with"
			}
		},
		"required": ["path", "old_string", "new_string"]
	}`)
}

func (t *FileEditTool) Execute(ctx context.Context, workingDir string, args json.RawMessage) (string, error) {
	var input fileEditInput
	if err := json.Unmarshal(args, &input); err != nil {
		return "", fmt.Errorf("file_edit: invalid args: %w", err)
	}
	if input.Path == "" {
		return "", fmt.Errorf("file_edit: path is required")
	}

	_, err := safeResolvePath(workingDir, AllowedReadDirsFromContext(ctx), input.Path)
	if err != nil {
		return "", fmt.Errorf("file_edit: %w", err)
	}

	safePath, err := safeResolvePath(workingDir, AllowedWriteDirsFromContext(ctx), input.Path)
	if err != nil {
		return "", fmt.Errorf("file_edit: %w", err)
	}

	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("file_edit: %w", err)
	}

	data, err := os.ReadFile(safePath)
	if err != nil {
		return "", fmt.Errorf("file_edit: read file: %w", err)
	}

	content := string(data)
	newContent := strings.Replace(content, input.OldString, input.NewString, 1)

	if newContent == content {
		return "", fmt.Errorf("file_edit: old_string not found in file")
	}

	// Preserve original file permissions
	perm := os.FileMode(0644)
	if fi, err := os.Stat(safePath); err == nil {
		perm = fi.Mode().Perm()
	}
	if err := os.WriteFile(safePath, []byte(newContent), perm); err != nil {
		return "", fmt.Errorf("file_edit: write file: %w", err)
	}

	result := content
	if len(result) > 500 {
		result = result[:500] + "..."
	}

	return result, nil
}
