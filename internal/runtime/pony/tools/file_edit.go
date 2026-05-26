package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

	absPath := input.Path
	if !filepath.IsAbs(input.Path) {
		absPath = filepath.Join(workingDir, input.Path)
	}
	absPath, err := filepath.Abs(absPath)
	if err != nil {
		return "", fmt.Errorf("file_edit: resolve path: %w", err)
	}

	wdAbs, err := filepath.Abs(workingDir)
	if err != nil {
		return "", fmt.Errorf("file_edit: resolve working dir: %w", err)
	}

	if !filepath.HasPrefix(absPath, wdAbs) {
		return "", fmt.Errorf("file_edit: path %q is outside the working directory %q", absPath, workingDir)
	}

	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("file_edit: %w", err)
	}

	data, err := os.ReadFile(absPath)
	if err != nil {
		return "", fmt.Errorf("file_edit: read file: %w", err)
	}

	content := string(data)
	newContent := strings.Replace(content, input.OldString, input.NewString, 1)

	if newContent == content {
		return "", fmt.Errorf("file_edit: old_string not found in file")
	}

	if err := os.WriteFile(absPath, []byte(newContent), 0644); err != nil {
		return "", fmt.Errorf("file_edit: write file: %w", err)
	}

	result := content
	if len(result) > 500 {
		result = result[:500] + "..."
	}

	return result, nil
}
