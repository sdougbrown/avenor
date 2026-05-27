package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func NewListDirTool() Tool {
	return &ListDirTool{}
}

type ListDirTool struct{}

type listDirInput struct {
	Path string `json:"path"`
}

func (t *ListDirTool) Name() string { return "list_dir" }

func (t *ListDirTool) Description() string {
	return "List files and directories in a directory. Returns entries with their type (file/dir) and size."
}

func (t *ListDirTool) Schema() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {
			"path": {
				"type": "string",
				"description": "Directory path relative to the working directory"
			}
		},
		"required": ["path"]
	}`)
}

func (t *ListDirTool) Execute(ctx context.Context, workingDir string, args json.RawMessage) (string, error) {
	var input listDirInput
	if err := json.Unmarshal(args, &input); err != nil {
		return "", fmt.Errorf("list_dir: invalid args: %w", err)
	}

	dirPath := input.Path
	if dirPath == "" {
		dirPath = "."
	}

	safePath, err := safeResolvePath(workingDir, dirPath)
	if err != nil {
		return "", fmt.Errorf("list_dir: %w", err)
	}

	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("list_dir: %w", err)
	}

	entries, err := os.ReadDir(safePath)
	if err != nil {
		return "", fmt.Errorf("list_dir: %w", err)
	}

	var lines []string
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}

		rel, err := filepath.Rel(workingDir, filepath.Join(safePath, entry.Name()))
		if err != nil {
			rel = entry.Name()
		}

		if entry.IsDir() {
			lines = append(lines, fmt.Sprintf("dir  %s", rel))
		} else {
			lines = append(lines, fmt.Sprintf("file %s (%d bytes)", rel, info.Size()))
		}
	}

	return strings.Join(lines, "\n"), nil
}
