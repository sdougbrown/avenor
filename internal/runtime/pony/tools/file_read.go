package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
)

const maxFileReadBytes = 32 << 10 // 32 KB

func NewFileReadTool() Tool {
	return &FileReadTool{}
}

type FileReadTool struct{}

type fileReadInput struct {
	Path string `json:"path"`
}

func (t *FileReadTool) Name() string { return "file_read" }

func (t *FileReadTool) Description() string {
	return fmt.Sprintf("Read the contents of a file at the given path. Use this to read files from the working directory. Output is capped at %d KB; for larger files use offset/limit parameters.", maxFileReadBytes/1024)
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

	safePath, err := safeResolvePath(workingDir, AllowedReadDirsFromContext(ctx), input.Path)
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

	if len(data) > maxFileReadBytes {
		truncated := data[:maxFileReadBytes]
		// Don't split a multi-byte UTF-8 character at the cut point.
		// Continuation bytes (0x80-0xBF) have bit pattern 10xxxxxx.
		for len(truncated) > 0 && truncated[len(truncated)-1]&0xC0 == 0x80 {
			truncated = truncated[:len(truncated)-1]
		}
		return fmt.Sprintf("%s\n\n[... file_read truncated at %d KB; file is %d bytes total]",
			string(truncated), maxFileReadBytes/1024, len(data)), nil
	}

	return string(data), nil
}
