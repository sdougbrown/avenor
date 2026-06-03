package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

const defaultFileReadBytes = 32 << 10 // 32 KB

// maxRepeatedReads is the number of times the same file+offset can be read
// before the tool returns a reminder instead of re-reading content.
const maxRepeatedReads = 2

// FileReadConfig allows overriding file_read tool defaults per-profile.
type FileReadConfig struct {
	MaxOutputBytes int `json:"max_output_bytes,omitempty"`
}

func NewFileReadTool() Tool {
	return NewFileReadToolWithConfig(nil)
}

// NewFileReadToolWithConfig creates a FileReadTool with the given config overrides.
// Nil config uses the default 32 KB cap. Zero or negative MaxOutputBytes also
// falls back to the default.
func NewFileReadToolWithConfig(cfg *FileReadConfig) Tool {
	t := &FileReadTool{
		maxBytes:   defaultFileReadBytes,
		readCounts: make(map[string]int),
	}
	if cfg != nil && cfg.MaxOutputBytes > 0 {
		t.maxBytes = cfg.MaxOutputBytes
	}
	return t
}

type FileReadTool struct {
	maxBytes   int
	mu         sync.Mutex
	readCounts map[string]int // key: "path@offset"
}

type fileReadInput struct {
	Path   string `json:"path"`
	Offset int    `json:"offset,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

func (t *FileReadTool) Name() string { return "file_read" }

func (t *FileReadTool) Description() string {
	return fmt.Sprintf("Read the contents of a file at the given path. Use this to read files from the working directory. Output is capped at %d KB per call; use offset and optional limit to read larger files in chunks.", t.maxBytes/1024)
}

func (t *FileReadTool) Schema() json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{
		"type": "object",
		"properties": {
			"path": {
				"type": "string",
				"description": "Path to the file to read, relative to the working directory"
			},
			"offset": {
				"type": "integer",
				"description": "Byte offset to start reading from. Defaults to 0 (start of file)."
			},
			"limit": {
				"type": "integer",
				"description": "Maximum bytes to read. Defaults to %d (the cap). Cannot exceed %d."
			}
		},
		"required": ["path"]
	}`, t.maxBytes, t.maxBytes))
}

func (t *FileReadTool) Execute(ctx context.Context, workingDir string, args json.RawMessage) (string, error) {
	var input fileReadInput
	if err := json.Unmarshal(args, &input); err != nil {
		return "", fmt.Errorf("file_read: invalid args: %w", err)
	}
	if input.Path == "" {
		return "", fmt.Errorf("file_read: path is required")
	}
	if input.Offset < 0 {
		return "", fmt.Errorf("file_read: offset must be >= 0")
	}
	if input.Limit < 0 {
		return "", fmt.Errorf("file_read: limit must be >= 0")
	}

	// Duplicate-read guard: if the same path+offset has been read more than
	// maxRepeatedReads times, return a reminder instead of re-reading content.
	// This breaks recall-loss loops where compaction erases context and the
	// model keeps re-reading the same slices.
	readKey := fmt.Sprintf("%s\x00%d", input.Path, input.Offset)
	t.mu.Lock()
	count := t.readCounts[readKey]
	t.readCounts[readKey] = count + 1
	t.mu.Unlock()
	if count >= maxRepeatedReads {
		return fmt.Sprintf("[already read %d times: %s (offset=%d). Request a different range or use the content you already have.]",
			count+1, input.Path, input.Offset), nil
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

	// Apply offset
	if input.Offset > 0 {
		if input.Offset >= len(data) {
			return fmt.Sprintf("[file_read: offset %d is past end of file (%d bytes)]", input.Offset, len(data)), nil
		}
		data = data[input.Offset:]
	}

	// Determine effective limit
	effectiveLimit := t.maxBytes
	if input.Limit > 0 && input.Limit < effectiveLimit {
		effectiveLimit = input.Limit
	}

	if len(data) > effectiveLimit {
		truncated := data[:effectiveLimit]
		// Don't split a multi-byte UTF-8 character at the cut point.
		for len(truncated) > 0 && truncated[len(truncated)-1]&0xC0 == 0x80 {
			truncated = truncated[:len(truncated)-1]
		}
		prefix := ""
		if input.Offset > 0 {
			prefix = fmt.Sprintf("[file_read: offset=%d, ", input.Offset)
		}
		return fmt.Sprintf("%s%s[... file_read truncated at %d KB; file is %d bytes total]",
			prefix, string(truncated), effectiveLimit/1024, len(data)+input.Offset), nil
	}

	if input.Offset > 0 {
		return fmt.Sprintf("[file_read: offset=%d]\n%s", input.Offset, string(data)), nil
	}

	return string(data), nil
}
