package tools

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileReadConfigMaxOutputBytesCapsOutput(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "large.txt")
	if err := os.WriteFile(path, []byte(strings.Repeat("a", 64)), 0o600); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	tool := NewFileReadToolWithConfig(&FileReadConfig{MaxOutputBytes: 16})
	result, err := tool.Execute(context.Background(), dir, json.RawMessage(`{"path":"large.txt"}`))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(result, "truncated at 0 KB") {
		t.Fatalf("result = %q, want truncation marker", result)
	}
	if data := strings.Split(result, "[...")[0]; len(data) != 16 {
		t.Fatalf("result data length = %d, want configured cap 16; result=%q", len(data), result)
	}
}
