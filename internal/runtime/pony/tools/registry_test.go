package tools

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestRegistry(t *testing.T) {
	t.Run("registers tools by name", func(t *testing.T) {
		tools := []Tool{NewFileReadTool()}
		r := NewRegistry(tools)

		all := r.AllTools()
		if len(all) != 1 {
			t.Fatalf("expected 1 tool, got %d", len(all))
		}
		if all[0].Name() != "file_read" {
			t.Errorf("expected tool name file_read, got %s", all[0].Name())
		}
	})

	t.Run("dispatch calls correct tool", func(t *testing.T) {
		tools := []Tool{NewFileReadTool()}
		r := NewRegistry(tools)

		// Create a temp file for testing
		tmpDir := t.TempDir()
		testFile := filepath.Join(tmpDir, "hello.txt")
		if err := os.WriteFile(testFile, []byte("hello world"), 0644); err != nil {
			t.Fatal(err)
		}

		args := json.RawMessage(`{"path":"hello.txt"}`)
		result, err := r.Dispatch(context.Background(), tmpDir, "file_read", args)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != "hello world" {
			t.Errorf("expected 'hello world', got %q", result)
		}
	})

	t.Run("dispatch returns error for unknown tool", func(t *testing.T) {
		r := NewRegistry(nil)
		_, err := r.Dispatch(context.Background(), "", "nonexistent", nil)
		if err == nil {
			t.Fatal("expected error for unknown tool")
		}
	})

	t.Run("file_read returns error for path outside working dir", func(t *testing.T) {
		tools := []Tool{NewFileReadTool()}
		r := NewRegistry(tools)

		args := json.RawMessage(`{"path":"/etc/passwd"}`)
		_, err := r.Dispatch(context.Background(), "/tmp/workdir", "file_read", args)
		if err == nil {
			t.Fatal("expected error for path outside working directory")
		}
	})

	t.Run("file_read returns error for nonexistent file", func(t *testing.T) {
		tmpDir := t.TempDir()
		tools := []Tool{NewFileReadTool()}
		r := NewRegistry(tools)

		args := json.RawMessage(`{"path":"nope.txt"}`)
		_, err := r.Dispatch(context.Background(), tmpDir, "file_read", args)
		if err == nil {
			t.Fatal("expected error for nonexistent file")
		}
	})
}
