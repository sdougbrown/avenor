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

	t.Run("file_read can access configured read-allowed dir", func(t *testing.T) {
		workDir := t.TempDir()
		readDir := t.TempDir()
		testFile := filepath.Join(readDir, "skill.md")
		if err := os.WriteFile(testFile, []byte("hello from skill"), 0644); err != nil {
			t.Fatal(err)
		}

		r := NewRegistry([]Tool{NewFileReadTool()})
		r.SetAllowedReadDirs([]string{readDir})

		args := json.RawMessage(`{"path":"` + testFile + `"}`)
		result, err := r.Dispatch(context.Background(), workDir, "file_read", args)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != "hello from skill" {
			t.Fatalf("expected read from allowed dir, got %q", result)
		}
	})

	t.Run("file_write cannot write to read-allowed dir", func(t *testing.T) {
		workDir := t.TempDir()
		readDir := t.TempDir()

		r := NewRegistry([]Tool{NewFileWriteTool()})
		r.SetAllowedReadDirs([]string{readDir})

		args := json.RawMessage(`{"path":"` + filepath.Join(readDir, "blocked.txt") + `","content":"x"}`)
		_, err := r.Dispatch(context.Background(), workDir, "file_write", args)
		if err == nil {
			t.Fatal("expected write to read-allowed dir to be rejected")
		}
	})

	t.Run("file_edit cannot edit file in read-allowed dir", func(t *testing.T) {
		workDir := t.TempDir()
		readDir := t.TempDir()
		target := filepath.Join(readDir, "skill.md")
		if err := os.WriteFile(target, []byte("old"), 0644); err != nil {
			t.Fatal(err)
		}

		r := NewRegistry([]Tool{NewFileEditTool()})
		r.SetAllowedReadDirs([]string{readDir})

		args := json.RawMessage(`{"path":"` + target + `","old_string":"old","new_string":"new"}`)
		_, err := r.Dispatch(context.Background(), workDir, "file_edit", args)
		if err == nil {
			t.Fatal("expected edit in read-allowed dir to be rejected")
		}
	})

	t.Run("set allowed dirs enables both read and write", func(t *testing.T) {
		workDir := t.TempDir()
		sharedDir := t.TempDir()
		target := filepath.Join(sharedDir, "shared.txt")
		if err := os.WriteFile(target, []byte("before"), 0644); err != nil {
			t.Fatal(err)
		}

		r := NewRegistry([]Tool{NewFileReadTool(), NewFileWriteTool()})
		r.SetAllowedDirs([]string{sharedDir})

		readArgs := json.RawMessage(`{"path":"` + target + `"}`)
		readResult, err := r.Dispatch(context.Background(), workDir, "file_read", readArgs)
		if err != nil {
			t.Fatalf("unexpected read error: %v", err)
		}
		if readResult != "before" {
			t.Fatalf("expected read value before, got %q", readResult)
		}

		writeArgs := json.RawMessage(`{"path":"` + target + `","content":"after"}`)
		if _, err := r.Dispatch(context.Background(), workDir, "file_write", writeArgs); err != nil {
			t.Fatalf("unexpected write error: %v", err)
		}

		data, err := os.ReadFile(target)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != "after" {
			t.Fatalf("expected updated file content, got %q", string(data))
		}
	})

	t.Run("empty registry allowlists shadow parent context", func(t *testing.T) {
		workDir := t.TempDir()
		parentAllowed := t.TempDir()
		target := filepath.Join(parentAllowed, "secret.txt")
		if err := os.WriteFile(target, []byte("nope"), 0644); err != nil {
			t.Fatal(err)
		}

		r := NewRegistry([]Tool{NewFileReadTool()})
		ctx := context.WithValue(context.Background(), AllowedReadDirsKey, []string{parentAllowed})

		args := json.RawMessage(`{"path":"` + target + `"}`)
		_, err := r.Dispatch(ctx, workDir, "file_read", args)
		if err == nil {
			t.Fatal("expected parent context allowlist to be shadowed")
		}
	})

	t.Run("file_edit requires both read and write allowlists", func(t *testing.T) {
		workDir := t.TempDir()
		sharedDir := t.TempDir()
		target := filepath.Join(sharedDir, "edit.txt")
		if err := os.WriteFile(target, []byte("old"), 0644); err != nil {
			t.Fatal(err)
		}

		r := NewRegistry([]Tool{NewFileEditTool()})
		r.SetAllowedWriteDirs([]string{sharedDir})

		args := json.RawMessage(`{"path":"` + target + `","old_string":"old","new_string":"new"}`)
		_, err := r.Dispatch(context.Background(), workDir, "file_edit", args)
		if err == nil {
			t.Fatal("expected edit to require read allowlist too")
		}
	})
}

func TestSafeResolvePath(t *testing.T) {
	t.Run("allows relative path inside working directory", func(t *testing.T) {
		workDir := t.TempDir()
		target := filepath.Join(workDir, "nested", "file.txt")
		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, []byte("ok"), 0644); err != nil {
			t.Fatal(err)
		}

		resolved, err := safeResolvePath(workDir, nil, filepath.Join("nested", "file.txt"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		wantResolved, err := filepath.EvalSymlinks(target)
		if err != nil {
			t.Fatal(err)
		}
		if resolved != wantResolved {
			t.Fatalf("expected %q, got %q", wantResolved, resolved)
		}
	})

	t.Run("rejects symlink escape for new file writes", func(t *testing.T) {
		workDir := t.TempDir()
		escapeRoot := t.TempDir()
		linkPath := filepath.Join(workDir, "link")
		if err := os.Symlink(escapeRoot, linkPath); err != nil {
			t.Fatal(err)
		}

		_, err := safeResolvePath(workDir, nil, filepath.Join("link", "newfile.txt"))
		if err == nil {
			t.Fatal("expected symlink escape to be rejected")
		}
	})

	t.Run("allows symlink target inside additional dir for reads", func(t *testing.T) {
		workDir := t.TempDir()
		allowedDir := t.TempDir()
		linkPath := filepath.Join(workDir, "skill-link")
		if err := os.Symlink(allowedDir, linkPath); err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(allowedDir, "skill.md")
		if err := os.WriteFile(target, []byte("skill"), 0644); err != nil {
			t.Fatal(err)
		}

		resolved, err := safeResolvePath(workDir, []string{allowedDir}, filepath.Join("skill-link", "skill.md"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		wantResolved, err := filepath.EvalSymlinks(target)
		if err != nil {
			t.Fatal(err)
		}
		if resolved != wantResolved {
			t.Fatalf("expected %q, got %q", wantResolved, resolved)
		}
	})
}
