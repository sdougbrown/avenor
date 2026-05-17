package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunStableWritesTombstoneOnStartFailure(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "socket-path-is-dir")
	if err := os.Mkdir(socketPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(socketPath, "keep-dir-nonempty"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	tombstonePath := socketPath + ".dead"
	if err := os.WriteFile(tombstonePath, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}

	code := runStable([]string{"--control-socket", socketPath})
	if code != 1 {
		t.Fatalf("runStable() = %d, want 1", code)
	}

	data, err := os.ReadFile(tombstonePath)
	if err != nil {
		t.Fatalf("tombstone not written: %v", err)
	}
	content := string(data)
	if strings.Contains(content, "stale") {
		t.Fatalf("tombstone = %q, was not overwritten", content)
	}
	if !strings.Contains(content, "reason=start_failed") {
		t.Fatalf("tombstone = %q, want reason=start_failed", content)
	}
}
