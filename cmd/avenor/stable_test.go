package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/admission"
)

func TestRunStableJoinsInheritedTreeBudget(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	root, err := admission.CreateRootInRuntimeState(2)
	if err != nil {
		t.Fatalf("create root tree budget: %v", err)
	}
	defer root.Close()
	t.Setenv(admission.EnvTreeBudget, root.Path())

	// An overlong Unix-socket path fails in Listen without probing an existing
	// path. A directory at the socket path can make Unix Dial block on Linux.
	socketPath := filepath.Join(tmpDir, strings.Repeat("s", 200))
	if code := runStable([]string{"--control-socket", socketPath}); code != 1 {
		t.Fatalf("runStable() = %d, want 1", code)
	}
	if _, err := os.Stat(root.Path()); err != nil {
		t.Fatalf("inherited root tree budget was removed: %v", err)
	}
}

func TestRunStableWritesTombstoneOnStartFailure(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
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

	budgetFiles, err := filepath.Glob(filepath.Join(tmpDir, ".avenor", "sockets", "*.tree-budget"))
	if err != nil {
		t.Fatalf("glob tree budget files: %v", err)
	}
	if len(budgetFiles) != 0 {
		t.Fatalf("tree budget files remain after start failure: %v", budgetFiles)
	}
}
