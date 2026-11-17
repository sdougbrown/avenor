package workflow

import (
	"os"
	"path/filepath"
	"testing"
)

// TestNewWorkflowStateIsPrivateByDefault proves the store creates its state
// owner-only, because it holds prompts, outputs, and staged evidence.
func TestNewWorkflowStateIsPrivateByDefault(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "workflow-root")
	s := New(root)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	staged, err := s.StageEvidence("wf1", writeTestFile(t, "evidence\n"), "artifact.json", false, "")
	if err != nil {
		t.Fatalf("StageEvidence: %v", err)
	}
	stagedPath := filepath.Join(root, "instances", "wf1", staged.StoredPath)

	for _, dir := range []struct {
		what string
		path string
	}{
		{"workflow root", root},
		{"templates", filepath.Join(root, "templates")},
		{"instances", filepath.Join(root, "instances")},
		{"instance", filepath.Join(root, "instances", "wf1")},
		{"evidence", filepath.Dir(stagedPath)},
	} {
		info, err := os.Stat(dir.path)
		if err != nil {
			t.Fatalf("stat %s: %v", dir.what, err)
		}
		if got := info.Mode().Perm(); got != 0o700 {
			t.Fatalf("%s mode = %o, want 700 (workflow state is owner-only)", dir.what, got)
		}
	}
	info, err := os.Stat(stagedPath)
	if err != nil {
		t.Fatalf("stat staged evidence: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("staged evidence mode = %o, want 600 (staged content is owner-only)", got)
	}
}

// TestExistingWorkflowRootKeepsItsMode proves the private modes apply only to
// what a new root creates, so an upgrade never re-permissions a deployment.
func TestExistingWorkflowRootKeepsItsMode(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "workflow-root")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}
	if err := New(root).CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	info, err := os.Stat(root)
	if err != nil {
		t.Fatalf("stat root: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o755 {
		t.Fatalf("existing root mode = %o, want it left at 755", got)
	}
}
