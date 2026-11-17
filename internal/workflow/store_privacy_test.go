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

	templateDir := filepath.Join(root, "templates", "privacy")
	// An instantiated instance regenerates its projection, so the projection
	// modes are the ones production writes.
	if err := s.StoreTemplate("privacy", "1", storeTemplateFixture(t, "privacy", "1")); err != nil {
		t.Fatalf("StoreTemplate: %v", err)
	}
	mustInstantiate(t, s, "wf2")

	for _, dir := range []struct {
		what string
		path string
	}{
		{"workflow root", root},
		{"templates", filepath.Join(root, "templates")},
		{"instances", filepath.Join(root, "instances")},
		{"instance", filepath.Join(root, "instances", "wf1")},
		{"evidence", filepath.Dir(stagedPath)},
		{"template", templateDir},
		{"projection nodes", filepath.Join(root, "instances", "wf2", "nodes")},
		{"projection node", filepath.Join(root, "instances", "wf2", "nodes", "start")},
	} {
		info, err := os.Stat(dir.path)
		if err != nil {
			t.Fatalf("stat %s: %v", dir.what, err)
		}
		if got := info.Mode().Perm(); got != 0o700 {
			t.Fatalf("%s mode = %o, want 700 (workflow state is owner-only)", dir.what, got)
		}
	}
	templateFile := filepath.Join(templateDir, "1.json")
	for _, file := range []struct {
		what string
		path string
	}{
		{"staged evidence", stagedPath},
		{"template", templateFile},
		{"workflow projection", filepath.Join(root, "instances", "wf2", "workflow.md")},
		{"node projection", filepath.Join(root, "instances", "wf2", "nodes", "start", "execution.md")},
	} {
		info, err := os.Stat(file.path)
		if err != nil {
			t.Fatalf("stat %s: %v", file.what, err)
		}
		if got := info.Mode().Perm(); got != 0o600 {
			t.Fatalf("%s mode = %o, want 600 (workflow state is owner-only)", file.what, got)
		}
	}
}

// TestExistingWorkflowRootKeepsItsMode proves the private modes apply only to
// what a new root creates, so an upgrade never re-permissions a deployment.
func TestExistingWorkflowRootKeepsItsMode(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "workflow-root")
	// Mkdir applies the umask, so pin the mode this test is about.
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}
	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatalf("chmod root: %v", err)
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

// TestRewrittenEventLogIsTightened proves an event log an earlier version wrote
// world-readable becomes owner-only when this version appends to it, so it never
// diverges from the snapshot beside it.
func TestRewrittenEventLogIsTightened(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	wf := WorkflowID("wf-legacy-log")
	mustInstantiate(t, s, wf)
	logPath := s.eventsPath(wf)
	if err := os.Chmod(logPath, 0o644); err != nil {
		t.Fatalf("chmod log: %v", err)
	}
	if _, err := s.ApplyCommand(wf, Command{
		ID:               NewCommandID(),
		Kind:             CommandCancel,
		ExpectedRevision: 1,
		IdempotencyKey:   "cancel-" + string(wf),
		Identity:         ExecutionIdentity{WorkflowID: wf},
		Actor:            "operator",
		Reason:           "privacy test",
	}); err != nil {
		t.Fatalf("ApplyCommand: %v", err)
	}
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("stat log: %v", err)
	}
	if got := info.Mode().Perm(); got != StateFilePerm {
		t.Fatalf("event log mode = %o, want %o", got, StateFilePerm)
	}
}

// TestRewrittenProjectionIsTightened proves a projection an earlier version wrote
// world-readable becomes owner-only the next time the store regenerates it.
func TestRewrittenProjectionIsTightened(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	wf := WorkflowID("wf-legacy-projection")
	mustInstantiate(t, s, wf)
	projPath := filepath.Join(s.instanceDir(wf), "workflow.md")
	if err := os.Chmod(projPath, 0o644); err != nil {
		t.Fatalf("chmod projection: %v", err)
	}
	if _, err := s.ApplyCommand(wf, Command{
		ID:               NewCommandID(),
		Kind:             CommandCancel,
		ExpectedRevision: 1,
		IdempotencyKey:   "cancel-" + string(wf),
		Identity:         ExecutionIdentity{WorkflowID: wf},
		Actor:            "operator",
		Reason:           "privacy test",
	}); err != nil {
		t.Fatalf("ApplyCommand: %v", err)
	}
	info, err := os.Stat(projPath)
	if err != nil {
		t.Fatalf("stat projection: %v", err)
	}
	if got := info.Mode().Perm(); got != StateFilePerm {
		t.Fatalf("projection mode = %o, want %o", got, StateFilePerm)
	}
}
