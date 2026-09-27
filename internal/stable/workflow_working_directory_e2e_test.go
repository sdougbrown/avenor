package stable

// workflow_working_directory_e2e_test.go runs the template-declared
// per-attempt working directory end to end over a real supervisor: real
// executors, a real enabled controller, real admission, and the real
// workflow.Manager. The only fake is the scripted provider behind the
// production provider-factory seam. AUTO nodes are never hand-driven.

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sdougbrown/avenor/internal/workflow"
)

// workingDirTemplate builds a one-auto-run-node template whose
// working_directory resolves from the worktree_path instance param (or with
// no working_directory at all, for the backward-compatibility cases).
func workingDirTemplate(t *testing.T, templateID string, withWorkingDirectory bool) []byte {
	t.Helper()
	node := map[string]any{
		"id":           "start",
		"action":       map[string]any{"type": "run", "prompt": "do the thing"},
		"dispatch":     map[string]any{"mode": "auto", "controller_id": "c1"},
		"outputs":      []any{map[string]any{"id": "summary", "name": "Summary", "type": "file", "required": true}},
		"completion":   map[string]any{"kind": "files", "artifacts": []any{map[string]any{"path": "result.md", "non_empty": true}}},
		"retry_policy": map[string]any{"max_attempts": 2, "exhaustion": "block"},
	}
	tmpl := map[string]any{
		"schema_version":    1,
		"template_id":       templateID,
		"template_version":  "1",
		"entry_nodes":       []string{"start"},
		"nodes":             []any{node},
		"terminal_outcomes": []string{"done"},
	}
	if withWorkingDirectory {
		tmpl["working_directory"] = map[string]any{"from_instance_param": "worktree_path"}
		tmpl["params"] = []any{map[string]any{"id": "worktree_path", "type": "string", "required": true}}
	}
	return mustJSON(t, tmpl)
}

// TestWorkflowWorkingDirectoryMissingDirFailsPreStart proves a declared
// working directory that does not exist at dispatch time fails the attempt
// pre-start: the provider is never invoked, nothing runs in the supervisor's
// working directory, and the node's retry policy applies before exhaustion
// blocks the node.
func TestWorkflowWorkingDirectoryMissingDirFailsPreStart(t *testing.T) {
	scratch := t.TempDir()
	t.Chdir(scratch)
	provider := &stableScriptedProvider{attempt: -1}
	f := newAutoHandoffFixture(t, "wd-missing-dir", provider)
	missing := filepath.Join(t.TempDir(), "never-created")
	wf := f.addWorkflow(t, "tmpl-wd-missing-dir", workingDirTemplate(t, "tmpl-wd-missing-dir", true),
		map[string]string{"worktree_path": missing})
	f.enableController(t, 2)

	f.waitForInstance(t, wf, "the node to exhaust its retries and block", func(inst *workflow.WorkflowInstance) bool {
		act := activationFor(inst, "start")
		return act != nil && act.Status == workflow.ActivationBlocked
	})

	inst := f.instance(t, wf)
	// The retry policy applied: one failed attempt per max_attempts, every
	// failure pre-start, and the provider (which would have run in the
	// supervisor cwd) was never invoked.
	attempts := attemptsForNode(&inst, "start")
	if len(attempts) != 2 {
		t.Fatalf("start recorded %d attempts, want exactly 2 (the declared max_attempts); observed %s",
			len(attempts), describeInstance(&inst, f.providerCalls.Load()))
	}
	for _, attempt := range attempts {
		if attempt.Status != workflow.AttemptFailed {
			t.Fatalf("start attempt %s status = %s, want failed (pre-start)", attempt.ID, attempt.Status)
		}
	}
	if calls := f.providerCalls.Load(); calls != 0 {
		t.Fatalf("provider invoked %d times, want 0 (the attempt never ran)", calls)
	}
	entries, err := os.ReadDir(scratch)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("supervisor working directory was used despite the failed dispatch: %v", entries)
	}
}

// TestWorkflowWorkingDirectoryCreatedAfterInstantiation proves existence is
// a dispatch-time check: a directory created between instantiation and
// dispatch is honored, the worker runs (and writes) in it, and the attempt
// records that working directory.
func TestWorkflowWorkingDirectoryCreatedAfterInstantiation(t *testing.T) {
	scratch := t.TempDir()
	t.Chdir(scratch)
	provider := &stableScriptedProvider{attempt: -1}
	f := newAutoHandoffFixture(t, "wd-created-late", provider)
	later := filepath.Join(t.TempDir(), "created-after-instantiation")
	// The scripted provider cannot write files itself; the handoff seam
	// writes the artifact into the spawn directory before dispatch.
	if err := os.MkdirAll(later, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(later, "result.md"), []byte("the artifact\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	appendSessionScript(t, provider, "ses_wd_late", nil, "end_turn")
	wf := f.addWorkflow(t, "tmpl-wd-late", workingDirTemplate(t, "tmpl-wd-late", true),
		map[string]string{"worktree_path": later})
	f.enableController(t, 2)

	f.waitForInstance(t, wf, "the node to satisfy through the supervisor handoff", func(inst *workflow.WorkflowInstance) bool {
		act := activationFor(inst, "start")
		return act != nil && act.Status == workflow.ActivationSatisfied
	})

	inst := f.instance(t, wf)
	attempts := attemptsForNode(&inst, "start")
	if len(attempts) != 1 || attempts[0].Status != workflow.AttemptSucceeded {
		t.Fatalf("start attempts = %+v, want exactly one succeeded", attempts)
	}
	if attempts[0].WorkingDirectory != later {
		t.Fatalf("attempt working_directory = %q, want %q", attempts[0].WorkingDirectory, later)
	}
	if calls := f.providerCalls.Load(); calls != 1 {
		t.Fatalf("provider invoked %d times, want exactly 1", calls)
	}
}

// TestWorkflowWorkingDirectoryDefaultsToSupervisorCwd pins backward
// compatibility: a template without a working_directory declaration runs its
// attempts in the supervisor's working directory, exactly as before, and the
// attempt records that directory.
func TestWorkflowWorkingDirectoryDefaultsToSupervisorCwd(t *testing.T) {
	workDir := t.TempDir()
	t.Chdir(workDir)
	provider := &stableScriptedProvider{attempt: -1}
	f := newAutoHandoffFixture(t, "wd-default-cwd", provider)
	// The worker writes its artifact through the handoff seam into its
	// environment (the supervisor cwd) and exits cleanly.
	if err := os.WriteFile(filepath.Join(workDir, "result.md"), []byte("cwd artifact\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	appendSessionScript(t, provider, "ses_wd_cwd", nil, "end_turn")
	wf := f.addWorkflow(t, "tmpl-wd-cwd", workingDirTemplate(t, "tmpl-wd-cwd", false))
	f.enableController(t, 2)

	f.waitForInstance(t, wf, "the node to satisfy through the supervisor handoff", func(inst *workflow.WorkflowInstance) bool {
		act := activationFor(inst, "start")
		return act != nil && act.Status == workflow.ActivationSatisfied
	})

	inst := f.instance(t, wf)
	attempts := attemptsForNode(&inst, "start")
	if len(attempts) != 1 || attempts[0].Status != workflow.AttemptSucceeded {
		t.Fatalf("start attempts = %+v, want exactly one succeeded", attempts)
	}
	if attempts[0].WorkingDirectory != workDir {
		t.Fatalf("attempt working_directory = %q, want the supervisor cwd %q", attempts[0].WorkingDirectory, workDir)
	}
}
