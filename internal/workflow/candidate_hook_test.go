package workflow

// Internal tests for the store commit hook that keeps the candidate index
// fresh between rebuilds, and for the recovered-empty path for an absent
// workflow root.

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestCommitHookKeepsCandidateIndexFresh proves that after one rebuild the
// index tracks every subsequent committed command without another rebuild:
// instantiate adds a candidate, claim removes it, and a failed attempt
// termination re-arms it with an advanced revision.
func TestCommitHookKeepsCandidateIndexFresh(t *testing.T) {
	m, s, wf := newAutoDispatchFixture(t, "hook-fresh", "ctl-a", 50)
	if err := m.RebuildCandidateIndex("sup-1"); err != nil {
		t.Fatalf("RebuildCandidateIndex: %v", err)
	}
	if cands, err := m.CandidatesForController("ctl-a", 10); err != nil || len(cands) != 1 {
		t.Fatalf("initial candidates = %v, err = %v, want exactly one", cands, err)
	}

	// Instantiate a second instance: it must appear in the index without a
	// rebuild.
	wf2 := mustInstantiateTemplate(t, m, "hook-fresh", "1")
	cands, err := m.CandidatesForController("ctl-a", 10)
	if err != nil {
		t.Fatalf("candidates after instantiate: %v", err)
	}
	if len(cands) != 2 {
		t.Fatalf("candidates after instantiate = %v, want two", cands)
	}

	snap, ok, err := s.loadCurrent(wf2)
	if err != nil || !ok {
		t.Fatalf("loadCurrent(%s): ok=%v err=%v", wf2, ok, err)
	}
	act := activationByNode(&snap.Instance, "start")

	// Claim removes the candidate without a rebuild.
	res := claimActivation(t, m, wf2, "start", string(act.ID), "alice")
	cands, err = m.CandidatesForController("ctl-a", 10)
	if err != nil {
		t.Fatalf("candidates after claim: %v", err)
	}
	if len(cands) != 1 || cands[0].Identity.WorkflowID == wf2 {
		t.Fatalf("candidates after claim = %v, want only %s", cands, wf)
	}

	// Start the claimed activation, then terminate the attempt with a
	// failure: the retry policy re-arms the activation and the candidate
	// reappears with the advanced revision.
	leaseID, _ := res["lease_id"].(string)
	token, _ := res["owner_token"].(string)
	started := startWithToken(t, m, wf2, "start", string(act.ID), leaseID, token)
	attemptID, ok := started["attempt_id"].(string)
	if !ok || attemptID == "" {
		t.Fatalf("start result missing attempt_id: %#v", started)
	}
	mustTerminate(t, m, wf2, "start", act.ID, attemptID, leaseID)
	cands, err = m.CandidatesForController("ctl-a", 10)
	if err != nil {
		t.Fatalf("candidates after terminate: %v", err)
	}
	if len(cands) != 2 {
		t.Fatalf("candidates after terminate = %v, want two (re-armed)", cands)
	}
	for _, c := range cands {
		if c.Identity.WorkflowID == wf2 && c.Revision <= snap.Instance.Revision {
			t.Fatalf("re-armed candidate revision = %d, want > %d", c.Revision, snap.Instance.Revision)
		}
	}
}

// TestCommitHookNoopBeforeRecovery proves the commit hook does not mark the
// index recovered: before a rebuild (or empty-mark) the query still reports
// ErrCandidatesNotRecovered even though commands have committed.
func TestCommitHookNoopBeforeRecovery(t *testing.T) {
	m, _, _ := newAutoDispatchFixture(t, "hook-noop", "ctl-a", 50)
	if _, err := m.CandidatesForController("ctl-a", 10); !errors.Is(err, ErrCandidatesNotRecovered) {
		t.Fatalf("candidates before recovery: err = %v, want ErrCandidatesNotRecovered", err)
	}
}

// TestMarkCandidateIndexEmptyAbsentRoot proves an absent workflow root is an
// empty catalog: the index is marked recovered-and-empty without creating the
// root, and later instances enter the index through the commit hook.
func TestMarkCandidateIndexEmptyAbsentRoot(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "wfroot")) // no CreateRoot: the root does not exist
	m := NewManager(s)

	if _, err := m.CandidatesForController("ctl-a", 10); !errors.Is(err, ErrCandidatesNotRecovered) {
		t.Fatalf("candidates before empty-mark: err = %v, want ErrCandidatesNotRecovered", err)
	}
	m.MarkCandidateIndexEmpty("sup-1")
	if cands, err := m.CandidatesForController("ctl-a", 10); err != nil || len(cands) != 0 {
		t.Fatalf("candidates after empty-mark = %v, err = %v, want empty and no error", cands, err)
	}
	if _, err := os.Stat(s.Root()); !os.IsNotExist(err) {
		t.Fatalf("root stat err = %v, want the root to stay absent", err)
	}

	// Once the root materializes, new instances land in the recovered index
	// via the commit hook, without another rebuild.
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m.RegisterExecutor(ActionRun, &fakeExecutor{})
	if _, err := m.WorkflowCreate(autoDispatchTemplateJSON("hook-empty", "ctl-a", 50)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	wf := mustInstantiateTemplate(t, m, "hook-empty", "1")
	cands, err := m.CandidatesForController("ctl-a", 10)
	if err != nil {
		t.Fatalf("candidates after instantiate: %v", err)
	}
	if len(cands) != 1 || cands[0].Identity.WorkflowID != wf {
		t.Fatalf("candidates after instantiate = %v, want exactly %s", cands, wf)
	}
}
