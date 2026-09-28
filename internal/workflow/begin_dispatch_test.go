package workflow

import (
	"errors"
	"testing"
)

// TestBeginDispatchRecordsClaimAndAttemptAtomically proves BeginDispatch
// records the claim lease and starting attempt in one command, pins the
// selection, and stores dispatch diagnostics without the raw owner token.
func TestBeginDispatchRecordsClaimAndAttemptAtomically(t *testing.T) {
	m, s, wf := newAutoDispatchFixture(t, "begin-atomic", "ctl-a", -1)
	if err := m.RebuildCandidateIndex("test"); err != nil {
		t.Fatalf("RebuildCandidateIndex: %v", err)
	}
	cands, err := m.CandidatesForController("ctl-a", 1)
	if err != nil || len(cands) != 1 {
		t.Fatalf("candidates: n=%d err=%v", len(cands), err)
	}
	cand := cands[0]
	sel := &ExecutionSelection{Backend: "codex", Agent: "agent-a", Model: "m1"}
	res, err := m.BeginDispatch(BeginDispatchRequest{
		WorkflowID:       wf,
		NodeID:           cand.Identity.NodeID,
		ActivationID:     cand.Identity.ActivationID,
		ExpectedRevision: cand.Revision,
		ControllerID:     "ctl-a",
		LeaderLeaseID:    "lease_leader_1",
		Selection:        sel,
	})
	if err != nil {
		t.Fatalf("BeginDispatch: %v", err)
	}
	if res.AttemptID == "" || res.LeaseID == "" || res.OwnerToken == "" {
		t.Fatalf("begin result incomplete: %+v", res)
	}
	snap, ok, err := s.loadCurrent(wf)
	if err != nil || !ok {
		t.Fatalf("loadCurrent: ok=%v err=%v", ok, err)
	}
	act := activationByNode(&snap.Instance, cand.Identity.NodeID)
	if act == nil || act.Status != ActivationRunning {
		t.Fatalf("activation status = %v, want running after one atomic command", act)
	}
	if len(act.AttemptIDs) != 1 || act.AttemptIDs[0] != res.AttemptID {
		t.Fatalf("attempt ids = %v, want [%s]", act.AttemptIDs, res.AttemptID)
	}
	if act.ActiveLease == nil || act.ActiveLease.ID != res.LeaseID {
		t.Fatalf("active lease = %+v, want %s", act.ActiveLease, res.LeaseID)
	}
	if act.Selection == nil || act.Selection.Agent != "agent-a" {
		t.Fatalf("pinned selection = %+v, want agent-a", act.Selection)
	}
	attempt := findAttempt(&snap, act, res.AttemptID)
	if attempt == nil || attempt.Status != AttemptStarting {
		t.Fatalf("attempt = %+v, want starting", attempt)
	}
	if attempt.Diagnostics == nil ||
		attempt.Diagnostics.ControllerID != "ctl-a" ||
		attempt.Diagnostics.LeaderLeaseID != "lease_leader_1" ||
		attempt.Diagnostics.ConcurrencyKey != "deploys" {
		t.Fatalf("attempt diagnostics = %+v", attempt.Diagnostics)
	}
	// The raw owner token must never become durable: the stored digest is not
	// the token itself.
	if act.ActiveLease.TokenDigest == res.OwnerToken || act.ActiveLease.TokenDigest == "" {
		t.Fatalf("token digest = %q, want a digest of the raw token", act.ActiveLease.TokenDigest)
	}
}

// TestBeginDispatchStaleCandidate appends no event and returns a typed error
// for a stale revision or foreign controller.
func TestBeginDispatchStaleCandidate(t *testing.T) {
	m, _, wf := newAutoDispatchFixture(t, "begin-stale", "ctl-a", -1)
	if err := m.RebuildCandidateIndex("test"); err != nil {
		t.Fatalf("RebuildCandidateIndex: %v", err)
	}
	cands, err := m.CandidatesForController("ctl-a", 1)
	if err != nil || len(cands) != 1 {
		t.Fatalf("candidates: n=%d err=%v", len(cands), err)
	}
	cand := cands[0]
	base := BeginDispatchRequest{
		WorkflowID:    wf,
		NodeID:        cand.Identity.NodeID,
		ActivationID:  cand.Identity.ActivationID,
		ControllerID:  "ctl-a",
		LeaderLeaseID: "lease_leader_1",
	}
	staleRev := base
	staleRev.ExpectedRevision = cand.Revision + 42
	if _, err := m.BeginDispatch(staleRev); !errors.Is(err, ErrStaleCandidate) {
		t.Fatalf("stale revision error = %v, want ErrStaleCandidate", err)
	}
	wrongController := base
	wrongController.ExpectedRevision = cand.Revision
	wrongController.ControllerID = "ctl-b"
	if _, err := m.BeginDispatch(wrongController); !errors.Is(err, ErrStaleCandidate) {
		t.Fatalf("wrong controller error = %v, want ErrStaleCandidate", err)
	}
	// No events: the candidate is still ready at the original revision.
	after, err := m.CandidatesForController("ctl-a", 1)
	if err != nil || len(after) != 1 || after[0].Revision != cand.Revision {
		t.Fatalf("candidate changed by rejected dispatch: %+v err=%v", after, err)
	}
}

// TestFinalizeDispatchIdempotent proves repeated finalization with the same
// attempt is safe and records runtime identity exactly once.
func TestFinalizeDispatchIdempotent(t *testing.T) {
	m, s, wf := newAutoDispatchFixture(t, "begin-finalize", "ctl-a", -1)
	if err := m.RebuildCandidateIndex("test"); err != nil {
		t.Fatalf("RebuildCandidateIndex: %v", err)
	}
	cands, err := m.CandidatesForController("ctl-a", 1)
	if err != nil || len(cands) != 1 {
		t.Fatalf("candidates: n=%d err=%v", len(cands), err)
	}
	cand := cands[0]
	res, err := m.BeginDispatch(BeginDispatchRequest{
		WorkflowID:       wf,
		NodeID:           cand.Identity.NodeID,
		ActivationID:     cand.Identity.ActivationID,
		ExpectedRevision: cand.Revision,
		ControllerID:     "ctl-a",
		LeaderLeaseID:    "lease_leader_1",
	})
	if err != nil {
		t.Fatalf("BeginDispatch: %v", err)
	}
	req := FinalizeDispatchRequest{
		WorkflowID:   wf,
		NodeID:       cand.Identity.NodeID,
		ActivationID: cand.Identity.ActivationID,
		AttemptID:    res.AttemptID,
		LeaseID:      res.LeaseID,
		RuntimeID:    "rt_9",
		SessionID:    "ses_9",
		RunID:        "run_1",
	}
	for i := 0; i < 3; i++ {
		if err := m.FinalizeDispatch(req); err != nil {
			t.Fatalf("FinalizeDispatch #%d: %v", i, err)
		}
	}
	snap, ok, err := s.loadCurrent(wf)
	if err != nil || !ok {
		t.Fatalf("loadCurrent: ok=%v err=%v", ok, err)
	}
	act := activationByNode(&snap.Instance, cand.Identity.NodeID)
	attempt := findAttempt(&snap, act, res.AttemptID)
	if attempt == nil || attempt.Identity.RuntimeID != "rt_9" || attempt.Identity.SessionID != "ses_9" {
		t.Fatalf("attempt identity = %+v, want rt_9/ses_9", attempt)
	}
	// Failure finalization via the same request shape terminates through the
	// existing terminate path.
	failReq := req
	failReq.FailureStatus = AttemptFailed
	if err := m.FinalizeDispatch(failReq); err != nil {
		t.Fatalf("failure FinalizeDispatch: %v", err)
	}
	snap, _, _ = s.loadCurrent(wf)
	act = activationByNode(&snap.Instance, cand.Identity.NodeID)
	terminated := findAttempt(&snap, act, res.AttemptID)
	if terminated == nil || terminated.Status != AttemptFailed {
		t.Fatalf("terminated attempt = %+v, want failed", terminated)
	}
}

// TestBeginDispatchKeyHeldRejectsSecondLiveAttempt proves a second live
// attempt with the same concurrency key is rejected with no event, while a
// re-dispatch of the same activation after recovery is allowed.
func TestBeginDispatchKeyHeldRejectsSecondLiveAttempt(t *testing.T) {
	m, s, wf := newAutoDispatchFixture(t, "begin-key", "ctl-a", -1)
	if err := m.RebuildCandidateIndex("test"); err != nil {
		t.Fatalf("RebuildCandidateIndex: %v", err)
	}
	cands, err := m.CandidatesForController("ctl-a", 1)
	if err != nil || len(cands) != 1 {
		t.Fatalf("candidates: n=%d err=%v", len(cands), err)
	}
	cand := cands[0]
	if _, err := m.BeginDispatch(BeginDispatchRequest{
		WorkflowID:       wf,
		NodeID:           cand.Identity.NodeID,
		ActivationID:     cand.Identity.ActivationID,
		ExpectedRevision: cand.Revision,
		ControllerID:     "ctl-a",
		LeaderLeaseID:    "lease_leader_1",
	}); err != nil {
		t.Fatalf("first BeginDispatch: %v", err)
	}
	// A second workflow instance sharing the key cannot dispatch.
	if _, err := m.WorkflowCreate(autoDispatchTemplateJSON("begin-key-2", "ctl-a", -1)); err != nil {
		t.Fatalf("WorkflowCreate 2: %v", err)
	}
	if err := m.store.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	wf2 := mustInstantiateTemplate(t, m, "begin-key-2", "1")
	if err := m.RebuildCandidateIndex("test"); err != nil {
		t.Fatalf("RebuildCandidateIndex: %v", err)
	}
	cands2, err := m.CandidatesForController("ctl-a", 10)
	if err != nil {
		t.Fatalf("candidates 2: %v", err)
	}
	var cand2 ReadyCandidate
	for _, c := range cands2 {
		if c.Identity.WorkflowID == wf2 {
			cand2 = c
		}
	}
	if cand2.Identity.WorkflowID != wf2 {
		t.Fatal("second candidate not found")
	}
	if _, err := m.BeginDispatch(BeginDispatchRequest{
		WorkflowID:       wf2,
		NodeID:           cand2.Identity.NodeID,
		ActivationID:     cand2.Identity.ActivationID,
		ExpectedRevision: cand2.Revision,
		ControllerID:     "ctl-a",
		LeaderLeaseID:    "lease_leader_1",
	}); !errors.Is(err, ErrConcurrencyKeyHeld) {
		t.Fatalf("second BeginDispatch error = %v, want ErrConcurrencyKeyHeld", err)
	}
	// The key-holder's own dead attempt must not block its replacement: the
	// same activation is dispatchable again after its lease expires.
	snap, _, _ := s.loadCurrent(wf)
	act := activationByNode(&snap.Instance, cand.Identity.NodeID)
	if act == nil || act.ActiveLease == nil {
		t.Fatal("first activation lost its lease")
	}
	past := snap.Instance.Revision
	if _, err := m.store.ApplyCommand(wf, Command{
		ID:               NewCommandID(),
		Kind:             CommandTerminate,
		ExpectedRevision: past,
		IdempotencyKey:   "terminate-" + string(cand.Identity.ActivationID) + "-t",
		Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: cand.Identity.NodeID, ActivationID: cand.Identity.ActivationID},
		AttemptStatus:    AttemptFailed,
	}); err != nil {
		t.Fatalf("terminate: %v", err)
	}
	reCands, err := m.CandidatesForController("ctl-a", 10)
	if err != nil {
		t.Fatalf("re-read candidates: %v", err)
	}
	found := false
	for _, c := range reCands {
		if c.Identity.WorkflowID == wf {
			found = true
			cand = c
		}
	}
	if !found {
		t.Fatal("terminated activation did not re-arm")
	}
	if _, err := m.BeginDispatch(BeginDispatchRequest{
		WorkflowID:       wf,
		NodeID:           cand.Identity.NodeID,
		ActivationID:     cand.Identity.ActivationID,
		ExpectedRevision: cand.Revision,
		ControllerID:     "ctl-a",
		LeaderLeaseID:    "lease_leader_1",
	}); err != nil {
		t.Fatalf("replacement BeginDispatch blocked by own dead attempt: %v", err)
	}
	_ = s
}

// mustTerminateRetryably fails the activation's latest attempt through the
// terminate command so the retry policy re-arms it to ready, and returns the
// re-armed candidate.
func mustTerminateRetryably(t *testing.T, m *Manager, s *Store, wf WorkflowID, cand ReadyCandidate) ReadyCandidate {
	t.Helper()
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	if _, err := m.store.ApplyCommand(wf, Command{
		ID:               NewCommandID(),
		Kind:             CommandTerminate,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "terminate-" + string(cand.Identity.ActivationID) + "-retry",
		Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: cand.Identity.NodeID, ActivationID: cand.Identity.ActivationID},
		AttemptStatus:    AttemptFailed,
	}); err != nil {
		t.Fatalf("terminate: %v", err)
	}
	reCands, err := m.CandidatesForController("ctl-a", 10)
	if err != nil {
		t.Fatalf("re-read candidates: %v", err)
	}
	for _, c := range reCands {
		if c.Identity.WorkflowID == wf && c.Identity.ActivationID == cand.Identity.ActivationID {
			return c
		}
	}
	t.Fatal("terminated activation did not re-arm")
	return ReadyCandidate{}
}

// TestBeginDispatchAcceptsIdenticalSelectionAfterRearm proves an identical
// selection never trips the conflict guard: (a) a controller re-dispatch of
// the re-armed activation with the pinned selection succeeds and leaves the
// pin unchanged, and (b) a manual claim + start with the same selection
// succeeds and the new attempt runs under the pinned selection.
func TestBeginDispatchAcceptsIdenticalSelectionAfterRearm(t *testing.T) {
	sel := &ExecutionSelection{Backend: "codex", Agent: "agent-a", Model: "m1"}

	// (a) BeginDispatch re-run with exactly the pinned selection.
	m, s, wf := newAutoDispatchFixture(t, "begin-same", "ctl-a", -1)
	if err := m.RebuildCandidateIndex("test"); err != nil {
		t.Fatalf("RebuildCandidateIndex: %v", err)
	}
	cands, err := m.CandidatesForController("ctl-a", 1)
	if err != nil || len(cands) != 1 {
		t.Fatalf("candidates: n=%d err=%v", len(cands), err)
	}
	if _, err := m.BeginDispatch(BeginDispatchRequest{
		WorkflowID:       wf,
		NodeID:           cands[0].Identity.NodeID,
		ActivationID:     cands[0].Identity.ActivationID,
		ExpectedRevision: cands[0].Revision,
		ControllerID:     "ctl-a",
		LeaderLeaseID:    "lease_leader_1",
		Selection:        sel,
	}); err != nil {
		t.Fatalf("first BeginDispatch: %v", err)
	}
	reCand := mustTerminateRetryably(t, m, s, wf, cands[0])
	if _, err := m.BeginDispatch(BeginDispatchRequest{
		WorkflowID:       wf,
		NodeID:           reCand.Identity.NodeID,
		ActivationID:     reCand.Identity.ActivationID,
		ExpectedRevision: reCand.Revision,
		ControllerID:     "ctl-a",
		LeaderLeaseID:    "lease_leader_1",
		Selection:        sel,
	}); err != nil {
		t.Fatalf("BeginDispatch with identical selection: %v", err)
	}
	snap, _, _ := s.loadCurrent(wf)
	act := activationByNode(&snap.Instance, reCand.Identity.NodeID)
	if act == nil || act.Selection == nil || *act.Selection != *sel {
		t.Fatalf("pinned selection = %+v, want unchanged %+v", act.Selection, sel)
	}

	// (b) Manual claim + start with exactly the pinned selection.
	m2, s2, wf2 := newAutoDispatchFixture(t, "begin-same-manual", "ctl-a", -1)
	if err := m2.RebuildCandidateIndex("test"); err != nil {
		t.Fatalf("RebuildCandidateIndex 2: %v", err)
	}
	cands2, err := m2.CandidatesForController("ctl-a", 1)
	if err != nil || len(cands2) != 1 {
		t.Fatalf("candidates 2: n=%d err=%v", len(cands2), err)
	}
	if _, err := m2.BeginDispatch(BeginDispatchRequest{
		WorkflowID:       wf2,
		NodeID:           cands2[0].Identity.NodeID,
		ActivationID:     cands2[0].Identity.ActivationID,
		ExpectedRevision: cands2[0].Revision,
		ControllerID:     "ctl-a",
		LeaderLeaseID:    "lease_leader_1",
		Selection:        sel,
	}); err != nil {
		t.Fatalf("first BeginDispatch 2: %v", err)
	}
	reCand2 := mustTerminateRetryably(t, m2, s2, wf2, cands2[0])
	leaseID, token := mustClaimWorkflow(t, m2, wf2, reCand2.Identity.NodeID, reCand2.Identity.ActivationID, "manual-1")
	mustManualStart(t, m2, wf2, reCand2.Identity.NodeID, reCand2.Identity.ActivationID, leaseID, token,
		map[string]any{"backend": "codex", "agent": "agent-a", "model": "m1"})
	snap2, _, _ := s2.loadCurrent(wf2)
	act2 := activationByNode(&snap2.Instance, reCand2.Identity.NodeID)
	if act2 == nil || act2.Status != ActivationRunning {
		t.Fatalf("activation 2 status = %v, want running after manual start", act2)
	}
	if act2.Selection == nil || *act2.Selection != *sel {
		t.Fatalf("pinned selection 2 = %+v, want %+v", act2.Selection, sel)
	}
	if len(act2.AttemptIDs) != 2 {
		t.Fatalf("attempt ids 2 = %v, want one per dispatch", act2.AttemptIDs)
	}
}

// TestFinalizeDispatchRetriesRevisionMismatch proves the identify commit
// retries when a concurrent command lands in the read–commit window: a claim
// on another node of the same workflow commits inside the window, the first
// identify commit hits a revision mismatch, and the retry records the runtime
// identity exactly once.
func TestFinalizeDispatchRetriesRevisionMismatch(t *testing.T) {
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	m.RegisterExecutor(ActionRun, &fakeExecutor{})
	if _, err := m.WorkflowCreate(twoNodeDispatchTemplateJSON("finalize-retry", "ctl-a", "deploys")); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	wf := mustInstantiateTemplate(t, m, "finalize-retry", "1")
	if err := m.RebuildCandidateIndex("test"); err != nil {
		t.Fatalf("RebuildCandidateIndex: %v", err)
	}
	cands, err := m.CandidatesForController("ctl-a", 10)
	if err != nil || len(cands) != 1 {
		t.Fatalf("candidates: n=%d err=%v", len(cands), err)
	}
	cand := cands[0]
	res, err := m.BeginDispatch(BeginDispatchRequest{
		WorkflowID:       wf,
		NodeID:           cand.Identity.NodeID,
		ActivationID:     cand.Identity.ActivationID,
		ExpectedRevision: cand.Revision,
		ControllerID:     "ctl-a",
		LeaderLeaseID:    "lease_leader_1",
	})
	if err != nil {
		t.Fatalf("BeginDispatch: %v", err)
	}

	// While FinalizeDispatch sits in its read window, a claim on the manual
	// side node commits and moves the revision.
	m.testHooks.finalizeDispatchPreCommit = func() {
		m.testHooks.finalizeDispatchPreCommit = nil
		side := activationByNode(mustLoadInstance(t, s, wf), "side")
		if side == nil {
			t.Error("side activation not found")
			return
		}
		mustClaimWorkflow(t, m, wf, "side", side.ID, "manual-1")
	}

	err = m.FinalizeDispatch(FinalizeDispatchRequest{
		WorkflowID:   wf,
		NodeID:       cand.Identity.NodeID,
		ActivationID: cand.Identity.ActivationID,
		AttemptID:    res.AttemptID,
		LeaseID:      res.LeaseID,
		RuntimeID:    "rt_retry",
		SessionID:    "ses_retry",
		RunID:        "run_retry",
	})
	if err != nil {
		t.Fatalf("FinalizeDispatch: %v", err)
	}
	identified := 0
	for _, e := range readEvents(t, s, wf) {
		if e.Kind == EventAttemptIdentified && e.AttemptID == res.AttemptID {
			identified++
		}
	}
	if identified != 1 {
		t.Fatalf("attempt_identified events = %d, want exactly 1", identified)
	}
	snap, ok, err := s.loadCurrent(wf)
	if err != nil || !ok {
		t.Fatalf("loadCurrent: ok=%v err=%v", ok, err)
	}
	act := activationByNode(&snap.Instance, cand.Identity.NodeID)
	attempt := findAttempt(&snap, act, res.AttemptID)
	if attempt == nil || attempt.Identity.RuntimeID != "rt_retry" || attempt.Identity.SessionID != "ses_retry" {
		t.Fatalf("attempt identity = %+v, want rt_retry/ses_retry", attempt)
	}
}
