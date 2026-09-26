package workflow

import (
	"encoding/json"
	"errors"
	"testing"
)

// manualKeyedTemplateJSON builds a single run-node template whose entry node
// is dispatched manually under concurrencyKey.
func manualKeyedTemplateJSON(templateID, concurrencyKey string) []byte {
	template := map[string]any{
		"schema_version":   1,
		"template_id":      templateID,
		"template_version": "1",
		"entry_nodes":      []string{"start"},
		"nodes": []any{map[string]any{
			"id":     "start",
			"action": map[string]any{"type": "run", "prompt": "do the thing"},
			"dispatch": map[string]any{
				"mode":            "manual",
				"concurrency_key": concurrencyKey,
			},
		}},
		"terminal_outcomes": []string{"done"},
		"default_lease_policy": map[string]any{
			"ttl_seconds": 900,
		},
	}
	data, err := json.Marshal(template)
	if err != nil {
		panic(err)
	}
	return data
}

// twoNodeDispatchTemplateJSON builds a template with an auto-dispatched keyed
// entry node "start" and a plain manual entry node "side".
func twoNodeDispatchTemplateJSON(templateID, controllerID, concurrencyKey string) []byte {
	template := map[string]any{
		"schema_version":   1,
		"template_id":      templateID,
		"template_version": "1",
		"entry_nodes":      []string{"start", "side"},
		"nodes": []any{
			map[string]any{
				"id":       "start",
				"action":   map[string]any{"type": "run", "prompt": "do the thing"},
				"dispatch": map[string]any{"mode": "auto", "controller_id": controllerID, "concurrency_key": concurrencyKey},
			},
			map[string]any{
				"id":     "side",
				"action": map[string]any{"type": "run", "prompt": "side work"},
				"dispatch": map[string]any{
					"mode": "manual",
				},
			},
		},
		"terminal_outcomes": []string{"done"},
		"default_lease_policy": map[string]any{
			"ttl_seconds": 900,
		},
	}
	data, err := json.Marshal(template)
	if err != nil {
		panic(err)
	}
	return data
}

// mustClaimWorkflow claims an activation through the command boundary and
// returns the raw (lease_id, owner_token) pair.
func mustClaimWorkflow(t *testing.T, m *Manager, wf WorkflowID, nodeID NodeID, activationID ActivationID, actor string) (LeaseID, string) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"op":            "claim",
		"node_id":       string(nodeID),
		"activation_id": string(activationID),
		"actor":         actor,
	})
	if err != nil {
		t.Fatalf("marshal claim: %v", err)
	}
	out, err := m.WorkflowCommand(string(wf), payload)
	if err != nil {
		t.Fatalf("claim %s/%s: %v", nodeID, activationID, err)
	}
	claim, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("claim result = %#v, want map", out)
	}
	leaseID, _ := claim["lease_id"].(string)
	token, _ := claim["owner_token"].(string)
	if leaseID == "" || token == "" {
		t.Fatalf("claim result incomplete: %#v", claim)
	}
	return LeaseID(leaseID), token
}

// mustManualStart starts a claimed activation through the command boundary
// with an optional selection payload entry.
func mustManualStart(t *testing.T, m *Manager, wf WorkflowID, nodeID NodeID, activationID ActivationID, leaseID LeaseID, token string, selection map[string]any) {
	t.Helper()
	payload := map[string]any{
		"op":            "start",
		"node_id":       string(nodeID),
		"activation_id": string(activationID),
		"lease_id":      string(leaseID),
		"owner_token":   token,
	}
	if selection != nil {
		payload["selection"] = selection
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal start: %v", err)
	}
	if _, err := m.WorkflowCommand(string(wf), data); err != nil {
		t.Fatalf("start %s/%s: %v", nodeID, activationID, err)
	}
}

// TestManualConcurrencyKeysSerializeAcrossBoundaries proves a key declared on
// a manual node serializes both directions: a manual keyed activation blocks
// a controller dispatch on the same key and a live controller attempt blocks
// the manual start. A keyed manual node never surfaces as a candidate.
func TestManualConcurrencyKeysSerializeAcrossBoundaries(t *testing.T) {
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	m.RegisterExecutor(ActionRun, &fakeExecutor{})
	if _, err := m.WorkflowCreate(manualKeyedTemplateJSON("manual-key-1", "deploys")); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	if _, err := m.WorkflowCreate(autoDispatchTemplateJSON("auto-key-1", "ctl-a", -1)); err != nil {
		t.Fatalf("WorkflowCreate 2: %v", err)
	}
	wfManual := mustInstantiateTemplate(t, m, "manual-key-1", "1")
	wfAuto := mustInstantiateTemplate(t, m, "auto-key-1", "1")
	if err := m.RebuildCandidateIndex("test"); err != nil {
		t.Fatalf("RebuildCandidateIndex: %v", err)
	}

	// The keyed manual node carries its policy but is never a candidate.
	cands, err := m.CandidatesForController("ctl-a", 10)
	if err != nil {
		t.Fatalf("CandidatesForController: %v", err)
	}
	if len(cands) != 1 || cands[0].Identity.WorkflowID != wfAuto {
		t.Fatalf("candidates = %+v, want only the auto workflow %s", cands, wfAuto)
	}
	snap, ok, err := s.loadCurrent(wfManual)
	if err != nil || !ok {
		t.Fatalf("loadCurrent: ok=%v err=%v", ok, err)
	}
	manualAct := activationByNode(&snap.Instance, "start")
	if manualAct == nil || manualAct.Dispatch == nil || manualAct.Dispatch.IsAuto() ||
		manualAct.Dispatch.ConcurrencyKey != "deploys" {
		t.Fatalf("manual activation dispatch = %+v, want manual/deploys", manualAct.Dispatch)
	}

	// A live controller attempt holds the key...
	autoAct := activationByNode(mustLoadInstance(t, s, wfAuto), "start")
	begin, err := m.BeginDispatch(BeginDispatchRequest{
		WorkflowID:       wfAuto,
		NodeID:           "start",
		ActivationID:     autoAct.ID,
		ExpectedRevision: mustLoadInstance(t, s, wfAuto).Revision,
		ControllerID:     "ctl-a",
		LeaderLeaseID:    "lease_leader_1",
	})
	if err != nil {
		t.Fatalf("auto BeginDispatch: %v", err)
	}

	// ...so the manual start of the keyed node is rejected at the start
	// boundary.
	manualAct = activationByNode(mustLoadInstance(t, s, wfManual), "start")
	leaseID, token := mustClaimWorkflow(t, m, wfManual, "start", manualAct.ID, "manual-1")
	payload, err := json.Marshal(map[string]any{
		"op":            "start",
		"node_id":       "start",
		"activation_id": string(manualAct.ID),
		"lease_id":      string(leaseID),
		"owner_token":   token,
	})
	if err != nil {
		t.Fatalf("marshal start: %v", err)
	}
	if _, err := m.WorkflowCommand(string(wfManual), payload); !errors.Is(err, ErrConcurrencyKeyHeld) {
		t.Fatalf("manual start error = %v, want ErrConcurrencyKeyHeld", err)
	}

	// After the controller attempt terminates, the key releases and the same
	// manual start (the claim's lease is still live) succeeds.
	if _, err := m.store.ApplyCommand(wfAuto, Command{
		ID:               NewCommandID(),
		Kind:             CommandTerminate,
		ExpectedRevision: mustLoadInstance(t, s, wfAuto).Revision,
		IdempotencyKey:   "terminate-" + string(begin.AttemptID),
		Identity:         ExecutionIdentity{WorkflowID: wfAuto, NodeID: "start", ActivationID: autoAct.ID, AttemptID: begin.AttemptID},
		AttemptStatus:    AttemptFailed,
	}); err != nil {
		t.Fatalf("terminate auto attempt: %v", err)
	}
	mustManualStart(t, m, wfManual, "start", manualAct.ID, leaseID, token, nil)

	// The reverse direction: the now-live manual attempt holds the key and a
	// controller dispatch of the re-armed auto candidate bounces.
	if err := m.RebuildCandidateIndex("test"); err != nil {
		t.Fatalf("RebuildCandidateIndex: %v", err)
	}
	cands, err = m.CandidatesForController("ctl-a", 10)
	if err != nil {
		t.Fatalf("CandidatesForController after re-arm: %v", err)
	}
	var reCand *ReadyCandidate
	for i := range cands {
		if cands[i].Identity.WorkflowID == wfAuto {
			reCand = &cands[i]
		}
	}
	if reCand == nil {
		t.Fatalf("re-armed auto candidate missing: %+v", cands)
	}
	if _, err := m.BeginDispatch(BeginDispatchRequest{
		WorkflowID:       wfAuto,
		NodeID:           reCand.Identity.NodeID,
		ActivationID:     reCand.Identity.ActivationID,
		ExpectedRevision: reCand.Revision,
		ControllerID:     "ctl-a",
		LeaderLeaseID:    "lease_leader_1",
	}); !errors.Is(err, ErrConcurrencyKeyHeld) {
		t.Fatalf("controller dispatch error = %v, want ErrConcurrencyKeyHeld", err)
	}
}

// mustLoadInstance loads the current snapshot's instance or fails the test.
func mustLoadInstance(t *testing.T, s *Store, wf WorkflowID) *WorkflowInstance {
	t.Helper()
	snap, ok, err := s.loadCurrent(wf)
	if err != nil || !ok {
		t.Fatalf("loadCurrent %s: ok=%v err=%v", wf, ok, err)
	}
	return &snap.Instance
}

// TestBeginDispatchRevisionRaceMapsToStaleCandidate proves that a command
// committing on the same workflow between the candidate read and the attempt
// commit maps the revision mismatch onto ErrStaleCandidate.
func TestBeginDispatchRevisionRaceMapsToStaleCandidate(t *testing.T) {
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	m.RegisterExecutor(ActionRun, &fakeExecutor{})
	if _, err := m.WorkflowCreate(twoNodeDispatchTemplateJSON("race-two-node", "ctl-a", "deploys")); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	wf := mustInstantiateTemplate(t, m, "race-two-node", "1")
	if err := m.RebuildCandidateIndex("test"); err != nil {
		t.Fatalf("RebuildCandidateIndex: %v", err)
	}
	cands, err := m.CandidatesForController("ctl-a", 10)
	if err != nil || len(cands) != 1 {
		t.Fatalf("candidates: n=%d err=%v", len(cands), err)
	}
	cand := cands[0]

	// While BeginDispatch sits in its read window, a claim on the manual side
	// node commits and moves the revision.
	orig := beginDispatchPreCommit
	beginDispatchPreCommit = func() {
		beginDispatchPreCommit = orig
		side := activationByNode(mustLoadInstance(t, s, wf), "side")
		if side == nil {
			t.Error("side activation not found")
			return
		}
		mustClaimWorkflow(t, m, wf, "side", side.ID, "manual-1")
	}
	defer func() { beginDispatchPreCommit = orig }()

	_, err = m.BeginDispatch(BeginDispatchRequest{
		WorkflowID:       wf,
		NodeID:           cand.Identity.NodeID,
		ActivationID:     cand.Identity.ActivationID,
		ExpectedRevision: cand.Revision,
		ControllerID:     "ctl-a",
		LeaderLeaseID:    "lease_leader_1",
	})
	if !errors.Is(err, ErrStaleCandidate) {
		t.Fatalf("BeginDispatch error = %v, want ErrStaleCandidate", err)
	}
	// No attempt was recorded for the target node.
	start := activationByNode(mustLoadInstance(t, s, wf), "start")
	if len(start.AttemptIDs) != 0 {
		t.Fatalf("attempt ids = %v, want none", start.AttemptIDs)
	}
}

// TestManualStartSelectionRespectsPinnedSelection proves a manual workflow
// start with a selection conflicting with the one pinned by BeginDispatch is
// rejected, and a start without a selection inherits the pinned one.
func TestManualStartSelectionRespectsPinnedSelection(t *testing.T) {
	m, s, wf := newAutoDispatchFixture(t, "begin-sel-manual", "ctl-a", -1)
	if err := m.RebuildCandidateIndex("test"); err != nil {
		t.Fatalf("RebuildCandidateIndex: %v", err)
	}
	cands, err := m.CandidatesForController("ctl-a", 1)
	if err != nil || len(cands) != 1 {
		t.Fatalf("candidates: n=%d err=%v", len(cands), err)
	}
	cand := cands[0]
	pinned := &ExecutionSelection{Backend: "codex", Agent: "agent-a", Model: "m1"}
	begin, err := m.BeginDispatch(BeginDispatchRequest{
		WorkflowID:       wf,
		NodeID:           cand.Identity.NodeID,
		ActivationID:     cand.Identity.ActivationID,
		ExpectedRevision: cand.Revision,
		ControllerID:     "ctl-a",
		LeaderLeaseID:    "lease_leader_1",
		Selection:        pinned,
	})
	if err != nil {
		t.Fatalf("BeginDispatch: %v", err)
	}
	// Fail the attempt so the retry policy re-arms the activation with the
	// selection still pinned.
	if err := m.FinalizeDispatch(FinalizeDispatchRequest{
		WorkflowID:    wf,
		NodeID:        cand.Identity.NodeID,
		ActivationID:  cand.Identity.ActivationID,
		AttemptID:     begin.AttemptID,
		LeaseID:       begin.LeaseID,
		FailureStatus: AttemptFailed,
	}); err != nil {
		t.Fatalf("FinalizeDispatch: %v", err)
	}
	if err := m.RebuildCandidateIndex("test"); err != nil {
		t.Fatalf("RebuildCandidateIndex after re-arm: %v", err)
	}
	reCands, err := m.CandidatesForController("ctl-a", 10)
	if err != nil || len(reCands) != 1 {
		t.Fatalf("re-armed candidates: n=%d err=%v", len(reCands), err)
	}
	leaseID, token := mustClaimWorkflow(t, m, wf, reCands[0].Identity.NodeID, reCands[0].Identity.ActivationID, "manual-1")

	// A conflicting selection is rejected without consuming the lease.
	conflictPayload, err := json.Marshal(map[string]any{
		"op":            "start",
		"node_id":       string(reCands[0].Identity.NodeID),
		"activation_id": string(reCands[0].Identity.ActivationID),
		"lease_id":      string(leaseID),
		"owner_token":   token,
		"selection":     map[string]any{"backend": "codex", "agent": "agent-b", "model": "m2"},
	})
	if err != nil {
		t.Fatalf("marshal start: %v", err)
	}
	if _, err := m.WorkflowCommand(string(wf), conflictPayload); !errors.Is(err, ErrSelectionConflict) {
		t.Fatalf("conflicting start error = %v, want ErrSelectionConflict", err)
	}

	// A start without a selection inherits the pinned one.
	mustManualStart(t, m, wf, reCands[0].Identity.NodeID, reCands[0].Identity.ActivationID, leaseID, token, nil)
	inst := mustLoadInstance(t, s, wf)
	act := activationByNode(inst, reCands[0].Identity.NodeID)
	if act == nil || act.Selection == nil || act.Selection.Agent != "agent-a" {
		t.Fatalf("activation selection = %+v, want pinned agent-a", act.Selection)
	}
	if act.ActiveLease == nil || act.ActiveLease.ID != leaseID {
		t.Fatalf("active lease = %+v, want %s", act.ActiveLease, leaseID)
	}
}
