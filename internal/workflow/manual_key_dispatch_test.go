package workflow

import (
	"encoding/json"
	"errors"
	"strings"
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

// TestBeginDispatchRejectsBothAutoApproveAndDecider proves the command
// boundary rejects a caller-supplied selection carrying both unattended
// permission policies: auto-approve and a permission decider are mutually
// exclusive, and the conflict must surface before any attempt is recorded.
func TestBeginDispatchRejectsBothAutoApproveAndDecider(t *testing.T) {
	t.Parallel()
	m, _, wf := newAutoDispatchFixture(t, "begin-permdec-both", "ctl-a", -1)
	if err := m.RebuildCandidateIndex("test"); err != nil {
		t.Fatalf("RebuildCandidateIndex: %v", err)
	}
	cands, err := m.CandidatesForController("ctl-a", 1)
	if err != nil || len(cands) != 1 {
		t.Fatalf("candidates: n=%d err=%v", len(cands), err)
	}
	_, err = m.BeginDispatch(BeginDispatchRequest{
		WorkflowID:       wf,
		NodeID:           cands[0].Identity.NodeID,
		ActivationID:     cands[0].Identity.ActivationID,
		ExpectedRevision: cands[0].Revision,
		ControllerID:     "ctl-a",
		LeaderLeaseID:    "lease_leader_1",
		Selection: &ExecutionSelection{
			AutoApprove:       true,
			PermissionDecider: &PermissionDecider{Entry: "rm-guard"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("BeginDispatch error = %v, want auto_approve/permission_decider mutual exclusion", err)
	}
}

// TestManualStartRejectsBothAutoApproveAndDecider proves the start command
// boundary rejects a caller-supplied selection carrying both unattended
// permission policies, without consuming the claim's lease: a valid start of
// the same activation still succeeds afterward.
func TestManualStartRejectsBothAutoApproveAndDecider(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	m.RegisterExecutor(ActionRun, &fakeExecutor{})
	if _, err := m.WorkflowCreate(manualKeyedTemplateJSON("manual-permdec-both", "deploys")); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	wf := mustInstantiateTemplate(t, m, "manual-permdec-both", "1")
	snap, ok, err := s.loadCurrent(wf)
	if err != nil || !ok {
		t.Fatalf("loadCurrent: ok=%v err=%v", ok, err)
	}
	act := activationByNode(&snap.Instance, "start")
	if act == nil {
		t.Fatal("start activation not found")
	}
	leaseID, token := mustClaimWorkflow(t, m, wf, "start", act.ID, "manual-1")
	payload, err := json.Marshal(map[string]any{
		"op":            "start",
		"node_id":       "start",
		"activation_id": string(act.ID),
		"lease_id":      string(leaseID),
		"owner_token":   token,
		"selection": map[string]any{
			"auto_approve":       true,
			"permission_decider": map[string]any{"entry": "rm-guard"},
		},
	})
	if err != nil {
		t.Fatalf("marshal start: %v", err)
	}
	if _, err := m.WorkflowCommand(string(wf), payload); err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("start error = %v, want auto_approve/permission_decider mutual exclusion", err)
	}
	mustManualStart(t, m, wf, "start", act.ID, leaseID, token, nil)
	inst := mustLoadInstance(t, s, wf)
	started := activationByNode(inst, "start")
	if started == nil || started.ActiveLease == nil || started.ActiveLease.ID != leaseID {
		t.Fatalf("activation after valid start = %+v, want the claim's lease still active", started)
	}
}

// TestManualConcurrencyKeysSerializeAcrossBoundaries proves a key declared on
// a manual node serializes both directions: a manual keyed activation blocks
// a controller dispatch on the same key and a live controller attempt blocks
// the manual start. A keyed manual node never surfaces as a candidate.
func TestManualConcurrencyKeysSerializeAcrossBoundaries(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	m.testHooks.beginDispatchPreCommit = func() {
		m.testHooks.beginDispatchPreCommit = nil
		side := activationByNode(mustLoadInstance(t, s, wf), "side")
		if side == nil {
			t.Error("side activation not found")
			return
		}
		mustClaimWorkflow(t, m, wf, "side", side.ID, "manual-1")
	}

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

// TestBeginDispatchPermissionDeciderSelectionReDispatch proves a re-dispatch
// (retry) of a permission_policy node does not conflict when the freshly
// resolved selection carries an equal-content decider under a different
// pointer, while a decider with different content is still rejected.
func TestBeginDispatchPermissionDeciderSelectionReDispatch(t *testing.T) {
	t.Parallel()
	m, _, wf := newAutoDispatchFixture(t, "begin-sel-permdec", "ctl-a", -1)
	if err := m.RebuildCandidateIndex("test"); err != nil {
		t.Fatalf("RebuildCandidateIndex: %v", err)
	}
	cands, err := m.CandidatesForController("ctl-a", 1)
	if err != nil || len(cands) != 1 {
		t.Fatalf("candidates: n=%d err=%v", len(cands), err)
	}
	cand := cands[0]
	begin, err := m.BeginDispatch(BeginDispatchRequest{
		WorkflowID:       wf,
		NodeID:           cand.Identity.NodeID,
		ActivationID:     cand.Identity.ActivationID,
		ExpectedRevision: cand.Revision,
		ControllerID:     "ctl-a",
		LeaderLeaseID:    "lease_leader_1",
		Selection: &ExecutionSelection{
			Backend: "pi", Agent: "worker", Model: "m1",
			PermissionDecider: &PermissionDecider{Entry: "rm-guard", Backend: "pi", Agent: "rm-guard", Model: "guard-m"},
		},
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

	// An equal-content decider resolved into a fresh pointer re-dispatches
	// without conflict.
	if _, err := m.BeginDispatch(BeginDispatchRequest{
		WorkflowID:       wf,
		NodeID:           reCands[0].Identity.NodeID,
		ActivationID:     reCands[0].Identity.ActivationID,
		ExpectedRevision: reCands[0].Revision,
		ControllerID:     "ctl-a",
		LeaderLeaseID:    "lease_leader_1",
		Selection: &ExecutionSelection{
			Backend: "pi", Agent: "worker", Model: "m1",
			PermissionDecider: &PermissionDecider{Entry: "rm-guard", Backend: "pi", Agent: "rm-guard", Model: "guard-m"},
		},
	}); err != nil {
		t.Fatalf("re-dispatch with equal-content decider: %v, want no conflict", err)
	}

	// A decider whose content differs still conflicts.
	m2, _, wf2 := newAutoDispatchFixture(t, "begin-sel-permdec-2", "ctl-a", -1)
	if err := m2.RebuildCandidateIndex("test"); err != nil {
		t.Fatalf("RebuildCandidateIndex: %v", err)
	}
	cands2, err := m2.CandidatesForController("ctl-a", 1)
	if err != nil || len(cands2) != 1 {
		t.Fatalf("candidates: n=%d err=%v", len(cands2), err)
	}
	cand2 := cands2[0]
	begin2, err := m2.BeginDispatch(BeginDispatchRequest{
		WorkflowID:       wf2,
		NodeID:           cand2.Identity.NodeID,
		ActivationID:     cand2.Identity.ActivationID,
		ExpectedRevision: cand2.Revision,
		ControllerID:     "ctl-a",
		LeaderLeaseID:    "lease_leader_1",
		Selection: &ExecutionSelection{
			Backend: "pi", Agent: "worker", Model: "m1",
			PermissionDecider: &PermissionDecider{Entry: "rm-guard", Backend: "pi", Agent: "rm-guard", Model: "guard-m"},
		},
	})
	if err != nil {
		t.Fatalf("BeginDispatch: %v", err)
	}
	_ = begin2
	// Fail the attempt so the retry policy re-arms the activation with the
	// selection still pinned.
	if err := m2.FinalizeDispatch(FinalizeDispatchRequest{
		WorkflowID:    wf2,
		NodeID:        cand2.Identity.NodeID,
		ActivationID:  cand2.Identity.ActivationID,
		AttemptID:     begin2.AttemptID,
		LeaseID:       begin2.LeaseID,
		FailureStatus: AttemptFailed,
	}); err != nil {
		t.Fatalf("FinalizeDispatch: %v", err)
	}
	if err := m2.RebuildCandidateIndex("test"); err != nil {
		t.Fatalf("RebuildCandidateIndex after re-arm: %v", err)
	}
	reCands2, err := m2.CandidatesForController("ctl-a", 10)
	if err != nil || len(reCands2) != 1 {
		t.Fatalf("re-armed candidates: n=%d err=%v", len(reCands2), err)
	}
	if _, err := m2.BeginDispatch(BeginDispatchRequest{
		WorkflowID:       wf2,
		NodeID:           reCands2[0].Identity.NodeID,
		ActivationID:     reCands2[0].Identity.ActivationID,
		ExpectedRevision: reCands2[0].Revision,
		ControllerID:     "ctl-a",
		LeaderLeaseID:    "lease_leader_1",
		Selection: &ExecutionSelection{
			Backend: "pi", Agent: "worker", Model: "m1",
			PermissionDecider: &PermissionDecider{Entry: "rm-guard", Backend: "pi", Agent: "rm-guard", Model: "other-m"},
		},
	}); !errors.Is(err, ErrSelectionConflict) {
		t.Fatalf("re-dispatch with differing decider error = %v, want ErrSelectionConflict", err)
	}

	// A pinned selection without a decider (reachable when a template gains a
	// permission policy across versions) conflicts with a freshly resolved
	// decider: nil and non-nil are never the same.
	m3, _, wf3 := newAutoDispatchFixture(t, "begin-sel-permdec-3", "ctl-a", -1)
	if err := m3.RebuildCandidateIndex("test"); err != nil {
		t.Fatalf("RebuildCandidateIndex: %v", err)
	}
	cands3, err := m3.CandidatesForController("ctl-a", 1)
	if err != nil || len(cands3) != 1 {
		t.Fatalf("candidates: n=%d err=%v", len(cands3), err)
	}
	begin3, err := m3.BeginDispatch(BeginDispatchRequest{
		WorkflowID:       wf3,
		NodeID:           cands3[0].Identity.NodeID,
		ActivationID:     cands3[0].Identity.ActivationID,
		ExpectedRevision: cands3[0].Revision,
		ControllerID:     "ctl-a",
		LeaderLeaseID:    "lease_leader_1",
		Selection:        &ExecutionSelection{Backend: "pi", Agent: "worker", Model: "m1"},
	})
	if err != nil {
		t.Fatalf("BeginDispatch: %v", err)
	}
	// Fail the attempt so the retry policy re-arms the activation with the
	// selection still pinned.
	if err := m3.FinalizeDispatch(FinalizeDispatchRequest{
		WorkflowID:    wf3,
		NodeID:        cands3[0].Identity.NodeID,
		ActivationID:  cands3[0].Identity.ActivationID,
		AttemptID:     begin3.AttemptID,
		LeaseID:       begin3.LeaseID,
		FailureStatus: AttemptFailed,
	}); err != nil {
		t.Fatalf("FinalizeDispatch: %v", err)
	}
	if err := m3.RebuildCandidateIndex("test"); err != nil {
		t.Fatalf("RebuildCandidateIndex after re-arm: %v", err)
	}
	reCands3, err := m3.CandidatesForController("ctl-a", 10)
	if err != nil || len(reCands3) != 1 {
		t.Fatalf("re-armed candidates: n=%d err=%v", len(reCands3), err)
	}
	if _, err := m3.BeginDispatch(BeginDispatchRequest{
		WorkflowID:       wf3,
		NodeID:           reCands3[0].Identity.NodeID,
		ActivationID:     reCands3[0].Identity.ActivationID,
		ExpectedRevision: reCands3[0].Revision,
		ControllerID:     "ctl-a",
		LeaderLeaseID:    "lease_leader_1",
		Selection: &ExecutionSelection{
			Backend: "pi", Agent: "worker", Model: "m1",
			PermissionDecider: &PermissionDecider{Entry: "rm-guard", Backend: "pi", Agent: "rm-guard", Model: "guard-m"},
		},
	}); !errors.Is(err, ErrSelectionConflict) {
		t.Fatalf("re-dispatch with decider over nil-pinned decider error = %v, want ErrSelectionConflict", err)
	}
}

// TestManualStartSelectionRespectsPinnedSelection proves a manual workflow
// start with a selection conflicting with the one pinned by BeginDispatch is
// rejected, and a start without a selection inherits the pinned one.
func TestManualStartSelectionRespectsPinnedSelection(t *testing.T) {
	t.Parallel()
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
