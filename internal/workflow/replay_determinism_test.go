package workflow

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// driveRealisticSequence runs instantiate -> claim -> start -> failed
// termination -> unblock -> claim -> start (store-assigned attempt) ->
// complete with a branch to a second node -> claim -> start -> complete
// through the store, returning the live snapshot.
func driveRealisticSequence(t *testing.T, s *Store, wf WorkflowID) Snapshot {
	t.Helper()
	payload, err := json.Marshal(InstanceRecord{
		TemplateID:       "t1",
		TemplateVersion:  "1",
		TerminalOutcomes: []OutcomeName{"done"},
		EntryNodes:       []NodeID{"start"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var snap Snapshot
	step := 0
	run := func(cmd Command) {
		t.Helper()
		step++
		cmd.ExpectedRevision = snap.Instance.Revision
		cmd.IdempotencyKey = "step-" + string(rune('a'+step))
		next, err := s.ApplyCommand(wf, cmd)
		if err != nil {
			t.Fatalf("step %d (%s): %v", step, cmd.Kind, err)
		}
		snap = next
	}
	ident := func(node NodeID, attempt AttemptID) ExecutionIdentity {
		id := ExecutionIdentity{WorkflowID: wf, NodeID: node, AttemptID: attempt}
		for _, a := range snap.Instance.Activations {
			if a.NodeID == node {
				id.ActivationID = a.ID
			}
		}
		return id
	}

	run(Command{Kind: CommandInstantiate, Identity: ExecutionIdentity{WorkflowID: wf}, Payload: payload})
	run(Command{Kind: CommandClaim, Identity: ident("start", ""), LeaseID: "lease-1", Actor: "alice"})
	run(Command{Kind: CommandStart, Identity: ident("start", "att-1"), LeaseID: "lease-1"})
	run(Command{Kind: CommandTerminate, Identity: ident("start", "att-1"), LeaseID: "lease-1", AttemptStatus: AttemptFailed})
	run(Command{Kind: CommandUnblock, Identity: ident("start", ""), Actor: "ops", Reason: "retry"})
	run(Command{Kind: CommandClaim, Identity: ident("start", ""), LeaseID: "lease-2", Actor: "alice"})
	run(Command{Kind: CommandStart, Identity: ident("start", ""), LeaseID: "lease-2"})

	branch, err := json.Marshal(Transition{
		ActivationID: ident("start", "").ActivationID,
		Outcome:      "next",
		TargetNodeID: "second",
	})
	if err != nil {
		t.Fatal(err)
	}
	run(Command{Kind: CommandComplete, Identity: ident("start", ""), LeaseID: "lease-2", Outcome: "next", Payload: branch})
	run(Command{Kind: CommandClaim, Identity: ident("second", ""), LeaseID: "lease-3", Actor: "alice"})
	run(Command{Kind: CommandStart, Identity: ident("second", ""), LeaseID: "lease-3"})
	run(Command{Kind: CommandComplete, Identity: ident("second", ""), LeaseID: "lease-3", Outcome: "done"})
	return snap
}

// TestReplayFromEmptyReproducesLiveSnapshot proves the event log alone is the
// source of truth: folding it from an empty snapshot yields exactly the live
// snapshot, every time.
func TestReplayFromEmptyReproducesLiveSnapshot(t *testing.T) {
	s := newStore(t)
	wf := WorkflowID("wf-determinism")
	live := driveRealisticSequence(t, s, wf)
	if len(live.Instance.Activations) != 2 || len(live.Instance.Attempts) != 3 {
		t.Fatalf("sequence shape: %d activations, %d attempts; want 2 and 3",
			len(live.Instance.Activations), len(live.Instance.Attempts))
	}

	first, _, _, err := replayEvents(Snapshot{}, s.eventsPath(wf), s.policies)
	if err != nil {
		t.Fatalf("first replay: %v", err)
	}
	second, _, _, err := replayEvents(Snapshot{}, s.eventsPath(wf), s.policies)
	if err != nil {
		t.Fatalf("second replay: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("two replays of the same log differ:\n%+v\n%+v", first, second)
	}
	if !reflect.DeepEqual(live, first) {
		t.Fatalf("replay differs from live snapshot:\nlive:   %+v\nreplay: %+v", live, first)
	}
}

// TestReplayLegacyEventsWithoutMintedFields replays events shaped like those
// written before the command boundary minted IDs and timestamps: no at,
// activation_ids, instance_id, or started attempt_id. The reducer must still
// fold them, filling the missing fields itself.
func TestReplayLegacyEventsWithoutMintedFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.ndjson")
	legacy := `{"id":"e1","kind":"workflow.event.instantiated","sequence":1,"identity":{"workflow_id":"wf-legacy"},"instantiated":{"template_id":"t1","template_version":"1","terminal_outcomes":["done"],"entry_nodes":["a","b"]}}
`
	if err := os.WriteFile(path, []byte(legacy), 0o644); err != nil {
		t.Fatal(err)
	}
	snap, n, _, err := replayEvents(Snapshot{}, path, nil)
	if err != nil || n != 1 {
		t.Fatalf("replay instantiated: n=%d err=%v", n, err)
	}
	inst := snap.Instance
	if inst.InstanceID == "" || inst.CreatedAt.IsZero() || inst.UpdatedAt.IsZero() {
		t.Fatalf("instance fields not filled: %+v", inst)
	}
	if len(inst.Activations) != 2 || inst.Activations[0].ID == "" || inst.Activations[0].ID == inst.Activations[1].ID {
		t.Fatalf("activations = %+v, want two distinct non-empty IDs", inst.Activations)
	}

	// A legacy instantiated event whose identity carries no workflow_id (and
	// no at or activation_ids) still yields a workflow identity: the reducer
	// mints one into the instance.
	anonymous := `{"id":"e1","kind":"workflow.event.instantiated","sequence":1,"instantiated":{"template_id":"t1","template_version":"1","terminal_outcomes":["done"],"entry_nodes":["a"]}}
`
	anonPath := filepath.Join(t.TempDir(), "events.ndjson")
	if err := os.WriteFile(anonPath, []byte(anonymous), 0o644); err != nil {
		t.Fatal(err)
	}
	anonSnap, n, _, err := replayEvents(Snapshot{}, anonPath, nil)
	if err != nil || n != 1 {
		t.Fatalf("replay anonymous instantiated: n=%d err=%v", n, err)
	}
	if anonSnap.Instance.WorkflowID == "" {
		t.Fatalf("workflow ID = %q, want a minted non-empty ID", anonSnap.Instance.WorkflowID)
	}

	// A legacy tail past a materialized snapshot resolves against the IDs the
	// snapshot already holds.
	actID := inst.Activations[0].ID
	tail := fmt.Sprintf(`{"id":"e2","kind":"workflow.event.leased","sequence":2,"identity":{"workflow_id":"wf-legacy","node_id":"a","activation_id":%q},"lease_id":"l1","actor":"alice"}
{"id":"e3","kind":"workflow.event.started","sequence":3,"identity":{"workflow_id":"wf-legacy","node_id":"a","activation_id":%q},"lease_id":"l1"}
`, actID, actID)
	if err := os.WriteFile(path, []byte(legacy+tail), 0o644); err != nil {
		t.Fatal(err)
	}
	snap, n, _, err = replayEvents(snap, path, nil)
	if err != nil || n != 3 {
		t.Fatalf("replay tail: n=%d err=%v", n, err)
	}
	if len(snap.Instance.Attempts) != 1 || snap.Instance.Attempts[0].ID == "" {
		t.Fatalf("attempts = %+v, want one with a filled ID", snap.Instance.Attempts)
	}
	if got := snap.Instance.Activations[0]; got.Status != ActivationRunning || got.UpdatedAt.IsZero() {
		t.Fatalf("activation = %+v, want running with an update time", got)
	}

	// Legacy transition and rerouted events carry no at and no activation_ids:
	// replay stamps the time and mints the ID, so the fold still creates the
	// new activation.
	branchTail := fmt.Sprintf(`{"id":"e4","kind":"workflow.event.transition","sequence":4,"identity":{"workflow_id":"wf-legacy","node_id":"a","activation_id":%q},"transition":{"activation_id":"","outcome":"next","target_node_id":"c"}}
{"id":"e5","kind":"workflow.event.rerouted","sequence":5,"identity":{"workflow_id":"wf-legacy","node_id":"d"}}
`, actID)
	if err := os.WriteFile(path, []byte(legacy+tail+branchTail), 0o644); err != nil {
		t.Fatal(err)
	}
	snap, n, _, err = replayEvents(snap, path, nil)
	if err != nil || n != 5 {
		t.Fatalf("replay legacy transition/rerouted: n=%d err=%v", n, err)
	}
	if got := len(snap.Instance.Activations); got != 4 {
		t.Fatalf("activations = %d, want 4 (two entries plus transition and rerouted)", got)
	}
	transitioned := snap.Instance.Activations[2]
	if transitioned.ID == "" || transitioned.NodeID != "c" {
		t.Fatalf("transition activation = %+v, want a minted ID on node c", transitioned)
	}
	rerouted := snap.Instance.Activations[3]
	if rerouted.ID == "" || rerouted.NodeID != "d" {
		t.Fatalf("rerouted activation = %+v, want a minted ID on node d", rerouted)
	}
}

// TestReplayWithPoliciesReproducesLiveSnapshot proves the same parity when
// the reducer consults template policies: an instance driven through a
// manager whose template carries a retry policy and a dispatch policy folds
// back from an empty snapshot — with the store's policies — into exactly the
// live snapshot, retry budget and stamped dispatch included.
func TestReplayWithPoliciesReproducesLiveSnapshot(t *testing.T) {
	template := []byte(`{
  "schema_version": 1,
  "template_id": "replay-policy",
  "template_version": "1",
  "entry_nodes": ["start"],
  "nodes": [
    {"id": "start", "action": {"type": "run", "prompt": "try"}, "retry_policy": {"max_attempts": 2, "exhaustion": "fail"}, "branches": {"next": "second"}},
    {"id": "second", "action": {"type": "run", "prompt": "continue"}, "dispatch": {"mode": "auto", "controller_id": "ctl-a"}}
  ],
  "terminal_outcomes": ["done"],
  "default_lease_policy": {"ttl_seconds": 900}
}`)
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	m.RegisterExecutor(ActionRun, &fakeExecutor{})
	if _, err := m.WorkflowCreate(template); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	wf := mustInstantiateTemplate(t, m, "replay-policy", "1")

	// First attempt fails: the retry policy re-arms the same activation so the
	// second attempt runs against the remaining budget.
	snap, ok, err := s.loadCurrent(wf)
	if err != nil || !ok {
		t.Fatalf("loadCurrent: ok=%v err=%v", ok, err)
	}
	start := activationByNode(&snap.Instance, "start")
	res := claimActivation(t, m, wf, "start", string(start.ID), "alice")
	out, err := m.WorkflowCommand(string(wf), startCommandPayload(t, "start", string(start.ID), res, nil))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	attemptID := out.(map[string]any)["attempt_id"].(string)
	if err := m.RecordAttemptTerminated(wf, "start", start.ID, AttemptID(attemptID), LeaseID(res["lease_id"].(string)), AttemptFailed); err != nil {
		t.Fatalf("terminate failed attempt: %v", err)
	}

	// Second attempt succeeds and completes with the "next" branch outcome,
	// transitioning to the auto-dispatched second node.
	snap, ok, err = s.loadCurrent(wf)
	if err != nil || !ok {
		t.Fatalf("reload: ok=%v err=%v", ok, err)
	}
	start = activationByNode(&snap.Instance, "start")
	res = claimActivation(t, m, wf, "start", string(start.ID), "alice")
	out, err = m.WorkflowCommand(string(wf), startCommandPayload(t, "start", string(start.ID), res, nil))
	if err != nil {
		t.Fatalf("second start: %v", err)
	}
	attemptID = out.(map[string]any)["attempt_id"].(string)
	complete := map[string]any{
		"op":            "complete",
		"node_id":       "start",
		"activation_id": string(start.ID),
		"attempt_id":    attemptID,
		"lease_id":      res["lease_id"].(string),
		"owner_token":   res["owner_token"],
		"outcome":       "next",
	}
	cdata, err := json.Marshal(complete)
	if err != nil {
		t.Fatalf("marshal complete: %v", err)
	}
	if _, err := m.WorkflowCommand(string(wf), cdata); err != nil {
		t.Fatalf("complete: %v", err)
	}

	live, ok, err := s.loadCurrent(wf)
	if err != nil || !ok {
		t.Fatalf("loadCurrent after complete: ok=%v err=%v", ok, err)
	}

	// The sequence must actually exercise both policies: the start activation
	// carries both attempts (the retry re-arm consumed one failed attempt),
	// and the second activation carries the template's stamped dispatch
	// policy.
	startAct := activationByNode(&live.Instance, "start")
	if startAct == nil || len(startAct.AttemptIDs) != 2 {
		t.Fatalf("start activation attempts = %+v, want two attempts on the re-armed activation", startAct)
	}
	second := activationByNode(&live.Instance, "second")
	if second == nil || second.Dispatch == nil || !second.Dispatch.IsAuto() {
		t.Fatalf("second activation dispatch = %+v, want the template's auto policy", second)
	}

	assertFromEmptyReplayParity(t, s, wf)
}

// TestStoreAndManagerShareOneClock proves a manager's clock stamps the
// store's events and readiness, so injected time shows up exactly in the
// snapshot.
func TestStoreAndManagerShareOneClock(t *testing.T) {
	wf := WorkflowID("wf-clock")
	times := []time.Time{
		time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC),
		time.Date(2030, 1, 2, 3, 5, 6, 0, time.UTC),
	}
	tick := 0
	s := newStore(t)
	NewManagerWithClock(s, func() time.Time {
		if tick < len(times) {
			tick++
			return times[tick-1]
		}
		return times[len(times)-1]
	})

	snap := mustInstantiate(t, s, wf)
	act := snap.Instance.Activations[0]
	if !snap.Instance.CreatedAt.Equal(times[0]) || !act.CreatedAt.Equal(times[0]) {
		t.Fatalf("created at = %v / %v, want %v", snap.Instance.CreatedAt, act.CreatedAt, times[0])
	}
	if !snap.Instance.UpdatedAt.Equal(times[0]) || !act.UpdatedAt.Equal(times[0]) {
		t.Fatalf("updated at = %v / %v, want %v", snap.Instance.UpdatedAt, act.UpdatedAt, times[0])
	}
	if act.ReadyAt == nil || !act.ReadyAt.Equal(times[0]) {
		t.Fatalf("ready_at = %v, want %v", act.ReadyAt, times[0])
	}

	// The next command stamps its own event time: the leased event the claim
	// produces stamps the activation's UpdatedAt but not the instance's.
	next, err := s.ApplyCommand(wf, Command{
		Kind:             CommandClaim,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "claim-1",
		Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: act.NodeID, ActivationID: act.ID},
		LeaseID:          "lease-1",
		Actor:            "alice",
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if next.Instance.UpdatedAt.Equal(times[1]) {
		t.Fatalf("instance updated at = %v, want it unchanged at %v", next.Instance.UpdatedAt, times[0])
	}
	if claimed := next.Instance.Activations[0]; !claimed.UpdatedAt.Equal(times[1]) {
		t.Fatalf("activation updated at = %v, want %v", claimed.UpdatedAt, times[1])
	}
}

// ---------------------------------------------------------------------------
// From-empty replay parity for the remaining event kinds
// ---------------------------------------------------------------------------

// assertFromEmptyReplayParity requires two from-empty folds of the
// workflow's event log — with the store's policies — to DeepEqual each
// other and the live snapshot, so every field the reducers touch must come
// from the events.
func assertFromEmptyReplayParity(t *testing.T, s *Store, wf WorkflowID) {
	t.Helper()
	live, ok, err := s.loadCurrent(wf)
	if err != nil || !ok {
		t.Fatalf("loadCurrent: ok=%v err=%v", ok, err)
	}
	var prev Snapshot
	for i := 1; i <= 2; i++ {
		replayed, _, _, err := replayEvents(Snapshot{}, s.eventsPath(wf), s.policies)
		if err != nil {
			t.Fatalf("replay %d: %v", i, err)
		}
		if i > 1 && !reflect.DeepEqual(prev, replayed) {
			t.Fatalf("replay %d differs from the earlier fold:\nfirst:  %+v\nsecond: %+v", i, prev, replayed)
		}
		if !reflect.DeepEqual(live, replayed) {
			t.Fatalf("replay %d differs from live snapshot:\nlive:   %+v\nreplay: %+v", i, live, replayed)
		}
		prev = replayed
	}
}

// managerRunningActivation claims and starts the fixture's single node
// through the manager, returning the activation ID, the claim result
// (lease_id/owner_token), and the started attempt ID.
func managerRunningActivation(t *testing.T, m *Manager, s *Store, wf WorkflowID, node string) (ActivationID, map[string]any, string) {
	t.Helper()
	snap, ok, err := s.loadCurrent(wf)
	if err != nil || !ok {
		t.Fatalf("loadCurrent: ok=%v err=%v", ok, err)
	}
	actID := activationByNode(&snap.Instance, NodeID(node)).ID
	res := claimActivation(t, m, wf, node, string(actID), "alice")
	out, err := m.WorkflowCommand(string(wf), startCommandPayload(t, node, string(actID), res, nil))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	attemptID, ok := out.(map[string]any)["attempt_id"].(string)
	if !ok || attemptID == "" {
		t.Fatalf("start result missing attempt_id: %#v", out)
	}
	return actID, res, attemptID
}

// TestReplayAttemptIdentifiedParity pins the attempt_identified kind: the
// runtime identity a dispatch finalization records must come wholly from the
// event, so a from-empty fold rebuilds the identified attempt exactly.
func TestReplayAttemptIdentifiedParity(t *testing.T) {
	m, s, wf, node := newManagerFixture(t)
	actID, res, attemptID := managerRunningActivation(t, m, s, wf, node)
	if err := m.FinalizeDispatch(FinalizeDispatchRequest{
		WorkflowID:   wf,
		NodeID:       NodeID(node),
		ActivationID: actID,
		AttemptID:    AttemptID(attemptID),
		LeaseID:      LeaseID(res["lease_id"].(string)),
		RuntimeID:    "rt-1",
		RunID:        "run-1",
		SessionID:    "sess-1",
	}); err != nil {
		t.Fatalf("FinalizeDispatch: %v", err)
	}
	assertFromEmptyReplayParity(t, s, wf)
}

// TestReplaySkippedParity pins the skipped kind through the store command
// boundary: the skip's actor, reason, and timestamp live on the event, so a
// from-empty fold rebuilds the skipped activation exactly.
func TestReplaySkippedParity(t *testing.T) {
	s := newStore(t)
	wf := WorkflowID("wf-replay-skipped")
	snap := mustInstantiate(t, s, wf)
	actID := snap.Instance.Activations[0].ID
	if _, err := s.ApplyCommand(wf, Command{
		Kind:             CommandSkip,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "skip-1",
		Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: "start", ActivationID: actID},
		Actor:            "alice",
		Reason:           "not needed",
	}); err != nil {
		t.Fatalf("skip: %v", err)
	}

	// The skip's timestamp rides on the event: the live activation must
	// carry that At as its UpdatedAt before the parity folds run.
	var skipAt time.Time
	for _, e := range readEvents(t, s, wf) {
		if e.Kind == EventSkipped {
			skipAt = e.At
		}
	}
	live, ok, err := s.loadCurrent(wf)
	if err != nil || !ok {
		t.Fatalf("loadCurrent: ok=%v err=%v", ok, err)
	}
	skipped := live.Instance.Activations[0]
	if skipped.UpdatedAt.IsZero() || !skipped.UpdatedAt.Equal(skipAt) {
		t.Fatalf("skipped activation updated_at = %v, want the skip event's %v", skipped.UpdatedAt, skipAt)
	}

	assertFromEmptyReplayParity(t, s, wf)
}

// TestReplayReroutedParity pins the rerouted kind through the store command
// boundary: the fresh activation's ID, iteration, and selection ride on the
// event, so a from-empty fold rebuilds the rerouted activation exactly.
func TestReplayReroutedParity(t *testing.T) {
	_, s, wf := newAutoDispatchFixture(t, "dispatch-reroute-replay", "ctl-a", -1)
	snap, ok, err := s.loadCurrent(wf)
	if err != nil || !ok {
		t.Fatalf("loadCurrent: ok=%v err=%v", ok, err)
	}
	payload, err := json.Marshal(struct {
		Selection *ExecutionSelection `json:"selection,omitempty"`
		Iteration int                 `json:"iteration,omitempty"`
	}{
		Selection: &ExecutionSelection{Role: "role-x", Backend: "b1", Model: "m1"},
		Iteration: 2,
	})
	if err != nil {
		t.Fatalf("marshal reroute payload: %v", err)
	}
	if _, err := s.ApplyCommand(wf, Command{
		Kind:             CommandReroute,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "reroute-1",
		Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: "start"},
		Payload:          payload,
	}); err != nil {
		t.Fatalf("reroute: %v", err)
	}
	assertFromEmptyReplayParity(t, s, wf)
}

// TestReplayCanceledParity pins the canceled kind: the cancel parks the
// live-leased activation and stamps the instance terminal, and a from-empty
// fold reproduces the released lease and the terminal attribution exactly.
func TestReplayCanceledParity(t *testing.T) {
	m, s, wf, node := newManagerFixture(t)
	managerRunningActivation(t, m, s, wf, node)
	payload, err := json.Marshal(map[string]string{"actor": "ops", "reason": "superseded by a newer run"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.WorkflowCancel(string(wf), payload); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	assertFromEmptyReplayParity(t, s, wf)
}

// TestReplayHeartbeatParity pins the heartbeat kind: the renewal's
// LastHeartbeatAt and ExpiresAt ride on the event's lease metadata, so a
// from-empty fold rebuilds the renewed liveness fields exactly.
func TestReplayHeartbeatParity(t *testing.T) {
	m, s, wf, node := newManagerFixture(t)
	actID, res, _ := managerRunningActivation(t, m, s, wf, node)
	if err := m.Heartbeat(wf, NodeID(node), actID, LeaseID(res["lease_id"].(string)), res["owner_token"].(string)); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	assertFromEmptyReplayParity(t, s, wf)
}

// TestReplayLeaseExpiredParity pins the lease_expired kind: the sweep's event
// carries the stamps the reducer copies (the re-armed readiness and the timed
// out attempt's end), so a from-empty fold rebuilds the expired activation
// exactly.
func TestReplayLeaseExpiredParity(t *testing.T) {
	m, s, wf, node := newManagerFixture(t)
	snap, ok, err := s.loadCurrent(wf)
	if err != nil || !ok {
		t.Fatalf("loadCurrent: ok=%v err=%v", ok, err)
	}
	nodeID := NodeID(node)
	actID := activationByNode(&snap.Instance, nodeID).ID
	past := time.Now().UTC().Add(-time.Hour)
	claimWithLease(t, s, wf, nodeID, Lease{
		ID:           "lease-ghost",
		ActivationID: actID,
		Owner:        "alice",
		TokenDigest:  ownerTokenDigest("ghost-token"),
		AcquiredAt:   past.Add(-time.Minute),
		ExpiresAt:    past,
	}, "alice")
	startWithToken(t, m, wf, node, string(actID), "lease-ghost", "ghost-token")
	if _, err := m.ExpireStaleLeases(); err != nil {
		t.Fatalf("ExpireStaleLeases: %v", err)
	}
	assertFromEmptyReplayParity(t, s, wf)
}

// TestReplayGateDecisionParity pins the gate kind: a resolving satisfy
// decision records the gate instance and follows the declared branch, and a
// from-empty fold rebuilds the decided gate and the branch activation
// exactly.
func TestReplayGateDecisionParity(t *testing.T) {
	m, s, wf := newCompleteFixture(t, completeGatedTemplateJSON, "complete-gated", "1")
	actID := parkGatedActivation(t, m, s, wf)
	if _, err := m.WorkflowCommand(string(wf), gateCommandPayload(t, string(actID), "review", "satisfy", map[string]any{
		"actor":        "alice",
		"reason":       "approved",
		"evidence_ids": []string{"ev_1"},
	})); err != nil {
		t.Fatalf("gate satisfy: %v", err)
	}
	assertFromEmptyReplayParity(t, s, wf)
}

// TestReplayChildAttachedAndOutcomeParity pins the composition kinds: the
// spawn's attach parks the parent activation awaiting_child, the child's
// terminal outcome resolves it, and from-empty folds rebuild both the parent
// and the child exactly.
func TestReplayChildAttachedAndOutcomeParity(t *testing.T) {
	m, s, wf, childID := compositionPhase2Fixture(t, map[OutcomeName]OutcomeName{"done": "done"})
	registerBoundedWorkflowExecutor(t, m, 10*time.Second)

	driveErr := make(chan error, 1)
	go func() {
		if err := waitParentAwaitingChild(s, wf, 10*time.Second); err != nil {
			driveErr <- err
			return
		}
		driveErr <- driveChildTerminalErr(s, childID, "done", nil)
	}()
	parentPayload, _ := claimStartSpawn(t, m, s, wf)
	if _, err := m.WorkflowCommand(string(wf), parentPayload); err != nil {
		t.Fatalf("start spawn: %v", err)
	}
	if err := <-driveErr; err != nil {
		t.Fatalf("child driver: %v", err)
	}
	assertFromEmptyReplayParity(t, s, wf)
	assertFromEmptyReplayParity(t, s, childID)
}

// TestReplayExternalParkedParity pins the external_parked kind: the
// controller's kernel-local park moves the auto external activation to
// awaiting_gate under the pinned success outcome, and a from-empty fold
// rebuilds the parked activation and its pinned gates exactly.
func TestReplayExternalParkedParity(t *testing.T) {
	m, s, wf := parkFixture(t, boundGateTemplateJSON)
	driveBoundPublication(t, m, s, wf, boundPublicationOutputs("org/repo", 42, "abc123"))
	review := latestActivation(t, s, wf, "review")
	rev := revision(t, s, wf)
	if _, err := m.ParkExternal(ParkExternalRequest{
		WorkflowID:       wf,
		NodeID:           "review",
		ActivationID:     review.ID,
		ExpectedRevision: rev,
		ControllerID:     "ctl",
	}); err != nil {
		t.Fatalf("ParkExternal: %v", err)
	}
	assertFromEmptyReplayParity(t, s, wf)
}

// ---------------------------------------------------------------------------
// Migration from the pre-change on-disk shape
// ---------------------------------------------------------------------------

// TestLegacySnapshotMigrationAcceptsNewCommands builds the pre-change
// on-disk shape — an events.ndjson whose lines carry no at and no
// activation_ids, plus the workflow.json snapshot one replay of that log
// materializes — then drives new commands through the manager. The
// commands must succeed, the appended events must carry the fields the
// command boundary mints, and folding the whole log from the legacy
// snapshot must reproduce the live snapshot exactly.
func TestLegacySnapshotMigrationAcceptsNewCommands(t *testing.T) {
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	template := Template{
		SchemaVersion:    1,
		TemplateID:       "t1",
		TemplateVersion:  "1",
		EntryNodes:       []NodeID{"start", "other"},
		TerminalOutcomes: []OutcomeName{"done"},
		Nodes: []NodeDefinition{
			{ID: "start", Action: Action{Kind: ActionManual, Manual: &ManualAction{Instructions: "do"}}, Branches: map[OutcomeName]NodeID{"next": "second"}},
			{ID: "other", Action: Action{Kind: ActionManual, Manual: &ManualAction{Instructions: "do"}}},
			{ID: "second", Action: Action{Kind: ActionManual, Manual: &ManualAction{Instructions: "do"}}},
		},
	}
	if err := s.StoreTemplate(template.TemplateID, template.TemplateVersion, template); err != nil {
		t.Fatalf("StoreTemplate: %v", err)
	}
	m := NewManager(s)
	wf := WorkflowID("wf-migrate")

	// The pre-change log: hand-written legacy lines with no at, no
	// activation_ids, and no instance_id, exactly like the events written
	// before the command boundary minted them.
	first := `{"id":"e1","kind":"workflow.event.instantiated","sequence":1,"identity":{"workflow_id":"wf-migrate"},"instantiated":{"template_id":"t1","template_version":"1","terminal_outcomes":["done"],"entry_nodes":["start","other"]}}
`
	if err := s.ensureInstanceDir(wf); err != nil {
		t.Fatalf("ensureInstanceDir: %v", err)
	}
	if err := os.WriteFile(s.eventsPath(wf), []byte(first), 0o644); err != nil {
		t.Fatal(err)
	}
	seed, n, _, err := replayEvents(Snapshot{}, s.eventsPath(wf), s.policies)
	if err != nil || n != 1 {
		t.Fatalf("replay legacy instantiated: n=%d err=%v", n, err)
	}
	// The legacy skipped line resolves against the activation ID the seed
	// fold minted, the way the pre-change snapshot held it.
	tail := fmt.Sprintf(`{"id":"e2","kind":"workflow.event.skipped","sequence":2,"identity":{"workflow_id":"wf-migrate","node_id":"other","activation_id":%q},"actor":"alice","reason":"not needed"}
`, seed.Instance.Activations[1].ID)
	legacy := first + tail
	if err := os.WriteFile(s.eventsPath(wf), []byte(legacy), 0o644); err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	for _, line := range strings.Split(strings.TrimSpace(legacy), "\n") {
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			t.Fatalf("legacy line: %v", err)
		}
		if _, ok := raw["at"]; ok {
			t.Fatalf("legacy line carries at: %s", line)
		}
		if _, ok := raw["activation_ids"]; ok {
			t.Fatalf("legacy line carries activation_ids: %s", line)
		}
	}
	legacySnap, n, _, err := replayEvents(seed, s.eventsPath(wf), s.policies)
	if err != nil || n != 2 {
		t.Fatalf("replay legacy log: n=%d err=%v", n, err)
	}
	if err := s.writeSnapshot(wf, legacySnap); err != nil {
		t.Fatalf("write legacy snapshot: %v", err)
	}

	// New commands land on the migrated instance through the manager.
	snap, ok, err := s.loadCurrent(wf)
	if err != nil || !ok {
		t.Fatalf("loadCurrent: ok=%v err=%v", ok, err)
	}
	start := activationByNode(&snap.Instance, "start")
	if start == nil || start.Status != ActivationPending {
		t.Fatalf("start activation = %+v, want pending after the legacy fold", start)
	}
	res := claimActivation(t, m, wf, "start", string(start.ID), "alice")
	out, err := m.WorkflowCommand(string(wf), startCommandPayload(t, "start", string(start.ID), res, nil))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	attemptID, ok := out.(map[string]any)["attempt_id"].(string)
	if !ok || attemptID == "" {
		t.Fatalf("start result missing attempt_id: %#v", out)
	}
	complete, err := json.Marshal(map[string]any{
		"op":            "complete",
		"node_id":       "start",
		"activation_id": string(start.ID),
		"attempt_id":    attemptID,
		"lease_id":      res["lease_id"],
		"owner_token":   res["owner_token"],
		"outcome":       "next",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.WorkflowCommand(string(wf), complete); err != nil {
		t.Fatalf("complete: %v", err)
	}

	// Every appended event carries the minted fields, and the transition
	// created the branch activation under a command-minted ID.
	events := readEvents(t, s, wf)
	if len(events) != 6 {
		t.Fatalf("events = %d, want 6 (two legacy plus leased, started, completed, transition)", len(events))
	}
	for _, e := range events[2:] {
		if e.At.IsZero() {
			t.Fatalf("appended event %s (%s) carries no at", e.ID, e.Kind)
		}
	}
	if got := events[3]; got.Kind != EventStarted || got.AttemptID == "" {
		t.Fatalf("started event = %+v, want a command-minted attempt id", got)
	}
	transition := events[5]
	if transition.Kind != EventTransition || len(transition.ActivationIDs) != 1 || transition.ActivationIDs[0] == "" {
		t.Fatalf("transition event = %+v, want one command-minted activation id", transition)
	}
	live, ok, err := s.loadCurrent(wf)
	if err != nil || !ok {
		t.Fatalf("loadCurrent: ok=%v err=%v", ok, err)
	}
	second := activationByNode(&live.Instance, "second")
	if second == nil || second.ID != transition.ActivationIDs[0] {
		t.Fatalf("branch activation = %+v, want the transition's minted id %s", second, transition.ActivationIDs[0])
	}

	// Folding the whole log — legacy lines included — from the legacy
	// snapshot reproduces the live snapshot exactly.
	replayed, n, _, err := replayEvents(legacySnap, s.eventsPath(wf), s.policies)
	if err != nil {
		t.Fatalf("replay from legacy snapshot: %v", err)
	}
	if n != len(events) {
		t.Fatalf("replayed %d events, want %d", n, len(events))
	}
	if !reflect.DeepEqual(live, replayed) {
		t.Fatalf("replay from the legacy snapshot differs from live:\nlive:   %+v\nreplay: %+v", live, replayed)
	}
}
