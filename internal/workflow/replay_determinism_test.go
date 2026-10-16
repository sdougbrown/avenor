package workflow

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
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

	for i := 1; i <= 2; i++ {
		replayed, _, _, err := replayEvents(Snapshot{}, s.eventsPath(wf), s.policies)
		if err != nil {
			t.Fatalf("replay %d: %v", i, err)
		}
		if !reflect.DeepEqual(live, replayed) {
			t.Fatalf("replay %d differs from live snapshot:\nlive:   %+v\nreplay: %+v", i, live, replayed)
		}
	}
}

// TestStoreAndManagerShareOneClock proves a manager's clock stamps the
// store's events and readiness, so injected time shows up exactly in the
// snapshot.
func TestStoreAndManagerShareOneClock(t *testing.T) {
	fixed := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	s := newStore(t)
	NewManagerWithClock(s, func() time.Time { return fixed })

	snap := mustInstantiate(t, s, "wf-clock")
	act := snap.Instance.Activations[0]
	if !snap.Instance.CreatedAt.Equal(fixed) || !act.CreatedAt.Equal(fixed) {
		t.Fatalf("created at = %v / %v, want %v", snap.Instance.CreatedAt, act.CreatedAt, fixed)
	}
	if act.ReadyAt == nil || !act.ReadyAt.Equal(fixed) {
		t.Fatalf("ready_at = %v, want %v", act.ReadyAt, fixed)
	}
}
