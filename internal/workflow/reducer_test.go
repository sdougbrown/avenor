package workflow

// Table-driven tests for the reducer contract (Apply + Reduce). These pin the
// observable behavior of the workflow store reducer: status transitions,
// lease handling, retry/exhaustion semantics, idempotency, and revision
// checks. They use only the standard library (plain testing, t.Fatalf).

import (
	"encoding/json"
	"reflect"
	"testing"
)

// actStart returns the live "start" activation of the instance.
func actStart(state Snapshot) *Activation {
	if len(state.Instance.Activations) == 0 {
		return nil
	}
	return &state.Instance.Activations[0]
}

// baseIdentity builds the shared execution identity for commands that target
// the single "start" activation of workflow "wf1".
func baseIdentity(actID ActivationID, attemptID AttemptID) ExecutionIdentity {
	return ExecutionIdentity{
		WorkflowID:   "wf1",
		NodeID:       "start",
		ActivationID: actID,
		AttemptID:    attemptID,
	}
}

// applyCmd applies a command and reduces every returned event in order. When
// any step errors, the error is returned together with the snapshot as it was
// before the failing reduction (the reducer never mutates on error).
func applyCmd(state Snapshot, cmd Command) (Snapshot, error) {
	events, err := Apply(state, cmd)
	if err != nil {
		return state, err
	}
	for _, e := range events {
		state, err = Reduce(state, e)
		if err != nil {
			return state, err
		}
	}
	return state, nil
}

// mustApply is applyCmd that fails the test on error.
func mustApply(t *testing.T, state Snapshot, cmd Command, what string) Snapshot {
	t.Helper()
	next, err := applyCmd(state, cmd)
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	return next
}

// newInstance builds a fresh instantiated instance by Apply'ing and Reducing
// a CommandInstantiate. The returned snapshot has Revision = the instantiate
// event sequence, an empty idempotency map entry for "inst", and exactly one
// pending "start" activation.
func newInstance(t *testing.T) Snapshot {
	t.Helper()
	payload, err := json.Marshal(InstanceRecord{
		TemplateID:       "t1",
		TemplateVersion:  "1",
		TerminalOutcomes: []OutcomeName{"done", "failed_out"},
		EntryNodes:       []NodeID{"start"},
	})
	if err != nil {
		t.Fatalf("marshal instance record: %v", err)
	}
	return mustApply(t, Snapshot{}, Command{
		Kind:             CommandInstantiate,
		ExpectedRevision: 0,
		IdempotencyKey:   "inst",
		Identity:         ExecutionIdentity{WorkflowID: "wf1"},
		Payload:          payload,
	}, "instantiate")
}

// runToStart claims and starts the single start activation, returning the
// claimed lease and the attempt id so callers can drive completion/termination.
func runToStart(t *testing.T, state Snapshot) (Snapshot, LeaseID, AttemptID) {
	t.Helper()
	actID := actStart(state).ID
	state = mustApply(t, state, Command{
		Kind:             CommandClaim,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "claim-1",
		Identity:         baseIdentity(actID, ""),
		LeaseID:          "lease-1",
		Actor:            "alice",
	}, "claim")
	state = mustApply(t, state, Command{
		Kind:             CommandStart,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "start-1",
		Identity:         baseIdentity(actID, "att-1"),
		LeaseID:          "lease-1",
	}, "start")
	return state, LeaseID("lease-1"), AttemptID("att-1")
}

// setRetryPolicy installs a package-global retry policy resolver for this
// subtest and guarantees it is reset to nil when the subtest finishes.
func setRetryPolicy(t *testing.T, maximum int, exhaustion RetryExhaustionKind, outcome OutcomeName) {
	t.Helper()
	SetRetryPolicyResolver(func(TemplateID, NodeID) *RetryPolicy {
		return &RetryPolicy{MaximumAttempts: maximum, Exhaustion: exhaustion, Outcome: outcome}
	})
	t.Cleanup(func() { SetRetryPolicyResolver(nil) })
}

func TestApply(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{"instantiate_creates_instance", testInstantiateCreatesInstance},
		{"claim_moves_pending_to_leased", testClaimLeases},
		{"start_moves_leased_to_running", testStartRunsAttempt},
		{"complete_terminal_outcome", testCompleteTerminalOutcome},
		{"complete_wrong_lease", testCompleteWrongLease},
		{"spawn_and_gate", testGateDecisions},
		{"skip_pending_activation", testSkip},
		{"unblock_blocked_activation", testUnblock},
		{"reroute_creates_new_activation", testReroute},
		{"heartbeat_updates_lease", testHeartbeat},
		{"revision_mismatch", testRevisionMismatch},
		{"duplicate_idempotency_key", testDuplicateIdempotencyKey},
		{"duplicate_event_id_idempotent", testDuplicateEventID},
		{"identity_mismatch", testIdentityMismatch},
		{"retry_rearms_same_activation", testRetryRearms},
		{"exhaustion_block", testExhaustionBlock},
		{"exhaustion_fail", testExhaustionFail},
		{"exhaustion_outcome", testExhaustionOutcome},
		{"completion_before_termination", testCompletionBeforeTermination},
		{"termination_before_completion", testTerminationBeforeCompletion},
		{"required_gate_pending_on_complete", testGatePendingOnComplete},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func testInstantiateCreatesInstance(t *testing.T) {
	state := newInstance(t)
	if state.Instance.WorkflowID != "wf1" {
		t.Fatalf("workflow id: got %q want %q", state.Instance.WorkflowID, "wf1")
	}
	if state.Instance.TemplateID != "t1" || state.Instance.TemplateVersion != "1" {
		t.Fatalf("template identity: got %q/%q want t1/1", state.Instance.TemplateID, state.Instance.TemplateVersion)
	}
	if state.Instance.Revision <= 0 {
		t.Fatalf("revision after instantiate: got %d want > 0", state.Instance.Revision)
	}
	if state.Instance.Status != WorkflowActive {
		t.Fatalf("status: got %q want %q", state.Instance.Status, WorkflowActive)
	}
	if len(state.Instance.Activations) != 1 {
		t.Fatalf("activation count: got %d want 1", len(state.Instance.Activations))
	}
	act := actStart(state)
	if act == nil || act.NodeID != "start" {
		t.Fatalf("activation node: got %+v want node start", act)
	}
	if act.Status != ActivationPending {
		t.Fatalf("activation status: got %q want %q", act.Status, ActivationPending)
	}
}

func testClaimLeases(t *testing.T) {
	state := newInstance(t)
	actID := actStart(state).ID

	// A claim without a lease id must error and leave the state untouched.
	noLease, err := applyCmd(state, Command{
		Kind:             CommandClaim,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "claim-nolease",
		Identity:         baseIdentity(actID, ""),
	})
	if err == nil {
		t.Fatalf("claim without LeaseID: expected error, got nil")
	}
	if !reflect.DeepEqual(noLease, state) {
		t.Fatalf("claim without LeaseID changed state: got %+v want %+v", noLease, state)
	}

	state = mustApply(t, state, Command{
		Kind:             CommandClaim,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "claim-1",
		Identity:         baseIdentity(actID, ""),
		LeaseID:          "lease-1",
		Actor:            "alice",
	}, "claim")
	act := actStart(state)
	if act.Status != ActivationLeased {
		t.Fatalf("after claim: status got %q want %q", act.Status, ActivationLeased)
	}
	if act.ActiveLease == nil {
		t.Fatalf("after claim: ActiveLease is nil, want set")
	}
	if act.ActiveLease.ID != "lease-1" {
		t.Fatalf("after claim: ActiveLease.ID got %q want %q", act.ActiveLease.ID, "lease-1")
	}
	if act.ActiveLease.ActivationID != act.ID {
		t.Fatalf("after claim: lease activation got %q want %q", act.ActiveLease.ActivationID, act.ID)
	}
	if act.ActiveLease.Owner != "alice" {
		t.Fatalf("after claim: lease owner got %q want %q", act.ActiveLease.Owner, "alice")
	}
}

func testStartRunsAttempt(t *testing.T) {
	state := newInstance(t)
	state, _, _ = runToStart(t, state)
	act := actStart(state)
	if act.Status != ActivationRunning {
		t.Fatalf("after start: status got %q want %q", act.Status, ActivationRunning)
	}
	if len(act.AttemptIDs) != 1 {
		t.Fatalf("after start: attempts got %d want 1", len(act.AttemptIDs))
	}
	if act.AttemptIDs[0] != "att-1" {
		t.Fatalf("after start: attempt id got %q want %q", act.AttemptIDs[0], "att-1")
	}
	if len(state.Instance.Attempts) != 1 {
		t.Fatalf("after start: attempt records got %d want 1", len(state.Instance.Attempts))
	}
	if got := state.Instance.Attempts[0].Status; got != AttemptStarting {
		t.Fatalf("after start: attempt status got %q want %q", got, AttemptStarting)
	}
	if got := state.Instance.Attempts[0].Identity.AttemptID; got != "att-1" {
		t.Fatalf("after start: attempt identity got %q want %q", got, "att-1")
	}
	if state.Instance.Status != WorkflowActive {
		t.Fatalf("after start: workflow status got %q want %q", state.Instance.Status, WorkflowActive)
	}

	// Starting an activation that is not leased must error.
	fresh := newInstance(t)
	freshActID := actStart(fresh).ID
	_, err := applyCmd(fresh, Command{
		Kind:             CommandStart,
		ExpectedRevision: fresh.Instance.Revision,
		IdempotencyKey:   "start-nolease",
		Identity:         baseIdentity(freshActID, "att-x"),
	})
	if err == nil {
		t.Fatalf("start on non-leased activation: expected error, got nil")
	}
}

func testCompleteTerminalOutcome(t *testing.T) {
	state := newInstance(t)
	state, leaseID, _ := runToStart(t, state)
	actID := actStart(state).ID

	state = mustApply(t, state, Command{
		Kind:             CommandComplete,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "complete-1",
		Identity:         baseIdentity(actID, "att-1"),
		LeaseID:          leaseID,
		Outcome:          "done",
	}, "complete")
	act := actStart(state)
	if act.Status != ActivationSatisfied {
		t.Fatalf("after complete: activation status got %q want %q", act.Status, ActivationSatisfied)
	}
	if act.SelectedOutcome != "done" {
		t.Fatalf("after complete: selected outcome got %q want %q", act.SelectedOutcome, "done")
	}
	if act.ActiveLease != nil {
		t.Fatalf("after complete: lease still held, want released")
	}
	if state.Instance.Status != WorkflowCompleted {
		t.Fatalf("after complete: workflow status got %q want %q", state.Instance.Status, WorkflowCompleted)
	}
	if state.Instance.TerminalOutcome != "done" {
		t.Fatalf("after complete: terminal outcome got %q want %q", state.Instance.TerminalOutcome, "done")
	}
}

func testCompleteWrongLease(t *testing.T) {
	state := newInstance(t)
	state, _, _ = runToStart(t, state)
	actID := actStart(state).ID
	before := state

	_, err := applyCmd(state, Command{
		Kind:             CommandComplete,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "complete-wrong",
		Identity:         baseIdentity(actID, "att-1"),
		LeaseID:          "not-the-active-lease",
		Outcome:          "done",
	})
	if err == nil {
		t.Fatalf("complete with wrong lease: expected error, got nil")
	}
	if !reflect.DeepEqual(state, before) {
		t.Fatalf("complete with wrong lease changed state: got %+v want %+v", state, before)
	}
	if got := actStart(state).Status; got != ActivationRunning {
		t.Fatalf("complete with wrong lease: activation status got %q want %q", got, ActivationRunning)
	}
	if state.Instance.Status != WorkflowActive {
		t.Fatalf("complete with wrong lease: workflow status got %q want %q", state.Instance.Status, WorkflowActive)
	}
}

func testGateDecisions(t *testing.T) {
	cases := []struct {
		name    string
		status  GateStatus
		outcome OutcomeName
		want    ActivationStatus
	}{
		{"gate_passed_satisfies", GatePassed, "done", ActivationSatisfied},
		{"gate_rejected_rejects", GateRejected, "", ActivationRejected},
		{"gate_pending_awaits", GatePending, "", ActivationAwaitingGate},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := newInstance(t)
			state, _, _ = runToStart(t, state)
			actID := actStart(state).ID
			state = mustApply(t, state, Command{
				Kind:             CommandGate,
				ExpectedRevision: state.Instance.Revision,
				IdempotencyKey:   "gate-1",
				Identity:         baseIdentity(actID, "att-1"),
				Gate: &GateInstance{
					ID:           "gate-1",
					GateID:       "gate1",
					ActivationID: actID,
					Status:       tc.status,
					Outcome:      tc.outcome,
				},
			}, "gate")
			act := actStart(state)
			if act.Status != tc.want {
				t.Fatalf("gate %s: activation status got %q want %q", tc.status, act.Status, tc.want)
			}
			if tc.want == ActivationSatisfied && act.SelectedOutcome != "done" {
				t.Fatalf("gate %s: selected outcome got %q want %q", tc.status, act.SelectedOutcome, "done")
			}
			if len(state.Instance.Gates) != 1 {
				t.Fatalf("gate %s: stored gates got %d want 1", tc.status, len(state.Instance.Gates))
			}
		})
	}
}

func testSkip(t *testing.T) {
	state := newInstance(t)
	actID := actStart(state).ID

	// Skip without actor or reason must error and leave the state untouched.
	_, err := applyCmd(state, Command{
		Kind:             CommandSkip,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "skip-bad",
		Identity:         baseIdentity(actID, ""),
	})
	if err == nil {
		t.Fatalf("skip without actor/reason: expected error, got nil")
	}

	state = mustApply(t, state, Command{
		Kind:             CommandSkip,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "skip-1",
		Identity:         baseIdentity(actID, ""),
		Actor:            "alice",
		Reason:           "not needed",
	}, "skip")
	if got := actStart(state).Status; got != ActivationSkipped {
		t.Fatalf("after skip: status got %q want %q", got, ActivationSkipped)
	}

	// Skipping a non-pending activation must error (leased activation).
	leasedState := newInstance(t)
	leased, err := applyCmd(leasedState, Command{
		Kind:             CommandClaim,
		ExpectedRevision: leasedState.Instance.Revision,
		IdempotencyKey:   "claim-1",
		Identity:         baseIdentity(actStart(leasedState).ID, ""),
		LeaseID:          "lease-1",
	})
	if err != nil {
		t.Fatalf("claim for leased fixture: %v", err)
	}
	_, err = applyCmd(leased, Command{
		Kind:             CommandSkip,
		ExpectedRevision: leased.Instance.Revision,
		IdempotencyKey:   "skip-nonpending",
		Identity:         baseIdentity(actStart(leased).ID, ""),
		Actor:            "alice",
		Reason:           "why",
	})
	if err == nil {
		t.Fatalf("skip on non-pending activation: expected error, got nil")
	}
}

func testUnblock(t *testing.T) {
	setRetryPolicy(t, 1, RetryExhaustionBlock, "")
	state := newInstance(t)
	actID := actStart(state).ID

	// Block the activation through exhaustion.
	state, _, _ = runToStart(t, state)
	state = mustApply(t, state, Command{
		Kind:             CommandTerminate,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "term-1",
		Identity:         baseIdentity(actID, "att-1"),
		AttemptStatus:    AttemptFailed,
	}, "terminate (exhaust)")
	if got := actStart(state).Status; got != ActivationBlocked {
		t.Fatalf("setup: expected blocked activation, got %q", got)
	}

	state = mustApply(t, state, Command{
		Kind:             CommandUnblock,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "unblock-1",
		Identity:         baseIdentity(actID, ""),
		Actor:            "ops",
		Reason:           "operator override",
	}, "unblock")
	if got := actStart(state).Status; got != ActivationReady {
		t.Fatalf("after unblock: status got %q want %q", got, ActivationReady)
	}

	// Unblock without actor/reason must error.
	_, err := applyCmd(state, Command{
		Kind:             CommandUnblock,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "unblock-bad",
		Identity:         baseIdentity(actID, ""),
	})
	if err == nil {
		t.Fatalf("unblock without actor/reason: expected error, got nil")
	}
}

func testReroute(t *testing.T) {
	state := newInstance(t)
	actID := actStart(state).ID
	prior := *actStart(state)
	revBefore := state.Instance.Revision

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
	state = mustApply(t, state, Command{
		Kind:             CommandReroute,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "reroute-1",
		Identity:         ExecutionIdentity{WorkflowID: "wf1", NodeID: "start"},
		Payload:          payload,
	}, "reroute")

	if len(state.Instance.Activations) != 2 {
		t.Fatalf("after reroute: activation count got %d want 2", len(state.Instance.Activations))
	}
	created := state.Instance.Activations[1]
	if created.NodeID != "start" {
		t.Fatalf("rerouted activation node got %q want %q", created.NodeID, "start")
	}
	if created.Status != ActivationPending {
		t.Fatalf("rerouted activation status got %q want %q", created.Status, ActivationPending)
	}
	if created.Iteration != 2 {
		t.Fatalf("rerouted activation iteration got %d want 2", created.Iteration)
	}
	if created.Selection == nil || created.Selection.Role != "role-x" {
		t.Fatalf("rerouted activation selection got %+v want role-x", created.Selection)
	}

	// The prior activation must be untouched.
	kept := state.Instance.Activations[0]
	if kept.ID != actID || kept.NodeID != prior.NodeID || kept.Status != prior.Status ||
		kept.Iteration != prior.Iteration || len(kept.AttemptIDs) != len(prior.AttemptIDs) ||
		kept.SelectedOutcome != prior.SelectedOutcome {
		t.Fatalf("reroute mutated prior activation: before %+v after %+v", prior, kept)
	}
	if state.Instance.Revision <= revBefore {
		t.Fatalf("reroute did not advance revision: got %d want > %d",
			state.Instance.Revision, revBefore)
	}
}

func testHeartbeat(t *testing.T) {
	state := newInstance(t)
	actID := actStart(state).ID

	_, err := applyCmd(state, Command{
		Kind:             CommandHeartbeat,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "heartbeat-nolease",
		Identity:         baseIdentity(actID, ""),
	})
	if err == nil {
		t.Fatalf("heartbeat without a lease: expected error, got nil")
	}

	state = mustApply(t, state, Command{
		Kind:             CommandClaim,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "claim-1",
		Identity:         baseIdentity(actID, ""),
		LeaseID:          "lease-1",
		Actor:            "alice",
	}, "claim")
	state = mustApply(t, state, Command{
		Kind:             CommandHeartbeat,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "heartbeat-1",
		Identity:         baseIdentity(actID, "att-1"),
		LeaseID:          "lease-1",
	}, "heartbeat")
	if actStart(state).ActiveLease == nil {
		t.Fatalf("after heartbeat: active lease missing")
	}
	if actStart(state).ActiveLease.LastHeartbeatAt == nil {
		t.Fatalf("after heartbeat: LastHeartbeatAt not set")
	}
}

func testRevisionMismatch(t *testing.T) {
	state := newInstance(t)
	actID := actStart(state).ID

	events, err := Apply(state, Command{
		Kind:             CommandClaim,
		ExpectedRevision: state.Instance.Revision + 1, // stale
		IdempotencyKey:   "claim-stale",
		Identity:         baseIdentity(actID, ""),
		LeaseID:          "lease-1",
	})
	if err == nil {
		t.Fatalf("stale expected revision: expected error, got nil")
	}
	if events != nil {
		t.Fatalf("stale expected revision: Apply returned %d events, want none", len(events))
	}
}

func testDuplicateIdempotencyKey(t *testing.T) {
	state := newInstance(t)
	actID := actStart(state).ID

	state = mustApply(t, state, Command{
		Kind:             CommandClaim,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "claim-dup",
		Identity:         baseIdentity(actID, ""),
		LeaseID:          "lease-1",
		Actor:            "alice",
	}, "claim (first)")
	if _, ok := state.Idempotency["claim-dup"]; !ok {
		t.Fatalf("claimed key not registered in Idempotency map")
	}

	before := state
	events, err := Apply(state, Command{
		Kind:             CommandClaim,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "claim-dup", // same key, retried command
		Identity:         baseIdentity(actID, ""),
		LeaseID:          "lease-2",
	})
	if err == nil {
		t.Fatalf("duplicate idempotency key: expected error, got nil")
	}
	if events != nil {
		t.Fatalf("duplicate idempotency key: Apply returned %d events, want none", len(events))
	}
	if !reflect.DeepEqual(state, before) {
		t.Fatalf("duplicate idempotency key changed state: got %+v want %+v", state, before)
	}
}

func testDuplicateEventID(t *testing.T) {
	state := newInstance(t)
	actID := actStart(state).ID

	events, err := Apply(state, Command{
		Kind:             CommandClaim,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "claim-1",
		Identity:         baseIdentity(actID, ""),
		LeaseID:          "lease-1",
		Actor:            "alice",
	})
	if err != nil {
		t.Fatalf("apply claim: %v", err)
	}
	first, err := Reduce(state, events[0])
	if err != nil {
		t.Fatalf("reduce claim (first): %v", err)
	}
	second, err := Reduce(first, events[0])
	if err != nil {
		t.Fatalf("reduce claim (duplicate): %v", err)
	}
	if !reflect.DeepEqual(second, first) {
		t.Fatalf("reducing the same event twice changed the snapshot")
	}
	if len(second.AppliedEventIDs) != len(first.AppliedEventIDs) {
		t.Fatalf("duplicate reduction grew AppliedEventIDs: got %d want %d",
			len(second.AppliedEventIDs), len(first.AppliedEventIDs))
	}
	if second.Instance.Revision != first.Instance.Revision {
		t.Fatalf("duplicate reduction changed revision: got %d want %d",
			second.Instance.Revision, first.Instance.Revision)
	}
}

func testIdentityMismatch(t *testing.T) {
	state := newInstance(t)
	actID := actStart(state).ID

	events, err := Apply(state, Command{
		Kind:             CommandClaim,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "claim-1",
		Identity:         baseIdentity(actID, ""),
		LeaseID:          "lease-1",
		Actor:            "alice",
	})
	if err != nil {
		t.Fatalf("apply claim: %v", err)
	}
	foreign := events[0]
	foreign.Identity.WorkflowID = "other-wf"
	foreign.ID = eventIDOutside(foreign.ID) // distinct so it is not a no-op

	before := state
	got, err := Reduce(state, foreign)
	if err == nil {
		t.Fatalf("event with mismatched workflow id: expected error, got nil")
	}
	if !reflect.DeepEqual(got, before) {
		t.Fatalf("mismatched-identity event changed the snapshot")
	}
}

// eventIDOutside brands an event ID so it is distinct from an already applied one.
func eventIDOutside(id EventID) EventID { return EventID(string(id) + "-foreign") }

func testRetryRearms(t *testing.T) {
	setRetryPolicy(t, 3, RetryExhaustionBlock, "")
	state := newInstance(t)
	actID := actStart(state).ID
	state, _, _ = runToStart(t, state)
	before := *actStart(state)
	activationCount := len(state.Instance.Activations)

	state = mustApply(t, state, Command{
		Kind:             CommandTerminate,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "term-1",
		Identity:         baseIdentity(actID, "att-1"),
		AttemptStatus:    AttemptFailed,
	}, "terminate (failed, budget left)")
	act := actStart(state)

	if len(state.Instance.Activations) != activationCount {
		t.Fatalf("retry created a new activation: got %d want %d",
			len(state.Instance.Activations), activationCount)
	}
	if act.Status != ActivationReady {
		t.Fatalf("after failed terminate w/ budget: status got %q want %q (re-armed for retry)",
			act.Status, ActivationReady)
	}
	if act.Status == ActivationBlocked {
		t.Fatalf("after failed terminate w/ budget: activation blocked, want re-armed")
	}
	if act.Iteration != before.Iteration {
		t.Fatalf("retry consumed loop iteration: got %d want %d", act.Iteration, before.Iteration)
	}
	if act.ActiveLease != nil {
		t.Fatalf("after failed terminate: lease still held, want released")
	}
	if len(act.AttemptIDs) != 1 {
		t.Fatalf("after failed terminate: attempts got %d want 1", len(act.AttemptIDs))
	}
	if state.Instance.Status != WorkflowActive {
		t.Fatalf("after failed terminate w/ budget: workflow status got %q want %q",
			state.Instance.Status, WorkflowActive)
	}
}

func testExhaustionBlock(t *testing.T) {
	setRetryPolicy(t, 1, RetryExhaustionBlock, "")
	state := newInstance(t)
	actID := actStart(state).ID
	state, _, _ = runToStart(t, state)

	state = mustApply(t, state, Command{
		Kind:             CommandTerminate,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "term-1",
		Identity:         baseIdentity(actID, "att-1"),
		AttemptStatus:    AttemptFailed,
	}, "terminate (exhaust block)")
	act := actStart(state)
	if act.Status != ActivationBlocked {
		t.Fatalf("exhaustion=block: status got %q want %q", act.Status, ActivationBlocked)
	}
	if act.SelectedOutcome != "" {
		t.Fatalf("exhaustion=block: selected outcome got %q want empty", act.SelectedOutcome)
	}
	if state.Instance.Status != WorkflowActive {
		t.Fatalf("exhaustion=block: workflow status got %q want %q (not failed)",
			state.Instance.Status, WorkflowActive)
	}
}

func testExhaustionFail(t *testing.T) {
	setRetryPolicy(t, 1, RetryExhaustionFail, "")
	state := newInstance(t)
	actID := actStart(state).ID
	state, _, _ = runToStart(t, state)

	state = mustApply(t, state, Command{
		Kind:             CommandTerminate,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "term-1",
		Identity:         baseIdentity(actID, "att-1"),
		AttemptStatus:    AttemptFailed,
	}, "terminate (exhaust fail)")
	if state.Instance.Status != WorkflowFailed {
		t.Fatalf("exhaustion=fail: workflow status got %q want %q",
			state.Instance.Status, WorkflowFailed)
	}
	// The reducer resolves exhaustion=fail to AttemptFailed for the activation.
	if got := actStart(state).Status; got != ActivationAttemptFailed {
		t.Fatalf("exhaustion=fail: activation status got %q want %q",
			got, ActivationAttemptFailed)
	}
}

func testExhaustionOutcome(t *testing.T) {
	setRetryPolicy(t, 1, RetryExhaustionOutcome, "failed_out")
	state := newInstance(t)
	actID := actStart(state).ID
	state, _, _ = runToStart(t, state)

	state = mustApply(t, state, Command{
		Kind:             CommandTerminate,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "term-1",
		Identity:         baseIdentity(actID, "att-1"),
		AttemptStatus:    AttemptFailed,
	}, "terminate (exhaust outcome)")
	act := actStart(state)
	if act.SelectedOutcome != "failed_out" {
		t.Fatalf("exhaustion=outcome: selected outcome got %q want %q",
			act.SelectedOutcome, "failed_out")
	}
	if act.Status != ActivationAttemptFailed {
		t.Fatalf("exhaustion=outcome: activation status got %q want %q",
			act.Status, ActivationAttemptFailed)
	}
	if state.Instance.Status != WorkflowActive {
		t.Fatalf("exhaustion=outcome: workflow status got %q want %q",
			state.Instance.Status, WorkflowActive)
	}
}

func testCompletionBeforeTermination(t *testing.T) {
	state := newInstance(t)
	actID := actStart(state).ID
	state, leaseID, attemptID := runToStart(t, state)

	// Complete first: satisfaction and workflow completion.
	state = mustApply(t, state, Command{
		Kind:             CommandComplete,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "complete-1",
		Identity:         baseIdentity(actID, attemptID),
		LeaseID:          leaseID,
		Outcome:          "done",
	}, "complete (first)")
	if got := actStart(state).Status; got != ActivationSatisfied {
		t.Fatalf("after complete: activation status got %q want %q", got, ActivationSatisfied)
	}
	if state.Instance.Status != WorkflowCompleted {
		t.Fatalf("after complete: workflow status got %q want %q", state.Instance.Status, WorkflowCompleted)
	}

	// A later out-of-order termination must not regress satisfaction.
	state = mustApply(t, state, Command{
		Kind:             CommandTerminate,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "term-late",
		Identity:         baseIdentity(actID, attemptID),
		AttemptStatus:    AttemptFailed,
	}, "terminate (after completion)")
	if got := actStart(state).Status; got != ActivationSatisfied {
		t.Fatalf("termination after completion regressed activation status: got %q want %q",
			got, ActivationSatisfied)
	}
	if state.Instance.Status != WorkflowCompleted {
		t.Fatalf("termination after completion regressed workflow status: got %q want %q",
			state.Instance.Status, WorkflowCompleted)
	}
}

func testTerminationBeforeCompletion(t *testing.T) {
	// MaximumAttempts=1 + Block turns the failed termination into a blocked
	// activation, which an unblock then returns to ready. The contract under
	// test: a completion is still accepted after a termination has been
	// recorded for the same attempt.
	setRetryPolicy(t, 1, RetryExhaustionBlock, "")
	state := newInstance(t)
	actID := actStart(state).ID
	state, leaseID, attemptID := runToStart(t, state)

	state = mustApply(t, state, Command{
		Kind:             CommandTerminate,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "term-1",
		Identity:         baseIdentity(actID, attemptID),
		AttemptStatus:    AttemptFailed,
	}, "terminate (first)")
	if got := actStart(state).Status; got != ActivationBlocked {
		t.Fatalf("setup: expected blocked activation, got %q", got)
	}

	state = mustApply(t, state, Command{
		Kind:             CommandUnblock,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "unblock-1",
		Identity:         baseIdentity(actID, ""),
		Actor:            "ops",
		Reason:           "retry after terminate",
	}, "unblock")
	if got := actStart(state).Status; got != ActivationReady {
		t.Fatalf("setup: expected ready activation after unblock, got %q", got)
	}

	// Completion after a recorded termination must be accepted.
	state = mustApply(t, state, Command{
		Kind:             CommandComplete,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "complete-1",
		Identity:         baseIdentity(actID, attemptID),
		LeaseID:          leaseID,
		Outcome:          "done",
	}, "complete (after termination)")
	if got := actStart(state).Status; got != ActivationSatisfied {
		t.Fatalf("completion after termination: activation status got %q want %q",
			got, ActivationSatisfied)
	}
	if state.Instance.Status != WorkflowCompleted {
		t.Fatalf("completion after termination: workflow status got %q want %q",
			state.Instance.Status, WorkflowCompleted)
	}
}

func testGatePendingOnComplete(t *testing.T) {
	// applyCompleted has no gate-awareness: there is no awaiting_gate-on-
	// complete representation. The reducer's actual behavior is pinned here:
	// a pending gate parks the activation in ActivationAwaitingGate, and a
	// completion command issued from that state is rejected, because
	// applyCompleted only accepts ActivationRunning/ActivationAwaitingCompletion.
	state := newInstance(t)
	actID := actStart(state).ID
	state, leaseID, _ := runToStart(t, state)

	state = mustApply(t, state, Command{
		Kind:             CommandGate,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "gate-pending",
		Identity:         baseIdentity(actID, "att-1"),
		Gate: &GateInstance{
			ID:           "gate-1",
			GateID:       "gate1",
			ActivationID: actID,
			Status:       GatePending,
		},
	}, "gate (pending)")
	if got := actStart(state).Status; got != ActivationAwaitingGate {
		t.Fatalf("pending gate: activation status got %q want %q",
			got, ActivationAwaitingGate)
	}

	_, err := applyCmd(state, Command{
		Kind:             CommandComplete,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "complete-with-gate-pending",
		Identity:         baseIdentity(actID, "att-1"),
		LeaseID:          leaseID,
		Outcome:          "done",
	})
	if err == nil {
		t.Fatalf("complete from awaiting_gate: expected error, got nil " +
			"(reducer has no gate-on-complete representation)")
	}
}

// TestReduceDoesNotMutateInput guards the reducer purity contract: a successful
// Reduce must never alias or mutate the caller's snapshot, including the nested
// activations array the reducer writes into (via act.Status). Before the
// cloneSnapshot fix this leaked into the caller's array and the test fails.
func TestReduceDoesNotMutateInput(t *testing.T) {
	state := newInstance(t)
	actID := actStart(state).ID
	events, err := Apply(state, Command{
		Kind:             CommandClaim,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "claim-1",
		Identity:         baseIdentity(actID, ""),
		LeaseID:          "lease-1",
		Actor:            "alice",
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	want := cloneSnapshot(state) // independent deep copy taken before Reduce
	if !reflect.DeepEqual(state, want) {
		t.Fatalf("baseline mismatch: apply mutated its input")
	}
	if _, err := Reduce(state, events[0]); err != nil {
		t.Fatalf("reduce: %v", err)
	}
	if !reflect.DeepEqual(state, want) {
		t.Fatalf("Reduce mutated its input snapshot (activation should still be %q)", state.Instance.Activations[0].Status)
	}
	if state.Instance.Activations[0].Status != ActivationPending {
		t.Fatalf("Reduce leaked mutation: activation became %q", state.Instance.Activations[0].Status)
	}
}
