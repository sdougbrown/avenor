package workflow

// Tests for the instance-cancel surface: the CommandCancel -> EventCanceled
// reducer path, the manager's WorkflowCancel command boundary, and the
// late-attempt-fact behavior of a canceled instance.

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// --- reducer-level tests (wired into the TestApply table) ---

func testCancelParksActivationsAndReleasesLeases(t *testing.T) {
	state, leaseID, _ := runToStart(t, newInstance(t))
	state = mustApply(t, state, Command{
		Kind:             CommandCancel,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "cancel-1",
		Identity:         ExecutionIdentity{WorkflowID: "wf1"},
		Actor:            "ops",
		Reason:           "superseded by another run",
	}, "cancel")
	if got := state.Instance.Status; got != WorkflowCanceled {
		t.Fatalf("instance status got %q want %q", got, WorkflowCanceled)
	}
	if state.Instance.CanceledBy != "ops" || state.Instance.CancelReason != "superseded by another run" {
		t.Fatalf("cancel attribution not recorded: %q / %q", state.Instance.CanceledBy, state.Instance.CancelReason)
	}
	act := actStart(state)
	if act.Status != ActivationCanceled {
		t.Fatalf("activation status got %q want %q", act.Status, ActivationCanceled)
	}
	if act.ActiveLease != nil {
		t.Fatalf("cancel did not release the live lease")
	}
	_ = leaseID
}

func testCancelRejectsAlreadyTerminal(t *testing.T) {
	state := newInstance(t)
	state = mustApply(t, state, Command{
		Kind:             CommandCancel,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "cancel-1",
		Identity:         ExecutionIdentity{WorkflowID: "wf1"},
		Actor:            "ops",
		Reason:           "why",
	}, "cancel")
	_, err := applyCmd(state, Command{
		Kind:             CommandCancel,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "cancel-2",
		Identity:         ExecutionIdentity{WorkflowID: "wf1"},
		Actor:            "ops",
		Reason:           "again",
	})
	if err == nil || !strings.Contains(err.Error(), "already") {
		t.Fatalf("re-cancel: expected already-terminal error, got %v", err)
	}
}

func testLateTerminationAfterCancelIsHarmless(t *testing.T) {
	state, _, attemptID := runToStart(t, newInstance(t))
	state = mustApply(t, state, Command{
		Kind:             CommandCancel,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "cancel-1",
		Identity:         ExecutionIdentity{WorkflowID: "wf1"},
		Actor:            "ops",
		Reason:           "why",
	}, "cancel")
	// The in-flight runtime's termination lands after the cancel: the attempt
	// fact is recorded, but the activation stays canceled and the instance
	// stays canceled (no lease work, no retry, no blocked regression).
	state = mustApply(t, state, Command{
		Kind:             CommandTerminate,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "term-late",
		Identity:         baseIdentity(actStart(state).ID, attemptID),
		AttemptStatus:    AttemptCanceled,
	}, "late terminate")
	if got := state.Instance.Status; got != WorkflowCanceled {
		t.Fatalf("late termination regressed instance status to %q", got)
	}
	if got := actStart(state).Status; got != ActivationCanceled {
		t.Fatalf("late termination regressed activation status to %q", got)
	}
	attempt := findAttempt(&state, actStart(state), attemptID)
	if attempt == nil || attempt.Status != AttemptCanceled {
		t.Fatalf("late termination did not record the attempt fact: %#v", attempt)
	}
}

func testClaimRejectedAfterCancel(t *testing.T) {
	state := newInstance(t)
	state = mustApply(t, state, Command{
		Kind:             CommandCancel,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "cancel-1",
		Identity:         ExecutionIdentity{WorkflowID: "wf1"},
		Actor:            "ops",
		Reason:           "why",
	}, "cancel")
	_, err := applyCmd(state, Command{
		Kind:             CommandClaim,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "claim-late",
		Identity:         baseIdentity(actStart(state).ID, ""),
		LeaseID:          "lease-late",
	})
	if err == nil || !strings.Contains(err.Error(), "canceled") {
		t.Fatalf("claim on canceled instance: expected canceled error, got %v", err)
	}
}

// testLateLeaseExpiredOnCanceledRejected pins the uniform rejection of a late
// lease_expired on a canceled instance: the guard fires before any
// lease-specific validation, so the error is the canceled-instance one no
// matter what lease state the cancel left behind.
func testLateLeaseExpiredOnCanceledRejected(t *testing.T) {
	state, _, attemptID := runToStart(t, newInstance(t))
	state = mustApply(t, state, Command{
		Kind:             CommandCancel,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "cancel-1",
		Identity:         ExecutionIdentity{WorkflowID: "wf1"},
		Actor:            "ops",
		Reason:           "why",
	}, "cancel")
	_, err := Reduce(state, Event{
		Kind:     EventLeaseExpired,
		Identity: baseIdentity(actStart(state).ID, attemptID),
		LeaseID:  "lease-1",
	})
	if err == nil || !strings.Contains(err.Error(), "canceled") {
		t.Fatalf("late lease_expired on canceled instance: expected canceled error, got %v", err)
	}
	if got := state.Instance.Status; got != WorkflowCanceled {
		t.Fatalf("late lease_expired regressed instance status to %q", got)
	}
}

// testLateGateDecisionOnCanceledDropped applies a resolving gate decision
// through the full Reduce path (mustApply → applyEvent) so BOTH guards are
// pinned: the canceledMutatingKinds rejection in applyEvent AND the
// applyGate-level late-decision drop. A regression that removes EventGate
// from canceledMutatingKinds must fail here, not only under applyGate.
func testLateGateDecisionOnCanceledDropped(t *testing.T) {
	state, _, attemptID := runToStart(t, newInstance(t))
	state = mustApply(t, state, Command{
		Kind:             CommandCancel,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "cancel-1",
		Identity:         ExecutionIdentity{WorkflowID: "wf1"},
		Actor:            "ops",
		Reason:           "why",
	}, "cancel")
	gateEvent := Event{
		Kind:     EventGate,
		Identity: baseIdentity(actStart(state).ID, attemptID),
		Gate: &GateInstance{
			ID:           "gate-1",
			GateID:       "gate1",
			ActivationID: actStart(state).ID,
			Status:       GatePassed,
			Outcome:      "done",
		},
	}
	next, err := Reduce(state, gateEvent)
	if err == nil {
		t.Fatalf("late gate decision on canceled instance: want EventGate rejected by the canceledMutatingKinds guard, got nil error")
	}
	if !strings.Contains(err.Error(), "canceled workflow instance") {
		t.Fatalf("late gate decision error = %v, want the canceled-instance rejection", err)
	}
	if !reflect.DeepEqual(next, state) {
		t.Fatalf("rejected gate decision mutated the snapshot: got %+v want %+v", next, state)
	}
}

func testCancelRequiresActorAndReason(t *testing.T) {
	state := newInstance(t)
	_, err := applyCmd(state, Command{
		Kind:             CommandCancel,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "cancel-bad",
		Identity:         ExecutionIdentity{WorkflowID: "wf1"},
	})
	if err == nil {
		t.Fatalf("cancel without actor/reason: expected error, got nil")
	}
}

// --- manager-level tests ---

// newCancelFixture builds an ephemeral store + manager with a stored manual
// template and one active instance.
func newCancelFixture(t *testing.T) (*Manager, WorkflowID) {
	t.Helper()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	if _, err := m.WorkflowCreate([]byte(managerTemplateJSON)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	payload, err := json.Marshal(map[string]string{
		"template_id": "mgr-test", "template_version": "1",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out, err := m.WorkflowInstantiate(payload)
	if err != nil {
		t.Fatalf("WorkflowInstantiate: %v", err)
	}
	wf, ok := out.(map[string]any)["workflow_id"].(string)
	if !ok {
		t.Fatalf("instantiate result missing workflow_id: %#v", out)
	}
	return m, WorkflowID(wf)
}

func cancelPayload(actor, reason string) json.RawMessage {
	data, err := json.Marshal(map[string]string{"actor": actor, "reason": reason})
	if err != nil {
		panic(err)
	}
	return data
}

func TestManagerWorkflowCancel(t *testing.T) {
	m, wf := newCancelFixture(t)

	resultAny, err := m.WorkflowCancel(string(wf), cancelPayload("ops", "operator abandon"))
	result, _ := resultAny.(map[string]any)
	if err != nil {
		t.Fatalf("WorkflowCancel: %v", err)
	}
	if got := result["status"]; got != string(WorkflowCanceled) {
		t.Fatalf("status got %v want canceled", got)
	}
	statusAny, err := m.WorkflowStatus(string(wf))
	status, _ := statusAny.(map[string]any)
	if err != nil {
		t.Fatalf("WorkflowStatus: %v", err)
	}
	if got := status["status"]; got != string(WorkflowCanceled) {
		t.Fatalf("post-cancel status got %v want canceled", got)
	}
	if got := status["cancel_reason"]; got != "operator abandon" {
		t.Fatalf("post-cancel cancel_reason got %v", got)
	}
	if got := status["canceled_by"]; got != "ops" {
		t.Fatalf("post-cancel canceled_by got %v", got)
	}
}

func TestManagerWorkflowCancelIdempotent(t *testing.T) {
	m, wf := newCancelFixture(t)
	if _, err := m.WorkflowCancel(string(wf), cancelPayload("ops", "first")); err != nil {
		t.Fatalf("first cancel: %v", err)
	}
	resultAny, err := m.WorkflowCancel(string(wf), cancelPayload("ops", "second"))
	result, _ := resultAny.(map[string]any)
	if err != nil {
		t.Fatalf("re-cancel: %v", err)
	}
	if got, ok := result["idempotent"].(bool); !ok || !got {
		t.Fatalf("re-cancel result not idempotent: %#v", result)
	}
}

// TestManagerWorkflowCancelDuplicatesUnderConcurrency pins the in-loop
// duplicate branch: a concurrent cancel that commits between this cancel's
// read and its apply turns the duplicate-key apply error into the same
// idempotent response the sequential pre-check returns, never a raw error.
func TestManagerWorkflowCancelDuplicatesUnderConcurrency(t *testing.T) {
	m, wf := newCancelFixture(t)
	// The first cancel's pre-commit hook issues a second WorkflowCancel for the
	// same workflow, which commits first; the outer apply then hits the
	// duplicate idempotency key.
	m.testHooks.cancelPreCommit = func() {
		if _, err := m.WorkflowCancel(string(wf), cancelPayload("ops", "concurrent duplicate")); err != nil {
			t.Errorf("concurrent cancel: %v", err)
		}
	}
	resultAny, err := m.WorkflowCancel(string(wf), cancelPayload("ops", "lost race"))
	if err != nil {
		t.Fatalf("WorkflowCancel: %v", err)
	}
	result, _ := resultAny.(map[string]any)
	if got, ok := result["idempotent"].(bool); !ok || !got {
		t.Fatalf("duplicate cancel result not idempotent: %#v", result)
	}
	if got := result["status"]; got != string(WorkflowCanceled) {
		t.Fatalf("duplicate cancel status got %v want canceled", got)
	}
}

func TestManagerWorkflowCancelRejectsTerminal(t *testing.T) {
	m, wf := newCancelFixture(t)
	// Drive claim → start → complete through the control-plane command path so
	// the instance reaches its terminal completed status, then verify a cancel
	// against it is a clear error.
	claimOut, err := m.WorkflowCommand(string(wf), []byte(`{"op":"claim","node_id":"start","actor":"cli"}`))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	claim, _ := claimOut.(map[string]any)
	leaseID, _ := claim["lease_id"].(string)
	ownerToken, _ := claim["owner_token"].(string)
	startOut, err := m.WorkflowCommand(string(wf), []byte(`{"op":"start","node_id":"start","lease_id":"`+leaseID+`","owner_token":"`+ownerToken+`"}`))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	start, _ := startOut.(map[string]any)
	attemptID, _ := start["attempt_id"].(string)
	if _, err := m.WorkflowCommand(string(wf), []byte(`{"op":"complete","node_id":"start","activation_id":"","attempt_id":"`+attemptID+`","lease_id":"`+leaseID+`","owner_token":"`+ownerToken+`","outcome":"done"}`)); err != nil {
		t.Fatalf("complete: %v", err)
	}
	_, err = m.WorkflowCancel(string(wf), cancelPayload("ops", "late"))
	if err == nil || !strings.Contains(err.Error(), "already completed") {
		t.Fatalf("cancel on completed instance: expected clear error, got %v", err)
	}
}

func TestManagerWorkflowCancelWaitTerminal(t *testing.T) {
	m, wf := newCancelFixture(t)
	if _, err := m.WorkflowCancel(string(wf), cancelPayload("ops", "why")); err != nil {
		t.Fatalf("WorkflowCancel: %v", err)
	}
	resultAny, err := m.WorkflowWait(string(wf), 0)
	result, _ := resultAny.(map[string]any)
	if err != nil {
		t.Fatalf("WorkflowWait: %v", err)
	}
	terminal, _ := result["terminal"].(bool)
	if !terminal {
		t.Fatalf("wait on canceled instance reported terminal=%v: %#v", terminal, result)
	}
}

// TestCancelParksEveryActivationAndReleasesEveryLease pins the multi-activation
// case: applyCanceled's loop must park BOTH activations in ActivationCanceled
// and release BOTH live leases, not just the first one it visits.
func TestCancelParksEveryActivationAndReleasesEveryLease(t *testing.T) {
	payload, err := json.Marshal(InstanceRecord{
		TemplateID:       "t2",
		TemplateVersion:  "1",
		TerminalOutcomes: []OutcomeName{"done", "failed_out"},
		EntryNodes:       []NodeID{"a", "b"},
	})
	if err != nil {
		t.Fatalf("marshal instance record: %v", err)
	}
	state := mustApply(t, Snapshot{}, Command{
		Kind:             CommandInstantiate,
		ExpectedRevision: 0,
		IdempotencyKey:   "inst",
		Identity:         ExecutionIdentity{WorkflowID: "wf1"},
		Payload:          payload,
	}, "instantiate")

	claim := func(node string, lease string) {
		var actID ActivationID
		for _, act := range state.Instance.Activations {
			if act.NodeID == NodeID(node) {
				actID = act.ID
				break
			}
		}
		if actID == "" {
			t.Fatalf("activation for %s not found", node)
		}
		identity := ExecutionIdentity{WorkflowID: "wf1", NodeID: NodeID(node), ActivationID: actID}
		state = mustApply(t, state, Command{
			Kind:             CommandClaim,
			ExpectedRevision: state.Instance.Revision,
			IdempotencyKey:   "claim-" + node,
			Identity:         identity,
			LeaseID:          LeaseID(lease),
			Actor:            "alice",
		}, "claim-"+node)
		startIdentity := identity
		startIdentity.AttemptID = AttemptID("att-" + node)
		state = mustApply(t, state, Command{
			Kind:             CommandStart,
			ExpectedRevision: state.Instance.Revision,
			IdempotencyKey:   "start-" + node,
			Identity:         startIdentity,
			LeaseID:          LeaseID(lease),
		}, "start-"+node)
	}
	claim("a", "lease-a")
	claim("b", "lease-b")

	state = mustApply(t, state, Command{
		Kind:             CommandCancel,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "cancel-1",
		Identity:         ExecutionIdentity{WorkflowID: "wf1"},
		Actor:            "ops",
		Reason:           "multi-activation pin",
	}, "cancel")

	if got := state.Instance.Status; got != WorkflowCanceled {
		t.Fatalf("instance status got %q want %q", got, WorkflowCanceled)
	}
	if len(state.Instance.Activations) != 2 {
		t.Fatalf("activations = %d, want 2", len(state.Instance.Activations))
	}
	for _, act := range state.Instance.Activations {
		if act.Status != ActivationCanceled {
			t.Fatalf("activation %s status = %q, want canceled", act.NodeID, act.Status)
		}
		if act.ActiveLease != nil {
			t.Fatalf("activation %s kept its lease after cancel", act.NodeID)
		}
	}
}

// TestManagerWorkflowCancelRetriesRevisionMismatch pins the optimistic
// concurrency path: a concurrent command that moves the revision while the
// cancel sits in its read window must not fail the cancel — the bounded retry
// re-reads and applies.
func TestManagerWorkflowCancelRetriesRevisionMismatch(t *testing.T) {
	m, wf := newCancelFixture(t)
	// While the cancel sits between its read and its commit, a claim commits
	// and moves the revision.
	m.testHooks.cancelPreCommit = func() {
		if _, err := m.WorkflowCommand(string(wf), []byte(`{"op":"claim","node_id":"start","actor":"cli"}`)); err != nil {
			t.Errorf("concurrent claim: %v", err)
		}
	}
	resultAny, err := m.WorkflowCancel(string(wf), cancelPayload("ops", "raced cancel"))
	if err != nil {
		t.Fatalf("WorkflowCancel: %v", err)
	}
	result, _ := resultAny.(map[string]any)
	if got := result["status"]; got != string(WorkflowCanceled) {
		t.Fatalf("status got %v want canceled", got)
	}
	if got, ok := result["idempotent"].(bool); ok && got {
		t.Fatalf("first cancel must not report idempotent: %#v", result)
	}
}

// TestManagerWorkflowCancelRejectsFailed pins the failed branch of the
// terminal-instance rejection: a cancel against an instance whose retry
// policy exhausted to fail is a clear error, not a silent no-op.
func TestManagerWorkflowCancelRejectsFailed(t *testing.T) {
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	m.RegisterExecutor(ActionRun, &fakeExecutor{})
	failTemplate := []byte(`{
  "schema_version": 1,
  "template_id": "cancel-fail",
  "template_version": "1",
  "entry_nodes": ["start"],
  "nodes": [{"id": "start", "action": {"type": "run", "prompt": "do the thing"}, "retry_policy": {"max_attempts": 1, "exhaustion": "fail"}}],
  "terminal_outcomes": ["done"]
}`)
	if _, err := m.WorkflowCreate(failTemplate); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	instOut, err := m.WorkflowInstantiate([]byte(`{"template_id":"cancel-fail","template_version":"1"}`))
	if err != nil {
		t.Fatalf("WorkflowInstantiate: %v", err)
	}
	inst, _ := instOut.(map[string]any)
	wf, _ := inst["workflow_id"].(string)
	if wf == "" {
		t.Fatalf("instantiate result missing workflow_id: %#v", instOut)
	}

	claimOut, err := m.WorkflowCommand(wf, []byte(`{"op":"claim","node_id":"start","actor":"cli"}`))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	claim, _ := claimOut.(map[string]any)
	leaseID, _ := claim["lease_id"].(string)
	ownerToken, _ := claim["owner_token"].(string)
	startOut, err := m.WorkflowCommand(wf, []byte(`{"op":"start","node_id":"start","lease_id":"`+leaseID+`","owner_token":"`+ownerToken+`"}`))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	start, _ := startOut.(map[string]any)
	attemptID, _ := start["attempt_id"].(string)
	// The single attempt fails; max_attempts 1 with fail exhaustion makes the
	// instance terminal failed.
	if err := m.RecordAttemptTerminated(WorkflowID(wf), "start", "", AttemptID(attemptID), LeaseID(leaseID), AttemptFailed); err != nil {
		t.Fatalf("RecordAttemptTerminated: %v", err)
	}
	if _, err := m.WorkflowStatus(wf); err != nil {
		t.Fatalf("WorkflowStatus: %v", err)
	}
	snapAny, err := m.WorkflowStatus(wf)
	snap, _ := snapAny.(map[string]any)
	if got := snap["status"]; got != string(WorkflowFailed) {
		t.Fatalf("instance after fail exhaustion = %v, want failed", got)
	}
	_, err = m.WorkflowCancel(wf, cancelPayload("ops", "late"))
	if err == nil || !strings.Contains(err.Error(), "already failed") {
		t.Fatalf("cancel on failed instance: expected clear error, got %v", err)
	}
}

// TestManagerWorkflowCancelBoundaryValidation pins the manager-boundary
// cancel contract: malformed JSON, a missing actor, and a missing reason
// each fail with their own error before any snapshot is loaded or command
// applied.
func TestManagerWorkflowCancelBoundaryValidation(t *testing.T) {
	m, wf := newCancelFixture(t)

	if _, err := m.WorkflowCancel(string(wf), []byte(`{not json`)); err == nil || !strings.Contains(err.Error(), "cancel payload") {
		t.Fatalf("malformed cancel payload: err = %v, want a cancel payload error", err)
	}
	if _, err := m.WorkflowCancel(string(wf), []byte(`{"reason":"no actor"}`)); err == nil || !strings.Contains(err.Error(), "requires actor") {
		t.Fatalf("missing actor: err = %v, want an actor error", err)
	}
	if _, err := m.WorkflowCancel(string(wf), []byte(`{"actor":"ops"}`)); err == nil || !strings.Contains(err.Error(), "requires reason") {
		t.Fatalf("missing reason: err = %v, want a reason error", err)
	}
	// The instance must be untouched by the rejected requests.
	snapAny, err := m.WorkflowStatus(string(wf))
	snap, _ := snapAny.(map[string]any)
	if err != nil {
		t.Fatalf("WorkflowStatus: %v", err)
	}
	if got := snap["status"]; got != string(WorkflowActive) {
		t.Fatalf("instance status after rejected cancels = %v, want active", got)
	}
}
