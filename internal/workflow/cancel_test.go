package workflow

// Tests for the instance-cancel surface: the CommandCancel -> EventCanceled
// reducer path, the manager's WorkflowCancel command boundary, and the
// late-attempt-fact behavior of a canceled instance.

import (
	"encoding/json"
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
