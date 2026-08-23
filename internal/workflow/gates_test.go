package workflow

// Tests for the CommandGate operation enum (Stage 12): reducer-side field
// validation, the operation-to-status mapping, and gate resolution of a
// parked awaiting_gate activation (branch, terminal, and reject paths).
// Reducer-level only: the same Apply/Reduce helpers as reducer_test.go.

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

// parkedGatedCompletion builds a snapshot in which a completion parked the
// single start activation in awaiting_gate because the resolver reports
// required gates, and returns the snapshot and the parked activation id. The
// resolver is installed for the lifetime of the test.
func parkedGatedCompletion(t *testing.T, defs []GateDefinition) (Snapshot, ActivationID) {
	t.Helper()
	SetCompletionGateResolver(func(TemplateID, TemplateVersion, NodeID) []GateDefinition {
		return defs
	})
	t.Cleanup(func() { SetCompletionGateResolver(nil) })
	state := newInstance(t)
	actID := actStart(state).ID
	state, leaseID, attemptID := runToStart(t, state)
	state = mustApply(t, state, Command{
		Kind:             CommandComplete,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "complete-1",
		Identity:         baseIdentity(actID, attemptID),
		LeaseID:          leaseID,
		Outcome:          "done",
		Evidence:         []Evidence{{ID: "ev-1", Kind: "artifact", Source: EvidenceMachine, StoredPath: "evidence/ev-1/file", ActivationID: actID}},
	}, "complete (gated park)")
	if got := actStart(state).Status; got != ActivationAwaitingGate {
		t.Fatalf("setup: activation status = %q, want awaiting_gate", got)
	}
	if state.Instance.Status != WorkflowActive {
		t.Fatalf("setup: workflow status = %q, want active", state.Instance.Status)
	}
	return state, actID
}

// humanGateCmd builds a structured CommandGate for a human operation.
func humanGateCmd(state Snapshot, idem string, actID ActivationID, gateID GateID, op GateOperation, gateStatus GateStatus, payload json.RawMessage) Command {
	return Command{
		Kind:             CommandGate,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   idem,
		Identity:         baseIdentity(actID, ""),
		Operation:        op,
		Outcome:          "done",
		Payload:          payload,
		Gate: &GateInstance{
			GateID:       gateID,
			ActivationID: actID,
			Status:       gateStatus,
			Outcome:      "done",
			Actor:        "reviewer",
			Reason:       "reviewed",
			EvidenceIDs:  []EvidenceID{"ev-1"},
		},
	}
}

func TestGateOperationValidation(t *testing.T) {
	// Each case mutates a valid satisfy command; the reducer must reject it
	// before any event is emitted and the state must be untouched.
	valid := func(actID ActivationID) Command {
		return humanGateCmd(Snapshot{}, "gate-v", actID, "review", GateOpSatisfy, GatePassed, nil)
	}
	tests := []struct {
		name    string
		mutate  func(*Command)
		wantErr string
	}{
		{"missing_actor", func(c *Command) { c.Gate.Actor = "" }, "requires an actor"},
		{"missing_reason", func(c *Command) { c.Gate.Reason = "" }, "requires a reason"},
		{"missing_evidence", func(c *Command) { c.Gate.EvidenceIDs = nil }, "requires at least one evidence"},
		{"unknown_operation", func(c *Command) { c.Operation = GateOperation("bogus") }, "unknown gate operation"},
		{"status_mismatch", func(c *Command) { c.Gate.Status = GatePending }, "inconsistent with operation"},
		{"subject_mismatch", func(c *Command) { c.Gate.ActivationID = "other-activation" }, "does not match command identity"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := newInstance(t)
			actID := actStart(state).ID
			state, _, _ = runToStart(t, state)
			cmd := valid(actID)
			cmd.ExpectedRevision = state.Instance.Revision
			tt.mutate(&cmd)
			before := state
			if _, err := applyCmd(state, cmd); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("%s: err = %v, want containing %q", tt.name, err, tt.wantErr)
			}
			if !reflect.DeepEqual(before, state) {
				t.Fatalf("%s: rejected gate command changed state", tt.name)
			}
		})
	}

	// An unknown operation emits no events at all (Apply returns none).
	state := newInstance(t)
	actID := actStart(state).ID
	cmd := valid(actID)
	cmd.ExpectedRevision = state.Instance.Revision
	cmd.Operation = GateOperation("bogus")
	events, err := Apply(state, cmd)
	if err == nil {
		t.Fatalf("unknown operation: Apply succeeded, want error")
	}
	if events != nil {
		t.Fatalf("unknown operation: Apply returned %d events, want none", len(events))
	}
}

func TestGateExternalResultValidation(t *testing.T) {
	ts := time.Now().UTC()
	observed := func() *time.Time { t := ts; return &t }
	external := func() Command {
		return Command{
			Kind:           CommandGate,
			IdempotencyKey: "gate-ext",
			Operation:      GateOpExternalResult,
			Outcome:        "passed",
			Gate: &GateInstance{
				GateID:       "review",
				ActivationID: "act-x",
				Status:       GatePassed,
				PollID:       "poll-1",
				Source:       "github",
				Subject:      &Subject{Type: "pull_request", Repository: "acme/app", Revision: "sha"},
				ResponseHash: "hash-1",
				EvidenceIDs:  []EvidenceID{"ev-1"},
				ObservedAt:   observed(),
			},
		}
	}
	tests := []struct {
		name    string
		mutate  func(*Command)
		wantErr string
	}{
		{"missing_poll_id", func(c *Command) { c.Gate.PollID = "" }, "requires a poll id"},
		{"missing_source", func(c *Command) { c.Gate.Source = "" }, "requires a source"},
		{"missing_observed_at", func(c *Command) { c.Gate.ObservedAt = nil }, "requires a non-zero observed_at"},
		{"zero_observed_at", func(c *Command) { c.Gate.ObservedAt = &time.Time{} }, "requires a non-zero observed_at"},
		{"missing_subject", func(c *Command) { c.Gate.Subject = nil }, "requires a subject"},
		{"missing_response_hash", func(c *Command) { c.Gate.ResponseHash = "" }, "requires a response hash"},
		{"missing_evidence", func(c *Command) { c.Gate.EvidenceIDs = nil }, "requires at least one evidence"},
		{"inconsistent_status", func(c *Command) { c.Gate.Status = GateRejected }, "inconsistent with operation"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := newInstance(t)
			actID := actStart(state).ID
			state, _, _ = runToStart(t, state)
			cmd := external()
			cmd.ExpectedRevision = state.Instance.Revision
			cmd.Identity = baseIdentity(actID, "")
			cmd.Gate.ActivationID = actID
			tt.mutate(&cmd)
			before := state
			if _, err := applyCmd(state, cmd); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("%s: err = %v, want containing %q", tt.name, err, tt.wantErr)
			}
			if !reflect.DeepEqual(before, state) {
				t.Fatalf("%s: rejected external_result changed state", tt.name)
			}
		})
	}
}

// TestGatePassResolvesParkedBranch is the heart of Stage 12: a satisfying
// gate decision on a parked awaiting_gate activation satisfies it and
// follows the declared branch exactly as a successful completion would.
func TestGatePassResolvesParkedBranch(t *testing.T) {
	state, actID := parkedGatedCompletion(t, []GateDefinition{{ID: "review", Type: GateHuman, Required: true}})
	branch, err := json.Marshal(Transition{ActivationID: actID, Outcome: "done", TargetNodeID: "next"})
	if err != nil {
		t.Fatalf("marshal transition: %v", err)
	}
	state = mustApply(t, state, humanGateCmd(state, "gate-1", actID, "review", GateOpSatisfy, GatePassed, branch), "gate satisfy")

	act := actStart(state)
	if act.Status != ActivationSatisfied {
		t.Fatalf("after gate satisfy: activation status got %q want satisfied", act.Status)
	}
	if act.SelectedOutcome != "done" {
		t.Fatalf("after gate satisfy: selected outcome got %q want done", act.SelectedOutcome)
	}
	if state.Instance.Status != WorkflowActive {
		t.Fatalf("after gate satisfy: workflow status got %q want active", state.Instance.Status)
	}
	if len(state.Instance.Activations) != 2 {
		t.Fatalf("activation count got %d want 2 (branch target)", len(state.Instance.Activations))
	}
	next := state.Instance.Activations[1]
	if next.NodeID != "next" || next.Status != ActivationPending || next.IncomingOutcome != "done" {
		t.Fatalf("branch target activation got %+v, want node next pending incoming done", next)
	}
	if len(state.Instance.Gates) != 1 || state.Instance.Gates[0].Status != GatePassed {
		t.Fatalf("stored gates got %+v, want the passed decision", state.Instance.Gates)
	}
}

// TestGatePassTerminalResolution: a satisfying gate with no declared branch
// completes the workflow with the selected outcome.
func TestGatePassTerminalResolution(t *testing.T) {
	state, actID := parkedGatedCompletion(t, []GateDefinition{{ID: "review", Type: GateHuman, Required: true}})
	terminal, err := json.Marshal(Transition{ActivationID: actID, Outcome: "done"})
	if err != nil {
		t.Fatalf("marshal transition: %v", err)
	}
	state = mustApply(t, state, humanGateCmd(state, "gate-1", actID, "review", GateOpSatisfy, GatePassed, terminal), "gate satisfy (terminal)")

	if got := actStart(state).Status; got != ActivationSatisfied {
		t.Fatalf("after gate satisfy: activation status got %q want satisfied", got)
	}
	if state.Instance.Status != WorkflowCompleted {
		t.Fatalf("after gate satisfy: workflow status got %q want completed", state.Instance.Status)
	}
	if state.Instance.TerminalOutcome != "done" {
		t.Fatalf("terminal outcome got %q want done", state.Instance.TerminalOutcome)
	}
	if len(state.Instance.Activations) != 1 {
		t.Fatalf("activation count got %d want 1 (terminal resolution)", len(state.Instance.Activations))
	}
}

// TestGatePassParksWhenOtherGateUnresolved: resolving one of two required
// gates leaves the activation parked until every required gate resolves.
func TestGatePassParksWhenOtherGateUnresolved(t *testing.T) {
	state, actID := parkedGatedCompletion(t, []GateDefinition{
		{ID: "review", Type: GateHuman, Required: true},
		{ID: "security", Type: GateExternal, Required: true},
	})
	state = mustApply(t, state, humanGateCmd(state, "gate-1", actID, "review", GateOpSatisfy, GatePassed, nil), "gate satisfy (first of two)")

	if got := actStart(state).Status; got != ActivationAwaitingGate {
		t.Fatalf("after first gate: activation status got %q want awaiting_gate", got)
	}
	if state.Instance.Status != WorkflowActive {
		t.Fatalf("after first gate: workflow status got %q want active", state.Instance.Status)
	}
	if len(state.Instance.Activations) != 1 {
		t.Fatalf("activation count got %d want 1 (no branch while gated)", len(state.Instance.Activations))
	}
	if len(state.Instance.Gates) != 1 {
		t.Fatalf("stored gates got %d want 1", len(state.Instance.Gates))
	}
}

// TestGateRejectDeclaredFailureBranch: a rejection with a declared failure
// branch rejects the activation and creates the failure-branch activation;
// the workflow stays active (never failed).
func TestGateRejectDeclaredFailureBranch(t *testing.T) {
	state, actID := parkedGatedCompletion(t, []GateDefinition{{ID: "review", Type: GateHuman, Required: true}})
	branch, err := json.Marshal(Transition{ActivationID: actID, Outcome: "needs_fix", TargetNodeID: "fixup"})
	if err != nil {
		t.Fatalf("marshal transition: %v", err)
	}
	cmd := humanGateCmd(state, "gate-1", actID, "review", GateOpReject, GateRejected, branch)
	cmd.Outcome = "needs_fix"
	state = mustApply(t, state, cmd, "gate reject")

	if got := actStart(state).Status; got != ActivationRejected {
		t.Fatalf("after gate reject: activation status got %q want rejected", got)
	}
	if state.Instance.Status != WorkflowActive {
		t.Fatalf("after gate reject: workflow status got %q want active (reducer never fails the workflow)", state.Instance.Status)
	}
	if len(state.Instance.Activations) != 2 {
		t.Fatalf("activation count got %d want 2 (failure branch)", len(state.Instance.Activations))
	}
	fix := state.Instance.Activations[1]
	if fix.NodeID != "fixup" || fix.Status != ActivationPending || fix.IncomingOutcome != "needs_fix" {
		t.Fatalf("failure-branch activation got %+v, want node fixup pending incoming needs_fix", fix)
	}
}

// TestGateRejectWithoutBranch: a rejection with no declared failure path
// rejects the activation and creates nothing.
func TestGateRejectWithoutBranch(t *testing.T) {
	state, actID := parkedGatedCompletion(t, []GateDefinition{{ID: "review", Type: GateHuman, Required: true}})
	state = mustApply(t, state, humanGateCmd(state, "gate-1", actID, "review", GateOpReject, GateRejected, nil), "gate reject")

	if got := actStart(state).Status; got != ActivationRejected {
		t.Fatalf("after gate reject: activation status got %q want rejected", got)
	}
	if state.Instance.Status != WorkflowActive {
		t.Fatalf("after gate reject: workflow status got %q want active", state.Instance.Status)
	}
	if len(state.Instance.Activations) != 1 {
		t.Fatalf("activation count got %d want 1 (no failure branch)", len(state.Instance.Activations))
	}
}

// TestGateIdempotentReissue: a duplicate idempotency key is rejected as a
// safe no-op, and a different operation for a different gate instance leaves
// the first issue's gate record untouched (append-only prior facts).
func TestGateIdempotentReissue(t *testing.T) {
	state, actID := parkedGatedCompletion(t, []GateDefinition{
		{ID: "review", Type: GateHuman, Required: true},
		{ID: "security", Type: GateExternal, Required: true},
	})
	first := humanGateCmd(state, "gate-a", actID, "review", GateOpSatisfy, GatePassed, nil)
	state = mustApply(t, state, first, "gate satisfy (first issue)")
	prior := state.Instance.Gates[0]

	// Re-issue under the same operator idempotency key: rejected, no change.
	before := state
	first.ExpectedRevision = state.Instance.Revision
	_, err := applyCmd(state, first)
	if err != errDuplicateIdempotency {
		t.Fatalf("duplicate idempotency key: err = %v, want errDuplicateIdempotency", err)
	}
	if !reflect.DeepEqual(before, state) {
		t.Fatalf("duplicate idempotency key changed state")
	}

	// A different operation for the OTHER gate does not mutate the prior
	// gate record; it appends its own.
	second := humanGateCmd(state, "gate-b", actID, "security", GateOpWaive, GateWaived, nil)
	state = mustApply(t, state, second, "gate waive (different gate)")
	if got := state.Instance.Gates[0]; !reflect.DeepEqual(got, prior) {
		t.Fatalf("prior gate record mutated: before %+v after %+v", prior, got)
	}
	if len(state.Instance.Gates) != 2 || state.Instance.Gates[1].GateID != "security" || state.Instance.Gates[1].Status != GateWaived {
		t.Fatalf("second gate record got %+v, want the waived decision", state.Instance.Gates[1:])
	}
	if got := actStart(state).Status; got != ActivationSatisfied {
		t.Fatalf("after both gates: activation status got %q want satisfied", got)
	}
}

// TestGateExternalResultPasses: an external_result mapped to passed resolves
// the parked activation (terminal, no branch payload).
func TestGateExternalResultPasses(t *testing.T) {
	state, actID := parkedGatedCompletion(t, []GateDefinition{{ID: "review", Type: GateExternal, Required: true}})
	ts := time.Now().UTC()
	cmd := Command{
		Kind:             CommandGate,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "gate-ext",
		Identity:         baseIdentity(actID, ""),
		Operation:        GateOpExternalResult,
		Outcome:          "passed",
		Gate: &GateInstance{
			GateID:       "review",
			ActivationID: actID,
			Status:       GatePassed,
			PollID:       "poll-1",
			Source:       "github",
			Subject:      &Subject{Type: "pull_request", Repository: "acme/app", Revision: "sha"},
			ResponseHash: "hash-1",
			EvidenceIDs:  []EvidenceID{"ev-ext"},
			ObservedAt:   &ts,
		},
	}
	state = mustApply(t, state, cmd, "external_result passed")

	if got := actStart(state).Status; got != ActivationSatisfied {
		t.Fatalf("external_result passed: activation status got %q want satisfied", got)
	}
	if state.Instance.Status != WorkflowCompleted {
		t.Fatalf("external_result passed: workflow status got %q want completed", state.Instance.Status)
	}
	if state.Instance.TerminalOutcome != "done" {
		t.Fatalf("external_result passed: terminal outcome got %q want done", state.Instance.TerminalOutcome)
	}
	if len(state.Instance.Gates) != 1 || state.Instance.Gates[0].Status != GatePassed {
		t.Fatalf("external_result passed: stored gates got %+v, want the passed decision", state.Instance.Gates)
	}
}

// TestGateExternalResultFails: an external_result mapped to failed rejects
// the parked activation; the workflow stays active.
func TestGateExternalResultFails(t *testing.T) {
	state, actID := parkedGatedCompletion(t, []GateDefinition{{ID: "review", Type: GateExternal, Required: true}})
	ts := time.Now().UTC()
	cmd := Command{
		Kind:             CommandGate,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "gate-ext",
		Identity:         baseIdentity(actID, ""),
		Operation:        GateOpExternalResult,
		Outcome:          "failed",
		Gate: &GateInstance{
			GateID:       "review",
			ActivationID: actID,
			Status:       GateFailed,
			PollID:       "poll-1",
			Source:       "github",
			Subject:      &Subject{Type: "pull_request", Repository: "acme/app", Revision: "sha"},
			ResponseHash: "hash-2",
			EvidenceIDs:  []EvidenceID{"ev-ext"},
			ObservedAt:   &ts,
		},
	}
	state = mustApply(t, state, cmd, "external_result failed")

	if got := actStart(state).Status; got != ActivationRejected {
		t.Fatalf("external_result failed: activation status got %q want rejected", got)
	}
	if state.Instance.Status != WorkflowActive {
		t.Fatalf("external_result failed: workflow status got %q want active", state.Instance.Status)
	}
	if len(state.Instance.Activations) != 1 {
		t.Fatalf("external_result failed: activation count got %d want 1 (no failure branch)", len(state.Instance.Activations))
	}
}
