package stable

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/sdougbrown/avenor/internal/admission"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/spawnselection"
	"github.com/sdougbrown/avenor/internal/workflow"
	"github.com/sdougbrown/avenor/internal/workflowcontroller"
)

// DispatchOutcomeKind classifies the result of one controller dispatch
// attempt.
type DispatchOutcomeKind string

const (
	// Dispatched means the node was claimed and its runtime started.
	Dispatched DispatchOutcomeKind = "dispatched"
	// DispatchCapacityBlocked means admission was unavailable; no workflow
	// event was appended and the node remains a candidate.
	DispatchCapacityBlocked DispatchOutcomeKind = "capacity_blocked"
	// DispatchStale means the candidate was no longer dispatchable (stale
	// revision, competing claim, or policy mismatch).
	DispatchStale DispatchOutcomeKind = "stale"
	// DispatchKeyHeld means another live attempt holds the node's
	// concurrency key.
	DispatchKeyHeld DispatchOutcomeKind = "key_held"
	// DispatchNotLeader means the caller's controller leader lease did not
	// validate.
	DispatchNotLeader DispatchOutcomeKind = "not_leader"
	// DispatchCanceled means the dispatch was canceled through its context.
	// Before the attempt intent is committed nothing is reserved and no event
	// is appended; after the commit the granted lease stays authoritative and
	// the attempt is finalized as canceled (retry semantics apply).
	DispatchCanceled DispatchOutcomeKind = "canceled"
	// DispatchStartFailed means the executor's runtime start failed after the
	// attempt intent was durably recorded; kernel attempt/retry semantics
	// apply.
	DispatchStartFailed DispatchOutcomeKind = "start_failed"
)

// DispatchRequest asks the supervisor to dispatch one ready workflow node on
// behalf of a controller leader.
type DispatchRequest struct {
	WorkflowID       string
	NodeID           string
	ActivationID     string
	ExpectedRevision int64
	ControllerID     string
	LeaderLeaseID    string
	OwnerEpoch       int64
	Selection        *workflow.ExecutionSelection
}

// DispatchOutcome reports the result of one dispatch attempt. Only
// Dispatched carries runtime identity.
type DispatchOutcome struct {
	Kind         DispatchOutcomeKind
	Source       string // capacity source: "local" or "tree"
	WorkflowID   string
	NodeID       string
	ActivationID string
	AttemptID    string
	RuntimeID    string
	Detail       string
}

// dispatchWorkflowNode dispatches one ready workflow node through stable
// admission and the manager's dispatch boundary. It is a Go-internal method
// for the controller runner: it is never registered on the control socket,
// CLI, MCP, or client.
//
// Order: selection validation → cancellation check → admission reservation
// (no locks held) → cancellation check → controller lease validation → root
// dispatch lock + claim/attempt-intent commit → cancellation check → executor
// start with the reservation handed off → finalization. Cancellation is
// honored only between these boundaries; the executor start itself runs
// uninterruptibly.
func (s *Supervisor) dispatchWorkflowNode(ctx context.Context, req DispatchRequest) (DispatchOutcome, error) {
	out := DispatchOutcome{WorkflowID: req.WorkflowID, NodeID: req.NodeID, ActivationID: req.ActivationID}
	mgr, controllerStore, err := s.workflowBarrierResult()
	if err != nil {
		return out, err
	}

	// Validate the declared selection against the same spawn-selection and
	// thinking rules the ordinary spawn path enforces.
	if req.Selection != nil {
		if err := spawnselection.Validate(spawnselection.Input{
			Agent:   req.Selection.Agent,
			Model:   req.Selection.Model,
			Backend: req.Selection.Backend,
		}, false); err != nil {
			return out, err
		}
		if err := runtime.ValidateThinkingForBackend(req.Selection.Backend, req.Selection.Thinking); err != nil {
			return out, err
		}
	}

	// A cancel before the reservation leaves nothing reserved and no event
	// appended.
	if err := ctx.Err(); err != nil {
		out.Kind = DispatchCanceled
		return out, nil
	}

	// Reserve admission while holding no workflow-store, controller-store, or
	// dispatch lock. A capacity denial is a no-event, no-revision-change
	// result; the node stays a candidate.
	res, err := s.reserveAdmission()
	if err != nil {
		var ce *admission.CapacityError
		if errors.As(err, &ce) {
			out.Kind, out.Source = DispatchCapacityBlocked, ce.Source
			return out, nil
		}
		return out, err
	}
	// Every path from here to the executor start releases the reservation.
	// On a successful start the ownership transferred to the runtime inside
	// spawnReserved (consumeLocked marks the reservation released), so the
	// deferred Release below is a no-op.
	defer res.Release()

	// A cancel after the reservation but before any workflow state: the
	// reservation is released unused (deferred) and nothing is appended.
	if err := ctx.Err(); err != nil {
		out.Kind = DispatchCanceled
		return out, nil
	}

	// Under the validated leader lease, claim the node and record the attempt
	// intent. Stale candidates and held keys release the reservation after
	// the locks are released.
	var begin workflow.BeginDispatchResult
	var beginErr error
	withLeaderErr := controllerStore.WithLeader(req.ControllerID, req.LeaderLeaseID, req.OwnerEpoch, func() error {
		begin, beginErr = mgr.BeginDispatch(workflow.BeginDispatchRequest{
			WorkflowID:       workflow.WorkflowID(req.WorkflowID),
			NodeID:           workflow.NodeID(req.NodeID),
			ActivationID:     workflow.ActivationID(req.ActivationID),
			ExpectedRevision: req.ExpectedRevision,
			ControllerID:     req.ControllerID,
			LeaderLeaseID:    req.LeaderLeaseID,
			Selection:        req.Selection,
		})
		return nil
	})
	if withLeaderErr != nil {
		if errors.Is(withLeaderErr, workflowcontroller.ErrNotLeader) {
			out.Kind = DispatchNotLeader
			return out, nil
		}
		return out, withLeaderErr
	}
	if beginErr != nil {
		switch {
		case errors.Is(beginErr, workflow.ErrStaleCandidate):
			out.Kind = DispatchStale
			return out, nil
		case errors.Is(beginErr, workflow.ErrConcurrencyKeyHeld):
			out.Kind = DispatchKeyHeld
			return out, nil
		default:
			return out, beginErr
		}
	}
	out.AttemptID = string(begin.AttemptID)

	// A cancel after the attempt intent was committed: the workflow-node lease
	// is already granted and stays authoritative, so the attempt is finalized
	// as a terminal pre-start cancellation (kernel retry semantics apply), the
	// reservation is released (deferred), and the cancellation is reported.
	if err := ctx.Err(); err != nil {
		kind, label := workflowMarkerForKind(begin.Action.Kind)
		if ferr := mgr.FinalizeDispatch(workflow.FinalizeDispatchRequest{
			WorkflowID:    workflow.WorkflowID(req.WorkflowID),
			NodeID:        workflow.NodeID(req.NodeID),
			ActivationID:  workflow.ActivationID(req.ActivationID),
			AttemptID:     begin.AttemptID,
			LeaseID:       begin.LeaseID,
			FailureStatus: workflow.AttemptCanceled,
			MarkerKind:    kind,
			MarkerLabel:   label,
		}); ferr != nil {
			fmt.Fprintf(os.Stderr, "avenor stable: workflow %s node %s attempt %s: record canceled dispatch: %v\n",
				req.WorkflowID, req.NodeID, begin.AttemptID, ferr)
		}
		out.Kind = DispatchCanceled
		return out, nil
	}

	// Start the declared executor with the reservation handed off, so the
	// node's first runtime consumes it instead of reserving again.
	ec := workflow.ExecutorContext{
		WorkflowID:   workflow.WorkflowID(req.WorkflowID),
		NodeID:       workflow.NodeID(req.NodeID),
		ActivationID: workflow.ActivationID(req.ActivationID),
		AttemptID:    begin.AttemptID,
		LeaseID:      begin.LeaseID,
		OwnerToken:   begin.OwnerToken,
		Action:       begin.Action,
		Selection:    begin.Selection,
		Admission:    res,
	}
	startErr := mgr.DispatchExecutor(begin.Action.Kind, ec)
	kind, label := workflowMarkerForKind(begin.Action.Kind)
	if startErr != nil {
		// Pre-start failure: record the terminal attempt fact (idempotent
		// with the executor's own termination record) and release the
		// reservation (deferred). Provider/session startup failures keep
		// kernel attempt/retry semantics and are not capacity failures.
		if ferr := mgr.FinalizeDispatch(workflow.FinalizeDispatchRequest{
			WorkflowID:    ec.WorkflowID,
			NodeID:        ec.NodeID,
			ActivationID:  ec.ActivationID,
			AttemptID:     begin.AttemptID,
			LeaseID:       begin.LeaseID,
			FailureStatus: workflow.AttemptFailed,
			MarkerKind:    kind,
			MarkerLabel:   label,
		}); ferr != nil {
			fmt.Fprintf(os.Stderr, "avenor stable: workflow %s node %s attempt %s: record start failure: %v\n",
				req.WorkflowID, req.NodeID, begin.AttemptID, ferr)
		}
		out.Kind = DispatchStartFailed
		out.Detail = startErr.Error()
		return out, nil
	}

	// Record the actual runtime identity of the started attempt.
	rtID, sessionID := s.runtimeForAttempt(string(begin.AttemptID))
	if ferr := mgr.FinalizeDispatch(workflow.FinalizeDispatchRequest{
		WorkflowID:   ec.WorkflowID,
		NodeID:       ec.NodeID,
		ActivationID: ec.ActivationID,
		AttemptID:    begin.AttemptID,
		LeaseID:      begin.LeaseID,
		RuntimeID:    rtID,
		RunID:        s.runID,
		SessionID:    sessionID,
	}); ferr != nil {
		fmt.Fprintf(os.Stderr, "avenor stable: workflow %s node %s attempt %s: record runtime identity: %v\n",
			req.WorkflowID, req.NodeID, begin.AttemptID, ferr)
	}
	out.Kind = Dispatched
	out.RuntimeID = rtID
	return out, nil
}

// runtimeForAttempt finds the child runtime registered for a workflow
// attempt, if it is live.
func (s *Supervisor) runtimeForAttempt(attemptID string) (runtimeID, sessionID string) {
	s.controlMu.Lock()
	child := (*childRuntime)(nil)
	for _, rt := range s.runtimes {
		rt.mu.Lock()
		match := rt.attemptID == attemptID && !rt.completed
		if match {
			child = rt
		}
		rt.mu.Unlock()
		if match {
			break
		}
	}
	s.controlMu.Unlock()
	if child == nil {
		return "", ""
	}
	return child.id, child.sessionID()
}
