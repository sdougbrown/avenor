package stable

import (
	"context"
	"errors"
	"fmt"

	"github.com/sdougbrown/avenor/internal/workflow"
)

// workflow_autocomplete.go holds the supervisor-side completion of
// auto-dispatched provider nodes. When a run/loop/team node dispatched with
// dispatch.mode auto exits successfully, the supervisor — which already
// holds the lease's owner token in its ExecutorContext — evaluates the
// node's declared contract against the attempt's working directory and, if
// the contract is met, completes the node itself. The worker never sees the
// owner token and never declares a result: completion is derived from the
// declared contract and the work product.

// finishWorkflowAttempt is the single terminal path for a workflow attempt
// whose runtime reached a terminal state. It stops the lease heartbeat only
// once no further workflow state depends on the lease staying alive: a
// successful auto-provider attempt keeps the heartbeat running through
// evaluation and completion, so the lease cannot expire between the
// terminal fact and the complete command landing.
func (s *Supervisor) finishWorkflowAttempt(ec workflow.ExecutorContext, hb *leaseHeartbeat, status workflow.AttemptStatus, stopReason, errorText string, child *childRuntime) {
	kind, label := workflowMarkerForKind(ec.Action.Kind)
	if stashed := child.terminalMarkerLabel(); stashed != "" {
		label = stashed
	}
	if status != workflow.AttemptSucceeded {
		hb.StopAndWait()
		s.recordAttemptTerminal(ec, child.dir, string(status), stopReason, errorText, kind, label, nil)
		return
	}

	mgr := s.workflowManager()
	// The child's lifecycle context carries the attempt's cancellation, so a
	// completion command is killed with its process group when the attempt is
	// canceled. The same applies when the runtime is torn down mid-check.
	ctx := child.lifecycleCtx
	if ctx == nil {
		ctx = context.Background()
	}
	decision, err := mgr.DecideAutoCompletion(
		ctx, ec.WorkflowID, ec.NodeID, ec.ActivationID, child.dir, label)
	if err != nil {
		fmt.Fprintf(s.stderrWriter(), "avenor stable: workflow %s node %s attempt %s: cannot decide auto-completion, recording the plain terminal fact: %v\n",
			ec.WorkflowID, ec.NodeID, ec.AttemptID, err)
	}
	if !decision.Eligible {
		hb.StopAndWait()
		s.recordAttemptTerminal(ec, child.dir, string(status), stopReason, errorText, kind, label, nil)
		return
	}
	if decision.Plan == nil {
		// Contract unmet: the node's retry policy and exhaustion apply, and the
		// activation is never left running after the worker exited. Any evidence
		// the declared check captured rides the failed attempt, so the rejection
		// stays inspectable after the log line scrolls away. A rejection that is
		// the attempt's own cancellation records a canceled attempt, matching
		// every other canceled terminal path.
		status, marker := workflow.AttemptFailed, "contract_unmet"
		if decision.Canceled {
			status, marker = workflow.AttemptCanceled, "canceled"
		}
		fmt.Fprintf(s.stderrWriter(), "avenor stable: workflow %s node %s attempt %s: auto-completion contract unmet: %s\n",
			ec.WorkflowID, ec.NodeID, ec.AttemptID, decision.Rejection)
		hb.StopAndWait()
		s.recordAttemptTerminal(ec, child.dir, string(status), stopReason, errorText, kind, marker, decision.FailureEvidence)
		return
	}
	plan := decision.Plan

	// The terminal fact precedes the complete command because files/git
	// completion contracts require it; the heartbeat still runs and keeps the
	// lease alive until the completion lands.
	if err := mgr.RecordAttemptTermination(ec.WorkflowID, ec.NodeID, ec.ActivationID, ec.AttemptID, ec.LeaseID, workflow.AttemptTermination{
		Status:           workflow.AttemptSucceeded,
		MarkerKind:       kind,
		MarkerLabel:      label,
		WorkingDirectory: child.dir,
	}); err != nil {
		hb.StopAndWait()
		fmt.Fprintf(s.stderrWriter(), "avenor stable: workflow %s node %s attempt %s: record success fact: %v\n",
			ec.WorkflowID, ec.NodeID, ec.AttemptID, err)
		return
	}
	err = nil
	if s.testHooks.completeAutoPre != nil {
		err = s.testHooks.completeAutoPre()
	}
	if err == nil {
		_, err = mgr.CompleteAuto(ec.WorkflowID, ec.NodeID, ec.ActivationID, ec.AttemptID, ec.LeaseID, ec.OwnerToken, plan)
	}
	if err != nil {
		if errors.Is(err, workflow.ErrCommandDurable) {
			// The completion's events are durable: a replay applies it, so the
			// node is satisfied and the stale lease is harmless. Do not promise a
			// replacement attempt the log already rules out.
			fmt.Fprintf(s.stderrWriter(), "avenor stable: workflow %s node %s attempt %s: supervisor auto-completion is recorded durably but its snapshot could not be materialized; replay recovers it and no replacement attempt is needed: %v\n",
				ec.WorkflowID, ec.NodeID, ec.AttemptID, err)
			hb.StopAndWait()
			return
		}
		// Residual: the success fact is recorded but the completion (evidence
		// staging, output recording, or the atomic command itself) failed. The
		// heartbeat stops here, so the lease goes stale; the live lease-expiry
		// sweep expires it when running, and restart recovery otherwise. The
		// controller then re-dispatches the node for a replacement attempt.
		recovery := "the lease expires on the next restart recovery"
		if s.leaseSweepRunning() {
			recovery = "the live lease sweep will expire the lease"
		}
		fmt.Fprintf(s.stderrWriter(), "avenor stable: workflow %s node %s attempt %s: supervisor auto-completion failed after the success fact (%s and the controller will re-dispatch the node): %v\n",
			ec.WorkflowID, ec.NodeID, ec.AttemptID, recovery, err)
	}
	hb.StopAndWait()
}

// recordAttemptTerminal records the terminal fact for an attempt the supervisor
// does not auto-complete: manual nodes, external nodes, non-success exits, and an
// unmet contract. It carries the working directory the attempt's runtime ran in
// and any inspection evidence captured for it.
func (s *Supervisor) recordAttemptTerminal(ec workflow.ExecutorContext, dir, status, stopReason, errorText, kind, label string, evidence []workflow.CommandCapture) {
	if err := s.workflowManager().RecordAttemptTermination(
		ec.WorkflowID, ec.NodeID, ec.ActivationID, ec.AttemptID, ec.LeaseID, workflow.AttemptTermination{
			Status:           workflow.AttemptStatus(status),
			MarkerKind:       kind,
			MarkerLabel:      label,
			StopReason:       stopReason,
			ErrorText:        errorText,
			WorkingDirectory: dir,
			Evidence:         evidence,
		}); err != nil {
		fmt.Fprintf(s.stderrWriter(), "avenor stable: workflow %s node %s attempt %s: record %s terminal fact: %v\n",
			ec.WorkflowID, ec.NodeID, ec.AttemptID, status, err)
	}
}
