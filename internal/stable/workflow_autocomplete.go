package stable

import (
	"fmt"
	"os"

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
func (s *Supervisor) finishWorkflowAttempt(ec workflow.ExecutorContext, hb *leaseHeartbeat, status workflow.AttemptStatus, child *childRuntime) {
	kind, label := workflowMarkerForKind(ec.Action.Kind)
	if stashed := child.terminalMarkerLabel(); stashed != "" {
		label = stashed
	}
	if status != workflow.AttemptSucceeded {
		hb.StopAndWait()
		s.recordAttemptTerminal(ec, child.dir, string(status), kind, label)
		return
	}

	mgr := s.workflowManager()
	eligible, plan, rejection, err := mgr.DecideAutoCompletion(
		ec.WorkflowID, ec.NodeID, ec.ActivationID, child.dir, label)
	if err != nil {
		fmt.Fprintf(os.Stderr, "avenor stable: workflow %s node %s attempt %s: cannot decide auto-completion, recording the plain terminal fact: %v\n",
			ec.WorkflowID, ec.NodeID, ec.AttemptID, err)
	}
	if !eligible {
		hb.StopAndWait()
		s.recordAttemptTerminal(ec, child.dir, string(status), kind, label)
		return
	}
	if plan == nil {
		// Contract unmet: the node's retry policy and exhaustion apply, and
		// the activation is never left running after the worker exited.
		fmt.Fprintf(os.Stderr, "avenor stable: workflow %s node %s attempt %s: auto-completion contract unmet: %s\n",
			ec.WorkflowID, ec.NodeID, ec.AttemptID, rejection)
		hb.StopAndWait()
		s.recordAttemptTerminal(ec, child.dir, string(workflow.AttemptFailed), kind, "contract_unmet")
		return
	}

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
		fmt.Fprintf(os.Stderr, "avenor stable: workflow %s node %s attempt %s: record success fact: %v\n",
			ec.WorkflowID, ec.NodeID, ec.AttemptID, err)
		return
	}
	if _, err := mgr.CompleteAuto(ec.WorkflowID, ec.NodeID, ec.ActivationID, ec.AttemptID, ec.LeaseID, ec.OwnerToken, plan); err != nil {
		// Residual: the success fact is recorded but the completion (evidence
		// staging, output recording, or the atomic command itself) failed. The
		// activation stays running with a held lease until the lease expires;
		// no new kernel command exists for this state.
		fmt.Fprintf(os.Stderr, "avenor stable: workflow %s node %s attempt %s: supervisor auto-completion failed after the success fact (activation stays running until the lease expires): %v\n",
			ec.WorkflowID, ec.NodeID, ec.AttemptID, err)
	}
	hb.StopAndWait()
}

// recordAttemptTerminal records the plain terminal fact for an attempt the
// supervisor does not auto-complete (manual nodes, external nodes,
// non-success exits), carrying the working directory the attempt's runtime
// ran in.
func (s *Supervisor) recordAttemptTerminal(ec workflow.ExecutorContext, dir, status string, kind, label string) {
	if err := s.workflowManager().RecordAttemptTermination(
		ec.WorkflowID, ec.NodeID, ec.ActivationID, ec.AttemptID, ec.LeaseID, workflow.AttemptTermination{
			Status:           workflow.AttemptStatus(status),
			MarkerKind:       kind,
			MarkerLabel:      label,
			WorkingDirectory: dir,
		}); err != nil {
		fmt.Fprintf(os.Stderr, "avenor stable: workflow %s node %s attempt %s: record %s terminal fact: %v\n",
			ec.WorkflowID, ec.NodeID, ec.AttemptID, status, err)
	}
}
