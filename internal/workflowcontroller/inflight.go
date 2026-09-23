// Package workflowcontroller implements the pure candidate selection policy
// for workflow controllers. It derives dispatch decisions entirely from the
// candidate and in-flight snapshots supplied by the caller; the caller owns
// the clock, the controller store, and all dispatch side effects.
package workflowcontroller

import (
	"github.com/sdougbrown/avenor/internal/workflow"
)

// heldKeys returns the concurrency keys held by any non-terminal in-flight
// attempt with a non-empty key. Keys are held regardless of which controller
// or caller (including manual starts) owns the attempt; terminal attempts
// release their key.
func heldKeys(inflight []InFlightAttempt) map[string]struct{} {
	keys := make(map[string]struct{})
	for _, attempt := range inflight {
		if attempt.Terminal || attempt.ConcurrencyKey == "" {
			continue
		}
		keys[attempt.ConcurrencyKey] = struct{}{}
	}
	return keys
}

// activeCount returns the number of non-terminal in-flight attempts owned by
// the given controller. Manual starts and other controllers' attempts do not
// count against this controller's capacity. Terminal attempts count for
// nothing.
func activeCount(inflight []InFlightAttempt, controllerID string) int {
	count := 0
	for _, attempt := range inflight {
		if attempt.Terminal || attempt.ControllerID != controllerID {
			continue
		}
		count++
	}
	return count
}

// activeActivations returns the set of activations (workflow, node,
// activation) that already have a non-terminal in-flight attempt, from any
// controller or manual caller. A candidate for such an activation is stale.
func activeActivations(inflight []InFlightAttempt) map[activationKey]struct{} {
	activations := make(map[activationKey]struct{})
	for _, attempt := range inflight {
		if attempt.Terminal {
			continue
		}
		activations[activationKey{
			WorkflowID:   attempt.Identity.WorkflowID,
			NodeID:       attempt.Identity.NodeID,
			ActivationID: attempt.Identity.ActivationID,
		}] = struct{}{}
	}
	return activations
}

type activationKey struct {
	WorkflowID   workflow.WorkflowID
	NodeID       workflow.NodeID
	ActivationID workflow.ActivationID
}
