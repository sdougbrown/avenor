package workflow

import (
	"errors"
	"fmt"
)

// gates.go holds the reducer-side helpers for the CommandGate operation enum
// (Stage 12, human and external gates). These are pure: they validate the
// command's fields before any event is emitted and map operations to the
// durable gate status; the activation resolution itself lives in applyGate.

// validGateOperation reports whether op is one of the closed gate operations.
func validGateOperation(op GateOperation) bool {
	switch op {
	case GateOpSatisfy, GateOpReject, GateOpWaive, GateOpExternalResult:
		return true
	}
	return false
}

// gateStatusLegal reports whether status is a legal durable gate status for
// the operation. For human operations the operation determines the status
// exactly; for external_result any of the five external result statuses is
// legal (the raw result enum mapping/validation is owned by the manager/CLI
// layer). Nothing references command.Outcome here.
func gateStatusLegal(op GateOperation, status GateStatus) bool {
	switch op {
	case GateOpSatisfy:
		return status == GatePassed
	case GateOpWaive:
		return status == GateWaived
	case GateOpReject:
		return status == GateRejected
	case GateOpExternalResult:
		switch status {
		case GatePending, GatePassed, GateFailed, GateActionRequired, GateChangesRequested:
			return true
		}
		return false
	}
	return false
}

// validateGateCommand is the reducer-authoritative field validation for
// CommandGate, called from buildCommandEvents before any event is emitted so
// an invalid command never mutates the store. An empty operation is the
// legacy bare-gate form (a plain GateInstance status, no structured
// decision): it only requires the gate instance and the exact-subject
// binding. Structured operations add their required-field sets and a
// status/operation consistency check.
func validateGateCommand(command Command) error {
	gate := command.Gate
	if gate == nil {
		return errors.New("gate command requires a gate instance")
	}
	// Exact-subject binding for all operations: the gate instance must
	// address the activation the command identity targets. Subject binding
	// against the node's declared SubjectType is enforced at the manager
	// layer; the reducer cannot see the template.
	if gate.ActivationID != "" && command.Identity.ActivationID != "" &&
		gate.ActivationID != command.Identity.ActivationID {
		return fmt.Errorf("gate activation %q does not match command identity activation %q",
			gate.ActivationID, command.Identity.ActivationID)
	}
	op := command.Operation
	if op == "" {
		return nil
	}
	if !validGateOperation(op) {
		return fmt.Errorf("unknown gate operation %q", op)
	}
	switch op {
	case GateOpSatisfy, GateOpReject, GateOpWaive:
		if gate.Actor == "" {
			return fmt.Errorf("gate operation %q requires an actor", op)
		}
		if gate.Reason == "" {
			return fmt.Errorf("gate operation %q requires a reason", op)
		}
		if len(gate.EvidenceIDs) == 0 {
			return fmt.Errorf("gate operation %q requires at least one evidence id", op)
		}
	case GateOpExternalResult:
		if gate.PollID == "" {
			return errors.New("external_result gate requires a poll id")
		}
		if gate.Source == "" {
			return errors.New("external_result gate requires a source")
		}
		if gate.ObservedAt == nil || gate.ObservedAt.IsZero() {
			return errors.New("external_result gate requires a non-zero observed_at timestamp")
		}
		if gate.Subject == nil {
			return errors.New("external_result gate requires a subject")
		}
		if gate.ResponseHash == "" {
			return errors.New("external_result gate requires a response hash")
		}
		if len(gate.EvidenceIDs) == 0 {
			return errors.New("external_result gate requires at least one evidence id")
		}
	}
	// Defense in depth: the recorded gate status must be a legal status for
	// the operation. For human operations the operation determines the status
	// exactly; for external_result the raw result string is validated and
	// mapped to a status at the manager/CLI layer and only the resulting
	// status reaches the reducer.
	if !gateStatusLegal(op, gate.Status) {
		return fmt.Errorf("gate status %q is inconsistent with operation %q", gate.Status, op)
	}
	return nil
}
