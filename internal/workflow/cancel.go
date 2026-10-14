package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
)

// cancel.go implements the manager's instance-cancel surface: an attributed
// operator command that terminates the whole workflow instance (status
// canceled), releases every live lease, and parks every non-terminal
// activation in the canceled status. The reducer (applyCanceled) stays
// authoritative; this file owns the command-boundary validation, the
// idempotency key, and the bounded-retry apply.

// commandCancelRequest is the wire shape of a cancel command. Actor and reason
// are required: a cancel is an attributed operator decision and the reason is
// its durable audit trail.
type commandCancelRequest struct {
	Actor  string `json:"actor,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// cancelIdempotencyKey returns the stable idempotency key for one instance
// cancel. The key is workflow-scoped: a re-issued cancel for the same instance
// is always the same command, so a retried delivery is a safe no-op rather
// than a second terminal event.
func cancelIdempotencyKey(wf WorkflowID) string {
	return "cancel-" + string(wf)
}

// WorkflowCancel cancels a whole workflow instance. It validates the request,
// rejects an already-terminal instance with a clear error, and applies the
// CommandCancel atomically (bounded retry on revision mismatch). Canceling
// in-flight attempt runtimes is the caller's (supervisor surface's) job: the
// manager owns only the durable kernel transition, and late
// attempt_terminated events from still-running runtimes land harmlessly on a
// canceled instance.
func (m *Manager) WorkflowCancel(id string, payload json.RawMessage) (any, error) {
	wf, err := m.checkWorkflowID(id)
	if err != nil {
		return nil, err
	}
	var req commandCancelRequest
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &req); err != nil {
			return nil, fmt.Errorf("cancel payload: %w", err)
		}
	}
	if req.Actor == "" {
		return nil, errors.New("cancel requires actor")
	}
	if req.Reason == "" {
		return nil, errors.New("cancel requires reason")
	}

	snap, exists, err := m.store.loadCurrent(wf)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("workflow not found: %s", wf)
	}
	// Idempotency before the status check: a retried cancel for an already
	// canceled instance is a safe no-op reporting the current state, while a
	// first cancel against a completed/failed instance is a clear error.
	idemKey := cancelIdempotencyKey(wf)
	if _, done := snap.Idempotency[idemKey]; done {
		return map[string]any{
			"idempotent": true,
			"status":     string(snap.Instance.Status),
			"revision":   snap.Instance.Revision,
		}, nil
	}
	if isTerminalStatus(snap.Instance.Status) {
		return nil, fmt.Errorf("cannot cancel workflow %s: instance is already %s", wf, snap.Instance.Status)
	}

	const maxCancelAttempts = 4
	for attempt := 0; attempt < maxCancelAttempts; attempt++ {
		fresh, exists, err := m.store.loadCurrent(wf)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, fmt.Errorf("workflow not found: %s", wf)
		}
		if isTerminalStatus(fresh.Instance.Status) {
			// A concurrent command terminated the instance between the read
			// and the apply; the cancel loses to the observed terminal fact.
			return nil, fmt.Errorf("cannot cancel workflow %s: instance is already %s", wf, fresh.Instance.Status)
		}
		if hook := m.testHooks.cancelPreCommit; hook != nil {
			m.testHooks.cancelPreCommit = nil
			hook()
		}
		result, err := m.store.ApplyCommand(wf, Command{
			ID:               NewCommandID(),
			Kind:             CommandCancel,
			ExpectedRevision: fresh.Instance.Revision,
			IdempotencyKey:   idemKey,
			Identity:         ExecutionIdentity{WorkflowID: wf},
			Actor:            req.Actor,
			Reason:           req.Reason,
		})
		if err == nil {
			return map[string]any{
				"status":   string(result.Instance.Status),
				"revision": result.Instance.Revision,
			}, nil
		}
		if errors.Is(err, errDuplicateIdempotency) {
			current, _, err := m.store.loadCurrent(wf)
			if err != nil {
				return nil, err
			}
			return map[string]any{
				"idempotent": true,
				"status":     string(current.Instance.Status),
				"revision":   current.Instance.Revision,
			}, nil
		}
		if !errors.Is(err, errRevisionMismatch) {
			return nil, err
		}
		// Optimistic-concurrency conflict; re-read under a fresh lock and retry.
	}
	return nil, fmt.Errorf("cancel workflow %s: revision kept moving under concurrent commands", wf)
}
