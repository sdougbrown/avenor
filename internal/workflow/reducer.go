package workflow

import "errors"

// Apply turns a validated command into the batch of events that records its
// effect. It is a pure projection of (Snapshot, Command) -> []Event: nothing
// is mutated and nothing is persisted here. The returned events must be
// reduced (via Reduce) in order to advance the instance.
//
// Apply is the only place that consults the caller-supplied expectations
// (expected revision, idempotency). Event-level state transitions live in
// transitions.go (applyEvent), which stays purely replay-driven.
func Apply(state Snapshot, command Command) ([]Event, error) {
	if command.ExpectedRevision != state.Instance.Revision {
		return nil, errors.New("revision mismatch")
	}
	if command.IdempotencyKey != "" {
		if _, ok := state.Idempotency[command.IdempotencyKey]; ok {
			return nil, errors.New("duplicate command idempotency key")
		}
	}
	return buildCommandEvents(state, command)
}

// Reduce applies one event to a snapshot and returns the next snapshot. It is
// idempotent per event ID: replaying an already-applied event is a no-op.
// Events sharing a command's idempotency key are also deduplicated so a
// retried batch never double-applies.
func Reduce(state Snapshot, event Event) (Snapshot, error) {
	if containsID(state.AppliedEventIDs, event.ID) {
		return state, nil
	}
	next := state
	if err := applyEvent(&next, event); err != nil {
		return state, err
	}
	next.AppliedEventIDs = append(next.AppliedEventIDs, event.ID)
	if event.IdempotencyKey != "" {
		if next.Idempotency == nil {
			next.Idempotency = map[string][]EventID{}
		}
		next.Idempotency[event.IdempotencyKey] = append(next.Idempotency[event.IdempotencyKey], event.ID)
	}
	next.Instance.Revision = event.Sequence
	return next, nil
}

// containsID reports whether id is present in ids.
func containsID(ids []EventID, id EventID) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}
