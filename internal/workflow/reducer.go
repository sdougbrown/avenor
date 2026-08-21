package workflow

import (
	"errors"
	"time"
)

// errRevisionMismatch is returned by Apply when the command's expected revision
// differs from the current instance revision (optimistic-concurrency guard).
var errRevisionMismatch = errors.New("revision mismatch")

// errDuplicateIdempotency is returned by Apply when a command reuses an already
// applied idempotency key (a retried command that already landed).
var errDuplicateIdempotency = errors.New("duplicate command idempotency key")

// cloneSnapshot returns a deep copy of state so Reduce never aliases or
// mutates the caller's snapshot. A JSON round-trip would trip the snapshot's
// own canonical null-rejection on zero-value slices (e.g. nil Activations
// before instantiate), so this is an explicit Go deep copy of the nested
// slices and pointers the reducer mutates.
func cloneSnapshot(state Snapshot) Snapshot {
	next := state
	next.AppliedEventIDs = append([]EventID(nil), state.AppliedEventIDs...)
	next.Idempotency = make(map[string][]EventID, len(state.Idempotency))
	for key, ids := range state.Idempotency {
		next.Idempotency[key] = append([]EventID(nil), ids...)
	}
	inst := &next.Instance
	inst.Metadata = cloneMap(inst.Metadata)
	inst.Activations = cloneSlice(inst.Activations, cloneActivation)
	inst.Attempts = cloneSlice(inst.Attempts, cloneAttempt)
	inst.Evidence = cloneSlice(inst.Evidence, cloneEvidence)
	inst.Gates = cloneSlice(inst.Gates, cloneGate)
	inst.Outputs = cloneSlice(inst.Outputs, cloneOutput)
	inst.Children = cloneSlice(inst.Children, cloneChild)
	return next
}

// cloneSlice returns a fresh slice whose elements are deep copies of src via fn.
func cloneSlice[T any](src []T, fn func(T) T) []T {
	if src == nil {
		return nil
	}
	out := make([]T, len(src))
	for i := range src {
		out[i] = fn(src[i])
	}
	return out
}

func cloneMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	out := make(map[string]any, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func cloneActivation(a Activation) Activation {
	a.AttemptIDs = append([]AttemptID(nil), a.AttemptIDs...)
	if a.Selection != nil {
		sel := *a.Selection
		a.Selection = &sel
	}
	if a.ActiveLease != nil {
		lease := *a.ActiveLease
		lease.LastHeartbeatAt = derefTime(a.ActiveLease.LastHeartbeatAt)
		lease.LastActivityAt = derefTime(a.ActiveLease.LastActivityAt)
		a.ActiveLease = &lease
	}
	return a
}

func cloneAttempt(at Attempt) Attempt {
	at.ArtifactPaths = append([]string(nil), at.ArtifactPaths...)
	at.EndedAt = derefTime(at.EndedAt)
	return at
}

func cloneEvidence(ev Evidence) Evidence {
	ev.Result = append([]byte(nil), ev.Result...)
	ev.Subject = cloneSubject(ev.Subject)
	return ev
}

func cloneGate(g GateInstance) GateInstance {
	g.EvidenceIDs = append([]EvidenceID(nil), g.EvidenceIDs...)
	g.Subject = cloneSubject(g.Subject)
	g.ObservedAt = derefTime(g.ObservedAt)
	g.DecidedAt = derefTime(g.DecidedAt)
	return g
}

func cloneOutput(o OutputValue) OutputValue {
	o.Value = append([]byte(nil), o.Value...)
	o.EvidenceIDs = append([]EvidenceID(nil), o.EvidenceIDs...)
	return o
}

func cloneChild(c ChildReference) ChildReference {
	c.Outputs = cloneSlice(c.Outputs, func(r OutputReference) OutputReference { return r })
	return c
}

func cloneSubject(s *Subject) *Subject {
	if s == nil {
		return nil
	}
	copy := *s
	return &copy
}

func derefTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	v := *t
	return &v
}

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
		return nil, errRevisionMismatch
	}
	if command.IdempotencyKey != "" {
		if _, ok := state.Idempotency[command.IdempotencyKey]; ok {
			return nil, errDuplicateIdempotency
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
	next := cloneSnapshot(state)
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
