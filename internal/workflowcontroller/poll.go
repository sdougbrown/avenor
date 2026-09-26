package workflowcontroller

// poll.go implements the durable poll cursor checkpoint for external-gate
// adapters. One cursor per (workflow, node, activation, gate, subject hash)
// persists in controller.json's poll_cursors map: the last poll time, the
// scheduled next poll, the retry count, the persisted monotonic poll count,
// and the poll ID committed before the adapter runs. The poll ID is
// deterministic over controller, workflow, node, activation, gate, subject
// hash, and count, so a retry after any crash reuses the same count and poll
// ID and the adapter protocol can treat it as its idempotency key.

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// PollCursor is one gate's persisted poll checkpoint.
type PollCursor struct {
	WorkflowID   string    `json:"workflow_id"`
	NodeID       string    `json:"node_id"`
	ActivationID string    `json:"activation_id"`
	GateID       string    `json:"gate_id"`
	AdapterID    string    `json:"adapter_id"`
	SubjectHash  string    `json:"subject_hash"`
	LastPollAt   time.Time `json:"last_poll_at,omitempty"`
	// NextPollAt is the scheduled next poll; zero while a committed poll is
	// in flight (its result has not landed yet).
	NextPollAt time.Time `json:"next_poll_at,omitempty"`
	// RetryCount is the number of consecutive unsuccessful polls (pending or
	// transient failure) used to double the backoff interval.
	RetryCount int    `json:"retry_count,omitempty"`
	PollCount  int64  `json:"poll_count"`
	PollID     string `json:"poll_id,omitempty"`
}

// PollCursorKey builds the poll_cursors map key for a cursor:
// workflow/node/activation/gate/subject-hash.
func PollCursorKey(c PollCursor) string {
	return c.WorkflowID + "/" + c.NodeID + "/" + c.ActivationID + "/" + c.GateID + "/" + c.SubjectHash
}

// DerivePollID deterministically derives the poll idempotency key from the
// controller, workflow, node, activation, gate, subject hash, and the
// persisted poll count.
func DerivePollID(controllerID string, c PollCursor, count int64) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		"poll", controllerID, c.WorkflowID, c.NodeID, c.ActivationID, c.GateID, c.SubjectHash,
		strconv.FormatInt(count, 10),
	}, "\x00")))
	return "poll_" + hex.EncodeToString(sum[:16])
}

// diagnosticKey builds the record's diagnostics map key.
func diagnosticKey(kind, name string) string { return kind + "/" + name }

// pollBaseDelay is the interval before the first poll after a gate parks;
// it doubles on every consecutive pending or transient-failed poll.
const pollBaseDelay = 30 * time.Second

// PollBackoffDelay returns the delay before the next poll after the k-th
// consecutive unsuccessful poll (retryCount = k) and the incremented retry
// count to persist. The interval doubles from the base — the first post-park
// poll already waited one base interval — and caps at five minutes. An
// adapter-requested retry_after_ms is honored after clamping into the
// [base, 5m] range, so an explicit zero lands on the base floor (only an
// absent retry_after follows the backoff schedule). jitter, when non-nil,
// returns a value in [-1, 1] scaling the delay by ±10%; the result always
// stays inside the clamped range.
func PollBackoffDelay(base time.Duration, retryCount int, retryAfterMS *int64, jitter func() float64) (time.Duration, int) {
	delay := time.Duration(0)
	if retryAfterMS != nil {
		delay = time.Duration(*retryAfterMS) * time.Millisecond
		delay = ClampRetryDelay(delay, base)
	} else {
		delay = base
		for i := 0; i <= retryCount && delay < AdapterMaxRetryDelay; i++ {
			delay *= 2
		}
		if delay > AdapterMaxRetryDelay || delay <= 0 {
			delay = AdapterMaxRetryDelay
		}
	}
	if delay < base {
		delay = base
	}
	if jitter != nil {
		scaled := time.Duration(float64(delay) * (1 + 0.1*jitter()))
		delay = ClampRetryDelay(scaled, base)
	}
	return delay, retryCount + 1
}

// inFlight reports whether the cursor has a committed poll whose result has
// not landed yet: a last poll with no scheduled retry.
func (c PollCursor) inFlight() bool {
	return !c.LastPollAt.IsZero() && c.NextPollAt.IsZero()
}

// EnsurePollCursor creates the cursor with its first poll scheduled at
// firstPollAt when no cursor exists under its key; an existing cursor is
// returned unchanged (created=false) so a restart never resets backoff.
func (s *ControllerStore) EnsurePollCursor(controllerID string, cursor PollCursor, firstPollAt time.Time) (PollCursor, bool, error) {
	if err := validateControllerID(controllerID); err != nil {
		return PollCursor{}, false, err
	}
	key := PollCursorKey(cursor)
	unlock, err := s.lockController(controllerID)
	if err != nil {
		return PollCursor{}, false, err
	}
	defer unlock()
	rec, err := s.loadForCommand(controllerID)
	if err != nil {
		return PollCursor{}, false, err
	}
	if rec.PollCursors != nil {
		if existing, ok := rec.PollCursors[key]; ok && existing != nil {
			return *existing, false, nil
		}
	}
	seed := cursor
	seed.NextPollAt = firstPollAt.UTC()
	now := s.now().UTC()
	event := ControllerEvent{
		Kind:      EventPollScheduled,
		Seq:       rec.Revision + 1,
		Time:      now,
		CursorKey: key,
		Cursor:    &seed,
	}
	if err := s.commitLocked(controllerID, &rec, []ControllerEvent{event}); err != nil {
		return PollCursor{}, false, err
	}
	return seed, true, nil
}

// CommitPoll makes one poll's intent durable BEFORE the adapter runs. On a
// fresh or completed cycle it increments the monotonic poll count and derives
// the poll ID from it; on a cursor already in flight (a crash between the
// commit and the result) it reuses the same count and poll ID. The next-poll
// schedule is cleared while the poll runs.
func (s *ControllerStore) CommitPoll(controllerID string, cursor PollCursor, now time.Time) (PollCursor, error) {
	if err := validateControllerID(controllerID); err != nil {
		return PollCursor{}, err
	}
	key := PollCursorKey(cursor)
	unlock, err := s.lockController(controllerID)
	if err != nil {
		return PollCursor{}, err
	}
	defer unlock()
	rec, err := s.loadForCommand(controllerID)
	if err != nil {
		return PollCursor{}, err
	}
	committed := cursor
	if existing, ok := rec.PollCursors[key]; ok && existing != nil {
		committed = *existing
	}
	if committed.PollID == "" || !committed.inFlight() {
		committed.PollCount++
		committed.PollID = DerivePollID(controllerID, committed, committed.PollCount)
	}
	committed.LastPollAt = now.UTC()
	committed.NextPollAt = time.Time{}
	// Carry forward the caller's identity fields in case the cursor was
	// seeded fresh.
	committed.WorkflowID, committed.NodeID, committed.ActivationID = cursor.WorkflowID, cursor.NodeID, cursor.ActivationID
	committed.GateID, committed.AdapterID, committed.SubjectHash = cursor.GateID, cursor.AdapterID, cursor.SubjectHash
	event := ControllerEvent{
		Kind:      EventPollCommitted,
		Seq:       rec.Revision + 1,
		Time:      s.now().UTC(),
		CursorKey: key,
		Cursor:    &committed,
	}
	if err := s.commitLocked(controllerID, &rec, []ControllerEvent{event}); err != nil {
		return PollCursor{}, err
	}
	return committed, nil
}

// SchedulePollRetry records the post-result schedule: the next poll time and
// the running retry count. A cursor whose schedule already matches is a
// no-op, so an idle pass appends no event.
func (s *ControllerStore) SchedulePollRetry(controllerID string, cursor PollCursor, next time.Time, retryCount int) (PollCursor, error) {
	if err := validateControllerID(controllerID); err != nil {
		return PollCursor{}, err
	}
	key := PollCursorKey(cursor)
	unlock, err := s.lockController(controllerID)
	if err != nil {
		return PollCursor{}, err
	}
	defer unlock()
	rec, err := s.loadForCommand(controllerID)
	if err != nil {
		return PollCursor{}, err
	}
	updated := cursor
	if existing, ok := rec.PollCursors[key]; ok && existing != nil {
		updated = *existing
	}
	if updated.NextPollAt.Equal(next.UTC()) && updated.RetryCount == retryCount {
		return updated, nil
	}
	updated.WorkflowID, updated.NodeID, updated.ActivationID = cursor.WorkflowID, cursor.NodeID, cursor.ActivationID
	updated.GateID, updated.AdapterID, updated.SubjectHash = cursor.GateID, cursor.AdapterID, cursor.SubjectHash
	updated.NextPollAt = next.UTC()
	updated.RetryCount = retryCount
	event := ControllerEvent{
		Kind:      EventPollScheduled,
		Seq:       rec.Revision + 1,
		Time:      s.now().UTC(),
		CursorKey: key,
		Cursor:    &updated,
	}
	if err := s.commitLocked(controllerID, &rec, []ControllerEvent{event}); err != nil {
		return PollCursor{}, err
	}
	return updated, nil
}

// ClearPollCursor removes a finished cursor (the gate resolved or the
// activation moved on). Clearing an absent cursor is a no-op.
func (s *ControllerStore) ClearPollCursor(controllerID string, cursor PollCursor) error {
	if err := validateControllerID(controllerID); err != nil {
		return err
	}
	key := PollCursorKey(cursor)
	unlock, err := s.lockController(controllerID)
	if err != nil {
		return err
	}
	defer unlock()
	rec, err := s.loadForCommand(controllerID)
	if err != nil {
		return err
	}
	if rec.PollCursors == nil || rec.PollCursors[key] == nil {
		return nil
	}
	event := ControllerEvent{
		Kind:      EventPollCleared,
		Seq:       rec.Revision + 1,
		Time:      s.now().UTC(),
		CursorKey: key,
	}
	return s.commitLocked(controllerID, &rec, []ControllerEvent{event})
}

// PollDue returns every cursor due for a poll at now: a scheduled next poll
// that has passed, or an in-flight poll left behind by a crash (its result
// never landed, so its count and poll ID are reused).
func (s *ControllerStore) PollDue(controllerID string, now time.Time) ([]PollCursor, error) {
	rec, ok, err := s.Get(controllerID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("controller %s: %w", controllerID, ErrNotFound)
	}
	due := make([]PollCursor, 0, len(rec.PollCursors))
	for _, cursor := range rec.PollCursors {
		if cursor == nil {
			continue
		}
		if cursor.inFlight() || (!cursor.NextPollAt.IsZero() && !cursor.NextPollAt.After(now)) {
			due = append(due, *cursor)
		}
	}
	return due, nil
}

// NextPollTime returns the earliest scheduled next poll across the record's
// cursors, if any.
func (s *ControllerStore) NextPollTime(controllerID string) (time.Time, bool, error) {
	rec, ok, err := s.Get(controllerID)
	if err != nil || !ok {
		return time.Time{}, false, err
	}
	var earliest time.Time
	for _, cursor := range rec.PollCursors {
		if cursor == nil || cursor.NextPollAt.IsZero() {
			continue
		}
		if earliest.IsZero() || cursor.NextPollAt.Before(earliest) {
			earliest = cursor.NextPollAt
		}
	}
	return earliest, !earliest.IsZero(), nil
}

// RecordDiagnostic records a structured diagnostic under kind/name. An
// unchanged diagnostic is a no-op (deduplicated), so repeated passes append
// no events.
func (s *ControllerStore) RecordDiagnostic(controllerID, kind, name, detail string) (bool, error) {
	if err := validateControllerID(controllerID); err != nil {
		return false, err
	}
	if kind == "" || name == "" {
		return false, errors.New("diagnostic requires kind and name")
	}
	unlock, err := s.lockController(controllerID)
	if err != nil {
		return false, err
	}
	defer unlock()
	rec, err := s.loadForCommand(controllerID)
	if err != nil {
		return false, err
	}
	if existing, ok := rec.Diagnostics[diagnosticKey(kind, name)]; ok && existing == detail {
		return false, nil
	}
	event := ControllerEvent{
		Kind:       EventDiagRecorded,
		Seq:        rec.Revision + 1,
		Time:       s.now().UTC(),
		DiagKind:   kind,
		DiagKey:    name,
		DiagDetail: detail,
	}
	if err := s.commitLocked(controllerID, &rec, []ControllerEvent{event}); err != nil {
		return false, err
	}
	return true, nil
}

// ClearDiagnostic removes a recorded diagnostic; clearing an absent one is a
// no-op.
func (s *ControllerStore) ClearDiagnostic(controllerID, kind, name string) error {
	if err := validateControllerID(controllerID); err != nil {
		return err
	}
	unlock, err := s.lockController(controllerID)
	if err != nil {
		return err
	}
	defer unlock()
	rec, err := s.loadForCommand(controllerID)
	if err != nil {
		return err
	}
	if _, ok := rec.Diagnostics[diagnosticKey(kind, name)]; !ok {
		return nil
	}
	event := ControllerEvent{
		Kind:     EventDiagCleared,
		Seq:      rec.Revision + 1,
		Time:     s.now().UTC(),
		DiagKind: kind,
		DiagKey:  name,
	}
	return s.commitLocked(controllerID, &rec, []ControllerEvent{event})
}
