package workflowcontroller

// Tests for the durable poll cursor: the count commits before the adapter
// runs, a crash between commit and result reuses the same count and poll ID,
// backoff doubles with bounded jitter and honors clamped retry_after values,
// schedules are idempotent, and diagnostics deduplicate.

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

func testCursor() PollCursor {
	return PollCursor{
		WorkflowID:   "wf-1",
		NodeID:       "review",
		ActivationID: "act-1",
		GateID:       "pr-review",
		AdapterID:    "gh-review",
		SubjectHash:  "abc123",
	}
}

func newPollStore(t *testing.T) (*ControllerStore, string) {
	t.Helper()
	s := NewStore(t.TempDir())
	const id = "c1"
	if _, err := s.Create(id, 2); err != nil {
		t.Fatalf("Create: %v", err)
	}
	return s, id
}

func TestCommitPollBeforeInvoke(t *testing.T) {
	s, id := newPollStore(t)
	c := testCursor()
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	committed, err := s.CommitPoll(id, c, now)
	if err != nil {
		t.Fatalf("CommitPoll: %v", err)
	}
	if committed.PollCount != 1 {
		t.Fatalf("poll count = %d, want 1", committed.PollCount)
	}
	if committed.PollID == "" || committed.PollID != DerivePollID(id, c, 1) {
		t.Fatalf("poll id = %q, want the derived count-1 id", committed.PollID)
	}
	if !committed.LastPollAt.Equal(now) {
		t.Fatalf("last poll = %v, want %v", committed.LastPollAt, now)
	}
	if !committed.NextPollAt.IsZero() {
		t.Fatalf("next poll = %v, want zero while in flight", committed.NextPollAt)
	}
	// The commit is durable in the record.
	rec, ok, err := s.Get(id)
	if err != nil || !ok {
		t.Fatalf("Get: %v %v", rec, err)
	}
	stored := rec.PollCursors[PollCursorKey(c)]
	if stored == nil || stored.PollCount != 1 || stored.PollID != committed.PollID {
		t.Fatalf("stored cursor = %+v, want the committed checkpoint", stored)
	}
}

func TestCrashAfterCommitReusesCountAndPollID(t *testing.T) {
	s, id := newPollStore(t)
	c := testCursor()
	first, err := s.CommitPoll(id, c, time.Now())
	if err != nil {
		t.Fatalf("CommitPoll: %v", err)
	}
	// A crash before the result: the cursor is still in flight, so the next
	// pass commits the same count and poll ID instead of advancing.
	second, err := s.CommitPoll(id, c, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("re-commit after crash: %v", err)
	}
	if second.PollCount != first.PollCount || second.PollID != first.PollID {
		t.Fatalf("re-committed cursor = %+v, want count/id reused from %+v", second, first)
	}
	// A completed cycle (schedule recorded) advances the count and derives a
	// fresh poll ID.
	if _, err := s.SchedulePollRetry(id, second, time.Now().Add(time.Minute), 1); err != nil {
		t.Fatalf("SchedulePollRetry: %v", err)
	}
	third, err := s.CommitPoll(id, c, time.Now().Add(2*time.Minute))
	if err != nil {
		t.Fatalf("CommitPoll after cycle: %v", err)
	}
	if third.PollCount != 2 {
		t.Fatalf("poll count = %d, want 2", third.PollCount)
	}
	if third.PollID == first.PollID {
		t.Fatalf("poll id %q reused across polls", third.PollID)
	}
}

func TestEnsurePollCursorIdempotent(t *testing.T) {
	s, id := newPollStore(t)
	c := testCursor()
	firstAt := time.Now().Add(30 * time.Second)
	seed, created, err := s.EnsurePollCursor(id, c, firstAt)
	if err != nil || !created {
		t.Fatalf("EnsurePollCursor: created=%v err=%v", created, err)
	}
	if !seed.NextPollAt.Equal(firstAt) {
		t.Fatalf("first poll = %v, want %v", seed.NextPollAt, firstAt)
	}
	again, created, err := s.EnsurePollCursor(id, c, time.Now().Add(time.Hour))
	if err != nil || created {
		t.Fatalf("re-EnsurePollCursor: created=%v err=%v", created, err)
	}
	if !again.NextPollAt.Equal(firstAt) {
		t.Fatalf("existing cursor mutated: %+v", again)
	}
	if err := s.ClearPollCursor(id, c); err != nil {
		t.Fatalf("ClearPollCursor: %v", err)
	}
	rec, _, _ := s.Get(id)
	if _, ok := rec.PollCursors[PollCursorKey(c)]; ok {
		t.Fatalf("cursor survived clear: %+v", rec.PollCursors)
	}
}

func TestSchedulePollRetrySkipsUnchanged(t *testing.T) {
	s, id := newPollStore(t)
	c := testCursor()
	next := time.Now().Add(time.Minute)
	before, _, _ := s.Get(id)
	if _, err := s.SchedulePollRetry(id, c, next, 1); err != nil {
		t.Fatalf("SchedulePollRetry: %v", err)
	}
	after, _, _ := s.Get(id)
	if after.Revision != before.Revision+1 {
		t.Fatalf("revision = %d, want one event", after.Revision)
	}
	if _, err := s.SchedulePollRetry(id, c, next, 1); err != nil {
		t.Fatalf("duplicate SchedulePollRetry: %v", err)
	}
	same, _, _ := s.Get(id)
	if same.Revision != after.Revision {
		t.Fatalf("unchanged schedule appended an event: %d -> %d", after.Revision, same.Revision)
	}
}

func TestPollDueIncludesCrashedInFlight(t *testing.T) {
	s, id := newPollStore(t)
	now := time.Now()
	crashed := testCursor()
	if _, err := s.CommitPoll(id, crashed, now.Add(-time.Hour)); err != nil {
		t.Fatalf("CommitPoll: %v", err)
	}
	scheduled := testCursor()
	scheduled.GateID = "other-gate"
	if _, _, err := s.EnsurePollCursor(id, scheduled, now.Add(-time.Minute)); err != nil {
		t.Fatalf("EnsurePollCursor: %v", err)
	}
	future := testCursor()
	future.GateID = "third-gate"
	if _, _, err := s.EnsurePollCursor(id, future, now.Add(time.Hour)); err != nil {
		t.Fatalf("EnsurePollCursor: %v", err)
	}
	due, err := s.PollDue(id, now)
	if err != nil {
		t.Fatalf("PollDue: %v", err)
	}
	got := map[string]bool{}
	for _, c := range due {
		got[c.GateID] = true
	}
	if !got["pr-review"] || !got["other-gate"] {
		t.Fatalf("due = %v, want the crashed in-flight and elapsed cursors", got)
	}
	if got["third-gate"] {
		t.Fatalf("due = %v, want the future cursor excluded", got)
	}
	if earliest, ok, _ := s.NextPollTime(id); !ok || earliest.IsZero() {
		t.Fatalf("NextPollTime = %v %v, want the earliest schedule", earliest, ok)
	}
}

func TestPollBackoffSequence(t *testing.T) {
	// With no jitter the post-park sequence continues at 60s (the first poll
	// after a park waits the 30s base), doubling to the 5m cap.
	want := []time.Duration{60 * time.Second, 120 * time.Second,
		240 * time.Second, 5 * time.Minute, 5 * time.Minute}
	retry := 0
	for i, d := range want {
		got, next := PollBackoffDelay(pollBaseDelay, retry, nil, nil)
		if got != d {
			t.Fatalf("step %d: delay = %v, want %v", i, got, d)
		}
		retry = next
	}
}

func TestPollBackoffJitterBounds(t *testing.T) {
	delay, _ := PollBackoffDelay(pollBaseDelay, 1, nil, func() float64 { return 0 })
	if delay != 120*time.Second {
		t.Fatalf("mid jitter = %v, want 120s", delay)
	}
	lo, _ := PollBackoffDelay(pollBaseDelay, 1, nil, func() float64 { return -1 })
	if lo != 108*time.Second {
		t.Fatalf("low jitter = %v, want 108s", lo)
	}
	hi, _ := PollBackoffDelay(pollBaseDelay, 1, nil, func() float64 { return 1 })
	if hi != 132*time.Second {
		t.Fatalf("high jitter = %v, want 132s", hi)
	}
}

func TestPollBackoffHonorsClampedRetryAfter(t *testing.T) {
	requested := int64(600000) // 10m: clamped to the 5m cap
	delay, _ := PollBackoffDelay(pollBaseDelay, 0, &requested, nil)
	if delay != 5*time.Minute {
		t.Fatalf("clamped delay = %v, want 5m", delay)
	}
	requested = int64(500) // 500ms: raised to the 30s floor
	delay, _ = PollBackoffDelay(pollBaseDelay, 0, &requested, nil)
	if delay != 30*time.Second {
		t.Fatalf("clamped delay = %v, want 30s", delay)
	}
	requested = int64(200000) // 200s: inside the range, honored as-is
	delay, _ = PollBackoffDelay(pollBaseDelay, 0, &requested, nil)
	if delay != 200*time.Second {
		t.Fatalf("honored delay = %v, want 3m20s", delay)
	}
	requested = int64(45000)
	delay, _ = PollBackoffDelay(pollBaseDelay, 4, &requested, nil)
	if delay != 45*time.Second {
		t.Fatalf("honored delay = %v, want 45s (retry count ignored)", delay)
	}
}

func TestRecordDiagnosticDeduplicates(t *testing.T) {
	s, id := newPollStore(t)
	recorded, err := s.RecordDiagnostic(id, "adapter_unavailable", "gh-review", "not registered")
	if err != nil || !recorded {
		t.Fatalf("RecordDiagnostic: recorded=%v err=%v", recorded, err)
	}
	mid, _, _ := s.Get(id)
	if mid.Diagnostics["adapter_unavailable/gh-review"] != "not registered" {
		t.Fatalf("diagnostics = %+v", mid.Diagnostics)
	}
	recorded, err = s.RecordDiagnostic(id, "adapter_unavailable", "gh-review", "not registered")
	if err != nil || recorded {
		t.Fatalf("duplicate RecordDiagnostic recorded=%v err=%v", recorded, err)
	}
	same, _, _ := s.Get(id)
	if same.Revision != mid.Revision {
		t.Fatalf("duplicate diagnostic appended an event")
	}
	if err := s.ClearDiagnostic(id, "adapter_unavailable", "gh-review"); err != nil {
		t.Fatalf("ClearDiagnostic: %v", err)
	}
	after, _, _ := s.Get(id)
	if len(after.Diagnostics) != 0 {
		t.Fatalf("diagnostics after clear = %+v", after.Diagnostics)
	}
}

func TestPollCursorsRoundTripThroughSnapshot(t *testing.T) {
	s, id := newPollStore(t)
	c := testCursor()
	if _, err := s.CommitPoll(id, c, time.Now()); err != nil {
		t.Fatalf("CommitPoll: %v", err)
	}
	data, err := os.ReadFile(s.snapshotPath(id))
	if err != nil {
		t.Fatalf("writeSnapshotForTest: %v", err)
	}
	if !strings.Contains(string(data), `"poll_cursors"`) || !strings.Contains(string(data), `"poll_id"`) {
		t.Fatalf("snapshot = %s", data)
	}
	var rec ControllerRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	stored := rec.PollCursors[PollCursorKey(c)]
	if stored == nil || stored.PollCount != 1 {
		t.Fatalf("round-tripped cursor = %+v", stored)
	}
}
