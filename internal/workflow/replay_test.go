package workflow

// Internal tests for replayEvents, the crash-window recovery primitive that
// replays an NDJSON event log beyond the snapshot's applied revision. These
// cover missing/empty logs, replay-beyond-snapshot, idempotent no-op replay,
// trailing-line truncation, interior corruption, and reducer error
// propagation. They use only the standard library.

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

// writeEvents appends complete JSON event lines to an NDJSON path.
func writeEvents(t *testing.T, path string, events ...Event) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for _, e := range events {
		b, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(append(b, '\n')); err != nil {
			t.Fatal(err)
		}
	}
}

// baseSnap builds the reduced initial snapshot (revision 1, one pending start
// activation) purely through the reducer, without touching a Store.
func baseSnap(t *testing.T) Snapshot {
	t.Helper()
	payload, err := json.Marshal(InstanceRecord{
		TemplateID:      "t1",
		TemplateVersion: "1",
		EntryNodes:      []NodeID{"start"},
	})
	if err != nil {
		t.Fatal(err)
	}
	cmd := Command{
		Kind:             CommandInstantiate,
		ExpectedRevision: 0,
		IdempotencyKey:   "inst",
		Identity:         ExecutionIdentity{WorkflowID: "wf1"},
		Payload:          payload,
	}
	events, err := Apply(Snapshot{}, cmd)
	if err != nil {
		t.Fatal(err)
	}
	snap := Snapshot{}
	for _, e := range events {
		snap, err = Reduce(snap, e)
		if err != nil {
			t.Fatal(err)
		}
	}
	return snap
}

// leasedEvent builds the EventLeased that claims activation[0] of baseSnap at
// sequence 2.
func leasedEvent(t *testing.T, base Snapshot) Event {
	t.Helper()
	return Event{
		ID:       NewEventID(),
		Kind:     EventLeased,
		Sequence: base.Instance.Revision + 1,
		Identity: ExecutionIdentity{
			WorkflowID:   base.Instance.WorkflowID,
			NodeID:       "start",
			ActivationID: base.Instance.Activations[0].ID,
		},
		LeaseID: "lease-r",
		Actor:   "alice",
	}
}

func TestReplay_MissingLog(t *testing.T) {
	base := baseSnap(t)
	path := t.TempDir() + "/missing.ndjson"

	got, n, tr, err := replayEvents(base, path)
	if err != nil {
		t.Fatalf("replayEvents: %v", err)
	}
	if n != 0 {
		t.Fatalf("n = %d, want 0", n)
	}
	if tr != 0 {
		t.Fatalf("tr = %d, want 0", tr)
	}
	if !reflect.DeepEqual(got, base) {
		t.Fatal("snapshot changed for a missing log")
	}
}

func TestReplay_EmptyLog(t *testing.T) {
	base := baseSnap(t)
	path := t.TempDir() + "/empty.ndjson"
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	got, n, tr, err := replayEvents(base, path)
	if err != nil {
		t.Fatalf("replayEvents: %v", err)
	}
	if n != 0 {
		t.Fatalf("n = %d, want 0", n)
	}
	if tr != 0 {
		t.Fatalf("tr = %d, want 0", tr)
	}
	if !reflect.DeepEqual(got, base) {
		t.Fatal("snapshot changed for an empty log")
	}
}

func TestReplay_ReplayBeyondSnapshot(t *testing.T) {
	base := baseSnap(t)
	path := t.TempDir() + "/events.ndjson"
	ev := leasedEvent(t, base)
	writeEvents(t, path, ev)

	got, n, tr, err := replayEvents(base, path)
	if err != nil {
		t.Fatalf("replayEvents: %v", err)
	}
	if n != 1 {
		t.Fatalf("n = %d, want 1", n)
	}
	if tr != 0 {
		t.Fatalf("tr = %d, want 0", tr)
	}
	if got.Instance.Revision != 2 {
		t.Fatalf("revision = %d, want 2", got.Instance.Revision)
	}
	act := got.Instance.Activations[0]
	if act.Status != ActivationLeased {
		t.Fatalf("status = %q, want %q", act.Status, ActivationLeased)
	}
	if act.ActiveLease == nil || act.ActiveLease.ID != "lease-r" {
		t.Fatalf("ActiveLease = %+v, want lease-r", act.ActiveLease)
	}
}

func TestReplay_IdempotentAlreadyApplied(t *testing.T) {
	base := baseSnap(t)
	ev := leasedEvent(t, base)

	// Reduce the event once to produce a snapshot that already carries its ID.
	s2, err := Reduce(base, ev)
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/events.ndjson"
	writeEvents(t, path, ev)

	// Replaying against s2 (which already applied ev) must be a no-op.
	got, n, tr, err := replayEvents(s2, path)
	if err != nil {
		t.Fatalf("replayEvents: %v", err)
	}
	if !reflect.DeepEqual(got, s2) {
		t.Fatal("snapshot changed on idempotent replay")
	}
	// The event line is processed but reduces to a no-op.
	if n != 1 {
		t.Fatalf("n = %d, want 1 (line processed as no-op)", n)
	}
	if tr != 0 {
		t.Fatalf("tr = %d, want 0", tr)
	}
	if len(got.Instance.Activations) != 1 {
		t.Fatalf("activation count = %d, want 1 (no double transition)", len(got.Instance.Activations))
	}
	if got.Instance.Revision != 2 {
		t.Fatalf("revision = %d, want 2 (unchanged)", got.Instance.Revision)
	}
}

func TestReplay_TruncatedFinalLine(t *testing.T) {
	base := baseSnap(t)
	ev := leasedEvent(t, base)
	path := t.TempDir() + "/events.ndjson"

	// A valid line, then a partial line with no trailing newline.
	writeEvents(t, path, ev)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"id":"wfe_partial","kind":"workflow.event.`); err != nil {
		f.Close()
		t.Fatal(err)
	}
	f.Close()

	goodLine := mustMarshalLine(t, ev)

	got, n, tr, err := replayEvents(base, path)
	if err != nil {
		t.Fatalf("replayEvents: %v", err)
	}
	if n != 1 {
		t.Fatalf("n = %d, want 1", n)
	}
	if tr != int64(len(goodLine)) {
		t.Fatalf("tr = %d, want %d (size of valid line)", tr, len(goodLine))
	}
	if got.Instance.Revision != 2 {
		t.Fatalf("revision = %d, want 2", got.Instance.Revision)
	}
	if got.Instance.Activations[0].Status != ActivationLeased {
		t.Fatalf("status = %q, want %q", got.Instance.Activations[0].Status, ActivationLeased)
	}

	// The file must have been truncated back to the last good line.
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != tr {
		t.Fatalf("file size = %d, want %d", st.Size(), tr)
	}
}

func TestReplay_MalformedInteriorLine(t *testing.T) {
	base := baseSnap(t)
	ev := leasedEvent(t, base)
	path := t.TempDir() + "/events.ndjson"

	// valid, garbage, then another complete event.
	writeEvents(t, path, ev)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("this is not json\n"); err != nil {
		f.Close()
		t.Fatal(err)
	}
	f.Close()
	writeEvents(t, path, leasedEvent(t, base))

	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	got, n, tr, err := replayEvents(base, path)
	if err == nil {
		t.Fatal("expected corruption error, got nil")
	}
	if !containsStr2(err.Error(), "corrupted") {
		t.Fatalf("error %q does not mention corrupted", err.Error())
	}
	if tr != 0 {
		t.Fatalf("tr = %d, want 0 on error path", tr)
	}
	_ = n
	_ = got

	// The file must NOT have been truncated on the error path.
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("file size changed from %d to %d on error path", before.Size(), after.Size())
	}
}

func TestReplay_ReduceErrorPropagates(t *testing.T) {
	base := baseSnap(t)
	if base.Instance.Activations[0].ActiveLease != nil {
		t.Fatal("base activation unexpectedly has a lease")
	}
	// EventLeaseExpired against an activation with no active lease must error.
	ev := Event{
		ID:       NewEventID(),
		Kind:     EventLeaseExpired,
		Sequence: base.Instance.Revision + 1,
		Identity: ExecutionIdentity{
			WorkflowID:   base.Instance.WorkflowID,
			NodeID:       "start",
			ActivationID: base.Instance.Activations[0].ID,
		},
		LeaseID: "lease-none",
	}
	path := t.TempDir() + "/events.ndjson"
	writeEvents(t, path, ev)

	_, _, _, err := replayEvents(base, path)
	if err == nil {
		t.Fatal("expected reducer error to propagate, got nil")
	}
	if !containsStr2(err.Error(), "active lease") {
		t.Fatalf("error %q does not mention active lease", err.Error())
	}
}

// mustMarshalLine marshals an event into a single line including trailing '\n'.
func mustMarshalLine(t *testing.T, e Event) []byte {
	t.Helper()
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return append(b, '\n')
}

// containsStr2 is a local substring helper (standard library only).
func containsStr2(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
