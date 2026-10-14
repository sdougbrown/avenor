package workflow

// Internal tests for the Store persistence and recovery layer. These pin the
// store's durability contract across the crash window between appending events
// and replacing the snapshot, lease expiry/recovery on catalog, lock
// serialization, and the revision/idempotency guards. They use only the
// standard library.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/durablefile"
)

// newStore builds an ephemeral store under a fresh temp directory.
func newStore(t *testing.T) *Store {
	t.Helper()
	return New(t.TempDir())
}

// mustInstantiate applies CommandInstantiate to a zero snapshot and returns the
// resulting revision-1 snapshot with one pending "start" activation.
func mustInstantiate(t *testing.T, s *Store, wf WorkflowID) Snapshot {
	t.Helper()
	payload, err := json.Marshal(InstanceRecord{
		TemplateID:       "t1",
		TemplateVersion:  "1",
		TerminalOutcomes: []OutcomeName{"done"},
		EntryNodes:       []NodeID{"start"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	snap, err := s.ApplyCommand(wf, Command{
		Kind:             CommandInstantiate,
		ExpectedRevision: 0,
		IdempotencyKey:   "inst-" + string(wf),
		Identity:         ExecutionIdentity{WorkflowID: wf},
		Payload:          payload,
	})
	if err != nil {
		t.Fatalf("instantiate: %v", err)
	}
	return snap
}

// claimCmd builds a claim command against the first activation of snap.
func claimCmd(snap Snapshot) Command {
	actID := snap.Instance.Activations[0].ID
	return Command{
		Kind:             CommandClaim,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "claim",
		Identity: ExecutionIdentity{
			WorkflowID:   snap.Instance.WorkflowID,
			NodeID:       "start",
			ActivationID: actID,
		},
		LeaseID: "lease-1",
		Actor:   "alice",
	}
}

// readFileBytes is a small convenience for reading a file or failing.
func readFileBytes(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func TestStore_InstantiatePersists(t *testing.T) {
	s := newStore(t)
	wf := WorkflowID("wf1")
	snap := mustInstantiate(t, s, wf)

	if _, err := os.Stat(s.workflowPath(wf)); err != nil {
		t.Fatalf("workflow.json not written: %v", err)
	}
	if _, err := os.Stat(s.eventsPath(wf)); err != nil {
		t.Fatalf("events.ndjson not written: %v", err)
	}
	if snap.Instance.Revision != 1 {
		t.Fatalf("revision = %d, want 1", snap.Instance.Revision)
	}
	if snap.Instance.WorkflowID != wf {
		t.Fatalf("workflow id = %q, want %q", snap.Instance.WorkflowID, wf)
	}

	// The log must contain exactly one event: instantiated.
	lines, err := readLines(s.eventsPath(wf))
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 1 {
		t.Fatalf("log has %d lines, want 1", len(lines))
	}
	var e Event
	if err := json.Unmarshal(lines[0], &e); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}
	if e.Kind != EventInstantiated {
		t.Fatalf("event kind = %q, want %q", e.Kind, EventInstantiated)
	}
}

// TestCommitObserverPanicIsolation proves that a panicking commit observer is
// isolated from the command path: the command still commits and the panic
// does not propagate past ApplyCommand.
func TestCommitObserverPanicIsolation(t *testing.T) {
	s := newStore(t)
	s.SetCommitObserver(func(wf WorkflowID, snap Snapshot) {
		panic("observer must not break the command path")
	})
	snap := mustInstantiate(t, s, "wf1")
	if snap.Instance.Revision != 1 {
		t.Fatalf("command did not commit: revision = %d, want 1", snap.Instance.Revision)
	}
}

func TestStore_ApplyAdvancesRevisionAndRoundTrips(t *testing.T) {
	s := newStore(t)
	wf := WorkflowID("wf1")
	snap := mustInstantiate(t, s, wf)

	next, err := s.ApplyCommand(wf, claimCmd(snap))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if next.Instance.Revision != 2 {
		t.Fatalf("revision = %d, want 2", next.Instance.Revision)
	}

	// Re-read the persisted snapshot fresh from disk.
	var persisted Snapshot
	data := readFileBytes(t, s.workflowPath(wf))
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatalf("unmarshal persisted: %v", err)
	}
	if persisted.Instance.Revision != 2 {
		t.Fatalf("persisted revision = %d, want 2", persisted.Instance.Revision)
	}
	if len(persisted.Instance.Activations) == 0 {
		t.Fatal("no activations in persisted snapshot")
	}
	if persisted.Instance.Activations[0].Status != ActivationLeased {
		t.Fatalf("activation status = %q, want %q",
			persisted.Instance.Activations[0].Status, ActivationLeased)
	}
}

func TestStore_RevisionConflictRejected(t *testing.T) {
	s := newStore(t)
	wf := WorkflowID("wf1")
	snap := mustInstantiate(t, s, wf)

	before := readFileBytes(t, s.workflowPath(wf))

	// A claim with a wrong expected revision and a distinct idempotency key.
	cmd := claimCmd(snap)
	cmd.ExpectedRevision = 999
	cmd.IdempotencyKey = "claim-wrong-rev"
	if _, err := s.ApplyCommand(wf, cmd); err == nil {
		t.Fatal("expected revision conflict to be rejected, got nil error")
	} else if !containsStr(err.Error(), "revision") {
		t.Fatalf("error %q does not mention revision", err.Error())
	}

	after := readFileBytes(t, s.workflowPath(wf))
	if !reflect.DeepEqual(before, after) {
		t.Fatal("workflow.json changed despite rejected command")
	}
}

func TestStore_DuplicateIdempotencyRejected(t *testing.T) {
	s := newStore(t)
	wf := WorkflowID("wf1")
	snap := mustInstantiate(t, s, wf)

	before := readFileBytes(t, s.workflowPath(wf))

	// Re-issue the same instantiate command with the same idempotency key and
	// the correct current expected revision so the idempotency guard (not the
	// revision guard) is what rejects the retry.
	payload, err := json.Marshal(InstanceRecord{
		TemplateID:       "t1",
		TemplateVersion:  "1",
		TerminalOutcomes: []OutcomeName{"done"},
		EntryNodes:       []NodeID{"start"},
	})
	if err != nil {
		t.Fatal(err)
	}
	dup := Command{
		Kind:             CommandInstantiate,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "inst-" + string(wf),
		Identity:         ExecutionIdentity{WorkflowID: wf},
		Payload:          payload,
	}
	if _, err := s.ApplyCommand(wf, dup); err == nil {
		t.Fatal("expected duplicate idempotency to be rejected, got nil error")
	} else if !containsStr(err.Error(), "duplicate") {
		t.Fatalf("error %q does not mention duplicate", err.Error())
	}

	after := readFileBytes(t, s.workflowPath(wf))
	if !reflect.DeepEqual(before, after) {
		t.Fatal("workflow.json changed despite duplicate rejection")
	}
}

func TestStore_MissingInstanceRejected(t *testing.T) {
	s := newStore(t)
	wf := WorkflowID("wf1")
	cmd := Command{
		Kind:             CommandClaim,
		ExpectedRevision: 0,
		IdempotencyKey:   "claim",
		Identity: ExecutionIdentity{
			WorkflowID: wf,
			NodeID:     "start",
		},
		LeaseID: "lease-1",
		Actor:   "alice",
	}
	if _, err := s.ApplyCommand(wf, cmd); err == nil {
		t.Fatal("expected missing-instance rejection, got nil error")
	} else if !containsStr(err.Error(), "does not exist") {
		t.Fatalf("error %q does not mention does not exist", err.Error())
	}
}

func TestStore_CrashAfterEventAppendBeforeSnapshotReplace(t *testing.T) {
	// CONTRACT under test: crash-window durability. An event durably appended
	// to events.ndjson but not yet reflected in workflow.json must be replayed on
	// recovery, the snapshot advanced, and workflow.json rewritten to match. That
	// is what this test verifies (revision advances and the snapshot persists
	// past the replayed event).
	//
	// The observed ActivationLeaseExpired outcome is an EMERGENT consequence of
	// the Stage 3 reducer's applyLeased, which stamps no ExpiresAt (zero TTL) on
	// the recovered lease. Because there is no store/manager channel to assign a
	// real lease TTL yet (Stage 6 owns lease TTL), the recovered zero-expiry
	// lease is conservatively swept. This is expected absent-manager behavior,
	// NOT the point of this test; real leases with a future ExpiresAt are
	// retained and verified separately by TestStore_LeaseRetainedOnRecovery.
	t.Run("via recoverInstance", func(t *testing.T) {
		s := newStore(t)
		wf := WorkflowID("wf1")
		snap := mustInstantiate(t, s, wf)

		// Simulate the crash window: append the leased event directly to the
		// log without ever updating workflow.json.
		e := Event{
			ID:       NewEventID(),
			Kind:     EventLeased,
			Sequence: snap.Instance.Revision + 1,
			Identity: ExecutionIdentity{
				WorkflowID:   wf,
				NodeID:       "start",
				ActivationID: snap.Instance.Activations[0].ID,
			},
			LeaseID: "lease-crash",
			Actor:   "alice",
		}
		appendEventLine(t, s.eventsPath(wf), e)

		got, ok, err := s.recoverInstance(wf)
		if err != nil {
			t.Fatalf("recoverInstance: %v", err)
		}
		if !ok {
			t.Fatal("recoverInstance reported instance does not exist")
		}
		// The crash-window leased event IS the durable event under contract. On
		// recovery it is replayed (revision advances past it) and the snapshot
		// rewritten. The subsequent sweep of the recovered zero-expiry lease
		// (Stage 3 applyLeased stamps no ExpiresAt; Stage 6 owns lease TTL) is
		// conservative absent-manager behavior that happens to push the revision
		// once more. The revision and lease-expired assertions truthfully reflect
		// observable behavior; the durability contract is what we are checking.
		if got.Instance.Revision != 3 {
			t.Fatalf("recovered revision = %d, want 3", got.Instance.Revision)
		}
		act := got.Instance.Activations[0]
		if act.Status != ActivationLeaseExpired {
			t.Fatalf("recovered status = %q, want %q", act.Status, ActivationLeaseExpired)
		}
		if act.ActiveLease != nil {
			t.Fatalf("recovered lease = %+v, want nil after expiry sweep", act.ActiveLease)
		}

		// workflow.json must have been rewritten to match the recovered state.
		var persisted Snapshot
		data := readFileBytes(t, s.workflowPath(wf))
		if err := json.Unmarshal(data, &persisted); err != nil {
			t.Fatalf("unmarshal persisted: %v", err)
		}
		if persisted.Instance.Revision != got.Instance.Revision {
			t.Fatalf("persisted revision = %d, want %d",
				persisted.Instance.Revision, got.Instance.Revision)
		}
	})

	t.Run("via Catalog", func(t *testing.T) {
		s := newStore(t)
		wf := WorkflowID("wf1")
		snap := mustInstantiate(t, s, wf)

		e := Event{
			ID:       NewEventID(),
			Kind:     EventLeased,
			Sequence: snap.Instance.Revision + 1,
			Identity: ExecutionIdentity{
				WorkflowID:   wf,
				NodeID:       "start",
				ActivationID: snap.Instance.Activations[0].ID,
			},
			LeaseID: "lease-crash",
			Actor:   "alice",
		}
		appendEventLine(t, s.eventsPath(wf), e)

		list, err := s.Catalog()
		if err != nil {
			t.Fatalf("Catalog: %v", err)
		}
		if len(list) != 1 {
			t.Fatalf("catalog has %d entries, want 1", len(list))
		}
		if list[0].WorkflowID != wf {
			t.Fatalf("cataloged workflow = %q, want %q", list[0].WorkflowID, wf)
		}
		// Same recovery contract as the recoverInstance case: the durable
		// crash-window leased event is replayed and the snapshot rewritten past
		// it. The zero-expiry recovered lease is conservatively swept absent a
		// manager-assigned TTL (Stage 6 owns lease TTL); these assertions reflect
		// that observable behavior while the durability contract is the point.
		act := list[0].Snapshot.Instance.Activations[0]
		if act.Status != ActivationLeaseExpired || act.ActiveLease != nil {
			t.Fatalf("cataloged snapshot not recovered: status=%q lease=%+v", act.Status, act.ActiveLease)
		}
		if list[0].Snapshot.Instance.Revision != 3 {
			t.Fatalf("cataloged revision = %d, want 3", list[0].Snapshot.Instance.Revision)
		}
	})
}

func TestStore_SnapshotReplaceBeforeProjectionNoop(t *testing.T) {
	s := newStore(t)
	wf := WorkflowID("wf1")
	snap := mustInstantiate(t, s, wf)

	next, err := s.ApplyCommand(wf, claimCmd(snap))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if next.Instance.Revision != 2 {
		t.Fatalf("revision = %d, want 2", next.Instance.Revision)
	}
	// Projections are a Stage-4 no-op; the call must not error.
	var persisted Snapshot
	data := readFileBytes(t, s.workflowPath(wf))
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatalf("unmarshal persisted: %v", err)
	}
	if persisted.Instance.Revision != 2 {
		t.Fatalf("persisted revision = %d, want 2", persisted.Instance.Revision)
	}
}

func TestStore_LockContentionSerializes(t *testing.T) {
	s := newStore(t)
	wf := WorkflowID("wf1")
	snap := mustInstantiate(t, s, wf)

	// Hold the instance lock manually outside ApplyCommand.
	unlock, err := durablefile.Lock(s.lockPath(wf))
	if err != nil {
		t.Fatalf("lock: %v", err)
	}

	type claimResult struct {
		snap Snapshot
		err  error
	}
	ch := make(chan claimResult, 1)
	go func() {
		next, err := s.ApplyCommand(wf, claimCmd(snap))
		ch <- claimResult{next, err}
	}()

	// While we hold the lock, the goroutine must not complete.
	select {
	case res := <-ch:
		t.Fatalf("ApplyCommand unexpectedly completed while lock held: snap=%+v err=%v", res.snap, res.err)
	case <-time.After(100 * time.Millisecond):
	}

	if err := unlock(); err != nil {
		t.Fatalf("unlock: %v", err)
	}

	select {
	case res := <-ch:
		if res.err != nil {
			t.Fatalf("ApplyCommand after release: %v", res.err)
		}
		if res.snap.Instance.Revision != 2 {
			t.Fatalf("revision after claim = %d, want 2", res.snap.Instance.Revision)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ApplyCommand did not complete within 5s after lock release")
	}
}

func TestStore_MissingWorkflowJSONNotCataloged(t *testing.T) {
	s := newStore(t)
	wf := WorkflowID("wf2")
	if err := os.MkdirAll(s.instanceDir(wf), 0o755); err != nil {
		t.Fatal(err)
	}
	// Only an events log; no workflow.json.
	if err := os.WriteFile(s.eventsPath(wf), []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	list, err := s.Catalog()
	if err != nil {
		t.Fatalf("Catalog: %v", err)
	}
	for _, c := range list {
		if c.WorkflowID == wf {
			t.Fatalf("Catalog included %q which has no workflow.json", wf)
		}
	}
}

func TestStore_CatalogHappyPath(t *testing.T) {
	s := newStore(t)
	wfA := WorkflowID("wfA")
	wfB := WorkflowID("wfB")
	snapA := mustInstantiate(t, s, wfA)
	snapB := mustInstantiate(t, s, wfB)

	list, err := s.Catalog()
	if err != nil {
		t.Fatalf("Catalog: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("catalog has %d entries, want 2", len(list))
	}

	byID := map[WorkflowID]Snapshot{}
	for _, c := range list {
		byID[c.WorkflowID] = c.Snapshot
	}
	if sA, ok := byID[wfA]; !ok {
		t.Fatalf("missing %q in catalog", wfA)
	} else {
		assertCataloged(t, sA, snapA, wfA)
	}
	if sB, ok := byID[wfB]; !ok {
		t.Fatalf("missing %q in catalog", wfB)
	} else {
		assertCataloged(t, sB, snapB, wfB)
	}
}

func assertCataloged(t *testing.T, got, wantFromWrite Snapshot, wf WorkflowID) {
	t.Helper()
	if got.Instance.WorkflowID != wf {
		t.Fatalf("cataloged workflow = %q, want %q", got.Instance.WorkflowID, wf)
	}
	if got.Instance.Revision < 1 {
		t.Fatalf("cataloged revision = %d, want >= 1", got.Instance.Revision)
	}
	// The cataloged snapshot should match the revision the write returned.
	if got.Instance.Revision != wantFromWrite.Instance.Revision {
		t.Fatalf("cataloged revision = %d, want %d",
			got.Instance.Revision, wantFromWrite.Instance.Revision)
	}
}

func TestStore_LeaseExpiredOnRecovery(t *testing.T) {
	s := newStore(t)
	wf := WorkflowID("wf1")
	mustInstantiate(t, s, wf)

	// Corrupt the persisted snapshot's lease to be stale.
	data := readFileBytes(t, s.workflowPath(wf))
	var persisted Snapshot
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}
	actID := persisted.Instance.Activations[0].ID
	persisted.Instance.Activations[0].ActiveLease = &Lease{
		ID:           "lease-stale",
		ActivationID: actID,
		AcquiredAt:   time.Now().Add(-2 * time.Hour),
		ExpiresAt:    time.Now().Add(-time.Hour),
	}
	if err := os.WriteFile(s.workflowPath(wf), mustMarshalSnapshot(t, persisted), 0o644); err != nil {
		t.Fatal(err)
	}

	list, err := s.Catalog()
	if err != nil {
		t.Fatalf("Catalog: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("catalog has %d entries, want 1", len(list))
	}
	act := list[0].Snapshot.Instance.Activations[0]
	if act.Status != ActivationLeaseExpired {
		t.Fatalf("status = %q, want %q", act.Status, ActivationLeaseExpired)
	}
	if act.ActiveLease != nil {
		t.Fatalf("ActiveLease = %+v, want nil after expiry", act.ActiveLease)
	}

	// The recovery must have appended a lease.expired event.
	lines, err := readLines(s.eventsPath(wf))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, ln := range lines {
		var e Event
		if err := json.Unmarshal(ln, &e); err != nil {
			continue
		}
		if e.Kind == EventLeaseExpired && e.Reason == "recovery" {
			found = true
		}
	}
	if !found {
		t.Fatal("no EventLeaseExpired with reason recovery appended")
	}
}

func TestStore_LeaseRetainedOnRecovery(t *testing.T) {
	s := newStore(t)
	wf := WorkflowID("wf1")
	mustInstantiate(t, s, wf)

	// Give the persisted snapshot a future lease.
	data := readFileBytes(t, s.workflowPath(wf))
	var persisted Snapshot
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}
	actID := persisted.Instance.Activations[0].ID
	persisted.Instance.Activations[0].Status = ActivationLeased
	persisted.Instance.Activations[0].ActiveLease = &Lease{
		ID:           "lease-future",
		ActivationID: actID,
		AcquiredAt:   time.Now().Add(-time.Hour),
		ExpiresAt:    time.Now().Add(time.Hour),
	}
	if err := os.WriteFile(s.workflowPath(wf), mustMarshalSnapshot(t, persisted), 0o644); err != nil {
		t.Fatal(err)
	}
	beforeLines, err := readLines(s.eventsPath(wf))
	if err != nil {
		t.Fatal(err)
	}

	list, err := s.Catalog()
	if err != nil {
		t.Fatalf("Catalog: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("catalog has %d entries, want 1", len(list))
	}
	act := list[0].Snapshot.Instance.Activations[0]
	if act.ActiveLease == nil || act.ActiveLease.ID != "lease-future" {
		t.Fatalf("ActiveLease = %+v, want lease-future retained", act.ActiveLease)
	}
	if act.Status != ActivationLeased {
		t.Fatalf("status = %q, want %q (retained)", act.Status, ActivationLeased)
	}

	// The log must not have gained a lease.expired line.
	afterLines, err := readLines(s.eventsPath(wf))
	if err != nil {
		t.Fatal(err)
	}
	if len(afterLines) != len(beforeLines) {
		t.Fatalf("log grew from %d to %d lines without an expiry", len(beforeLines), len(afterLines))
	}
	for _, ln := range afterLines {
		var e Event
		if err := json.Unmarshal(ln, &e); err == nil && e.Kind == EventLeaseExpired {
			t.Fatalf("unexpected EventLeaseExpired in log: %+v", e)
		}
	}
}

// ---- helpers ----

// appendEventLine appends a single marshaled event line to an NDJSON log.
func appendEventLine(t *testing.T, path string, e Event) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	data, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		t.Fatal(err)
	}
}

// readLines splits an NDJSON file into its raw lines (without trailing '\n').
func readLines(path string) ([][]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var lines [][]byte
	start := 0
	for i, b := range data {
		if b == '\n' {
			lines = append(lines, data[start:i])
			start = i + 1
		}
	}
	if start < len(data) {
		lines = append(lines, data[start:])
	}
	return lines, nil
}

// mustMarshalSnapshot marshals a snapshot via its canonical MarshalJSON.
func mustMarshalSnapshot(t *testing.T, snap Snapshot) []byte {
	t.Helper()
	data, err := snap.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// containsStr reports whether s contains substr.
func containsStr(s, substr string) bool {
	return len(substr) == 0 || (len(s) >= len(substr) && containsAt(s, substr))
}

// containsAt implements a simple substring search.
func containsAt(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestStore_ReducerRejectedCommandLeavesLogClean is the regression test for
// issue #228: a command whose built events the reducer rejects must fail
// without writing anything to the event log, and the instance must remain
// loadable and advanceable. Before the reduce-before-append reorder in
// applyLocked, the rejected batch was already fsynced and every later replay
// failed on it, bricking the instance.
func TestStore_ReducerRejectedCommandLeavesLogClean(t *testing.T) {
	s := newStore(t)
	wf := WorkflowID("wf-reject")
	snap := mustInstantiate(t, s, wf)

	beforeLines, err := readLines(s.eventsPath(wf))
	if err != nil {
		t.Fatal(err)
	}

	// child_outcome against the entry activation: Apply builds the event batch
	// (it checks only revision and idempotency), but the reducer requires the
	// activation to be awaiting_child, which a fresh entry activation is not.
	actID := snap.Instance.Activations[0].ID
	childOutcomeCmd := func(key string) Command {
		return Command{
			Kind:             CommandChildOutcome,
			ExpectedRevision: snap.Instance.Revision,
			IdempotencyKey:   key,
			Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: "start", ActivationID: actID, AttemptID: "att-1"},
			Outcome:          "done",
		}
	}
	_, err = s.ApplyCommand(wf, childOutcomeCmd("child-outcome-reject"))
	if err == nil {
		t.Fatal("expected the reducer to reject the child_outcome command, got nil error")
	}
	// The rejection must come from the reduce phase, not an earlier command
	// guard: otherwise the log-clean property would hold trivially and the
	// test would no longer guard append-before-reduce reintroduction.
	if !containsStr(err.Error(), "cannot resolve child outcome for activation in status") {
		t.Fatalf("expected the reducer's own rejection, got %q", err)
	}

	afterLines, err := readLines(s.eventsPath(wf))
	if err != nil {
		t.Fatal(err)
	}
	if len(afterLines) != len(beforeLines) {
		t.Fatalf("event log grew from %d to %d lines on a rejected command", len(beforeLines), len(afterLines))
	}
	for i := range afterLines {
		if i >= len(beforeLines) || string(afterLines[i]) != string(beforeLines[i]) {
			t.Fatalf("event log changed at line %d on a rejected command", i)
		}
	}

	// The instance must still load cleanly.
	loaded, exists, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent after rejection: %v", err)
	}
	if !exists {
		t.Fatal("instance disappeared after rejection")
	}
	if loaded.Instance.Revision != snap.Instance.Revision {
		t.Fatalf("revision = %d, want unchanged %d", loaded.Instance.Revision, snap.Instance.Revision)
	}

	// And the command path must still work: a distinct command fails with the
	// reducer's own rejection, not a replay failure from a poisoned log.
	_, err = s.ApplyCommand(wf, childOutcomeCmd("child-outcome-retry"))
	if err == nil {
		t.Fatal("expected the retry to be rejected by the reducer, got nil error")
	}
	if !containsStr(err.Error(), "cannot resolve child outcome for activation in status") {
		t.Fatalf("expected the retry to fail in the reduce phase, got %q", err)
	}
}

// storeTemplateFixture builds a minimal one-node manual template with the
// given id and version for StoreTemplate tests.
func storeTemplateFixture(t *testing.T, id, version string) Template {
	t.Helper()
	data := fmt.Sprintf(`{"schema_version":1,"template_id":%q,"template_version":%q,"entry_nodes":["start"],"nodes":[{"id":"start","action":{"type":"manual"}}],"terminal_outcomes":["done"]}`, id, version)
	var tmpl Template
	if err := json.Unmarshal([]byte(data), &tmpl); err != nil {
		t.Fatalf("unmarshal fixture: %v", err)
	}
	return tmpl
}

// TestStoreTemplateIdenticalRecreateIsNoOp asserts re-registering a template
// with identical content under the same id+version succeeds without touching
// the stored file: the stored bytes are unchanged after the second store.
func TestStoreTemplateIdenticalRecreateIsNoOp(t *testing.T) {
	s := newStore(t)
	tmpl := storeTemplateFixture(t, "dup", "1")
	if err := s.StoreTemplate("dup", "1", tmpl); err != nil {
		t.Fatalf("first StoreTemplate: %v", err)
	}
	path := filepath.Join(s.root, "templates", "dup", "1.json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read stored template: %v", err)
	}
	if err := s.StoreTemplate("dup", "1", tmpl); err != nil {
		t.Fatalf("identical re-store: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read stored template after re-store: %v", err)
	}
	if string(before) != string(after) {
		t.Fatal("identical re-store replaced the stored template file")
	}
}

// TestStoreTemplateRejectsConflictingContent asserts storing different content
// under an already-registered id+version fails with a TemplateConflictError
// and leaves the originally stored template intact.
func TestStoreTemplateRejectsConflictingContent(t *testing.T) {
	s := newStore(t)
	if err := s.StoreTemplate("dup", "1", storeTemplateFixture(t, "dup", "1")); err != nil {
		t.Fatalf("first StoreTemplate: %v", err)
	}
	conflicting := storeTemplateFixture(t, "dup", "1")
	conflicting.Nodes[0].Name = "renamed"
	err := s.StoreTemplate("dup", "1", conflicting)
	if err == nil {
		t.Fatal("expected conflicting re-store to fail, got nil error")
	}
	var conflict *TemplateConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("error = %T (%v), want *TemplateConflictError", err, err)
	}
	if conflict.TemplateID != "dup" || conflict.TemplateVersion != "1" {
		t.Fatalf("conflict names %q/%q, want dup/1", conflict.TemplateID, conflict.TemplateVersion)
	}
	loaded, err := s.LoadTemplate("dup", "1")
	if err != nil {
		t.Fatalf("LoadTemplate after conflict: %v", err)
	}
	if loaded.Nodes[0].Name == "renamed" {
		t.Fatal("original template was replaced by the conflicting content")
	}
}

// TestStoreTemplateConcurrentConflictingCreateHasOneWinner hammers
// StoreTemplate with concurrent creates of different content under the same
// id+version and asserts exactly one registration wins: the losing callers
// all get a TemplateConflictError and the stored file holds the winner's
// content, never a mixture or a silent overwrite by a later writer.
func TestStoreTemplateConcurrentConflictingCreateHasOneWinner(t *testing.T) {
	s := newStore(t)
	const writers = 32
	errs := make([]error, writers)

	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tmpl := storeTemplateFixture(t, "race", "1")
			tmpl.Nodes[0].Name = fmt.Sprintf("writer-%d", i)
			errs[i] = s.StoreTemplate("race", "1", tmpl)
		}(i)
	}
	wg.Wait()

	winners := 0
	winnerIndex := -1
	for i, err := range errs {
		if err == nil {
			winners++
			winnerIndex = i
			continue
		}
		var conflict *TemplateConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("writer %d: error = %T (%v), want *TemplateConflictError", i, err, err)
		}
	}
	if winners != 1 {
		t.Fatalf("winning registrations = %d, want exactly 1", winners)
	}
	loaded, err := s.LoadTemplate("race", "1")
	if err != nil {
		t.Fatalf("LoadTemplate after race: %v", err)
	}
	if want := fmt.Sprintf("writer-%d", winnerIndex); loaded.Nodes[0].Name != want {
		t.Fatalf("stored template holds node name %q, want the single winner's %q", loaded.Nodes[0].Name, want)
	}
}

// TestStoreTemplateCorruptStoredFileIsNotAConflict asserts a stored template
// file that cannot be parsed reports a TemplateCorruptError rather than
// claiming the content differs, and leaves the stored file untouched.
func TestStoreTemplateCorruptStoredFileIsNotAConflict(t *testing.T) {
	s := newStore(t)
	dir := filepath.Join(s.root, "templates", "corrupt")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	path := filepath.Join(dir, "1.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatalf("write corrupt template: %v", err)
	}
	err := s.StoreTemplate("corrupt", "1", storeTemplateFixture(t, "corrupt", "1"))
	if err == nil {
		t.Fatal("expected corrupt stored template to fail, got nil error")
	}
	var corrupt *TemplateCorruptError
	if !errors.As(err, &corrupt) {
		t.Fatalf("error = %T (%v), want *TemplateCorruptError", err, err)
	}
	if errors.As(err, new(*TemplateConflictError)) {
		t.Fatal("corrupt stored template was misreported as a content conflict")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read stored template after corrupt error: %v", err)
	}
	if string(after) != "{not json" {
		t.Fatalf("stored template was modified: %q", after)
	}
}

// TestStoreTemplateAcceptsCanonicallyEquivalentRestores asserts the
// canonicalization fallback: a stored file whose bytes are not the canonical
// serialization (different key order and spacing) but decode to a
// semantically equal template is accepted as a no-op re-store.
func TestStoreTemplateAcceptsCanonicallyEquivalentRestores(t *testing.T) {
	s := newStore(t)
	tmpl := storeTemplateFixture(t, "canon", "1")
	if err := s.StoreTemplate("canon", "1", tmpl); err != nil {
		t.Fatalf("first StoreTemplate: %v", err)
	}
	// Rewrite the stored file with different key order and spacing.
	nonCanonical := `{"terminal_outcomes":["done"],"nodes":[{"action":{"type":"manual"},"id":"start"}],"entry_nodes":["start"],"template_version":"1","template_id":"canon","schema_version":1}`
	path := filepath.Join(s.root, "templates", "canon", "1.json")
	if err := os.WriteFile(path, []byte(nonCanonical), 0o644); err != nil {
		t.Fatalf("rewrite stored template: %v", err)
	}
	if err := s.StoreTemplate("canon", "1", tmpl); err != nil {
		t.Fatalf("canonically equivalent re-store: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read stored template after re-store: %v", err)
	}
	if string(after) != nonCanonical {
		t.Fatal("canonically equivalent re-store replaced the stored file")
	}
}

// TestStoreTemplateCanonicalizesMetadataNumbers pins the canonicalization
// fallback against number-formatting drift (the decodeStrict path decodes
// with UseNumber, so metadata values become json.Number): a stored template
// whose metadata numbers are formatted differently from the incoming
// canonical form ("1.0" vs 1.0) must be judged by semantic equality, not raw
// bytes — equal content is a no-op, different content is a conflict.
func TestStoreTemplateCanonicalizesMetadataNumbers(t *testing.T) {
	s := newStore(t)
	tmpl := storeTemplateFixture(t, "num", "1")
	tmpl.Metadata = map[string]any{"limit": json.Number("1.0"), "name": "x"}
	if err := s.StoreTemplate("num", "1", tmpl); err != nil {
		t.Fatalf("first StoreTemplate: %v", err)
	}
	// Same semantic metadata with an equivalent-but-differently-keyed stored
	// file: still a no-op.
	same := storeTemplateFixture(t, "num", "1")
	same.Metadata = map[string]any{"name": "x", "limit": json.Number("1.0")}
	if err := s.StoreTemplate("num", "1", same); err != nil {
		t.Fatalf("equivalent re-store with json.Number metadata: %v", err)
	}
	// Different metadata values under the same id+version: conflict.
	diff := storeTemplateFixture(t, "num", "1")
	diff.Metadata = map[string]any{"name": "x", "limit": json.Number("2.0")}
	err := s.StoreTemplate("num", "1", diff)
	var conflict *TemplateConflictError
	if err == nil || !errors.As(err, &conflict) {
		t.Fatalf("re-store with different metadata = %v, want TemplateConflictError", err)
	}
}
