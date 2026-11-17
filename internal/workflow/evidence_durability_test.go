package workflow

// evidence_durability_test.go covers the store-and-evidence contract around the
// crash window between the durable event append and the materialized snapshot
// replace. A command whose events are (or may be) durable must keep the evidence
// those events reference. A replay then never recovers a reference to a deleted
// file, and an uncommitted command's staged copies are still removed.

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// failNextSnapshotReplace installs the store's crash-window fault for one
// command: the events are appended durably, then the snapshot replace fails.
// Filesystem-level injection cannot isolate that write, because every command
// also repaves the snapshot from the log first.
func failNextSnapshotReplace(t *testing.T, s *Store) {
	t.Helper()
	s.SetSnapshotWriteFault(func(WorkflowID, Snapshot) error { return errors.New("injected snapshot replace failure") })
	t.Cleanup(func() { s.SetSnapshotWriteFault(nil) })
}

// stagedEvidenceCopies returns every staged evidence file under the instance,
// keyed by its path relative to the instance directory, so a test can assert
// exact bytes without knowing the evidence ID the store minted.
func stagedEvidenceCopies(t *testing.T, s *Store, wf WorkflowID) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	root := filepath.Join(s.Root(), "instances", string(wf), "evidence")
	entries, err := os.ReadDir(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return out
		}
		t.Fatalf("read evidence dir: %v", err)
	}
	for _, entry := range entries {
		copyDir := filepath.Join(root, entry.Name())
		files, err := os.ReadDir(copyDir)
		if err != nil {
			t.Fatalf("read evidence copy dir %s: %v", copyDir, err)
		}
		for _, file := range files {
			data, err := os.ReadFile(filepath.Join(copyDir, file.Name()))
			if err != nil {
				t.Fatalf("read staged copy %s: %v", file.Name(), err)
			}
			out[filepath.Join("evidence", entry.Name(), file.Name())] = data
		}
	}
	return out
}

// artifactCompletionPlan drives the single-node files-contract fixture to a
// running activation holding a live lease and a succeeded terminal fact, and
// returns the completion plan that stages one artifact of known bytes.
func artifactCompletionPlan(t *testing.T, m *Manager, s *Store, wf WorkflowID) (*AutoCompletion, ActivationID, AttemptID, LeaseID, string) {
	t.Helper()
	res, actID, attemptID := completeToRunning(t, m, s, wf)
	src := filepath.Join(t.TempDir(), "evidence.txt")
	body := "collected output\n"
	if err := os.WriteFile(src, []byte(body), StateFilePerm); err != nil {
		t.Fatalf("write artifact: %v", err)
	}
	plan := &AutoCompletion{
		Outcome:   "done",
		Outputs:   []CompleteOutput{{DefinitionID: "summary", Value: json.RawMessage(`"ready"`)}},
		Artifacts: []CompleteArtifact{{SrcPath: src, StoredPath: "evidence.txt", NonEmpty: true}},
	}
	leaseID, _ := res["lease_id"].(string)
	ownerToken, _ := res["owner_token"].(string)
	return plan, actID, attemptID, LeaseID(leaseID), ownerToken
}

// TestCompleteEvidenceSurvivesSnapshotWriteFailure proves the completion's
// evidence is kept when its events are durable but the snapshot replace failed. A
// replay then recovers the reference and the exact bytes.
func TestCompleteEvidenceSurvivesSnapshotWriteFailure(t *testing.T) {
	t.Parallel()
	m, s, wf := newCompleteFixture(t, completeTemplateJSON, "complete-test", "1")
	plan, actID, attemptID, leaseID, ownerToken := artifactCompletionPlan(t, m, s, wf)
	want := "collected output\n"

	failNextSnapshotReplace(t, s)
	_, err := m.CompleteAuto(wf, "start", actID, attemptID, leaseID, ownerToken, plan)
	s.SetSnapshotWriteFault(nil)
	if !errors.Is(err, ErrCommandDurable) {
		t.Fatalf("error = %v, want ErrCommandDurable (events durable, snapshot behind)", err)
	}

	// The appended events reference this exact copy: it must still be on disk.
	staged := stagedEvidenceCopies(t, s, wf)
	if len(staged) != 1 {
		t.Fatalf("staged evidence = %d copies, want the durable command's one", len(staged))
	}
	for _, data := range staged {
		if string(data) != want {
			t.Fatalf("staged bytes = %q, want the artifact", data)
		}
	}

	// A real replay rebuilds the snapshot from the durable log.
	replayed, exists, err := s.recoverInstance(wf)
	if err != nil || !exists {
		t.Fatalf("recoverInstance: %v (exists %v)", err, exists)
	}
	if replayed.Instance.Status != WorkflowCompleted {
		t.Fatalf("replayed status = %s, want the committed completion recovered", replayed.Instance.Status)
	}
	if len(replayed.Instance.Evidence) != 1 {
		t.Fatalf("replayed evidence = %+v, want the completion's artifact", replayed.Instance.Evidence)
	}
	evidence := replayed.Instance.Evidence[0]
	if _, ok := staged[evidence.StoredPath]; !ok {
		t.Fatalf("replayed evidence points at %q, which was deleted", evidence.StoredPath)
	}
	stored, err := os.ReadFile(filepath.Join(s.Root(), "instances", string(wf), evidence.StoredPath))
	if err != nil {
		t.Fatalf("read replayed evidence: %v", err)
	}
	if string(stored) != want || sha256Hex(string(stored)) != evidence.SHA256 {
		t.Fatalf("replayed copy = %q (digest %s), want the exact staged bytes", stored, evidence.SHA256)
	}
	if evidence.ActivationID != actID {
		t.Fatalf("replayed evidence activation = %s, want %s", evidence.ActivationID, actID)
	}
}

// TestCompleteEvidenceRetryAfterSnapshotWriteFailureIsIdempotent proves a
// repeated completion after the durable-but-unmaterialized command is an
// idempotent no-op: no second evidence entry, no orphaned copy from the retry.
func TestCompleteEvidenceRetryAfterSnapshotWriteFailureIsIdempotent(t *testing.T) {
	t.Parallel()
	m, s, wf := newCompleteFixture(t, completeTemplateJSON, "complete-test", "1")
	plan, actID, attemptID, leaseID, ownerToken := artifactCompletionPlan(t, m, s, wf)

	failNextSnapshotReplace(t, s)
	if _, err := m.CompleteAuto(wf, "start", actID, attemptID, leaseID, ownerToken, plan); !errors.Is(err, ErrCommandDurable) {
		s.SetSnapshotWriteFault(nil)
		t.Fatalf("error = %v, want ErrCommandDurable", err)
	}
	s.SetSnapshotWriteFault(nil)

	out, err := m.CompleteAuto(wf, "start", actID, attemptID, leaseID, ownerToken, plan)
	if err != nil {
		t.Fatalf("retried CompleteAuto: %v", err)
	}
	if result, ok := out.(map[string]any); !ok || result["already_completed"] != true {
		t.Fatalf("retried completion = %#v, want the idempotent shape", out)
	}
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if len(snap.Instance.Evidence) != 1 {
		t.Fatalf("instance evidence = %+v, want the one committed artifact", snap.Instance.Evidence)
	}
	if staged := stagedEvidenceCopies(t, s, wf); len(staged) != 1 {
		t.Fatalf("staged evidence = %d copies, want the retry's copy discarded", len(staged))
	}
}

// TestUncommittedAppendRemovesStagedEvidence proves the other half of the rule.
// When the event append itself fails, nothing is durable, so the staged copies are
// removed again and an uncommitted command leaks no evidence.
func TestUncommittedAppendRemovesStagedEvidence(t *testing.T) {
	t.Parallel()
	m, s, wf := newCompleteFixture(t, completeTemplateJSON, "complete-test", "1")
	plan, actID, attemptID, leaseID, ownerToken := artifactCompletionPlan(t, m, s, wf)

	injected := errors.New("injected append failure")
	s.SetAppendFault(func(string) error { return injected })
	t.Cleanup(func() { s.SetAppendFault(nil) })
	_, err := m.CompleteAuto(wf, "start", actID, attemptID, leaseID, ownerToken, plan)
	if err == nil {
		t.Fatal("a completion with an unwritable event log was accepted")
	}
	if errors.Is(err, ErrCommandDurable) {
		t.Fatalf("error = %v, want an uncommitted failure", err)
	}
	if staged := stagedEvidenceCopies(t, s, wf); len(staged) != 0 {
		t.Fatalf("staged evidence = %+v, want the uncommitted artifact removed", staged)
	}
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if len(snap.Instance.Evidence) != 0 {
		t.Fatalf("instance evidence = %+v, want none", snap.Instance.Evidence)
	}
}

// TestAppendEventsRollsBackAPartialBatch proves a batch that fails part-way is
// truncated back to the log's previous length. "Append failed" then means "not
// durable", so its evidence may be removed without stranding a replayed reference.
func TestAppendEventsRollsBackAPartialBatch(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	wf := WorkflowID("wf-partial")
	mustInstantiate(t, s, wf)

	info, err := os.Stat(s.eventsPath(wf))
	if err != nil {
		t.Fatalf("stat event log: %v", err)
	}
	before := info.Size()
	// The second event cannot be marshaled (an invalid raw JSON value), so the
	// first one lands and the batch then fails part-way.
	err = s.appendEvents(wf, []Event{
		{ID: NewEventID(), Kind: EventLeased, Sequence: 1},
		{ID: NewEventID(), Kind: EventStarted, Sequence: 2,
			Evidence: []Evidence{{ID: "ev-bad", Result: json.RawMessage("{not json")}}},
	})
	if err == nil {
		t.Fatal("a batch with an unmarshalable event was appended")
	}
	if errors.Is(err, ErrCommandDurable) {
		t.Fatalf("error = %v, want the rollback to have succeeded", err)
	}
	after, err := os.Stat(s.eventsPath(wf))
	if err != nil {
		t.Fatalf("stat event log: %v", err)
	}
	if after.Size() != before {
		t.Fatalf("event log size = %d, want it rolled back to %d", after.Size(), before)
	}
	// The rollback leaves a readable log: replay yields the instance unchanged.
	snap, exists, err := s.recoverInstance(wf)
	if err != nil || !exists {
		t.Fatalf("recoverInstance: %v (exists %v)", err, exists)
	}
	if snap.Instance.Revision != 1 {
		t.Fatalf("replayed revision = %d, want the instantiation only", snap.Instance.Revision)
	}
}

// TestRollbackEventAppendReportsDurableWhenItCannotTruncate pins the fallback
// contract. A rollback that cannot truncate reports ErrCommandDurable, because the
// log may still hold a prefix of the failed batch and a replay could recover it.
func TestRollbackEventAppendReportsDurableWhenItCannotTruncate(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "events.ndjson")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, StateFilePerm)
	if err != nil {
		t.Fatalf("create event log: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close event log: %v", err)
	}
	// A closed file cannot be truncated: exactly the fallback's failure shape.
	err = rollbackEventAppend(f, 0)
	if !errors.Is(err, ErrCommandDurable) {
		t.Fatalf("error = %v, want ErrCommandDurable (the log may hold a batch prefix)", err)
	}
}

// TestDiscardStagedNamesTheOrphanItCouldNotRemove proves a failed call reports the
// copies it could not remove alongside its own failure. The original error keeps
// its identity, and the orphan is named instead of being dropped silently: it
// still holds the staged content.
func TestDiscardStagedNamesTheOrphanItCouldNotRemove(t *testing.T) {
	t.Parallel()
	m, s, wf, _ := newManagerFixture(t)
	evID := EvidenceID("ev-orphan")
	if err := os.MkdirAll(evidenceDir(s.instanceDir(wf), evID), StateDirPerm); err != nil {
		t.Fatalf("create evidence dir: %v", err)
	}
	blockEvidenceRemoval(t, s)

	cause := errors.New("stage evidence failed")
	err := discardStaged(m, wf, []StagedEvidence{{EvidenceID: evID}}, cause)
	if !errors.Is(err, cause) {
		t.Fatalf("error = %v, want the original failure preserved", err)
	}
	if !strings.Contains(err.Error(), string(evID)) {
		t.Fatalf("error = %v, want it to name the orphaned evidence ID %s", err, evID)
	}
}

// TestConcurrentDuplicateCompletionDiscardsItsCopy proves the losing half of a
// concurrent duplicate. One call stages its copies, a second lands the completion
// first, and the first then answers idempotently: the completion it staged belongs
// to no fact, so its copy is removed instead of accumulating beside the recorded
// one. The staging sync is the synchronization point, so the two calls are ordered
// without a wall-clock race.
func TestConcurrentDuplicateCompletionDiscardsItsCopy(t *testing.T) {
	t.Parallel()
	m, s, wf := newCompleteFixture(t, completeTemplateJSON, "complete-test", "1")
	plan, actID, attemptID, leaseID, ownerToken := artifactCompletionPlan(t, m, s, wf)

	staged := make(chan struct{})
	release := make(chan struct{})
	var first atomic.Bool
	s.evidenceFsync = func(string) error {
		if first.CompareAndSwap(false, true) {
			close(staged)
			<-release
		}
		return nil
	}
	t.Cleanup(func() { s.evidenceFsync = nil })

	type outcome struct {
		res any
		err error
	}
	loser := make(chan outcome, 1)
	go func() {
		res, err := m.CompleteAuto(wf, "start", actID, attemptID, leaseID, ownerToken, plan)
		loser <- outcome{res: res, err: err}
	}()
	select {
	case <-staged:
	case got := <-loser:
		t.Fatalf("the duplicate completion finished before staging: %v", got.err)
	case <-time.After(30 * time.Second):
		t.Fatal("the duplicate completion never staged its evidence")
	}

	// The second call lands the completion while the first is still staging.
	if _, err := m.CompleteAuto(wf, "start", actID, attemptID, leaseID, ownerToken, plan); err != nil {
		close(release)
		t.Fatalf("the landing completion: %v", err)
	}
	close(release)

	got := <-loser
	if got.err != nil {
		t.Fatalf("the duplicate completion: %v", got.err)
	}
	if result, ok := got.res.(map[string]any); !ok || result["already_completed"] != true {
		t.Fatalf("the duplicate completion = %#v, want the idempotent shape", got.res)
	}
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if len(snap.Instance.Evidence) != 1 {
		t.Fatalf("instance evidence = %+v, want the one recorded artifact", snap.Instance.Evidence)
	}
	if copies := stagedEvidenceCopies(t, s, wf); len(copies) != 1 {
		t.Fatalf("staged evidence = %d copies, want the duplicate's copy discarded", len(copies))
	}
}

// blockEvidenceRemoval forces DiscardEvidence to fail, whatever user runs the
// test, so the cleanup path is verified in a privileged environment too.
func blockEvidenceRemoval(t *testing.T, s *Store) {
	t.Helper()
	s.SetDiscardFault(func(EvidenceID) error { return errors.New("injected discard failure") })
	t.Cleanup(func() { s.SetDiscardFault(nil) })
}
