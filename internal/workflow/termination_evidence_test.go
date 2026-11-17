package workflow

// termination_evidence_test.go covers the evidence a terminal attempt fact
// carries. A failed completion check's bounded streams and run summary are staged,
// recorded on the instance with the terminal fact, and discoverable through the
// inspect projection. A terminal fact that never lands leaves no staged copy
// behind.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// newManualFixture builds a store, manager, and instantiated single-node
// manual template, so an attempt exists without a registered executor.
func newManualFixture(t *testing.T) (*Manager, *Store, WorkflowID) {
	t.Helper()
	m, s, wf, _ := newManagerFixture(t)
	return m, s, wf
}

// startedNodeAttempt claims and starts one node's attempt and returns its
// activation, attempt, lease, and owner token.
func startedNodeAttempt(t *testing.T, m *Manager, s *Store, wf WorkflowID, nodeID NodeID) (ActivationID, AttemptID, LeaseID, string) {
	t.Helper()
	snap, exists, err := s.loadCurrent(wf)
	if err != nil || !exists {
		t.Fatalf("load current: %v (exists %v)", err, exists)
	}
	act := activationByNode(&snap.Instance, nodeID)
	if act == nil {
		t.Fatalf("no %s activation", nodeID)
	}
	res := claimActivation(t, m, wf, string(nodeID), string(act.ID), "alice")
	leaseID, _ := res["lease_id"].(string)
	ownerToken, _ := res["owner_token"].(string)
	payload := startCommandPayload(t, string(nodeID), string(act.ID), res, nil)
	if _, err := m.WorkflowCommand(string(wf), payload); err != nil {
		t.Fatalf("start: %v", err)
	}
	snap, exists, err = s.loadCurrent(wf)
	if err != nil || !exists {
		t.Fatalf("load current: %v (exists %v)", err, exists)
	}
	act = activationByNode(&snap.Instance, nodeID)
	if act == nil || len(act.AttemptIDs) != 1 {
		t.Fatalf("activation = %+v, want one attempt", act)
	}
	return act.ID, act.AttemptIDs[0], LeaseID(leaseID), ownerToken
}

// failedCheckCaptures is one failed check's captures, exactly as
// evalCommandContract produces them.
func failedCheckCaptures(t *testing.T) []CommandCapture {
	t.Helper()
	script := commandScript(t, t.TempDir(), "check.sh", `printf 'looked\n'; exit 3`)
	node := commandNode(script, nil, 10, nil)
	_, err := evaluateAuto(evalTemplate(node, "done"), node, t.TempDir(), "")
	unmet := contractUnmet(t, err)
	if len(unmet.Evidence) == 0 {
		t.Fatal("the rejected check captured no evidence")
	}
	return unmet.Evidence
}

// passedCheckCaptures is one passing check's captures, exactly as
// evalCommandContract produces them.
func passedCheckCaptures(t *testing.T, stdout string) []CommandCapture {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "stdout.txt"), []byte(stdout), StateFilePerm); err != nil {
		t.Fatalf("write check stdout: %v", err)
	}
	script := commandScript(t, dir, "check.sh", "cat stdout.txt\n")
	node := commandNode(script, nil, 10, nil)
	plan, err := evaluateAuto(evalTemplate(node, "done"), node, dir, "")
	if err != nil {
		t.Fatalf("evaluate a passing check: %v", err)
	}
	if len(plan.Captures) == 0 {
		t.Fatal("the passing check captured no evidence")
	}
	return plan.Captures
}

func TestDuplicateTerminationRemainsSuccessfulWhenCleanupFails(t *testing.T) {
	t.Parallel()
	m, s, wf := newManualFixture(t)
	actID, attemptID, leaseID, _ := startedNodeAttempt(t, m, s, wf, "start")
	termination := AttemptTermination{Status: AttemptFailed, Evidence: failedCheckCaptures(t)}
	if err := m.RecordAttemptTermination(wf, "start", actID, attemptID, leaseID, termination); err != nil {
		t.Fatalf("first termination: %v", err)
	}
	before, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatal(err)
	}
	// The duplicate's copy cannot be removed; the recorded evidence is unchanged.
	blockEvidenceRemoval(t, s)
	if err := m.RecordAttemptTermination(wf, "start", actID, attemptID, leaseID, termination); err != nil {
		t.Fatalf("duplicate termination: %v", err)
	}
	after, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Instance.Evidence) != len(before.Instance.Evidence) {
		t.Fatalf("evidence before = %d, after = %d", len(before.Instance.Evidence), len(after.Instance.Evidence))
	}
	if got := len(stagedEvidenceNames(t, s, wf)); got <= len(before.Instance.Evidence) {
		t.Fatalf("staged directories = %d, want undiscardable duplicate", got)
	}
}

// TestRecordAttemptTerminationRecordsCheckEvidence checks the inspect projection.
func TestRecordAttemptTerminationRecordsCheckEvidence(t *testing.T) {
	t.Parallel()
	m, s, wf := newManualFixture(t)
	actID, attemptID, leaseID, _ := startedNodeAttempt(t, m, s, wf, "start")
	artifacts := failedCheckCaptures(t)

	if err := m.RecordAttemptTermination(wf, "start", actID, attemptID, leaseID, AttemptTermination{
		Status:      AttemptFailed,
		MarkerKind:  "run",
		MarkerLabel: "contract_unmet",
		Evidence:    artifacts,
	}); err != nil {
		t.Fatalf("RecordAttemptTermination: %v", err)
	}

	snap, exists, err := s.loadCurrent(wf)
	if err != nil || !exists {
		t.Fatalf("load current: %v (exists %v)", err, exists)
	}
	attempt := findAttempt(&snap, activationByNode(&snap.Instance, "start"), attemptID)
	if attempt == nil || attempt.Status != AttemptFailed || attempt.MarkerLabel != "contract_unmet" {
		t.Fatalf("attempt = %+v, want failed/contract_unmet", attempt)
	}
	if len(snap.Instance.Evidence) != len(artifacts) {
		t.Fatalf("instance evidence = %+v, want the check's %d captures", snap.Instance.Evidence, len(artifacts))
	}

	// Discoverable through the inspect projection clients read.
	inspect, ok := inspectMap(snap)["evidence"].([]Evidence)
	if !ok || len(inspect) != len(artifacts) {
		t.Fatalf("inspect evidence = %#v, want the check's captures", inspectMap(snap)["evidence"])
	}
	var stdout *Evidence
	for i := range inspect {
		if strings.HasSuffix(inspect[i].StoredPath, completionCommandStdoutStoredPath) {
			stdout = &inspect[i]
			break
		}
	}
	if stdout == nil {
		t.Fatalf("inspect evidence = %+v, want the captured stdout", inspect)
	}
	if stdout.ActivationID != actID || stdout.Source != EvidenceMachine || stdout.Size == 0 || stdout.SHA256 == "" {
		t.Fatalf("captured stdout evidence = %+v, want machine evidence for the activation with a digest", *stdout)
	}
	if !strings.Contains(string(stdout.Result), `"exit_code":3`) {
		t.Fatalf("evidence result = %s, want the check's exit code", stdout.Result)
	}
	if !strings.Contains(string(stdout.Result), `"termination":"exited"`) {
		t.Fatalf("evidence result = %s, want the check's termination", stdout.Result)
	}

	// The inspect payload an operator reads (the CLI marshals this map) carries
	// the summary inline, not just the path to it.
	inspectJSON, err := json.Marshal(inspectMap(snap))
	if err != nil {
		t.Fatalf("marshal inspect: %v", err)
	}
	for _, want := range []string{`"termination":"exited"`, `"exit_code":3`, `"stdout_sha256":"`} {
		if !strings.Contains(string(inspectJSON), want) {
			t.Fatalf("inspect JSON lacks %s: %s", want, inspectJSON)
		}
	}

	// The staged copy is the exact bounded stream, and only it remains.
	stored, err := os.ReadFile(filepath.Join(s.Root(), "instances", string(wf), stdout.StoredPath))
	if err != nil {
		t.Fatalf("read staged stdout: %v", err)
	}
	if string(stored) != "looked\n" {
		t.Fatalf("staged stdout = %q, want the check's bytes", stored)
	}
}

// rewindSnapshotForReplay restores a pre-command snapshot file, simulating the
// store's crash window: the events are durable, the snapshot replace is not.
func rewindSnapshotForReplay(t *testing.T, s *Store, wf WorkflowID, saved []byte) {
	t.Helper()
	if err := os.WriteFile(s.workflowPath(wf), saved, StateFilePerm); err != nil {
		t.Fatalf("rewind snapshot: %v", err)
	}
}

// snapshotBytes reads the current snapshot file for a later rewind.
func snapshotBytes(t *testing.T, s *Store, wf WorkflowID) []byte {
	t.Helper()
	data, err := os.ReadFile(s.workflowPath(wf))
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	return data
}

// TestRecordAttemptTerminationEvidenceSurvivesReplay proves a failed attempt's
// evidence is part of the durable event log. Replaying that log onto a snapshot
// that never saw the terminal fact rebuilds the evidence, digest and summary
// included.
func TestRecordAttemptTerminationEvidenceSurvivesReplay(t *testing.T) {
	t.Parallel()
	m, s, wf := newManualFixture(t)
	actID, attemptID, leaseID, _ := startedNodeAttempt(t, m, s, wf, "start")
	before := snapshotBytes(t, s, wf)
	if err := m.RecordAttemptTermination(wf, "start", actID, attemptID, leaseID, AttemptTermination{
		Status:   AttemptFailed,
		Evidence: failedCheckCaptures(t),
	}); err != nil {
		t.Fatalf("RecordAttemptTermination: %v", err)
	}
	// The event is durable; the snapshot replace "never happened".
	rewindSnapshotForReplay(t, s, wf, before)

	replayed, exists, err := s.recoverInstance(wf)
	if err != nil || !exists {
		t.Fatalf("recoverInstance: %v (exists %v)", err, exists)
	}
	if len(replayed.Instance.Evidence) == 0 {
		t.Fatal("the recorded evidence did not survive replay")
	}
	last := replayed.Instance.Evidence[len(replayed.Instance.Evidence)-1]
	if last.AttemptID != attemptID || len(last.Result) == 0 || last.SHA256 == "" {
		t.Fatalf("replayed evidence = %+v, want the attempt's capture with its summary and digest", last)
	}
}

// TestCompletePathEvidenceSurvivesReplay proves the same for the success path.
// Evidence a completion stages from the supervisor's captures is carried by the
// completed event, so replaying the log rebuilds it.
func TestCompletePathEvidenceSurvivesReplay(t *testing.T) {
	t.Parallel()
	m, s, wf := newManualFixture(t)
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	act := activationByNode(&snap.Instance, "start")
	res := claimActivation(t, m, wf, "start", string(act.ID), "alice")
	if _, err := m.WorkflowCommand(string(wf), startCommandPayload(t, "start", string(act.ID), res, nil)); err != nil {
		t.Fatalf("start: %v", err)
	}
	// The start command creates the attempt the completion belongs to.
	snapAfter, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	attempts := activationByNode(&snapAfter.Instance, "start").AttemptIDs
	if len(attempts) != 1 {
		t.Fatalf("activation attempts = %v, want one", attempts)
	}
	attemptID := string(attempts[0])
	before := snapshotBytes(t, s, wf)

	leasID, _ := res["lease_id"].(string)
	ownerToken, _ := res["owner_token"].(string)
	summary := mustMarshal(t, completionCommandSummary{Argv: []string{"/usr/local/bin/check"}, Termination: completionCommandExited, DurationMS: 12})
	if _, err := m.CompleteAuto(wf, "start", act.ID, AttemptID(attemptID), LeaseID(leasID), ownerToken, &AutoCompletion{
		Outcome: "done",
		Captures: []CommandCapture{{
			StoredPath: completionCommandStdoutStoredPath,
			Data:       []byte("{\"state\":\"ready\"}\n"),
			Result:     summary,
		}},
	}); err != nil {
		t.Fatalf("CompleteAuto: %v", err)
	}
	// The completed event is durable; the snapshot replace "never happened".
	rewindSnapshotForReplay(t, s, wf, before)

	replayed, exists, err := s.recoverInstance(wf)
	if err != nil || !exists {
		t.Fatalf("recoverInstance: %v (exists %v)", err, exists)
	}
	if replayed.Instance.Status != WorkflowCompleted {
		t.Fatalf("replayed status = %s, want the completion replayed", replayed.Instance.Status)
	}
	if len(replayed.Instance.Evidence) != 1 {
		t.Fatalf("replayed evidence = %+v, want the completion's capture", replayed.Instance.Evidence)
	}
	evidence := replayed.Instance.Evidence[0]
	if evidence.AttemptID != AttemptID(attemptID) {
		t.Fatalf("replayed evidence attempt = %s, want %s", evidence.AttemptID, attemptID)
	}
	if evidence.StoredPath == "" || evidence.SHA256 == "" || len(evidence.Result) == 0 {
		t.Fatalf("replayed evidence = %+v, want its stored copy, digest, and summary", evidence)
	}
	stored, err := os.ReadFile(filepath.Join(s.Root(), "instances", string(wf), evidence.StoredPath))
	if err != nil {
		t.Fatalf("read replayed evidence copy: %v", err)
	}
	if string(stored) != "{\"state\":\"ready\"}\n" || sha256Hex(string(stored)) != evidence.SHA256 {
		t.Fatalf("replayed evidence copy = %q (digest %s), want the captured bytes", stored, evidence.SHA256)
	}
}

// TestRecordAttemptTerminationEvidenceIsOnceOnly proves a repeated terminal
// fact records no duplicate evidence: the second call reuses the attempt's
// idempotency key and its freshly staged copies are discarded.
func TestRecordAttemptTerminationEvidenceIsOnceOnly(t *testing.T) {
	t.Parallel()
	m, s, wf := newManualFixture(t)
	actID, attemptID, leaseID, _ := startedNodeAttempt(t, m, s, wf, "start")
	termination := AttemptTermination{Status: AttemptFailed, Evidence: failedCheckCaptures(t)}
	if err := m.RecordAttemptTermination(wf, "start", actID, attemptID, leaseID, termination); err != nil {
		t.Fatalf("RecordAttemptTermination: %v", err)
	}
	if err := m.RecordAttemptTermination(wf, "start", actID, attemptID, leaseID, termination); err != nil {
		t.Fatalf("repeat RecordAttemptTermination: %v", err)
	}

	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if got, want := len(snap.Instance.Evidence), len(termination.Evidence); got != want {
		t.Fatalf("instance evidence = %d entries, want the first call's %d", got, want)
	}
	// No orphaned evidence directory from the discarded second staging.
	dirs, err := os.ReadDir(filepath.Join(s.Root(), "instances", string(wf), "evidence"))
	if err != nil {
		t.Fatalf("read evidence dir: %v", err)
	}
	if len(dirs) != len(termination.Evidence) {
		t.Fatalf("evidence directory holds %d entries, want %d (the repeat staged no orphan)", len(dirs), len(termination.Evidence))
	}
}

// TestRecordAttemptTerminationDiscardsEvidenceWhenTheFactFails proves a
// terminal fact that the kernel refuses leaves no staged copy behind, so the
// store and the filesystem never diverge.
func TestRecordAttemptTerminationDiscardsEvidenceWhenTheFactFails(t *testing.T) {
	t.Parallel()
	m, s, wf := newManualFixture(t)
	actID, _, leaseID, _ := startedNodeAttempt(t, m, s, wf, "start")

	// An attempt ID the activation never carried is refused by the reducer.
	err := m.RecordAttemptTermination(wf, "start", actID, "att-absent", leaseID, AttemptTermination{
		Status:   AttemptFailed,
		Evidence: failedCheckCaptures(t),
	})
	if err == nil {
		t.Fatal("a termination for an unknown attempt was accepted")
	}

	snap, _, loadErr := s.loadCurrent(wf)
	if loadErr != nil {
		t.Fatalf("load current: %v", loadErr)
	}
	if len(snap.Instance.Evidence) != 0 {
		t.Fatalf("instance evidence = %+v, want none after a refused fact", snap.Instance.Evidence)
	}
	entries, readErr := os.ReadDir(filepath.Join(s.Root(), "instances", string(wf), "evidence"))
	if readErr != nil && !os.IsNotExist(readErr) {
		t.Fatalf("read evidence dir: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("evidence directory holds %d entries, want none after a refused fact", len(entries))
	}
}

// TestPublicCompleteRequestCannotSupplyInlineEvidence proves the public
// workflow.complete surface keeps its file-sourced artifacts. An inline content
// field is not part of the request, so it stages nothing. The completion is
// refused rather than accepting caller-supplied bytes as evidence.
func TestPublicCompleteRequestCannotSupplyInlineEvidence(t *testing.T) {
	t.Parallel()
	m, s, wf := newManualFixture(t)
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	act := activationByNode(&snap.Instance, "start")
	res := claimActivation(t, m, wf, "start", string(act.ID), "alice")
	if _, err := m.WorkflowCommand(string(wf), startCommandPayload(t, "start", string(act.ID), res, nil)); err != nil {
		t.Fatalf("start: %v", err)
	}
	// The claim result names the attempt the start command created.
	attemptID, _ := res["attempt_id"].(string)
	if attemptID == "" {
		snapAfter, _, err := s.loadCurrent(wf)
		if err != nil {
			t.Fatalf("load current: %v", err)
		}
		attempts := activationByNode(&snapAfter.Instance, "start").AttemptIDs
		if len(attempts) != 1 {
			t.Fatalf("activation attempts = %v, want one", attempts)
		}
		attemptID = string(attempts[0])
	}
	payload := mustMarshal(t, map[string]any{
		"op":            "complete",
		"node_id":       "start",
		"activation_id": string(act.ID),
		"attempt_id":    attemptID,
		"lease_id":      res["lease_id"],
		"owner_token":   res["owner_token"],
		"outcome":       "done",
		"artifacts": []any{map[string]any{
			"stored_path": "inline.txt",
			"content":     "aW5saW5lIGJ5dGVz",
			"result":      map[string]any{"any": "provenance"},
		}},
	})
	if _, err := m.WorkflowCommand(string(wf), payload); err == nil {
		t.Fatal("a complete request with inline content was accepted")
	} else if !strings.Contains(err.Error(), "stage evidence") {
		t.Fatalf("error = %v, want the artifact to be staged from a path and refused", err)
	}
	fresh, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if len(fresh.Instance.Evidence) != 0 {
		t.Fatalf("instance evidence = %+v, want no inline evidence accepted", fresh.Instance.Evidence)
	}
}

// TestAlreadyTerminalAttemptStillRecordsEvidence proves the reducer keeps a late
// termination's evidence instead of orphaning the copies the supervisor already
// staged. A second terminate command for an attempt that is already terminal
// records evidence and changes no status. The second command uses a different
// idempotency key, which is the only way to reach the reducer's already-terminal
// no-op through the store.
func TestAlreadyTerminalAttemptStillRecordsEvidence(t *testing.T) {
	t.Parallel()
	m, s, wf := newManualFixture(t)
	actID, attemptID, leaseID, _ := startedNodeAttempt(t, m, s, wf, "start")
	if err := m.RecordAttemptTermination(wf, "start", actID, attemptID, leaseID, AttemptTermination{
		Status:   AttemptFailed,
		Evidence: failedCheckCaptures(t),
	}); err != nil {
		t.Fatalf("first RecordAttemptTermination: %v", err)
	}
	before, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	attemptBefore := findAttempt(&before, activationByNode(&before.Instance, "start"), attemptID)

	// A second terminal fact for the same attempt, carrying its own capture.
	late, err := s.stageEvidenceBytes(wf, []byte("late check output\n"), "completion-command.stdout", false, "")
	if err != nil {
		t.Fatalf("stage late capture: %v", err)
	}
	_, err = s.ApplyCommand(wf, Command{
		ID:               NewCommandID(),
		Kind:             CommandTerminate,
		ExpectedRevision: before.Instance.Revision,
		IdempotencyKey:   "terminate-" + string(attemptID) + "-late",
		Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: "start", ActivationID: actID, AttemptID: attemptID},
		LeaseID:          leaseID,
		AttemptStatus:    AttemptFailed,
		MarkerLabel:      "contract_unmet",
		Evidence: []Evidence{{
			ID:           late.EvidenceID,
			Kind:         "artifact",
			Source:       EvidenceMachine,
			StoredPath:   late.StoredPath,
			Size:         late.Size,
			SHA256:       late.SHA256,
			ActivationID: actID,
			AttemptID:    attemptID,
			CreatedAt:    m.now(),
		}},
	})
	if err != nil {
		t.Fatalf("late terminate: %v", err)
	}

	after, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	attemptAfter := findAttempt(&after, activationByNode(&after.Instance, "start"), attemptID)
	if attemptAfter.Status != attemptBefore.Status || attemptAfter.MarkerLabel != attemptBefore.MarkerLabel {
		t.Fatalf("late terminate changed the attempt fact: %+v -> %+v", attemptBefore, attemptAfter)
	}
	if got, want := len(after.Instance.Evidence), len(before.Instance.Evidence)+1; got != want {
		t.Fatalf("instance evidence = %d entries, want the late capture recorded (%d)", got, want)
	}
	if last := after.Instance.Evidence[len(after.Instance.Evidence)-1]; last.ID != late.EvidenceID || last.AttemptID != attemptID {
		t.Fatalf("late evidence = %+v, want the late capture recorded for attempt %s", last, attemptID)
	}
}

// TestAttemptEvidenceAuthorityIsTheAttemptsOwnLease proves the recorded authority
// is this attempt's lease owner while that lease still stands, and empty once it is
// gone. It is never a newer lease's owner, which would attribute a canceled or
// re-armed attempt's captures to the wrong lease.
func TestAttemptEvidenceAuthorityIsTheAttemptsOwnLease(t *testing.T) {
	t.Parallel()
	m, s, wf := newRetryableManualFixture(t)
	actID, attemptID, leaseID, _ := startedNodeAttempt(t, m, s, wf, "start")
	captures := failedCheckCaptures(t)

	// The attempt's own lease is live: its owner is the authority.
	records, staged, err := m.stageAttemptEvidence(wf, "start", actID, attemptID, leaseID, captures)
	if err != nil {
		t.Fatalf("stageAttemptEvidence: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("no evidence records staged")
	}
	if records[0].Authority != "alice" {
		t.Fatalf("authority = %q, want the attempt's lease owner", records[0].Authority)
	}
	if records[0].AttemptID != attemptID {
		t.Fatalf("attempt id = %q, want %q", records[0].AttemptID, attemptID)
	}
	if err := cleanupStaged(m, wf, staged); err != nil {
		t.Fatalf("cleanupStaged: %v", err)
	}

	// Record the failure without evidence: the retry policy re-arms the
	// activation, and a new claim takes a new lease for a new attempt.
	if err := m.RecordAttemptTermination(wf, "start", actID, attemptID, leaseID, AttemptTermination{Status: AttemptFailed}); err != nil {
		t.Fatalf("RecordAttemptTermination: %v", err)
	}
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if act := activationByNode(&snap.Instance, "start"); act == nil || act.Status != ActivationReady {
		t.Fatalf("activation = %+v, want a re-armed ready activation", act)
	}
	second := claimActivation(t, m, wf, "start", string(actID), "bob")
	secondLease, _ := second["lease_id"].(string)
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if act := activationByNode(&snap.Instance, "start"); act == nil || act.ActiveLease == nil || string(act.ActiveLease.ID) != secondLease {
		t.Fatalf("activation lease = %+v, want the new claim %q", act, secondLease)
	}

	// The old attempt's evidence must not be attributed to the new lease.
	records, staged, err = m.stageAttemptEvidence(wf, "start", actID, attemptID, leaseID, captures)
	if err != nil {
		t.Fatalf("stageAttemptEvidence: %v", err)
	}
	if records[0].Authority != "" {
		t.Fatalf("authority = %q, want none for a lease the attempt no longer holds", records[0].Authority)
	}
	if err := cleanupStaged(m, wf, staged); err != nil {
		t.Fatalf("cleanupStaged: %v", err)
	}
}

// TestRecordAttemptTerminationDiscardsPartiallyStagedEvidence proves a staging
// failure part-way through a terminal fact's captures leaves nothing behind. The
// copies that already landed are removed, so the store and filesystem stay in
// agreement even on the error path.
func TestRecordAttemptTerminationDiscardsPartiallyStagedEvidence(t *testing.T) {
	t.Parallel()
	m, s, wf := newManualFixture(t)
	actID, attemptID, leaseID, _ := startedNodeAttempt(t, m, s, wf, "start")
	good := failedCheckCaptures(t)[0]
	bad := CommandCapture{StoredPath: "../escape.stdout", Data: []byte("x")}

	err := m.RecordAttemptTermination(wf, "start", actID, attemptID, leaseID, AttemptTermination{
		Status:   AttemptFailed,
		Evidence: []CommandCapture{good, bad},
	})
	if err == nil || !strings.Contains(err.Error(), "stage attempt evidence") {
		t.Fatalf("error = %v, want the staging failure", err)
	}
	snap, _, loadErr := s.loadCurrent(wf)
	if loadErr != nil {
		t.Fatalf("load current: %v", loadErr)
	}
	if len(snap.Instance.Evidence) != 0 {
		t.Fatalf("instance evidence = %+v, want none after a failed staging", snap.Instance.Evidence)
	}
	entries, readErr := os.ReadDir(filepath.Join(s.Root(), "instances", string(wf), "evidence"))
	if readErr != nil && !os.IsNotExist(readErr) {
		t.Fatalf("read evidence dir: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("evidence directory holds %d entries, want the partial copy removed", len(entries))
	}
}

// TestRecordAttemptTerminationRefusesOversizedProvenance proves a capture
// whose summary would exceed the evidence bound is refused before anything is
// staged, so no single entry can grow a snapshot.
func TestRecordAttemptTerminationRefusesOversizedProvenance(t *testing.T) {
	t.Parallel()
	m, s, wf := newManualFixture(t)
	actID, attemptID, leaseID, _ := startedNodeAttempt(t, m, s, wf, "start")
	oversized := []CommandCapture{{
		StoredPath: "completion-command.stdout",
		Data:       []byte("x"),
		Result:     make([]byte, maxEvidenceResultBytes+1),
	}}
	err := m.RecordAttemptTermination(wf, "start", actID, attemptID, leaseID, AttemptTermination{
		Status:   AttemptFailed,
		Evidence: oversized,
	})
	if err == nil || !strings.Contains(err.Error(), "result exceeds") {
		t.Fatalf("error = %v, want the oversized-result rejection", err)
	}
	entries, readErr := os.ReadDir(filepath.Join(s.Root(), "instances", string(wf), "evidence"))
	if readErr != nil && !os.IsNotExist(readErr) {
		t.Fatalf("read evidence dir: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("evidence directory holds %d entries, want none", len(entries))
	}
}

// newRetryableManualFixture builds a store, manager, and instantiated manual
// template whose node re-arms once after a failed attempt. A test can then hold an
// old attempt's lease ID against a newer lease.
func newRetryableManualFixture(t *testing.T) (*Manager, *Store, WorkflowID) {
	t.Helper()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	if _, err := m.WorkflowCreate([]byte(retryableManualTemplateJSON)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	out, err := m.WorkflowInstantiate(mustMarshal(t, map[string]string{
		"template_id": "mgr-retry", "template_version": "1",
	}))
	if err != nil {
		t.Fatalf("WorkflowInstantiate: %v", err)
	}
	wf, ok := out.(map[string]any)["workflow_id"].(string)
	if !ok || wf == "" {
		t.Fatalf("instantiate result missing workflow_id: %#v", out)
	}
	return m, s, WorkflowID(wf)
}

// retryableManualTemplateJSON is one manual node that retries once before it
// blocks, so a failed attempt re-arms the same activation.
const retryableManualTemplateJSON = `{
  "schema_version": 1,
  "template_id": "mgr-retry",
  "template_version": "1",
  "entry_nodes": ["start"],
  "nodes": [{"id": "start", "action": {"type": "manual"},
             "retry_policy": {"max_attempts": 2, "exhaustion": "block"}}],
  "terminal_outcomes": ["done"]
}`
