package workflow

// Tests for the atomic machine-completion handler (workflow.complete,
// Stage 11 phase 2). They use only the standard library.

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// completeTemplateJSON is a single manual node with a declared machine
// contract: two outputs (one required), a files completion requiring a
// non-empty artifact, and a terminal "done" outcome.
const completeTemplateJSON = `{
  "schema_version": 1,
  "template_id": "complete-test",
  "template_version": "1",
  "entry_nodes": ["start"],
  "nodes": [{
    "id": "start",
    "action": {"type": "manual"},
    "outputs": [
      {"id": "summary", "name": "Summary", "type": "string", "required": true},
      {"id": "log", "name": "Log", "type": "file"}
    ],
    "completion": {"kind": "files", "artifacts": [{"path": "evidence.txt", "non_empty": true}]},
    "outcomes": [{"name": "done", "terminal": true}]
  }],
  "terminal_outcomes": ["done"]
}`

// completeGatedTemplateJSON is the same contract plus a required human gate,
// with "done" declared as a branch to a second node so the gated completion's
// branch-suppression is observable (a premature transition would create the
// "next" activation while the node is parked awaiting_gate).
const completeGatedTemplateJSON = `{
  "schema_version": 1,
  "template_id": "complete-gated",
  "template_version": "1",
  "entry_nodes": ["start"],
  "nodes": [
    {
      "id": "start",
      "action": {"type": "manual"},
      "outputs": [
        {"id": "summary", "name": "Summary", "type": "string", "required": true},
        {"id": "log", "name": "Log", "type": "file"}
      ],
      "completion": {"kind": "files", "artifacts": [{"path": "evidence.txt", "non_empty": true}]},
      "branches": {"done": "next"},
      "gates": [{"id": "review", "type": "human", "required": true}]
    },
    {"id": "next", "action": {"type": "manual"}}
  ],
  "terminal_outcomes": ["done"]
}`

// newCompleteFixture builds an ephemeral store + manager with the given
// template stored and a fresh instance of it.
func newCompleteFixture(t *testing.T, templateJSON, templateID, templateVersion string) (*Manager, *Store, WorkflowID) {
	t.Helper()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	if _, err := m.WorkflowCreate([]byte(templateJSON)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	payload, err := json.Marshal(map[string]string{
		"template_id": templateID, "template_version": templateVersion,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out, err := m.WorkflowInstantiate(payload)
	if err != nil {
		t.Fatalf("WorkflowInstantiate: %v", err)
	}
	wf, ok := out.(map[string]any)["workflow_id"].(string)
	if !ok {
		t.Fatalf("instantiate result missing workflow_id: %#v", out)
	}
	return m, s, WorkflowID(wf)
}

// completeToRunning drives a fresh instance to a running activation with a
// live lease and a succeeded terminal attempt fact (the files completion
// contract depends on terminal output), returning the claim result
// (lease_id/owner_token), the activation id, and the attempt id.
func completeToRunning(t *testing.T, m *Manager, s *Store, wf WorkflowID) (map[string]any, ActivationID, AttemptID) {
	t.Helper()
	snap, ok, err := s.loadCurrent(wf)
	if err != nil || !ok {
		t.Fatalf("load current: %v", err)
	}
	actID := snap.Instance.Activations[0].ID
	res := claimActivation(t, m, wf, "start", string(actID), "alice")
	out, err := m.WorkflowCommand(string(wf), startCommandPayload(t, "start", string(actID), res, nil))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	attemptID := AttemptID(out.(map[string]any)["attempt_id"].(string))
	leaseID := LeaseID(res["lease_id"].(string))
	if err := m.RecordAttemptTerminated(wf, "start", actID, attemptID, leaseID, AttemptSucceeded); err != nil {
		t.Fatalf("RecordAttemptTerminated: %v", err)
	}
	return res, actID, attemptID
}

// completePayload builds a complete command payload from the claim result.
func completePayload(t *testing.T, actID, attemptID string, res map[string]any, outcome string, outputs, artifacts []map[string]any) json.RawMessage {
	t.Helper()
	payload := map[string]any{
		"node_id":       "start",
		"activation_id": actID,
		"attempt_id":    attemptID,
		"lease_id":      res["lease_id"],
		"owner_token":   res["owner_token"],
		"outcome":       outcome,
	}
	if outputs != nil {
		payload["outputs"] = outputs
	}
	if artifacts != nil {
		payload["artifacts"] = artifacts
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal complete: %v", err)
	}
	return data
}

// standardOutputs is the full declared output set for the complete templates.
func standardOutputs() []map[string]any {
	return []map[string]any{
		{"definition_id": "summary", "value": "all good"},
		{"definition_id": "log", "value": "evidence.txt"},
	}
}

// writeEvidence writes a non-empty temp evidence file and returns its path and
// SHA-256 digest.
func writeEvidence(t *testing.T, name, content string) (string, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	digest, err := sha256File(path)
	if err != nil {
		t.Fatalf("sha256File: %v", err)
	}
	return path, digest
}

// revision returns the instance's current revision.
func revision(t *testing.T, s *Store, wf WorkflowID) int64 {
	t.Helper()
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	return snap.Instance.Revision
}

// evidenceEntries lists the staged evidence directories under the instance.
func evidenceEntries(t *testing.T, s *Store, wf WorkflowID) int {
	t.Helper()
	dir := filepath.Join(s.instanceDir(wf), "evidence")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0
		}
		t.Fatalf("read evidence dir: %v", err)
	}
	return len(entries)
}

func TestCommandCompleteSatisfies(t *testing.T) {
	m, s, wf := newCompleteFixture(t, completeTemplateJSON, "complete-test", "1")
	res, actID, attemptID := completeToRunning(t, m, s, wf)
	src, digest := writeEvidence(t, "evidence.txt", "run output\n")

	out, err := m.commandComplete(wf, completePayload(t, string(actID), string(attemptID), res, "done", standardOutputs(), []map[string]any{{
		"src_path": src, "stored_path": "evidence.txt", "non_empty": true, "sha256": digest,
	}}))
	if err != nil {
		t.Fatalf("commandComplete: %v", err)
	}
	mm, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("complete result = %#v, want map", out)
	}
	if mm["status"] != "completed" || mm["activation_status"] != "satisfied" || mm["outcome"] != "done" {
		t.Fatalf("complete result = %#v", mm)
	}

	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	act := activationByNode(&snap.Instance, "start")
	if act.Status != ActivationSatisfied {
		t.Fatalf("activation status = %q, want satisfied", act.Status)
	}
	if act.ActiveLease != nil {
		t.Fatalf("lease not released: %+v", act.ActiveLease)
	}
	if act.SelectedOutcome != "done" {
		t.Fatalf("selected outcome = %q, want done", act.SelectedOutcome)
	}
	if snap.Instance.Status != WorkflowCompleted || snap.Instance.TerminalOutcome != "done" {
		t.Fatalf("workflow = %q terminal %q, want completed/done", snap.Instance.Status, snap.Instance.TerminalOutcome)
	}
	var summary *OutputValue
	for i := range snap.Instance.Outputs {
		if snap.Instance.Outputs[i].DefinitionID == "summary" {
			summary = &snap.Instance.Outputs[i]
		}
	}
	if summary == nil || summary.Revision != 1 {
		t.Fatalf("summary output = %+v, want revision 1", summary)
	}
	if len(snap.Instance.Evidence) != 1 {
		t.Fatalf("evidence = %+v, want exactly 1 record", snap.Instance.Evidence)
	}
	ev := snap.Instance.Evidence[0]
	if ev.Size <= 0 || ev.SHA256 != digest || !strings.HasPrefix(ev.StoredPath, "evidence/") {
		t.Fatalf("evidence record = %+v, want staged artifact", ev)
	}
}

func TestCommandCompleteMissingRequiredOutput(t *testing.T) {
	m, s, wf := newCompleteFixture(t, completeTemplateJSON, "complete-test", "1")
	res, actID, attemptID := completeToRunning(t, m, s, wf)
	src, _ := writeEvidence(t, "evidence.txt", "run output\n")

	// The required "summary" output is omitted.
	payload := completePayload(t, string(actID), string(attemptID), res, "done",
		[]map[string]any{{"definition_id": "log", "value": "evidence.txt"}},
		[]map[string]any{{"src_path": src, "stored_path": "evidence.txt", "non_empty": true}})
	revBefore := revision(t, s, wf)
	_, err := m.commandComplete(wf, payload)
	if err == nil {
		t.Fatal("complete without required output: expected error, got nil")
	}
	if got := revision(t, s, wf); got != revBefore {
		t.Fatalf("revision changed %d -> %d", revBefore, got)
	}
	// Validation fails before staging: nothing is written to disk.
	if n := evidenceEntries(t, s, wf); n != 0 {
		t.Fatalf("evidence staged before validation: %d entries", n)
	}
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if got := activationByNode(&snap.Instance, "start").Status; got != ActivationRunning {
		t.Fatalf("activation status = %q, want running (no completion event)", got)
	}
}

func TestCommandCompleteUndeclaredOutcome(t *testing.T) {
	m, s, wf := newCompleteFixture(t, completeTemplateJSON, "complete-test", "1")
	res, actID, attemptID := completeToRunning(t, m, s, wf)
	src, _ := writeEvidence(t, "evidence.txt", "run output\n")

	payload := completePayload(t, string(actID), string(attemptID), res, "bogus", standardOutputs(),
		[]map[string]any{{"src_path": src, "stored_path": "evidence.txt", "non_empty": true}})
	revBefore := revision(t, s, wf)
	_, err := m.commandComplete(wf, payload)
	if err == nil {
		t.Fatal("complete with undeclared outcome: expected error, got nil")
	}
	if got := revision(t, s, wf); got != revBefore {
		t.Fatalf("revision changed %d -> %d", revBefore, got)
	}
	// Outcome validation fails before staging: the evidence dir is untouched.
	if n := evidenceEntries(t, s, wf); n != 0 {
		t.Fatalf("evidence staged before validation: %d entries", n)
	}
}

func TestCommandCompleteBadEvidence(t *testing.T) {
	m, s, wf := newCompleteFixture(t, completeTemplateJSON, "complete-test", "1")
	res, actID, attemptID := completeToRunning(t, m, s, wf)
	src, _ := writeEvidence(t, "evidence.txt", "run output\n")
	missing := filepath.Join(t.TempDir(), "does-not-exist.txt")

	// The first artifact stages; the second source is missing. The failure
	// must clean up the first staging so store and filesystem agree.
	payload := completePayload(t, string(actID), string(attemptID), res, "done", standardOutputs(), []map[string]any{
		{"src_path": src, "stored_path": "evidence.txt", "non_empty": true},
		{"src_path": missing, "stored_path": "missing.txt", "non_empty": true},
	})
	revBefore := revision(t, s, wf)
	_, err := m.commandComplete(wf, payload)
	if err == nil {
		t.Fatal("complete with missing evidence source: expected error, got nil")
	}
	if got := revision(t, s, wf); got != revBefore {
		t.Fatalf("revision changed %d -> %d", revBefore, got)
	}
	if n := evidenceEntries(t, s, wf); n != 0 {
		t.Fatalf("freshly staged evidence not cleaned up: %d entries", n)
	}
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if got := activationByNode(&snap.Instance, "start").Status; got != ActivationRunning {
		t.Fatalf("activation status = %q, want running (no completion event)", got)
	}
}

func TestCommandCompleteHashMismatch(t *testing.T) {
	m, s, wf := newCompleteFixture(t, completeTemplateJSON, "complete-test", "1")
	res, actID, attemptID := completeToRunning(t, m, s, wf)
	src, _ := writeEvidence(t, "evidence.txt", "run output\n")

	payload := completePayload(t, string(actID), string(attemptID), res, "done", standardOutputs(), []map[string]any{{
		"src_path": src, "stored_path": "evidence.txt", "non_empty": true,
		"sha256": strings.Repeat("0", 64), // wrong digest
	}})
	revBefore := revision(t, s, wf)
	_, err := m.commandComplete(wf, payload)
	if err == nil {
		t.Fatal("complete with mismatched sha256: expected error, got nil")
	}
	if got := revision(t, s, wf); got != revBefore {
		t.Fatalf("revision changed %d -> %d", revBefore, got)
	}
	if n := evidenceEntries(t, s, wf); n != 0 {
		t.Fatalf("evidence staged despite hash mismatch: %d entries", n)
	}
}

func TestCommandCompleteIdempotentDuplicate(t *testing.T) {
	m, s, wf := newCompleteFixture(t, completeTemplateJSON, "complete-test", "1")
	res, actID, attemptID := completeToRunning(t, m, s, wf)
	src, digest := writeEvidence(t, "evidence.txt", "run output\n")

	out, err := m.commandComplete(wf, completePayload(t, string(actID), string(attemptID), res, "done", standardOutputs(), []map[string]any{{
		"src_path": src, "stored_path": "evidence.txt", "non_empty": true, "sha256": digest,
	}}))
	if err != nil {
		t.Fatalf("first complete: %v", err)
	}
	if mm := out.(map[string]any); mm["status"] != "completed" {
		t.Fatalf("first complete result = %#v", mm)
	}
	revAfter := revision(t, s, wf)

	// Repeating workflow.complete for the same attempt (even with a different
	// artifact path) is a safe no-op: no error, no re-advance.
	src2, _ := writeEvidence(t, "second.txt", "second\n")
	out2, err := m.commandComplete(wf, completePayload(t, string(actID), string(attemptID), res, "done", standardOutputs(), []map[string]any{{
		"src_path": src2, "stored_path": "second.txt", "non_empty": true,
	}}))
	if err != nil {
		t.Fatalf("duplicate complete: %v", err)
	}
	mm2, ok := out2.(map[string]any)
	if !ok {
		t.Fatalf("duplicate complete result = %#v, want map", out2)
	}
	if mm2["idempotent"] != true || mm2["already_completed"] != true {
		t.Fatalf("duplicate complete result = %#v, want idempotent no-op", mm2)
	}
	if got := revision(t, s, wf); got != revAfter {
		t.Fatalf("revision advanced by duplicate: %d -> %d", revAfter, got)
	}
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if len(snap.Instance.Evidence) != 1 {
		t.Fatalf("evidence duplicated: %d records, want 1", len(snap.Instance.Evidence))
	}
	if len(snap.Instance.Outputs) != 2 {
		t.Fatalf("outputs duplicated: %d records, want 2", len(snap.Instance.Outputs))
	}
	if got := activationByNode(&snap.Instance, "start").Status; got != ActivationSatisfied {
		t.Fatalf("activation status = %q, want satisfied", got)
	}
}

// TestNextRevisionForAppendOnly pins the append-only output-revision contract:
// later authorized activations produce new output revisions without mutating
// prior facts (the prior OutputValue stays byte-identical; the new entry
// carries revision+1).
func TestNextRevisionForAppendOnly(t *testing.T) {
	inst := &WorkflowInstance{}
	if got := nextRevisionFor(inst, "summary"); got != 1 {
		t.Fatalf("next revision for empty instance = %d, want 1", got)
	}
	// Activation A completes first.
	first := OutputValue{
		ID: "out-a", DefinitionID: "summary", ActivationID: "act-a",
		Revision: nextRevisionFor(inst, "summary"), Value: json.RawMessage(`"v1"`),
	}
	inst.Outputs = append(inst.Outputs, first)
	// A later activation of the same node completes with a new value.
	second := OutputValue{
		ID: "out-b", DefinitionID: "summary", ActivationID: "act-b",
		Revision: nextRevisionFor(inst, "summary"), Value: json.RawMessage(`"v2"`),
	}
	inst.Outputs = append(inst.Outputs, second)
	if got := nextRevisionFor(inst, "summary"); got != 3 {
		t.Fatalf("next revision = %d, want 3", got)
	}
	if b, err := json.Marshal(inst.Outputs[0]); err == nil {
		if want, _ := json.Marshal(first); string(b) != string(want) {
			t.Fatalf("prior OutputValue mutated: got %s want %s", b, want)
		}
	}
	if inst.Outputs[1].Revision != first.Revision+1 {
		t.Fatalf("new OutputValue revision = %d, want %d", inst.Outputs[1].Revision, first.Revision+1)
	}
	// Unrelated definitions keep their own revision counter.
	if got := nextRevisionFor(inst, "log"); got != 1 {
		t.Fatalf("unrelated definition revision = %d, want 1", got)
	}
}

func TestCommandCompleteGatedAwaitsGate(t *testing.T) {
	m, s, wf := newCompleteFixture(t, completeGatedTemplateJSON, "complete-gated", "1")
	res, actID, attemptID := completeToRunning(t, m, s, wf)
	src, digest := writeEvidence(t, "evidence.txt", "run output\n")

	out, err := m.commandComplete(wf, completePayload(t, string(actID), string(attemptID), res, "done", standardOutputs(), []map[string]any{{
		"src_path": src, "stored_path": "evidence.txt", "non_empty": true, "sha256": digest,
	}}))
	if err != nil {
		t.Fatalf("gated complete: %v", err)
	}
	mm, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("gated complete result = %#v, want map", out)
	}
	if mm["status"] != "completed" || mm["activation_status"] != string(ActivationAwaitingGate) {
		t.Fatalf("gated complete result = %#v, want completed/awaiting_gate", mm)
	}

	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	act := activationByNode(&snap.Instance, "start")
	if act.Status != ActivationAwaitingGate {
		t.Fatalf("activation status = %q, want awaiting_gate", act.Status)
	}
	if act.ActiveLease != nil {
		t.Fatalf("lease not released: %+v", act.ActiveLease)
	}
	if snap.Instance.Status != WorkflowActive || snap.Instance.TerminalOutcome != "" {
		t.Fatalf("workflow = %q terminal %q, want active (gated completion must not complete the workflow)",
			snap.Instance.Status, snap.Instance.TerminalOutcome)
	}
	// The branch payload is suppressed while gated: no target activation is
	// created prematurely.
	if len(snap.Instance.Activations) != 1 {
		t.Fatalf("activation count = %d, want 1 (no premature transition)", len(snap.Instance.Activations))
	}
	// Evidence and outputs are still recorded atomically with the park.
	if len(snap.Instance.Evidence) != 1 || len(snap.Instance.Outputs) != 2 {
		t.Fatalf("gated completion dropped records: evidence=%d outputs=%d",
			len(snap.Instance.Evidence), len(snap.Instance.Outputs))
	}
}

// completeToRunningNoTerminal drives a fresh instance to a running activation
// with a live lease but NO terminal attempt fact yet (start only, no
// RecordAttemptTerminated).
func completeToRunningNoTerminal(t *testing.T, m *Manager, s *Store, wf WorkflowID) (map[string]any, ActivationID, AttemptID) {
	t.Helper()
	snap, ok, err := s.loadCurrent(wf)
	if err != nil || !ok {
		t.Fatalf("load current: %v", err)
	}
	actID := snap.Instance.Activations[0].ID
	res := claimActivation(t, m, wf, "start", string(actID), "alice")
	out, err := m.WorkflowCommand(string(wf), startCommandPayload(t, "start", string(actID), res, nil))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	attemptID := AttemptID(out.(map[string]any)["attempt_id"].(string))
	return res, actID, attemptID
}

// TestCommandCompleteWaitsForTerminalFact pins the wait-for-terminal-fact
// contract: a machine contract depending on terminal output waits for the
// corresponding termination fact; explicit handoff may not precede it.
func TestCommandCompleteWaitsForTerminalFact(t *testing.T) {
	m, s, wf := newCompleteFixture(t, completeTemplateJSON, "complete-test", "1")
	res, actID, attemptID := completeToRunningNoTerminal(t, m, s, wf)
	src, digest := writeEvidence(t, "evidence.txt", "run output\n")
	payload := completePayload(t, string(actID), string(attemptID), res, "done", standardOutputs(), []map[string]any{{
		"src_path": src, "stored_path": "evidence.txt", "non_empty": true, "sha256": digest,
	}})

	// No terminal fact yet: the completion must be rejected and nothing may
	// change (no revision bump, no completion event, no staged evidence).
	revBefore := revision(t, s, wf)
	_, err := m.commandComplete(wf, payload)
	if err == nil {
		t.Fatal("complete before terminal fact: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "terminal") {
		t.Fatalf("error = %q, want mention of terminal fact", err)
	}
	if got := revision(t, s, wf); got != revBefore {
		t.Fatalf("revision changed %d -> %d", revBefore, got)
	}
	if n := evidenceEntries(t, s, wf); n != 0 {
		t.Fatalf("evidence staged before terminal fact: %d entries", n)
	}
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if got := activationByNode(&snap.Instance, "start").Status; got != ActivationRunning {
		t.Fatalf("activation status = %q, want running (no completion event)", got)
	}

	// Once the attempt's terminal fact is recorded, the same payload succeeds.
	leaseID := LeaseID(res["lease_id"].(string))
	if err := m.RecordAttemptTerminated(wf, "start", actID, attemptID, leaseID, AttemptSucceeded); err != nil {
		t.Fatalf("RecordAttemptTerminated: %v", err)
	}
	out, err := m.commandComplete(wf, payload)
	if err != nil {
		t.Fatalf("complete after terminal fact: %v", err)
	}
	if mm := out.(map[string]any); mm["status"] != "completed" || mm["activation_status"] != "satisfied" {
		t.Fatalf("complete after terminal fact = %#v", mm)
	}
}

// TestCommandCompleteForeignAttemptRejected pins attempt-ownership validation:
// a complete presenting an attempt that is not in the activation's attempt
// list is rejected, and the foreign idempotency key is not consumed (the
// legitimate completion still succeeds afterward).
func TestCommandCompleteForeignAttemptRejected(t *testing.T) {
	m, s, wf := newCompleteFixture(t, completeTemplateJSON, "complete-test", "1")
	res, actID, attemptID := completeToRunning(t, m, s, wf)
	src, digest := writeEvidence(t, "evidence.txt", "run output\n")
	artifacts := []map[string]any{{"src_path": src, "stored_path": "evidence.txt", "non_empty": true, "sha256": digest}}

	// A foreign attempt ID (not in act.AttemptIDs) must be rejected.
	foreign := string(NewAttemptID())
	revBefore := revision(t, s, wf)
	_, err := m.commandComplete(wf, completePayload(t, string(actID), foreign, res, "done", standardOutputs(), artifacts))
	if err == nil {
		t.Fatal("complete with foreign attempt: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "does not belong") {
		t.Fatalf("error = %q, want ownership rejection", err)
	}
	if got := revision(t, s, wf); got != revBefore {
		t.Fatalf("revision changed %d -> %d", revBefore, got)
	}
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if got := activationByNode(&snap.Instance, "start").Status; got != ActivationRunning {
		t.Fatalf("activation status = %q, want running (no completion event)", got)
	}

	// The real attempt's completion still succeeds: the foreign key was not
	// consumed and did not block the owning activation.
	out, err := m.commandComplete(wf, completePayload(t, string(actID), string(attemptID), res, "done", standardOutputs(), artifacts))
	if err != nil {
		t.Fatalf("genuine complete after foreign attempt rejection: %v", err)
	}
	if mm := out.(map[string]any); mm["status"] != "completed" || mm["activation_status"] != "satisfied" {
		t.Fatalf("genuine complete result = %#v", mm)
	}
}

// TestCommandCompleteViaWorkflowCommand pins the public surface path: the
// workflow command dispatcher routes op "complete" to the same atomic handler.
func TestCommandCompleteViaWorkflowCommand(t *testing.T) {
	m, s, wf := newCompleteFixture(t, completeTemplateJSON, "complete-test", "1")
	res, actID, attemptID := completeToRunning(t, m, s, wf)
	src, digest := writeEvidence(t, "evidence.txt", "run output\n")

	opPayload := map[string]any{
		"op":            "complete",
		"node_id":       "start",
		"activation_id": string(actID),
		"attempt_id":    string(attemptID),
		"lease_id":      res["lease_id"],
		"owner_token":   res["owner_token"],
		"outcome":       "done",
		"outputs":       []map[string]any{{"definition_id": "summary", "value": "all good"}, {"definition_id": "log", "value": "evidence.txt"}},
		"artifacts":     []map[string]any{{"src_path": src, "stored_path": "evidence.txt", "non_empty": true, "sha256": digest}},
	}
	data, err := json.Marshal(opPayload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	out, err := m.WorkflowCommand(string(wf), data)
	if err != nil {
		t.Fatalf("WorkflowCommand complete: %v", err)
	}
	mm, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("complete result = %#v, want map", out)
	}
	if mm["status"] != "completed" || mm["activation_status"] != "satisfied" {
		t.Fatalf("complete via surface = %#v", mm)
	}
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if got := activationByNode(&snap.Instance, "start").Status; got != ActivationSatisfied {
		t.Fatalf("activation status = %q, want satisfied", got)
	}
}

// selfBranchTemplateJSON is a single manual node whose outcome "again" is a
// branch back to itself, exercising a real later activation of the same node;
// "ok" is the template terminal outcome.
const selfBranchTemplateJSON = `{
  "schema_version": 1,
  "template_id": "self-branch",
  "template_version": "1",
  "entry_nodes": ["start"],
  "nodes": [{
    "id": "start",
    "action": {"type": "manual"},
    "outputs": [{"id": "summary", "name": "Summary", "type": "string", "required": true}],
    "outcomes": [{"name": "again", "target_node_id": "start"}]
  }],
  "terminal_outcomes": ["ok"]
}`

// TestCommandCompleteAppendOnlyRevisionE2E pins append-only output revisions
// end to end: a second activation of the same node produces a new OutputValue
// with revision 2, while the first record stays byte-identical (revision 1,
// same value) — prior facts are never mutated.
func TestCommandCompleteAppendOnlyRevisionE2E(t *testing.T) {
	m, s, wf := newCompleteFixture(t, selfBranchTemplateJSON, "self-branch", "1")

	// Activation 1: claim, start, complete with the self-branch outcome.
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	act1 := snap.Instance.Activations[0].ID
	res1 := claimActivation(t, m, wf, "start", string(act1), "alice")
	out1, err := m.WorkflowCommand(string(wf), startCommandPayload(t, "start", string(act1), res1, nil))
	if err != nil {
		t.Fatalf("start 1: %v", err)
	}
	att1 := AttemptID(out1.(map[string]any)["attempt_id"].(string))
	done1, err := m.commandComplete(wf, completePayload(t, string(act1), string(att1), res1, "again",
		[]map[string]any{{"definition_id": "summary", "value": "first"}}, nil))
	if err != nil {
		t.Fatalf("complete 1: %v", err)
	}
	if mm := done1.(map[string]any); mm["activation_status"] != "satisfied" {
		t.Fatalf("complete 1 result = %#v, want satisfied", mm)
	}

	// The branch created a second pending activation of "start".
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	var act2 *Activation
	startCount := 0
	for i := range snap.Instance.Activations {
		if snap.Instance.Activations[i].NodeID == "start" {
			startCount++
			if snap.Instance.Activations[i].Status != ActivationSatisfied {
				act2 = &snap.Instance.Activations[i]
			}
		}
	}
	if startCount != 2 {
		t.Fatalf("start activation count = %d, want 2 (satisfied + branch-created)", startCount)
	}
	if act2 == nil {
		t.Fatal("no non-satisfied activation found for the branch target")
	}

	// Capture the first OutputValue record; it must be unchanged afterward.
	first := OutputValue{}
	found := false
	for i := range snap.Instance.Outputs {
		if snap.Instance.Outputs[i].DefinitionID == "summary" {
			first = snap.Instance.Outputs[i]
			found = true
		}
	}
	if !found || first.Revision != 1 {
		t.Fatalf("first summary output = %+v found=%v, want revision 1", first, found)
	}

	// Activation 2: claim, start, complete with the terminal outcome.
	res2 := claimActivation(t, m, wf, "start", string(act2.ID), "bob")
	out2, err := m.WorkflowCommand(string(wf), startCommandPayload(t, "start", string(act2.ID), res2, nil))
	if err != nil {
		t.Fatalf("start 2: %v", err)
	}
	att2 := AttemptID(out2.(map[string]any)["attempt_id"].(string))
	done2, err := m.commandComplete(wf, completePayload(t, string(act2.ID), string(att2), res2, "ok",
		[]map[string]any{{"definition_id": "summary", "value": "second"}}, nil))
	if err != nil {
		t.Fatalf("complete 2: %v", err)
	}
	if mm := done2.(map[string]any); mm["status"] != "completed" {
		t.Fatalf("complete 2 result = %#v, want completed", mm)
	}

	// Verify append-only revisions: the new entry is revision 2, and the
	// first record is byte-identical to the captured one.
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	var second *OutputValue
	for i := range snap.Instance.Outputs {
		if snap.Instance.Outputs[i].DefinitionID == "summary" && snap.Instance.Outputs[i].ActivationID == act2.ID {
			second = &snap.Instance.Outputs[i]
		}
	}
	if second == nil {
		t.Fatal("second summary output not recorded")
	}
	if second.Revision != 2 {
		t.Fatalf("second summary revision = %d, want 2", second.Revision)
	}
	if got, err := json.Marshal(first); err == nil {
		for i := range snap.Instance.Outputs {
			if snap.Instance.Outputs[i].ID == first.ID {
				b, _ := json.Marshal(snap.Instance.Outputs[i])
				if string(b) != string(got) {
					t.Fatalf("prior OutputValue mutated: got %s want %s", b, got)
				}
				if snap.Instance.Outputs[i].Revision != 1 {
					t.Fatalf("prior OutputValue revision = %d, want 1", snap.Instance.Outputs[i].Revision)
				}
				if string(snap.Instance.Outputs[i].Value) != `"first"` {
					t.Fatalf("prior OutputValue value = %s, want \"first\"", snap.Instance.Outputs[i].Value)
				}
			}
		}
	}
	if snap.Instance.Status != WorkflowCompleted || snap.Instance.TerminalOutcome != "ok" {
		t.Fatalf("workflow = %q terminal %q, want completed/ok", snap.Instance.Status, snap.Instance.TerminalOutcome)
	}
}
