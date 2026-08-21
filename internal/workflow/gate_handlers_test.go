package workflow

// Tests for the gate control surface (workflow gate/skip/unblock commands,
// Stage 12 phase 2). They build on the completion fixture helpers: a gated
// instance is parked awaiting_gate through the machine completion path, then
// resolved through the manager's WorkflowCommand dispatch.

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// gateTerminalTemplateJSON is a gated node with NO branch for "done": a
// passing gate resolution completes the workflow instead of following a
// branch.
const gateTerminalTemplateJSON = `{
  "schema_version": 1,
  "template_id": "gate-terminal",
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
    "outcomes": [{"name": "done", "terminal": true}],
    "gates": [{"id": "review", "type": "human", "required": true}]
  }],
  "terminal_outcomes": ["done"]
}`

// gateDoubleTemplateJSON is a gated node with TWO required gates, so skip
// must waive both and the final waive resolves the branch.
const gateDoubleTemplateJSON = `{
  "schema_version": 1,
  "template_id": "gate-double",
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
      "gates": [
        {"id": "review", "type": "human", "required": true},
        {"id": "signoff", "type": "human", "required": true}
      ]
    },
    {"id": "next", "action": {"type": "manual"}}
  ],
  "terminal_outcomes": ["done"]
}`

const unblockTemplateJSON = `{
  "schema_version": 1,
  "template_id": "unblock-test",
  "template_version": "1",
  "entry_nodes": ["start"],
  "nodes": [{"id": "start", "action": {"type": "manual"}}],
  "terminal_outcomes": ["done"]
}`

// gateRejectBranchTemplateJSON is a gated node where a reject outcome
// "failed" resolves to a declared branch target ("fix"), so a reject with an
// explicit outcome must follow that branch (creating the fix activation)
// rather than merely rejecting the parked activation.
const gateRejectBranchTemplateJSON = `{
  "schema_version": 1,
  "template_id": "gate-reject-branch",
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
      "branches": {"done": "next", "failed": "fix"},
      "gates": [{"id": "review", "type": "human", "required": true}]
    },
    {"id": "next", "action": {"type": "manual"}},
    {"id": "fix", "action": {"type": "manual"}}
  ],
  "terminal_outcomes": ["done"]
}`

// parkGatedActivation drives a gated instance (start node with the
// complete-template contract) to a parked awaiting_gate activation and
// returns the activation ID.
func parkGatedActivation(t *testing.T, m *Manager, s *Store, wf WorkflowID) ActivationID {
	t.Helper()
	res, actID, attemptID := completeToRunning(t, m, s, wf)
	src, digest := writeEvidence(t, "evidence.txt", "run output\n")
	out, err := m.commandComplete(wf, completePayload(t, string(actID), string(attemptID), res, "done", standardOutputs(), []map[string]any{{
		"src_path": src, "stored_path": "evidence.txt", "non_empty": true, "sha256": digest,
	}}))
	if err != nil {
		t.Fatalf("gated complete: %v", err)
	}
	if mm := out.(map[string]any); mm["activation_status"] != string(ActivationAwaitingGate) {
		t.Fatalf("gated complete result = %#v, want parked awaiting_gate", mm)
	}
	return actID
}

// gateCommandPayload builds a workflow gate command payload with the given
// operation and extra fields (actor, reason, evidence_ids, ...).
func gateCommandPayload(t *testing.T, actID, gateID, op string, extra map[string]any) json.RawMessage {
	t.Helper()
	payload := map[string]any{
		"op":            "gate",
		"node_id":       "start",
		"activation_id": actID,
		"gate_id":       gateID,
		"operation":     op,
	}
	for k, v := range extra {
		payload[k] = v
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal gate: %v", err)
	}
	return data
}

// snapshotUnchanged asserts the revision and the start activation status are
// exactly as before a rejected command.
func snapshotUnchanged(t *testing.T, s *Store, wf WorkflowID, rev int64, status ActivationStatus) {
	t.Helper()
	if got := revision(t, s, wf); got != rev {
		t.Fatalf("revision changed %d -> %d", rev, got)
	}
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if got := activationByNode(&snap.Instance, "start").Status; got != status {
		t.Fatalf("activation status = %q, want %q (no mutation)", got, status)
	}
}

func TestCommandGateSatisfyResolvesBranch(t *testing.T) {
	m, s, wf := newCompleteFixture(t, completeGatedTemplateJSON, "complete-gated", "1")
	actID := parkGatedActivation(t, m, s, wf)
	revBefore := revision(t, s, wf)

	out, err := m.WorkflowCommand(string(wf), gateCommandPayload(t, string(actID), "review", "satisfy", map[string]any{
		"actor": "alice", "reason": "reviewed the diff", "evidence_ids": []string{"ev_1"},
	}))
	if err != nil {
		t.Fatalf("gate satisfy: %v", err)
	}
	mm, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("gate result = %#v, want map", out)
	}
	if mm["status"] != "completed" || mm["activation_status"] != string(ActivationSatisfied) || mm["gate_status"] != string(GatePassed) {
		t.Fatalf("gate result = %#v", mm)
	}
	if mm["revision"].(int64) <= revBefore {
		t.Fatalf("revision not advanced: %v", mm["revision"])
	}

	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if got := activationByNode(&snap.Instance, "start").Status; got != ActivationSatisfied {
		t.Fatalf("start activation status = %q, want satisfied", got)
	}
	if activationByNode(&snap.Instance, "next") == nil {
		t.Fatal("branch target activation \"next\" was not created")
	}
	if snap.Instance.Status != WorkflowActive || snap.Instance.TerminalOutcome != "" {
		t.Fatalf("workflow = %q terminal %q, want active (branch followed)", snap.Instance.Status, snap.Instance.TerminalOutcome)
	}
	if len(snap.Instance.Gates) != 1 || snap.Instance.Gates[0].Status != GatePassed {
		t.Fatalf("gate instances = %+v, want one passed", snap.Instance.Gates)
	}
}

func TestCommandGateSatisfyTerminalWorkflow(t *testing.T) {
	m, s, wf := newCompleteFixture(t, gateTerminalTemplateJSON, "gate-terminal", "1")
	actID := parkGatedActivation(t, m, s, wf)

	out, err := m.WorkflowCommand(string(wf), gateCommandPayload(t, string(actID), "review", "satisfy", map[string]any{
		"actor": "alice", "reason": "approved", "evidence_ids": []string{"ev_1"},
	}))
	if err != nil {
		t.Fatalf("gate satisfy: %v", err)
	}
	mm := out.(map[string]any)
	if mm["status"] != "completed" || mm["activation_status"] != string(ActivationSatisfied) {
		t.Fatalf("gate result = %#v", mm)
	}
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if snap.Instance.Status != WorkflowCompleted || snap.Instance.TerminalOutcome != "done" {
		t.Fatalf("workflow = %q terminal %q, want completed/done", snap.Instance.Status, snap.Instance.TerminalOutcome)
	}
	if len(snap.Instance.Activations) != 1 {
		t.Fatalf("activation count = %d, want 1 (no branch, no new activation)", len(snap.Instance.Activations))
	}
}

// externalResultExtras is a valid external_result field envelope (its result
// is set per test).
func externalResultExtras(result string) map[string]any {
	now := time.Now().UTC()
	return map[string]any{
		"result":        result,
		"poll_id":       "poll_1",
		"source":        "ci",
		"subject":       map[string]any{"type": "repository", "revision": "abc123"},
		"response_hash": strings.Repeat("f", 64),
		"observed_at":   now,
		"evidence_ids":  []string{"ev_ext"},
	}
}

func TestCommandGateExternalResultPassedResolves(t *testing.T) {
	m, s, wf := newCompleteFixture(t, completeGatedTemplateJSON, "complete-gated", "1")
	actID := parkGatedActivation(t, m, s, wf)

	out, err := m.WorkflowCommand(string(wf), gateCommandPayload(t, string(actID), "review", "external_result", externalResultExtras("passed")))
	if err != nil {
		t.Fatalf("gate external_result passed: %v", err)
	}
	mm := out.(map[string]any)
	if mm["status"] != "completed" || mm["activation_status"] != string(ActivationSatisfied) {
		t.Fatalf("gate result = %#v", mm)
	}
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if activationByNode(&snap.Instance, "next") == nil {
		t.Fatal("branch target activation \"next\" was not created")
	}
	if snap.Instance.Status != WorkflowActive {
		t.Fatalf("workflow = %q, want active", snap.Instance.Status)
	}
}

func TestCommandGateExternalResultFailedRejects(t *testing.T) {
	m, s, wf := newCompleteFixture(t, completeGatedTemplateJSON, "complete-gated", "1")
	actID := parkGatedActivation(t, m, s, wf)

	out, err := m.WorkflowCommand(string(wf), gateCommandPayload(t, string(actID), "review", "external_result", externalResultExtras("failed")))
	if err != nil {
		t.Fatalf("gate external_result failed: %v", err)
	}
	mm := out.(map[string]any)
	if mm["status"] != "rejected" || mm["activation_status"] != string(ActivationRejected) {
		t.Fatalf("gate result = %#v", mm)
	}
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if snap.Instance.Status == WorkflowCompleted || snap.Instance.Status == WorkflowFailed {
		t.Fatalf("workflow = %q, want non-terminal (a gate failure rejects the activation only)", snap.Instance.Status)
	}
}

// TestCommandGateExternalResultPollProgression catches the external_result
// idempotency-key regression: a later, distinct report on the same gate
// (pending -> passed) must be a distinct command, not swallowed as an
// idempotent no-op of the first report. Without the result in the key, the
// gate could never progress past its first report.
func TestCommandGateExternalResultPollProgression(t *testing.T) {
	m, s, wf := newCompleteFixture(t, completeGatedTemplateJSON, "complete-gated", "1")
	actID := parkGatedActivation(t, m, s, wf)

	// First report: pending. The activation stays parked awaiting_gate and a
	// GatePending instance is recorded.
	out, err := m.WorkflowCommand(string(wf), gateCommandPayload(t, string(actID), "review", "external_result", externalResultExtras("pending")))
	if err != nil {
		t.Fatalf("external_result pending: %v", err)
	}
	mm := out.(map[string]any)
	if mm["idempotent"] == true {
		t.Fatalf("first report must not be idempotent: %#v", mm)
	}
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if got := activationByNode(&snap.Instance, "start").Status; got != ActivationAwaitingGate {
		t.Fatalf("after pending: activation status = %q, want awaiting_gate", got)
	}
	if len(snap.Instance.Gates) != 1 || snap.Instance.Gates[0].Status != GatePending {
		t.Fatalf("gate instances = %+v, want one pending", snap.Instance.Gates)
	}

	// Second report: passed. This is a distinct command (different result in
	// the key) and must resolve the activation, not be swallowed as a no-op.
	out, err = m.WorkflowCommand(string(wf), gateCommandPayload(t, string(actID), "review", "external_result", externalResultExtras("passed")))
	if err != nil {
		t.Fatalf("external_result passed: %v", err)
	}
	mm = out.(map[string]any)
	if mm["idempotent"] == true {
		t.Fatalf("distinct passed report swallowed as idempotent no-op: %#v", mm)
	}
	if mm["status"] != "completed" || mm["activation_status"] != string(ActivationSatisfied) {
		t.Fatalf("passed report result = %#v, want completed/satisfied", mm)
	}
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if got := activationByNode(&snap.Instance, "start").Status; got != ActivationSatisfied {
		t.Fatalf("after passed: activation status = %q, want satisfied", got)
	}
	if activationByNode(&snap.Instance, "next") == nil {
		t.Fatal("branch target activation \"next\" was not created")
	}
}

// TestCommandGateRejectResolvesBranch pins the sibling-transition payload for
// a reject: an explicit outcome that resolves to a declared branch target
// creates the branch activation while the parked one is rejected.
func TestCommandGateRejectResolvesBranch(t *testing.T) {
	m, s, wf := newCompleteFixture(t, gateRejectBranchTemplateJSON, "gate-reject-branch", "1")
	actID := parkGatedActivation(t, m, s, wf)

	out, err := m.WorkflowCommand(string(wf), gateCommandPayload(t, string(actID), "review", "reject", map[string]any{
		"actor": "alice", "reason": "r", "evidence_ids": []string{"ev_1"}, "outcome": "failed",
	}))
	if err != nil {
		t.Fatalf("gate reject: %v", err)
	}
	mm := out.(map[string]any)
	if mm["status"] != "rejected" || mm["activation_status"] != string(ActivationRejected) {
		t.Fatalf("reject result = %#v, want rejected", mm)
	}
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if got := activationByNode(&snap.Instance, "start").Status; got != ActivationRejected {
		t.Fatalf("start activation status = %q, want rejected", got)
	}
	if activationByNode(&snap.Instance, "fix") == nil {
		t.Fatal("branch target activation \"fix\" was not created")
	}
}

// TestCommandGateExternalResultActionRequired pins the no-payload parking
// case: an action_required report leaves the activation awaiting_gate with no
// branch and no workflow completion.
func TestCommandGateExternalResultActionRequired(t *testing.T) {
	m, s, wf := newCompleteFixture(t, completeGatedTemplateJSON, "complete-gated", "1")
	actID := parkGatedActivation(t, m, s, wf)

	out, err := m.WorkflowCommand(string(wf), gateCommandPayload(t, string(actID), "review", "external_result", externalResultExtras("action_required")))
	if err != nil {
		t.Fatalf("external_result action_required: %v", err)
	}
	mm := out.(map[string]any)
	if mm["activation_status"] != string(ActivationAwaitingGate) {
		t.Fatalf("action_required result = %#v, want awaiting_gate", mm)
	}
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if got := activationByNode(&snap.Instance, "start").Status; got != ActivationAwaitingGate {
		t.Fatalf("start activation status = %q, want awaiting_gate", got)
	}
	if activationByNode(&snap.Instance, "next") != nil {
		t.Fatal("action_required must not create a branch activation")
	}
	if snap.Instance.Status == WorkflowCompleted || snap.Instance.Status == WorkflowFailed {
		t.Fatalf("workflow = %q, want non-terminal", snap.Instance.Status)
	}
}

func TestCommandGateUnknownExternalResultUnchanged(t *testing.T) {
	m, s, wf := newCompleteFixture(t, completeGatedTemplateJSON, "complete-gated", "1")
	actID := parkGatedActivation(t, m, s, wf)
	revBefore := revision(t, s, wf)

	_, err := m.WorkflowCommand(string(wf), gateCommandPayload(t, string(actID), "review", "external_result", externalResultExtras("weird")))
	if err == nil {
		t.Fatal("external_result with unknown result: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "unknown external result") {
		t.Fatalf("error = %q, want unknown external result", err)
	}
	snapshotUnchanged(t, s, wf, revBefore, ActivationAwaitingGate)
}

func TestCommandGateUnknownOperation(t *testing.T) {
	m, s, wf := newCompleteFixture(t, completeGatedTemplateJSON, "complete-gated", "1")
	actID := parkGatedActivation(t, m, s, wf)
	revBefore := revision(t, s, wf)

	_, err := m.WorkflowCommand(string(wf), gateCommandPayload(t, string(actID), "review", "bogus", map[string]any{
		"actor": "alice", "reason": "r", "evidence_ids": []string{"ev_1"},
	}))
	if err == nil {
		t.Fatal("gate with unknown operation: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "unknown gate operation") {
		t.Fatalf("error = %q, want unknown gate operation", err)
	}
	snapshotUnchanged(t, s, wf, revBefore, ActivationAwaitingGate)
}

func TestCommandGateMissingActorUnchanged(t *testing.T) {
	m, s, wf := newCompleteFixture(t, completeGatedTemplateJSON, "complete-gated", "1")
	actID := parkGatedActivation(t, m, s, wf)
	revBefore := revision(t, s, wf)

	_, err := m.WorkflowCommand(string(wf), gateCommandPayload(t, string(actID), "review", "satisfy", map[string]any{
		"reason": "approved", "evidence_ids": []string{"ev_1"},
	}))
	if err == nil {
		t.Fatal("gate satisfy without actor: expected error, got nil")
	}
	snapshotUnchanged(t, s, wf, revBefore, ActivationAwaitingGate)
}

func TestCommandGateUndeclaredGate(t *testing.T) {
	m, s, wf := newCompleteFixture(t, completeGatedTemplateJSON, "complete-gated", "1")
	actID := parkGatedActivation(t, m, s, wf)
	revBefore := revision(t, s, wf)

	_, err := m.WorkflowCommand(string(wf), gateCommandPayload(t, string(actID), "nope", "satisfy", map[string]any{
		"actor": "alice", "reason": "r", "evidence_ids": []string{"ev_1"},
	}))
	if err == nil {
		t.Fatal("gate on undeclared gate: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "not declared") {
		t.Fatalf("error = %q, want not declared", err)
	}
	snapshotUnchanged(t, s, wf, revBefore, ActivationAwaitingGate)
}

func TestCommandGateActivationNotAwaitingGate(t *testing.T) {
	// A fresh instance's start activation is still pending, not parked.
	m, s, wf := newCompleteFixture(t, completeGatedTemplateJSON, "complete-gated", "1")
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	actID := snap.Instance.Activations[0].ID
	revBefore := revision(t, s, wf)

	_, err = m.WorkflowCommand(string(wf), gateCommandPayload(t, string(actID), "review", "satisfy", map[string]any{
		"actor": "alice", "reason": "r", "evidence_ids": []string{"ev_1"},
	}))
	if err == nil {
		t.Fatal("gate on pending activation: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "cannot gate activation in status") {
		t.Fatalf("error = %q, want status rejection", err)
	}
	snapshotUnchanged(t, s, wf, revBefore, ActivationPending)
}

func TestCommandGateIdempotentReissue(t *testing.T) {
	m, s, wf := newCompleteFixture(t, completeGatedTemplateJSON, "complete-gated", "1")
	actID := parkGatedActivation(t, m, s, wf)
	extra := map[string]any{"actor": "alice", "reason": "approved", "evidence_ids": []string{"ev_1"}}
	payload := gateCommandPayload(t, string(actID), "review", "satisfy", extra)

	out, err := m.WorkflowCommand(string(wf), payload)
	if err != nil {
		t.Fatalf("first gate: %v", err)
	}
	mm := out.(map[string]any)
	if mm["idempotent"] == true || mm["status"] != "completed" {
		t.Fatalf("first gate result = %#v", mm)
	}
	revAfter := revision(t, s, wf)
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	gatesBefore := len(snap.Instance.Gates)

	// Re-issuing the SAME decision is a safe no-op: the prior facts are
	// immutable and the activation is already resolved.
	out2, err := m.WorkflowCommand(string(wf), payload)
	if err != nil {
		t.Fatalf("duplicate gate: %v", err)
	}
	mm2, ok := out2.(map[string]any)
	if !ok {
		t.Fatalf("duplicate gate result = %#v, want map", out2)
	}
	if mm2["idempotent"] != true || mm2["status"] != "completed" || mm2["activation_status"] != string(ActivationSatisfied) {
		t.Fatalf("duplicate gate result = %#v, want idempotent no-op", mm2)
	}
	if got := revision(t, s, wf); got != revAfter {
		t.Fatalf("revision advanced by duplicate: %d -> %d", revAfter, got)
	}
	snap2, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if len(snap2.Instance.Gates) != gatesBefore {
		t.Fatalf("gate instances = %d, want %d (prior facts immutable)", len(snap2.Instance.Gates), gatesBefore)
	}
	if len(snap2.Instance.Activations) != len(snap.Instance.Activations) {
		t.Fatalf("activation count changed: %d -> %d", len(snap.Instance.Activations), len(snap2.Instance.Activations))
	}
}

func TestCommandSkipWaivesRequiredGates(t *testing.T) {
	m, s, wf := newCompleteFixture(t, gateDoubleTemplateJSON, "gate-double", "1")
	parkGatedActivation(t, m, s, wf)

	out, err := m.WorkflowCommand(string(wf), skipCommandPayload(t, "alice", "not needed right now", "ev_skip"))
	if err != nil {
		t.Fatalf("skip: %v", err)
	}
	mm, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("skip result = %#v, want map", out)
	}
	if mm["skipped"] != true || mm["activation_status"] != string(ActivationSatisfied) {
		t.Fatalf("skip result = %#v", mm)
	}
	waived, ok := mm["waived_gates"].([]string)
	if !ok || len(waived) != 2 {
		t.Fatalf("waived_gates = %#v, want both required gates", mm["waived_gates"])
	}

	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if got := activationByNode(&snap.Instance, "start").Status; got != ActivationSatisfied {
		t.Fatalf("start activation status = %q, want satisfied", got)
	}
	if activationByNode(&snap.Instance, "next") == nil {
		t.Fatal("branch target activation \"next\" was not created")
	}
	// Both gates are waived, recorded with the actor/reason/evidence.
	seen := map[GateID]GateStatus{}
	for _, gi := range snap.Instance.Gates {
		seen[gi.GateID] = gi.Status
	}
	if seen["review"] != GateWaived || seen["signoff"] != GateWaived {
		t.Fatalf("gate statuses = %#v, want both waived", seen)
	}
}

func TestCommandSkipAlreadyResolvedRejected(t *testing.T) {
	m, s, wf := newCompleteFixture(t, completeGatedTemplateJSON, "complete-gated", "1")
	actID := parkGatedActivation(t, m, s, wf)

	// Satisfy the only gate first so the activation resolves.
	if _, err := m.WorkflowCommand(string(wf), gateCommandPayload(t, string(actID), "review", "satisfy", map[string]any{
		"actor": "alice", "reason": "approved", "evidence_ids": []string{"ev_1"},
	})); err != nil {
		t.Fatalf("gate satisfy: %v", err)
	}
	revBefore := revision(t, s, wf)

	// A skip after the gates are all satisfied is rejected: there is no
	// unsatisfied required gate left to waive, and the activation is no
	// longer parked.
	_, err := m.WorkflowCommand(string(wf), skipCommandPayload(t, "alice", "r", "ev_skip"))
	if err == nil {
		t.Fatal("skip after resolution: expected error, got nil")
	}
	if got := revision(t, s, wf); got != revBefore {
		t.Fatalf("revision changed %d -> %d", revBefore, got)
	}
}

// TestRemainingRequiredGatesEmptyWhenAllPassed pins the helper's empty case
// (the defensive "no unsatisfied required gate to skip" error in commandSkip
// is only reachable through a state the reducer itself cannot produce, so the
// helper is tested directly).
func TestRemainingRequiredGatesEmptyWhenAllPassed(t *testing.T) {
	node := &NodeDefinition{Gates: []GateDefinition{
		{ID: "review", Required: true},
		{ID: "signoff", Required: true},
		{ID: "cc", Required: false},
	}}
	inst := &WorkflowInstance{Gates: []GateInstance{
		{ActivationID: "act_1", GateID: "review", Status: GatePassed},
		{ActivationID: "act_1", GateID: "signoff", Status: GateWaived},
		{ActivationID: "act_other", GateID: "review", Status: GateFailed},
	}}
	if remaining := remainingRequiredGates(inst, node, "act_1", "", ""); len(remaining) != 0 {
		t.Fatalf("remaining = %+v, want empty (all required gates resolved)", remaining)
	}
	// The in-flight decision is folded in: a pass for review while signoff
	// is still missing leaves exactly signoff.
	inst2 := &WorkflowInstance{Gates: []GateInstance{{ActivationID: "act_1", GateID: "signoff", Status: GateRejected}}}
	if remaining := remainingRequiredGates(inst2, node, "act_1", "review", GatePassed); len(remaining) != 1 || remaining[0].ID != "signoff" {
		t.Fatalf("remaining = %+v, want [signoff]", remaining)
	}
}

// blockActivation drives a plain manual activation to blocked via claim,
// start, and a failed attempt termination (no retry policy: a single failure
// exhausts to blocked).
func blockActivation(t *testing.T, m *Manager, s *Store, wf WorkflowID) {
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
	if err := m.RecordAttemptTerminated(wf, "start", actID, attemptID, leaseID, AttemptFailed); err != nil {
		t.Fatalf("RecordAttemptTerminated: %v", err)
	}
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if got := activationByNode(&snap.Instance, "start").Status; got != ActivationBlocked {
		t.Fatalf("activation status = %q, want blocked", got)
	}
}

func TestCommandUnblockBlockedActivation(t *testing.T) {
	m, s, wf := newCompleteFixture(t, unblockTemplateJSON, "unblock-test", "1")
	blockActivation(t, m, s, wf)

	out, err := m.WorkflowCommand(string(wf), unblockCommandPayload(t, "alice", "root cause fixed"))
	if err != nil {
		t.Fatalf("unblock: %v", err)
	}
	mm, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("unblock result = %#v, want map", out)
	}
	if mm["status"] != "unblocked" || mm["activation_status"] != string(ActivationReady) {
		t.Fatalf("unblock result = %#v", mm)
	}
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if got := activationByNode(&snap.Instance, "start").Status; got != ActivationReady {
		t.Fatalf("activation status = %q, want ready", got)
	}
}

// TestCommandUnblockAfterReblock pins the block-episode-scoped unblock
// idempotency key: the same activation may block again after being unblocked
// to ready, and the second unblock must succeed (not be swallowed as a false
// idempotent no-op against a permanent key).
func TestCommandUnblockAfterReblock(t *testing.T) {
	m, s, wf := newCompleteFixture(t, unblockTemplateJSON, "unblock-test", "1")

	// First block episode: fail to exhaustion, then unblock back to ready.
	blockActivation(t, m, s, wf)
	out, err := m.WorkflowCommand(string(wf), unblockCommandPayload(t, "alice", "root cause fixed"))
	if err != nil {
		t.Fatalf("first unblock: %v", err)
	}
	if mm := out.(map[string]any); mm["status"] != "unblocked" || mm["activation_status"] != string(ActivationReady) {
		t.Fatalf("first unblock result = %#v", mm)
	}

	// Drive it to blocked again: a new claim/start/failed-attempt episode adds
	// a fresh attempt, so the attempt count strictly increases.
	blockActivation(t, m, s, wf)

	// Second unblock must succeed and return to ready, not report a false
	// idempotent no-op against the first episode's key.
	out, err = m.WorkflowCommand(string(wf), unblockCommandPayload(t, "alice", "root cause fixed again"))
	if err != nil {
		t.Fatalf("second unblock: %v", err)
	}
	mm := out.(map[string]any)
	if mm["idempotent"] == true {
		t.Fatalf("second unblock swallowed as false idempotent no-op: %#v", mm)
	}
	if mm["status"] != "unblocked" || mm["activation_status"] != string(ActivationReady) {
		t.Fatalf("second unblock result = %#v, want unblocked/ready", mm)
	}
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if got := activationByNode(&snap.Instance, "start").Status; got != ActivationReady {
		t.Fatalf("activation status after second unblock = %q, want ready", got)
	}
}

func TestCommandUnblockNonBlockedRejected(t *testing.T) {
	// A fresh instance's start activation is pending, not blocked.
	m, s, wf := newCompleteFixture(t, unblockTemplateJSON, "unblock-test", "1")
	revBefore := revision(t, s, wf)

	_, err := m.WorkflowCommand(string(wf), unblockCommandPayload(t, "alice", "r"))
	if err == nil {
		t.Fatal("unblock a pending activation: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "cannot unblock activation in status") {
		t.Fatalf("error = %q, want status rejection", err)
	}
	if got := revision(t, s, wf); got != revBefore {
		t.Fatalf("revision changed %d -> %d", revBefore, got)
	}
}

func TestCommandUnblockIdempotentReissue(t *testing.T) {
	m, s, wf := newCompleteFixture(t, unblockTemplateJSON, "unblock-test", "1")
	blockActivation(t, m, s, wf)
	payload := unblockCommandPayload(t, "alice", "root cause fixed")

	out, err := m.WorkflowCommand(string(wf), payload)
	if err != nil {
		t.Fatalf("first unblock: %v", err)
	}
	if mm := out.(map[string]any); mm["status"] != "unblocked" {
		t.Fatalf("first unblock result = %#v", mm)
	}
	revAfter := revision(t, s, wf)

	out2, err := m.WorkflowCommand(string(wf), payload)
	if err != nil {
		t.Fatalf("duplicate unblock: %v", err)
	}
	mm2 := out2.(map[string]any)
	if mm2["idempotent"] != true || mm2["status"] != "unblocked" {
		t.Fatalf("duplicate unblock result = %#v, want idempotent no-op", mm2)
	}
	if got := revision(t, s, wf); got != revAfter {
		t.Fatalf("revision advanced by duplicate: %d -> %d", revAfter, got)
	}
}

func skipCommandPayload(t *testing.T, actor, reason, evidenceID string) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(map[string]any{
		"op":           "skip",
		"node_id":      "start",
		"actor":        actor,
		"reason":       reason,
		"evidence_ids": []string{evidenceID},
	})
	if err != nil {
		t.Fatalf("marshal skip: %v", err)
	}
	return data
}

func unblockCommandPayload(t *testing.T, actor, reason string) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(map[string]any{
		"op":      "unblock",
		"node_id": "start",
		"actor":   actor,
		"reason":  reason,
	})
	if err != nil {
		t.Fatalf("marshal unblock: %v", err)
	}
	return data
}
