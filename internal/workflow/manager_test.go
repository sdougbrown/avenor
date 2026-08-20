package workflow

// Internal tests for the Manager API surface (create/instantiate/read/wait/
// events plus the command stub). They use only the standard library.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const managerTemplateJSON = `{
  "schema_version": 1,
  "template_id": "mgr-test",
  "template_version": "1",
  "entry_nodes": ["start"],
  "nodes": [{"id": "start", "action": {"type": "manual"}}],
  "terminal_outcomes": ["done"]
}`

const managerWorkflowActionTemplateJSON = `{
  "schema_version": 1,
  "template_id": "mgr-child",
  "template_version": "1",
  "entry_nodes": ["spawn"],
  "nodes": [{"id": "spawn", "action": {"type": "workflow", "template_id": "child", "template_version": "1", "child_key": "c1", "outcome_map": {"done": "done"}}}],
  "terminal_outcomes": ["done"]
}`

// runTemplateJSON is a single-node template whose start action is provider-
// backed ("run") so the executor seam can be exercised.
const runTemplateJSON = `{
  "schema_version": 1,
  "template_id": "exec-test",
  "template_version": "1",
  "entry_nodes": ["start"],
  "nodes": [{"id": "start", "action": {"type": "run", "prompt": "do the thing"}}],
  "terminal_outcomes": ["done"]
}`

// newRunTemplateFixture builds an ephemeral store + manager with a stored
// "run"-action template and a fresh instance of it.
func newRunTemplateFixture(t *testing.T) (*Manager, *Store, WorkflowID) {
	t.Helper()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	if _, err := m.WorkflowCreate([]byte(runTemplateJSON)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	payload, err := json.Marshal(map[string]string{
		"template_id": "exec-test", "template_version": "1",
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

// claimCommandPayload builds a claim command payload.
func claimCommandPayload(t *testing.T, nodeID, activationID, actor string) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(map[string]any{
		"op":            "claim",
		"node_id":       nodeID,
		"activation_id": activationID,
		"actor":         actor,
	})
	if err != nil {
		t.Fatalf("marshal claim: %v", err)
	}
	return data
}

// claimActivation issues a claim through WorkflowCommand and returns its
// result map, failing the test on error.
func claimActivation(t *testing.T, m *Manager, wf WorkflowID, nodeID, activationID, actor string) map[string]any {
	t.Helper()
	out, err := m.WorkflowCommand(string(wf), claimCommandPayload(t, nodeID, activationID, actor))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	mm, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("claim result = %#v, want map", out)
	}
	return mm
}

// startCommandPayload builds a start command payload from a claim result map
// (lease_id/owner_token are pulled from res so tests can override them for
// mismatch cases).
func startCommandPayload(t *testing.T, nodeID, activationID string, res map[string]any, selection map[string]any) json.RawMessage {
	t.Helper()
	payload := map[string]any{
		"op":            "start",
		"node_id":       nodeID,
		"activation_id": activationID,
		"lease_id":      res["lease_id"],
		"owner_token":   res["owner_token"],
	}
	if selection != nil {
		payload["selection"] = selection
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal start: %v", err)
	}
	return data
}

// activationByNode returns the first activation for a node in an instance, or
// nil when the node has none.
func activationByNode(inst *WorkflowInstance, nodeID NodeID) *Activation {
	for i := range inst.Activations {
		if inst.Activations[i].NodeID == nodeID {
			return &inst.Activations[i]
		}
	}
	return nil
}

// newManagerFixture builds an ephemeral store + manager and stores the
// single-node test template.
func newManagerFixture(t *testing.T) (*Manager, *Store, WorkflowID, string) {
	t.Helper()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	if _, err := m.WorkflowCreate([]byte(managerTemplateJSON)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	// Instantiate a fresh instance for tests that need one.
	payload, err := json.Marshal(map[string]string{
		"template_id": "mgr-test", "template_version": "1",
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
	return m, s, WorkflowID(wf), "start"
}

// runToTerminalStore drives a store instance claim -> start -> complete so the
// workflow reaches WorkflowCompleted.
func runToTerminalStore(t *testing.T, s *Store, wf WorkflowID) {
	t.Helper()
	snap, ok, err := s.loadSnapshot(wf)
	if err != nil || !ok {
		t.Fatalf("load snapshot: %v", err)
	}
	actID := snap.Instance.Activations[0].ID
	identity := ExecutionIdentity{WorkflowID: wf, NodeID: "start", ActivationID: actID}
	snap = mustStoreApply(t, s, wf, Command{
		Kind:             CommandClaim,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "claim",
		Identity:         identity,
		LeaseID:          "lease-1",
		Actor:            "alice",
	})
	snap = mustStoreApply(t, s, wf, Command{
		Kind:             CommandStart,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "start",
		Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: "start", ActivationID: actID, AttemptID: "att-1"},
		LeaseID:          "lease-1",
	})
	snap = mustStoreApply(t, s, wf, Command{
		Kind:             CommandComplete,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "complete",
		Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: "start", ActivationID: actID, AttemptID: "att-1"},
		LeaseID:          "lease-1",
		Outcome:          "done",
	})
}

// mustStoreApply applies a command to the store, failing the test on error.
func mustStoreApply(t *testing.T, s *Store, wf WorkflowID, cmd Command) Snapshot {
	t.Helper()
	snap, err := s.ApplyCommand(wf, cmd)
	if err != nil {
		t.Fatalf("%s: %v", cmd.Kind, err)
	}
	return snap
}

func TestManagerCreateStoresTemplate(t *testing.T) {
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	out, err := m.WorkflowCreate([]byte(managerTemplateJSON))
	if err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	mm, ok := out.(map[string]any)
	if !ok || mm["template_id"] != "mgr-test" || mm["template_version"] != "1" {
		t.Fatalf("create result = %#v, want template_id=mgr-test template_version=1", out)
	}
	loaded, err := s.LoadTemplate("mgr-test", "1")
	if err != nil {
		t.Fatalf("LoadTemplate: %v", err)
	}
	if loaded.TemplateID != "mgr-test" || len(loaded.Nodes) != 1 {
		t.Fatalf("loaded template = %+v", loaded)
	}
}

func TestManagerCreateRejectsInvalidTemplate(t *testing.T) {
	s := newStore(t)
	m := NewManager(s)
	if _, err := m.WorkflowCreate([]byte(`{"schema_version": 1}`)); err == nil {
		t.Fatal("WorkflowCreate accepted an invalid template")
	}
	if _, err := m.WorkflowCreate([]byte(`not json`)); err == nil {
		t.Fatal("WorkflowCreate accepted non-JSON")
	}
}

func TestManagerInstantiateCreatesActiveInstance(t *testing.T) {
	m, _, wf, _ := newManagerFixture(t)
	st, err := m.WorkflowStatus(string(wf))
	if err != nil {
		t.Fatalf("WorkflowStatus: %v", err)
	}
	mm := st.(map[string]any)
	if mm["status"] != string(WorkflowActive) {
		t.Fatalf("status = %v, want active", mm["status"])
	}
	if mm["template_id"] != "mgr-test" || mm["template_version"] != "1" {
		t.Fatalf("template fields = %#v", mm)
	}
	// Missing template must error.
	if _, err := m.WorkflowInstantiate([]byte(`{"template_id":"nope","template_version":"1"}`)); err == nil {
		t.Fatal("instantiate of missing template did not error")
	}
	// Missing fields must error.
	if _, err := m.WorkflowInstantiate([]byte(`{"template_version":"1"}`)); err == nil {
		t.Fatal("instantiate without template_id did not error")
	}
}

func TestManagerInstantiateRejectsWorkflowAction(t *testing.T) {
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	if _, err := m.WorkflowCreate([]byte(managerWorkflowActionTemplateJSON)); err != nil {
		t.Fatalf("WorkflowCreate of workflow-action template: %v", err)
	}
	payload, _ := json.Marshal(map[string]string{
		"template_id": "mgr-child", "template_version": "1",
	})
	out, err := m.WorkflowInstantiate(payload)
	if err == nil {
		t.Fatalf("instantiate of workflow-action template succeeded: %#v", out)
	}
	if !strings.Contains(err.Error(), "workflow actions") {
		t.Fatalf("error = %v, want workflow-action rejection", err)
	}
	// The rejection must not create any instance on disk.
	entries, err := os.ReadDir(filepath.Join(s.Root(), "instances"))
	if err != nil {
		t.Fatalf("read instances dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("instances created despite rejection: %d entries", len(entries))
	}
}

func TestManagerStatusMissingErrors(t *testing.T) {
	s := newStore(t)
	m := NewManager(s)
	if _, err := m.WorkflowStatus("wf-missing"); err == nil || !strings.Contains(err.Error(), "workflow not found") {
		t.Fatalf("status of missing workflow: err = %v, want not found", err)
	}
	if _, err := m.WorkflowStatus("../etc/passwd"); err == nil || !strings.Contains(err.Error(), "invalid workflow id") {
		t.Fatalf("status of traversal id: err = %v, want invalid workflow id", err)
	}
}

func TestManagerInspectReturnsDetail(t *testing.T) {
	m, _, wf, _ := newManagerFixture(t)
	out, err := m.WorkflowInspect(string(wf))
	if err != nil {
		t.Fatalf("WorkflowInspect: %v", err)
	}
	mm := out.(map[string]any)
	for _, key := range []string{"workflow_id", "instance", "revision", "activations", "attempts", "evidence", "gates", "outputs"} {
		if _, ok := mm[key]; !ok {
			t.Fatalf("inspect result missing key %q: %#v", key, out)
		}
	}
	inst, ok := mm["instance"].(WorkflowInstance)
	if !ok {
		t.Fatalf("instance not a WorkflowInstance: %#v", mm["instance"])
	}
	if inst.WorkflowID != wf || inst.Status != WorkflowActive {
		t.Fatalf("instance = %+v", inst)
	}
	acts, ok := mm["activations"].([]Activation)
	if !ok || len(acts) != 1 {
		t.Fatalf("activations = %#v", mm["activations"])
	}
}

func TestManagerEventsAfterSeqLimit(t *testing.T) {
	m, _, wf, _ := newManagerFixture(t)
	// After instantiate only sequence 1 exists.
	out, err := m.WorkflowEvents(string(wf), 0, 0)
	if err != nil {
		t.Fatalf("WorkflowEvents: %v", err)
	}
	mm := out.(map[string]any)
	events := mm["events"].([]map[string]any)
	if len(events) != 1 {
		t.Fatalf("events = %#v, want 1", events)
	}
	if events[0]["kind"] != string(EventInstantiated) {
		t.Fatalf("event kind = %v, want instantiated", events[0]["kind"])
	}
	// Nothing after sequence 1.
	out, err = m.WorkflowEvents(string(wf), 1, 0)
	if err != nil {
		t.Fatalf("WorkflowEvents: %v", err)
	}
	events = out.(map[string]any)["events"].([]map[string]any)
	if len(events) != 0 {
		t.Fatalf("events after seq 1 = %#v, want 0", events)
	}
	// limit caps results: claim adds sequence 2, then limit 1 must truncate.
	snap, _, _ := m.store.loadCurrent(wf)
	actID := snap.Instance.Activations[0].ID
	mustStoreApply(t, m.store, wf, Command{
		Kind:             CommandClaim,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "claim",
		Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: "start", ActivationID: actID},
		LeaseID:          "lease-1",
		Actor:            "alice",
	})
	out, err = m.WorkflowEvents(string(wf), 0, 1)
	if err != nil {
		t.Fatalf("WorkflowEvents: %v", err)
	}
	mm = out.(map[string]any)
	events = mm["events"].([]map[string]any)
	if len(events) != 1 || events[0]["sequence"] != float64(1) {
		t.Fatalf("limited events = %#v, want only seq 1", events)
	}
	if mm["after_seq"] != int64(0) || mm["limit"] != 1 {
		t.Fatalf("envelope = %#v", mm)
	}
}

func TestManagerWaitTerminal(t *testing.T) {
	m, s, wf, _ := newManagerFixture(t)
	runToTerminalStore(t, s, wf)
	started := time.Now()
	out, err := m.WorkflowWait(string(wf), 5*time.Second)
	if err != nil {
		t.Fatalf("WorkflowWait: %v", err)
	}
	mm := out.(map[string]any)
	if mm["terminal"] != true || mm["timed_out"] != false {
		t.Fatalf("wait flags = terminal:%v timed_out:%v, want true/false", mm["terminal"], mm["timed_out"])
	}
	inst := mm["instance"].(WorkflowInstance)
	if inst.Status != WorkflowCompleted || inst.TerminalOutcome != "done" {
		t.Fatalf("instance = %+v", inst)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("wait on terminal workflow took %v", elapsed)
	}
}

func TestManagerWaitTimeout(t *testing.T) {
	m, _, wf, _ := newManagerFixture(t)
	started := time.Now()
	out, err := m.WorkflowWait(string(wf), 100*time.Millisecond)
	if err != nil {
		t.Fatalf("WorkflowWait: %v", err)
	}
	mm := out.(map[string]any)
	if mm["terminal"] != false || mm["timed_out"] != true {
		t.Fatalf("wait flags = terminal:%v timed_out:%v, want false/true", mm["terminal"], mm["timed_out"])
	}
	if elapsed := time.Since(started); elapsed < 50*time.Millisecond {
		t.Fatalf("wait returned in %v, expected ~100ms poll timeout", elapsed)
	}
}

func TestManagerCommandStub(t *testing.T) {
	s := newStore(t)
	m := NewManager(s)
	_, err := m.WorkflowCommand("wf1", json.RawMessage(`{}`))
	if err == nil || !strings.Contains(err.Error(), "unsupported until a later stage") {
		t.Fatalf("WorkflowCommand stub err = %v", err)
	}
}

func TestManagerCommandUnknownOp(t *testing.T) {
	m, _, wf, _ := newManagerFixture(t)
	_, err := m.WorkflowCommand(string(wf), json.RawMessage(`{"op":"complete","node_id":"start"}`))
	if err == nil || !strings.Contains(err.Error(), "unsupported until a later stage") {
		t.Fatalf("unknown op err = %v, want unsupported until a later stage", err)
	}
}

func TestManagerClaimGrantsLease(t *testing.T) {
	m, _, wf, node := newManagerFixture(t)
	snap, _, err := m.store.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	actID := activationByNode(&snap.Instance, NodeID(node)).ID
	res := claimActivation(t, m, wf, node, string(actID), "alice")

	leaseID, ok := res["lease_id"].(string)
	if !ok || leaseID == "" {
		t.Fatalf("claim lease_id = %#v, want non-empty string", res["lease_id"])
	}
	token, ok := res["owner_token"].(string)
	if !ok || token == "" {
		t.Fatalf("claim owner_token = %#v, want non-empty string", res["owner_token"])
	}
	expiresAt, ok := res["expires_at"].(time.Time)
	if !ok {
		t.Fatalf("claim expires_at = %#v, want time.Time", res["expires_at"])
	}
	if !expiresAt.After(time.Now().UTC()) {
		t.Fatalf("expires_at = %v, want in the future", expiresAt)
	}
	action, ok := res["action"].(Action)
	if !ok || action.Kind != ActionManual {
		t.Fatalf("claim action = %#v, want the declared manual action", res["action"])
	}

	// The stamped lease must be durable on disk with the same expiry and a
	// digest of the returned raw token.
	snap, _, err = m.store.loadCurrent(wf)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	act := activationByNode(&snap.Instance, NodeID(node))
	if act.Status != ActivationLeased {
		t.Fatalf("activation status = %s, want leased", act.Status)
	}
	if act.ActiveLease == nil {
		t.Fatal("activation has no active lease on disk")
	}
	if act.ActiveLease.ID != LeaseID(leaseID) {
		t.Fatalf("on-disk lease id = %s, want %s", act.ActiveLease.ID, leaseID)
	}
	if !act.ActiveLease.ExpiresAt.After(time.Now().UTC()) {
		t.Fatalf("on-disk expires_at = %v, want in the future", act.ActiveLease.ExpiresAt)
	}
	if act.ActiveLease.TokenDigest != ownerTokenDigest(token) {
		t.Fatalf("on-disk token digest = %s, want digest of the returned token", act.ActiveLease.TokenDigest)
	}
	if act.ActiveLease.Owner != "alice" {
		t.Fatalf("on-disk owner = %s, want alice", act.ActiveLease.Owner)
	}
}

func TestManagerClaimRejectsAlreadyLeased(t *testing.T) {
	m, _, wf, node := newManagerFixture(t)
	snap, _, err := m.store.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	actID := activationByNode(&snap.Instance, NodeID(node)).ID
	claimActivation(t, m, wf, node, string(actID), "alice")
	_, err = m.WorkflowCommand(string(wf), claimCommandPayload(t, node, string(actID), "bob"))
	if err == nil || !strings.Contains(err.Error(), "cannot claim") {
		t.Fatalf("re-claim err = %v, want cannot claim", err)
	}
}

func TestManagerClaimUnknownActivation(t *testing.T) {
	m, _, wf, node := newManagerFixture(t)
	_, err := m.WorkflowCommand(string(wf), claimCommandPayload(t, node, "act-nope", "alice"))
	if err == nil || !strings.Contains(err.Error(), "activation not found") {
		t.Fatalf("claim unknown activation err = %v, want activation not found", err)
	}
}

func TestManagerClaimMissingWorkflow(t *testing.T) {
	s := newStore(t)
	m := NewManager(s)
	_, err := m.WorkflowCommand("wf-missing", claimCommandPayload(t, "start", "", "alice"))
	if err == nil || !strings.Contains(err.Error(), "workflow not found") {
		t.Fatalf("claim missing workflow err = %v, want workflow not found", err)
	}
}

func TestManagerStartManualRequiresCompletion(t *testing.T) {
	m, _, wf, node := newManagerFixture(t)
	snap, _, err := m.store.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	actID := activationByNode(&snap.Instance, NodeID(node)).ID
	res := claimActivation(t, m, wf, node, string(actID), "alice")

	out, err := m.WorkflowCommand(string(wf), startCommandPayload(t, node, string(actID), res, map[string]any{"role": "reviewer"}))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	mm, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("start result = %#v, want map", out)
	}
	if mm["requires_complete"] != true {
		t.Fatalf("requires_complete = %#v, want true", mm["requires_complete"])
	}
	if mm["status"] != string(ActivationRunning) {
		t.Fatalf("status = %v, want running", mm["status"])
	}
	attemptID, _ := mm["attempt_id"].(string)
	if attemptID == "" {
		t.Fatal("start result missing attempt_id")
	}
	action, _ := mm["action"].(Action)
	if action.Kind != ActionManual {
		t.Fatalf("action = %#v, want manual", mm["action"])
	}

	snap, _, err = m.store.loadCurrent(wf)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	act := activationByNode(&snap.Instance, NodeID(node))
	if act.Status != ActivationRunning {
		t.Fatalf("activation status = %s, want running", act.Status)
	}
	if act.Selection == nil || act.Selection.Role != "reviewer" {
		t.Fatalf("pinned selection = %+v, want role=reviewer", act.Selection)
	}
	if len(snap.Instance.Attempts) != 1 {
		t.Fatalf("attempts = %d, want 1", len(snap.Instance.Attempts))
	}
	attempt := snap.Instance.Attempts[0]
	if attempt.ID != AttemptID(attemptID) {
		t.Fatalf("attempt id = %s, want %s", attempt.ID, attemptID)
	}
	if attempt.Identity.RuntimeID != "" || attempt.Backend != "" {
		t.Fatalf("manual attempt carries a runtime: runtime_id=%q backend=%q", attempt.Identity.RuntimeID, attempt.Backend)
	}
}

func TestManagerStartLeaseMismatch(t *testing.T) {
	m, _, wf, node := newManagerFixture(t)
	snap, _, err := m.store.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	actID := activationByNode(&snap.Instance, NodeID(node)).ID
	res := claimActivation(t, m, wf, node, string(actID), "alice")

	// Wrong lease id.
	bad := map[string]any{"lease_id": "lease-wrong", "owner_token": res["owner_token"]}
	_, err = m.WorkflowCommand(string(wf), startCommandPayload(t, node, string(actID), bad, nil))
	if err == nil || !strings.Contains(err.Error(), "does not match the active lease") {
		t.Fatalf("start with wrong lease_id err = %v, want lease mismatch", err)
	}

	// Wrong owner token.
	bad = map[string]any{"lease_id": res["lease_id"], "owner_token": "wrong-token"}
	_, err = m.WorkflowCommand(string(wf), startCommandPayload(t, node, string(actID), bad, nil))
	if err == nil || !strings.Contains(err.Error(), "owner token does not match") {
		t.Fatalf("start with wrong owner_token err = %v, want owner token mismatch", err)
	}
}

func TestManagerStartProviderNoExecutor(t *testing.T) {
	m, s, wf := newRunTemplateFixture(t)
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	actID := activationByNode(&snap.Instance, NodeID("start")).ID
	res := claimActivation(t, m, wf, "start", string(actID), "alice")
	// Baseline revision AFTER the legitimate claim: the rejected start must
	// not advance it further.
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	rev := snap.Instance.Revision

	_, err = m.WorkflowCommand(string(wf), startCommandPayload(t, "start", string(actID), res, nil))
	if err == nil || !strings.Contains(err.Error(), "unsupported until a later stage") {
		t.Fatalf("start err = %v, want unsupported until a later stage", err)
	}
	// The rejected command must leave the revision unchanged.
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if snap.Instance.Revision != rev {
		t.Fatalf("revision = %d after rejected start, want unchanged %d", snap.Instance.Revision, rev)
	}
}

func TestManagerClaimLeaseSurvivesReplay(t *testing.T) {
	m, s, wf, node := newManagerFixture(t)
	snap, _, err := m.store.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	preClaim, err := snap.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal pre-claim snapshot: %v", err)
	}
	actID := activationByNode(&snap.Instance, NodeID(node)).ID
	claimActivation(t, m, wf, node, string(actID), "alice")

	// Roll the on-disk snapshot back to pre-claim so a fresh loadCurrent must
	// rebuild the leased state purely by replaying the NDJSON log.
	if err := os.WriteFile(s.workflowPath(wf), preClaim, 0o644); err != nil {
		t.Fatalf("write pre-claim snapshot: %v", err)
	}
	fresh, ok, err := s.loadCurrent(wf)
	if err != nil || !ok {
		t.Fatalf("loadCurrent after replay: ok=%v err=%v", ok, err)
	}
	act := activationByNode(&fresh.Instance, NodeID(node))
	if act == nil || act.Status != ActivationLeased {
		t.Fatalf("post-replay activation = %+v, want leased", act)
	}
	if act.ActiveLease == nil {
		t.Fatal("post-replay activation has no active lease")
	}
	if !act.ActiveLease.ExpiresAt.After(time.Now().UTC()) {
		t.Fatalf("post-replay expires_at = %v, want in the future (lease must not be swept)", act.ActiveLease.ExpiresAt)
	}
}

// TestManagerInstantiateRejectsUnsafeTemplateID covers the safeComponent
// rejection path in WorkflowInstantiate (path separators, NUL, absolute/..).
func TestManagerInstantiateRejectsUnsafeTemplateID(t *testing.T) {
	m, _, _, _ := newManagerFixture(t)
	for _, tid := range []string{"../evil", "a/b", `a\b`, "x\x00y"} {
		payload, err := json.Marshal(map[string]string{"template_id": tid, "template_version": "1"})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if _, err := m.WorkflowInstantiate(payload); err == nil || !strings.Contains(err.Error(), "invalid template id") {
			t.Fatalf("instantiate template_id=%q err=%v, want invalid template id", tid, err)
		}
	}
}

// TestManagerStartRequiresFields covers commandStart's empty-field validation.
func TestManagerStartRequiresFields(t *testing.T) {
	m, _, wf, _ := newManagerFixture(t)
	cases := []struct {
		payload map[string]string
		want    string
	}{
		{map[string]string{"op": "start"}, "start requires node_id"},
		{map[string]string{"op": "start", "node_id": "start"}, "start requires lease_id"},
		{map[string]string{"op": "start", "node_id": "start", "lease_id": "L"}, "start requires owner_token"},
	}
	for _, c := range cases {
		p, _ := json.Marshal(c.payload)
		if _, err := m.WorkflowCommand(string(wf), p); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("payload=%v err=%v, want %q", c.payload, err, c.want)
		}
	}
}

// TestManagerStartRejectsUnclaimedActivation covers the not-in-ActivationLeased
// error path for a freshly instantiated (never claimed) activation.
func TestManagerStartRejectsUnclaimedActivation(t *testing.T) {
	m, s, wf, _ := newManagerFixture(t)
	snap, exists, err := s.loadSnapshot(wf)
	if err != nil || !exists {
		t.Fatalf("loadSnapshot: %v %v", err, exists)
	}
	actID := snap.Instance.Activations[0].ID
	p, _ := json.Marshal(map[string]any{
		"op": "start", "node_id": "start",
		"activation_id": string(actID), "lease_id": "L", "owner_token": "T",
	})
	if _, err := m.WorkflowCommand(string(wf), p); err == nil || !strings.Contains(err.Error(), "cannot start activation in status") {
		t.Fatalf("err=%v, want cannot-start-status rejection", err)
	}
}

// TestManagerWaitUnknownWorkflow covers the not-found error path in WorkflowWait.
func TestManagerWaitUnknownWorkflow(t *testing.T) {
	m, _, _, _ := newManagerFixture(t)
	if _, err := m.WorkflowWait("doesnotexist", 10*time.Millisecond); err == nil || !strings.Contains(err.Error(), "workflow not found") {
		t.Fatalf("err=%v, want workflow not found", err)
	}
}

// TestRecordAttemptTerminatedRecordsTerminalFact verifies the Stage-7 manager
// termination path: a failed attempt records a failed terminal fact and
// regresses the activation; a succeeded attempt records a succeeded terminal
// fact but is inert on activation/lease state (acceptance requires completion,
// Stage 11).
func TestRecordAttemptTerminatedRecordsTerminalFact(t *testing.T) {
	findAttemptByID := func(inst *WorkflowInstance, id AttemptID) *Attempt {
		for i := range inst.Attempts {
			if inst.Attempts[i].ID == id {
				return &inst.Attempts[i]
			}
		}
		return nil
	}

	// Failure path.
	{
		m, s, wf := newRunTemplateFixture(t)
		fake := &fakeExecutor{}
		m.RegisterExecutor(ActionRun, fake)
		snap, _, err := s.loadCurrent(wf)
		if err != nil {
			t.Fatalf("loadCurrent: %v", err)
		}
		actID := activationByNode(&snap.Instance, NodeID("start")).ID
		res := claimActivation(t, m, wf, "start", string(actID), "alice")
		leaseID, _ := res["lease_id"].(string)
		out, err := m.WorkflowCommand(string(wf), startCommandPayload(t, "start", string(actID), res, nil))
		if err != nil {
			t.Fatalf("start: %v", err)
		}
		attemptID, _ := out.(map[string]any)["attempt_id"].(string)
		if attemptID == "" {
			t.Fatal("start result missing attempt_id")
		}
		if err := m.RecordAttemptTerminated(wf, NodeID("start"), ActivationID(actID), AttemptID(attemptID), LeaseID(leaseID), AttemptFailed); err != nil {
			t.Fatalf("RecordAttemptTerminated(failed): %v", err)
		}
		snap, _, err = s.loadCurrent(wf)
		if err != nil {
			t.Fatalf("reload after failed: %v", err)
		}
		act := activationByNode(&snap.Instance, NodeID("start"))
		// The run template's retry policy exhausts to a block on the first
		// plain failure, so the activation is blocked (not attempt_failed).
		if act.Status != ActivationBlocked {
			t.Fatalf("activation status after failed termination = %s, want blocked", act.Status)
		}
		at := findAttemptByID(&snap.Instance, AttemptID(attemptID))
		if at == nil || at.Status != AttemptFailed || at.EndedAt == nil {
			t.Fatalf("attempt after failed termination = %+v, want status failed + ended", at)
		}
	}

	// Success path (inert on activation/lease).
	{
		m, s, wf := newRunTemplateFixture(t)
		fake := &fakeExecutor{}
		m.RegisterExecutor(ActionRun, fake)
		snap, _, err := s.loadCurrent(wf)
		if err != nil {
			t.Fatalf("loadCurrent: %v", err)
		}
		actID := activationByNode(&snap.Instance, NodeID("start")).ID
		res := claimActivation(t, m, wf, "start", string(actID), "alice")
		leaseID, _ := res["lease_id"].(string)
		out, err := m.WorkflowCommand(string(wf), startCommandPayload(t, "start", string(actID), res, nil))
		if err != nil {
			t.Fatalf("start: %v", err)
		}
		attemptID, _ := out.(map[string]any)["attempt_id"].(string)
		if attemptID == "" {
			t.Fatal("start result missing attempt_id")
		}
		if err := m.RecordAttemptTerminated(wf, NodeID("start"), ActivationID(actID), AttemptID(attemptID), LeaseID(leaseID), AttemptSucceeded); err != nil {
			t.Fatalf("RecordAttemptTerminated(succeeded): %v", err)
		}
		snap, _, err = s.loadCurrent(wf)
		if err != nil {
			t.Fatalf("reload after succeeded: %v", err)
		}
		act := activationByNode(&snap.Instance, NodeID("start"))
		if act.Status != ActivationRunning {
			t.Fatalf("activation status after succeeded termination = %s, want running (inert)", act.Status)
		}
		if act.ActiveLease == nil {
			t.Fatal("ActiveLease released on succeeded termination, want retained")
		}
		at := findAttemptByID(&snap.Instance, AttemptID(attemptID))
		if at == nil || at.Status != AttemptSucceeded || at.EndedAt == nil {
			t.Fatalf("attempt after succeeded termination = %+v, want status succeeded + ended", at)
		}
	}
}
