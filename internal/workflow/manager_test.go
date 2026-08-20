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
