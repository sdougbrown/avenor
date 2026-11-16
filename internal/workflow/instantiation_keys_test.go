package workflow

// Internal tests for the caller-supplied instantiation idempotency key: the
// same-key retry converging on one workflow, the conflict on a different
// request, concurrency and restart (lost-response) convergence, and the
// crash-window fall-through that completes an interrupted attempt.

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// instantiateWithKey is the test helper for one idempotent instantiate call
// over the mgr-test template fixture.
func instantiateWithKey(t *testing.T, m *Manager, key string, params map[string]string) map[string]any {
	t.Helper()
	payload := map[string]any{
		"template_id":      "mgr-test",
		"template_version": "1",
	}
	if key != "" {
		payload["idempotency_key"] = key
	}
	if params != nil {
		payload["params"] = params
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	out, err := m.WorkflowInstantiate(raw)
	if err != nil {
		t.Fatalf("WorkflowInstantiate: %v", err)
	}
	result, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("WorkflowInstantiate result = %T, want map[string]any", out)
	}
	return result
}

// countInstantiatedInstances counts the instance directories under the
// store's root.
func countInstantiatedInstances(t *testing.T, s *Store) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(s.root, "instances"))
	if err != nil {
		t.Fatalf("read instances dir: %v", err)
	}
	return len(entries)
}

func TestManagerInstantiateIdempotencyKeyRetriesConverge(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	if _, err := m.WorkflowCreate([]byte(managerTemplateJSON)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}

	first := instantiateWithKey(t, m, "unit-42", nil)
	second := instantiateWithKey(t, m, "unit-42", nil)

	if first["workflow_id"] != second["workflow_id"] {
		t.Fatalf("retry returned workflow_id %v, want %v", second["workflow_id"], first["workflow_id"])
	}
	if first["revision"] != second["revision"] {
		t.Fatalf("retry returned revision %v, want %v", second["revision"], first["revision"])
	}
	if got := countInstantiatedInstances(t, s); got != 1 {
		t.Fatalf("instance count after retry = %d, want 1", got)
	}
}

func TestManagerInstantiateIdempotencyKeyConflict(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	if _, err := m.WorkflowCreate(paramsTemplateJSON("idem-params", "static")); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}

	instantiate := func(params map[string]string) map[string]any {
		t.Helper()
		payload, err := json.Marshal(map[string]any{
			"template_id": "idem-params", "template_version": "1",
			"idempotency_key": "unit-42", "params": params,
		})
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}
		out, err := m.WorkflowInstantiate(payload)
		if err != nil {
			t.Fatalf("WorkflowInstantiate: %v", err)
		}
		return out.(map[string]any)
	}

	first := instantiate(map[string]string{"worktree": "a"})

	// Same key, different params: conflict, and the original binding survives.
	_, err := m.WorkflowInstantiate([]byte(`{"template_id": "idem-params", "template_version": "1", "idempotency_key": "unit-42", "params": {"worktree": "other"}}`))
	if !errors.Is(err, ErrInstantiationKeyConflict) {
		t.Fatalf("different params error = %v, want ErrInstantiationKeyConflict", err)
	}
	again := instantiate(map[string]string{"worktree": "a"})
	if again["workflow_id"] != first["workflow_id"] {
		t.Fatalf("workflow_id after conflict = %v, want %v", again["workflow_id"], first["workflow_id"])
	}

	// Same key, different template: conflict.
	otherTemplate := `{"schema_version":1,"template_id":"idem-params-2","template_version":"1","entry_nodes":["start"],"nodes":[{"id":"start","action":{"type":"manual"}}],"terminal_outcomes":["done"]}`
	if _, err := m.WorkflowCreate([]byte(otherTemplate)); err != nil {
		t.Fatalf("WorkflowCreate second template: %v", err)
	}
	_, err = m.WorkflowInstantiate([]byte(`{"template_id": "idem-params-2", "template_version": "1", "idempotency_key": "unit-42"}`))
	if !errors.Is(err, ErrInstantiationKeyConflict) {
		t.Fatalf("different template error = %v, want ErrInstantiationKeyConflict", err)
	}
}

func TestManagerInstantiateIdempotencyKeyMetadataOutsideFingerprint(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	if _, err := m.WorkflowCreate([]byte(managerTemplateJSON)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}

	// Same key, same request, different metadata on each call: metadata is
	// caller bookkeeping and must not conflict or fork the workflow.
	first, err := m.WorkflowInstantiate([]byte(`{"template_id": "mgr-test", "template_version": "1", "idempotency_key": "unit-42", "metadata": {"attempt": 1}}`))
	if err != nil {
		t.Fatalf("first instantiate: %v", err)
	}
	retry, err := m.WorkflowInstantiate([]byte(`{"template_id": "mgr-test", "template_version": "1", "idempotency_key": "unit-42", "metadata": {"attempt": 2}}`))
	if err != nil {
		t.Fatalf("retry instantiate: %v", err)
	}
	firstID := first.(map[string]any)["workflow_id"]
	if firstID != retry.(map[string]any)["workflow_id"] {
		t.Fatalf("retry workflow_id = %v, want %v", retry.(map[string]any)["workflow_id"], firstID)
	}
	if got := countInstantiatedInstances(t, s); got != 1 {
		t.Fatalf("instance count after metadata-differing retry = %d, want 1", got)
	}
}

func TestManagerInstantiateIdempotencyKeyRejectsInvalidKey(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	if _, err := m.WorkflowCreate([]byte(managerTemplateJSON)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}

	long := strings.Repeat("k", 257)
	if _, err := m.WorkflowInstantiate([]byte(`{"template_id": "mgr-test", "template_version": "1", "idempotency_key": "` + long + `"}`)); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("overlong key error = %v, want length rejection", err)
	}
	if _, err := m.WorkflowInstantiate([]byte(`{"template_id": "mgr-test", "template_version": "1", "idempotency_key": "bad\u0000key"}`)); err == nil || !strings.Contains(err.Error(), "control characters") {
		t.Fatalf("control-character key error = %v, want control-character rejection", err)
	}
	if got := countInstantiatedInstances(t, s); got != 0 {
		t.Fatalf("instance count after invalid keys = %d, want 0", got)
	}
}

func TestManagerInstantiateIdempotencyKeySurvivesSnapshotlessCrash(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	if _, err := m.WorkflowCreate([]byte(managerTemplateJSON)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}

	first := instantiateWithKey(t, m, "unit-42", nil)
	wf := WorkflowID(first["workflow_id"].(string))

	// Simulate the crash between appendEvents and writeSnapshot: the event log
	// is committed but the snapshot file is gone. The retry must resolve the
	// instance through replay, not re-instantiate over the orphan (which would
	// brick every later read of the instance).
	if err := os.Remove(s.workflowPath(wf)); err != nil {
		t.Fatalf("remove snapshot: %v", err)
	}
	if _, ok, err := s.loadCurrent(wf); err != nil || !ok {
		t.Fatalf("loadCurrent after snapshotless crash: exists=%v err=%v, want replayed instance", ok, err)
	}
	retry := instantiateWithKey(t, m, "unit-42", nil)
	if retry["workflow_id"] != first["workflow_id"] {
		t.Fatalf("retry workflow_id = %v, want %v", retry["workflow_id"], first["workflow_id"])
	}

	// Every later read of the instance must still work: the orphaned event
	// must not have bricked replay.
	if _, ok, err := s.loadCurrent(wf); err != nil || !ok {
		t.Fatalf("loadCurrent after recovery: exists=%v err=%v", ok, err)
	}
}

func TestManagerInstantiateIdempotencyKeyRejectsCorruptRecord(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	if _, err := m.WorkflowCreate([]byte(managerTemplateJSON)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}

	if err := os.MkdirAll(s.instantiationKeysDir(), 0o755); err != nil {
		t.Fatalf("mkdir keys dir: %v", err)
	}
	if err := os.WriteFile(s.instantiationKeyPath("unit-42"), []byte("{not json"), 0o600); err != nil {
		t.Fatalf("write corrupt record: %v", err)
	}
	if _, err := m.WorkflowInstantiate([]byte(`{"template_id": "mgr-test", "template_version": "1", "idempotency_key": "unit-42"}`)); err == nil {
		t.Fatal("corrupt record accepted, want error")
	}
	if got := countInstantiatedInstances(t, s); got != 0 {
		t.Fatalf("instance count after corrupt record = %d, want 0", got)
	}
}

func TestManagerInstantiateIdempotencyKeyNotBoundByFailedValidation(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	if _, err := m.WorkflowCreate([]byte(managerTemplateJSON)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}

	// A keyed request that fails before key resolution — here an unknown
	// template — must not consume the key.
	if _, err := m.WorkflowInstantiate([]byte(`{"template_id": "missing-template", "template_version": "1", "idempotency_key": "unit-42"}`)); err == nil {
		t.Fatal("unknown template accepted, want error")
	}
	result := instantiateWithKey(t, m, "unit-42", nil)
	if got, want := fmt.Sprint(result["revision"]), "1"; got != want {
		t.Fatalf("revision = %s, want %s (fresh instance, key not pre-bound)", got, want)
	}
}

func TestManagerInstantiateIdempotencyKeyReturnsCurrentRevisionAfterTerminal(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	if _, err := m.WorkflowCreate([]byte(managerTemplateJSON)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}

	first := instantiateWithKey(t, m, "unit-42", nil)
	wf := WorkflowID(first["workflow_id"].(string))

	// Drive the manual start node to its terminal outcome so the workflow
	// completes and its revision advances past the initial instantiate.
	snap, ok, err := s.loadCurrent(wf)
	if err != nil || !ok {
		t.Fatalf("loadCurrent: exists=%v err=%v", ok, err)
	}
	start := activationByNode(&snap.Instance, "start")
	if start == nil {
		t.Fatal("start activation not found")
	}
	res := claimActivation(t, m, wf, "start", string(start.ID), "alice")
	out, err := m.WorkflowCommand(string(wf), startCommandPayload(t, "start", string(start.ID), res, nil))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	attemptID := out.(map[string]any)["attempt_id"].(string)
	leaseID := res["lease_id"].(string)
	if err := m.RecordAttemptTerminated(wf, "start", start.ID, AttemptID(attemptID), LeaseID(leaseID), AttemptSucceeded); err != nil {
		t.Fatalf("RecordAttemptTerminated: %v", err)
	}
	complete, err := json.Marshal(map[string]any{
		"op": "complete", "node_id": "start", "activation_id": string(start.ID),
		"attempt_id": attemptID, "lease_id": leaseID, "owner_token": res["owner_token"],
		"outcome": "done",
	})
	if err != nil {
		t.Fatalf("marshal complete: %v", err)
	}
	if _, err := m.WorkflowCommand(string(wf), complete); err != nil {
		t.Fatalf("complete: %v", err)
	}

	// The key is still bound after the workflow went terminal: the retry
	// returns the same workflow at its current revision, never a fresh one.
	retry := instantiateWithKey(t, m, "unit-42", nil)
	if retry["workflow_id"] != first["workflow_id"] {
		t.Fatalf("retry workflow_id = %v, want %v", retry["workflow_id"], first["workflow_id"])
	}
	if retry["revision"] == first["revision"] {
		t.Fatalf("retry revision = %v, want the current revision (past %v)", retry["revision"], first["revision"])
	}
}

func TestManagerInstantiateIdempotencyKeyConcurrent(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	if _, err := m.WorkflowCreate([]byte(managerTemplateJSON)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}

	const callers = 8
	ids := make(chan any, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result := instantiateWithKey(t, m, "unit-42", nil)
			ids <- result["workflow_id"]
		}()
	}
	wg.Wait()
	close(ids)

	unique := map[any]bool{}
	for id := range ids {
		unique[id] = true
	}
	if len(unique) != 1 {
		t.Fatalf("concurrent same-key callers produced %d distinct workflow IDs, want 1: %v", len(unique), unique)
	}
	if got := countInstantiatedInstances(t, s); got != 1 {
		t.Fatalf("instance count after concurrent calls = %d, want 1", got)
	}
}

func TestManagerInstantiateIdempotencyKeyAfterRestart(t *testing.T) {
	t.Parallel()
	root := t.TempDir()

	s := New(root)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	if _, err := m.WorkflowCreate([]byte(managerTemplateJSON)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	first := instantiateWithKey(t, m, "unit-42", nil)

	// A fresh store and manager over the same root — the restart (or a second
	// supervisor process sharing the root) — must resolve the same workflow.
	s2 := New(root)
	m2 := NewManager(s2)
	second := instantiateWithKey(t, m2, "unit-42", nil)

	if first["workflow_id"] != second["workflow_id"] {
		t.Fatalf("restart retry workflow_id = %v, want %v", second["workflow_id"], first["workflow_id"])
	}
}

func TestManagerInstantiateIdempotencyKeyCompletesCrashedAttempt(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	if _, err := m.WorkflowCreate([]byte(managerTemplateJSON)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}

	// Simulate the crash window: the key record is durably bound, but no
	// instance was committed (nothing survived under the recorded workflow
	// ID). A retry must complete the interrupted attempt under the same ID
	// rather than minting a duplicate.
	first := instantiateWithKey(t, m, "unit-42", nil)
	wf := first["workflow_id"].(string)
	if err := os.RemoveAll(filepath.Join(s.root, "instances", wf)); err != nil {
		t.Fatalf("remove instance dir: %v", err)
	}
	if got := countInstantiatedInstances(t, s); got != 0 {
		t.Fatalf("instance count after simulated crash = %d, want 0", got)
	}

	retry := instantiateWithKey(t, m, "unit-42", nil)
	if retry["workflow_id"] != wf {
		t.Fatalf("crash-recovery retry workflow_id = %v, want %v", retry["workflow_id"], wf)
	}
	if got := countInstantiatedInstances(t, s); got != 1 {
		t.Fatalf("instance count after crash recovery = %d, want 1", got)
	}
}

func TestManagerInstantiateWithoutKeyMintsFreshIDs(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	if _, err := m.WorkflowCreate([]byte(managerTemplateJSON)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}

	first := instantiateWithKey(t, m, "", nil)
	second := instantiateWithKey(t, m, "", nil)
	if first["workflow_id"] == second["workflow_id"] {
		t.Fatalf("keyless retries shared workflow_id %v, want distinct IDs", first["workflow_id"])
	}
	if got := countInstantiatedInstances(t, s); got != 2 {
		t.Fatalf("instance count after keyless calls = %d, want 2", got)
	}
}
