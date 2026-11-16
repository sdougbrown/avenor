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

	first := instantiateWithKey(t, m, "unit-42", nil)
	retry := instantiateWithKey(t, m, "unit-42", nil)
	if first["workflow_id"] != retry["workflow_id"] {
		t.Fatalf("retry workflow_id = %v, want %v", retry["workflow_id"], first["workflow_id"])
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
