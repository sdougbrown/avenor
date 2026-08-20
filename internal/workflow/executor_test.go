package workflow

// Internal tests for the manager's typed Executor seam. They drive start
// dispatch against a fake executor; no real provider is started.

import (
	"context"
	"strings"
	"sync"
	"testing"
)

// fakeExecutor records every dispatch for assertions. It is concurrency-safe.
type fakeExecutor struct {
	mu    sync.Mutex
	calls []ExecutorContext
}

func (f *fakeExecutor) Dispatch(ctx context.Context, ec ExecutorContext) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, ec)
	return nil
}

func (f *fakeExecutor) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeExecutor) last() ExecutorContext {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[len(f.calls)-1]
}

func TestStartDispatchesToRegisteredExecutorOnce(t *testing.T) {
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

	out, err := m.WorkflowCommand(string(wf), startCommandPayload(t, "start", string(actID), res, map[string]any{"model": "m1"}))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	mm, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("start result = %#v, want map", out)
	}
	if mm["status"] != string(ActivationRunning) {
		t.Fatalf("status = %v, want running", mm["status"])
	}
	attemptID, _ := mm["attempt_id"].(string)
	if attemptID == "" {
		t.Fatal("start result missing attempt_id")
	}

	if n := fake.count(); n != 1 {
		t.Fatalf("executor dispatch count = %d, want exactly 1", n)
	}
	call := fake.last()
	if call.WorkflowID != wf {
		t.Errorf("dispatch workflow = %s, want %s", call.WorkflowID, wf)
	}
	if call.NodeID != "start" {
		t.Errorf("dispatch node = %s, want start", call.NodeID)
	}
	if call.ActivationID != ActivationID(actID) {
		t.Errorf("dispatch activation = %s, want %s", call.ActivationID, actID)
	}
	if call.AttemptID != AttemptID(attemptID) {
		t.Errorf("dispatch attempt = %s, want %s", call.AttemptID, attemptID)
	}
	if call.LeaseID != LeaseID(leaseID) {
		t.Errorf("dispatch lease = %s, want the claim lease %s", call.LeaseID, leaseID)
	}
	if call.Action.Kind != ActionRun {
		t.Errorf("dispatch action kind = %s, want run", call.Action.Kind)
	}
	if call.Selection == nil || call.Selection.Model != "m1" {
		t.Errorf("dispatch selection = %+v, want model=m1 passed through", call.Selection)
	}
}

func TestStartProviderWithoutExecutor(t *testing.T) {
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

	// The rejection must leave the snapshot unchanged.
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if snap.Instance.Revision != rev {
		t.Fatalf("revision = %d after rejected start, want unchanged %d", snap.Instance.Revision, rev)
	}
	if act := activationByNode(&snap.Instance, NodeID("start")); act.Status != ActivationLeased {
		t.Fatalf("activation status = %s after rejected start, want leased", act.Status)
	}
}
