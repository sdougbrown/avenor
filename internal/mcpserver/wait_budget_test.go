package mcpserver

import (
	"context"
	"testing"
	"time"
)

func newWaitBudgetServer(t *testing.T, maxWait time.Duration) (*Server, *fakeClient) {
	t.Helper()
	fake := &fakeClient{statusResult: map[string]any{"status": "running", "phase": "working"}}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
		MaxWait:       maxWait,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	s.clock = func() time.Time { return now }
	s.sleep = func(ctx context.Context, delay time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		now = now.Add(delay)
		return nil
	}
	return s, fake
}

func TestMaxWaitClampsOverMaxStatusWait(t *testing.T) {
	s, _ := newWaitBudgetServer(t, 25*time.Second)
	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{
		RunID: "run-1", WaitFor: "terminal", Timeout: "10m", View: "lifecycle",
	})
	if err != nil {
		t.Fatal(err)
	}
	status := statusOutputMap(t, result)
	if status["timed_out"] != true || status["wait_clamped"] != true {
		t.Fatalf("status = %#v, want timed_out and wait_clamped", status)
	}
}

func TestMaxWaitDoesNotFlagUnderMaxStatusWait(t *testing.T) {
	s, _ := newWaitBudgetServer(t, 25*time.Second)
	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{
		RunID: "run-1", WaitFor: "terminal", Timeout: "5s", View: "lifecycle",
	})
	if err != nil {
		t.Fatal(err)
	}
	status := statusOutputMap(t, result)
	if status["timed_out"] != true {
		t.Fatalf("status = %#v, want timed_out", status)
	}
	if _, ok := status["wait_clamped"]; ok {
		t.Fatalf("status = %#v, want no wait_clamped", status)
	}
}

func TestMaxWaitZeroDisablesClamp(t *testing.T) {
	s, _ := newWaitBudgetServer(t, 0)

	// Unbounded wait (no timeout) is not clamped: the fake sleep surfaces
	// context.Canceled to end the otherwise endless poll loop.
	s.sleep = func(context.Context, time.Duration) error { return context.Canceled }
	_, _, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{
		RunID: "run-1", WaitFor: "terminal",
	})
	if err != context.Canceled {
		t.Fatalf("error = %v, want context.Canceled from unbounded wait", err)
	}

	// An explicit timeout still bounds the wait and never flags a clamp.
	s2, _ := newWaitBudgetServer(t, 0)
	_, result, err := s2.handleAvenorStatus(context.Background(), nil, statusArgs{
		RunID: "run-1", WaitFor: "terminal", Timeout: "5s", View: "lifecycle",
	})
	if err != nil {
		t.Fatal(err)
	}
	status := statusOutputMap(t, result)
	if status["timed_out"] != true {
		t.Fatalf("status = %#v, want timed_out", status)
	}
	if _, ok := status["wait_clamped"]; ok {
		t.Fatalf("status = %#v, want no wait_clamped", status)
	}
}

func TestMaxWaitClampsUnboundedResultWait(t *testing.T) {
	s, _ := newWaitBudgetServer(t, 25*time.Second)
	_, value, err := s.handleAvenorResult(context.Background(), nil, resultArgs{RunID: "run-1"})
	if err != nil {
		t.Fatal(err)
	}
	result := value.(map[string]any)
	if result["ready"] != false || result["timed_out"] != true || result["wait_clamped"] != true {
		t.Fatalf("result = %#v, want ready=false, timed_out, wait_clamped", result)
	}
}

func TestMaxWaitDoesNotFlagSatisfiedWait(t *testing.T) {
	fake := &fakeClient{statusResult: map[string]any{"status": "done"}}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
		MaxWait:       25 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{
		RunID: "run-1", WaitFor: "terminal", View: "lifecycle",
	})
	if err != nil {
		t.Fatal(err)
	}
	status := statusOutputMap(t, result)
	if status["status"] != "done" || status["timed_out"] == true {
		t.Fatalf("status = %#v, want satisfied wait", status)
	}
	if _, ok := status["wait_clamped"]; ok {
		t.Fatalf("status = %#v, want no wait_clamped", status)
	}

	_, value, err := s.handleAvenorResult(context.Background(), nil, resultArgs{RunID: "run-1"})
	if err != nil {
		t.Fatal(err)
	}
	res := value.(map[string]any)
	if res["ready"] != true {
		t.Fatalf("result = %#v, want ready", res)
	}
	if _, ok := res["wait_clamped"]; ok {
		t.Fatalf("result = %#v, want no wait_clamped", res)
	}
}

func TestWorkflowWaitClampPassesEffectiveTimeout(t *testing.T) {
	newS := func(maxWait time.Duration) (*Server, *fakeClient) {
		s, fake := newWaitBudgetServer(t, maxWait)
		return s, fake
	}

	// Clamped: default 30s request exceeds the 25s budget.
	s, fake := newS(25 * time.Second)
	fake.workflowWaitResult = map[string]any{"terminal": false, "timed_out": true}
	_, value, err := s.handleAvenorWorkflowWait(context.Background(), nil, workflowWaitArgs{WorkflowID: "wf-1"})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(fake.workflowWaitCalls); got != 1 {
		t.Fatalf("workflow wait calls = %d, want 1", got)
	}
	if got := fake.workflowWaitCalls[0].timeout; got != 25*time.Second {
		t.Fatalf("WorkflowWait timeout = %v, want 25s", got)
	}
	result := value.(map[string]any)
	if result["wait_clamped"] != true {
		t.Fatalf("result = %#v, want wait_clamped", result)
	}

	// Terminal result inside the budget is never flagged.
	s, fake = newS(25 * time.Second)
	fake.workflowWaitResult = map[string]any{"terminal": true, "timed_out": false}
	_, value, err = s.handleAvenorWorkflowWait(context.Background(), nil, workflowWaitArgs{WorkflowID: "wf-1"})
	if err != nil {
		t.Fatal(err)
	}
	result = value.(map[string]any)
	if _, ok := result["wait_clamped"]; ok {
		t.Fatalf("result = %#v, want no wait_clamped", result)
	}

	// Under-max explicit timeout passes through unflagged.
	s, fake = newS(25 * time.Second)
	fake.workflowWaitResult = map[string]any{"terminal": false, "timed_out": true}
	_, _, err = s.handleAvenorWorkflowWait(context.Background(), nil, workflowWaitArgs{WorkflowID: "wf-1", Timeout: "10s"})
	if err != nil {
		t.Fatal(err)
	}
	if got := fake.workflowWaitCalls[0].timeout; got != 10*time.Second {
		t.Fatalf("WorkflowWait timeout = %v, want 10s", got)
	}

	// MaxWait=0 passes the raw request through.
	s, fake = newS(0)
	fake.workflowWaitResult = map[string]any{"terminal": false, "timed_out": true}
	_, _, err = s.handleAvenorWorkflowWait(context.Background(), nil, workflowWaitArgs{WorkflowID: "wf-1"})
	if err != nil {
		t.Fatal(err)
	}
	if got := fake.workflowWaitCalls[0].timeout; got != 30*time.Second {
		t.Fatalf("WorkflowWait timeout = %v, want 30s", got)
	}
}
