package control

import (
	"encoding/json"
	"testing"

	"github.com/sdougbrown/avenor/internal/events"
)

// The one-shot server owns exactly one run and publishes events without a
// runtime id. Await resolves a run with list, then reads status/history/result
// by the id list reports, so those verbs must work without a supervisor.
func TestOneShotServerAnswersTheAwaitSurface(t *testing.T) {
	state := NewState("run_1", "demo", 0)
	s, _ := NewTestServer(state)

	// Before any event the owned run still reports a status a caller can wait on.
	early, _ := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 10, Method: "status", Params: json.RawMessage(`{"runtime_id":"run_1"}`)}).Result.(map[string]any)
	if early["status"] != "running" {
		t.Fatalf("pre-event status = %#v, want running", early)
	}

	s.PublishEvent(events.Event{Event: "session.start", SessionID: "ses_1"})

	list, ok := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 1, Method: "list"}).Result.([]map[string]any)
	if !ok || len(list) != 1 {
		t.Fatalf("list = %#v, want one entry", list)
	}
	for field, want := range map[string]any{
		"runtime_id": "run_1",
		"run_id":     "run_1",
		"label":      "demo",
		"status":     "running",
	} {
		if got := list[0][field]; got != want {
			t.Errorf("list[%q] = %v, want %v", field, got, want)
		}
	}
	// cmd/avenor/await.go reads these keys to resolve and follow the run, so a
	// missing or renamed one breaks await even when the value checks above pass.
	for _, key := range []string{"runtime_id", "run_id", "label", "started_at", "status", "phase", "pending_permission"} {
		if _, ok := list[0][key]; !ok {
			t.Errorf("list entry is missing %q", key)
		}
	}

	status, ok := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 2, Method: "status", Params: json.RawMessage(`{"runtime_id":"run_1"}`)}).Result.(map[string]any)
	if !ok {
		t.Fatal("scoped status did not return a run entry")
	}
	if status["runtime_id"] != "run_1" || status["run_id"] != "run_1" || status["status"] != "running" {
		t.Fatalf("status = %#v, want the run entry for run_1 running", status)
	}
	unscoped, ok := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 9, Method: "status"}).Result.(Snapshot)
	if !ok || unscoped.Status != "running" {
		t.Fatalf("unscoped status = %#v, want running", unscoped)
	}

	history, ok := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 3, Method: "history", Params: json.RawMessage(`{"runtime_id":"run_1"}`)}).Result.(map[string]any)
	if !ok {
		t.Fatal("scoped history did not return a result")
	}
	if got := history["events"].([]map[string]any); len(got) != 1 || got[0]["event"] != "session.start" {
		t.Fatalf("history = %#v, want the run's events under its run id", got)
	}

	s.PublishEvent(events.Event{Event: "session.end", SessionID: "ses_1", Fields: map[string]any{
		"stop_reason":  "end_turn",
		"exit_code":    0,
		"final_output": "done",
	}})
	status, _ = s.dispatch(nil, Request{JSONRPC: "2.0", ID: 4, Method: "status", Params: json.RawMessage(`{"runtime_id":"run_1"}`)}).Result.(map[string]any)
	if status["status"] != "ended" || status["stop_reason"] != "end_turn" || status["exit_code"] != 0 {
		t.Fatalf("terminal status = %#v, want ended/end_turn/0", status)
	}
	if got := s.state.FinalOutput(); got != "done" {
		t.Fatalf("final output = %q, want done", got)
	}

	result, ok := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 5, Method: "result", Params: json.RawMessage(`{"runtime_id":"run_1"}`)}).Result.(map[string]any)
	if !ok || result["final_output"] != "done" {
		t.Fatalf("result = %#v, want the terminal output", result)
	}

	resp := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 7, Method: "status", Params: json.RawMessage(`{"runtime_id":"run_other"}`)})
	if resp.Error == nil || resp.Error.Code != -32000 {
		t.Fatalf("unknown run status error = %#v, want -32000", resp.Error)
	}
	resp = s.dispatch(nil, Request{JSONRPC: "2.0", ID: 8, Method: "result", Params: json.RawMessage(`{"runtime_id":"run_other"}`)})
	if resp.Error == nil || resp.Error.Code != -32000 {
		t.Fatalf("unknown run result error = %#v, want -32000", resp.Error)
	}

	// A retry after a settled attempt starts a new session; the status must
	// return to running rather than stay ended.
	s.PublishEvent(events.Event{Event: "session.start", SessionID: "ses_2"})
	restarted, _ := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 11, Method: "status", Params: json.RawMessage(`{"runtime_id":"run_1"}`)}).Result.(map[string]any)
	if restarted["status"] != "running" || restarted["stop_reason"] != "" || restarted["exit_code"] != 0 {
		t.Fatalf("status after restart = %#v, want running with the previous attempt cleared", restarted)
	}
}

// A supervisor publishes its child runtimes' events through the same handler.
// Those events must not overwrite the server's own snapshot status: on a
// supervisor that snapshot belongs to no child, and flapping it would report
// whichever runtime started or ended last.
func TestSupervisorEventsDoNotWriteOwnedStatus(t *testing.T) {
	state := NewState("run_1", "demo", 0)
	s, _ := NewTestServer(state)
	s.SetStableHandler(&mockStableHandler{})

	s.PublishEvent(events.Event{Event: "session.start", Fields: map[string]any{"runtime_id": "rt_1"}})
	s.PublishEvent(events.Event{Event: "session.end", Fields: map[string]any{
		"runtime_id":  "rt_1",
		"stop_reason": "end_turn",
		"exit_code":   0,
	}})

	snap := state.Snapshot()
	if snap.Status != "" || snap.StopReason != "" || snap.ExitCode != nil {
		t.Fatalf("supervisor child events wrote the server snapshot: %#v", snap)
	}
}

func TestOneShotSetRunSettledPublishesTerminalState(t *testing.T) {
	state := NewState("run_retry", "retry", 1)
	s, _ := NewTestServer(state)
	state.Update(func(ss *Snapshot) { ss.RetryAttempt = 1 })
	s.PublishEvent(events.Event{Event: "session.start"})

	// A retryable failure is suppressed while the loop may retry, then the loop
	// declines to start another attempt and settles the run itself.
	s.PublishEvent(events.Event{Event: "session.end", Fields: map[string]any{"stop_reason": "error", "exit_code": 1}})
	if got := state.Snapshot().Status; got != "running" {
		t.Fatalf("status before settle = %q, want running", got)
	}
	s.SetRunSettled(1, "error")
	snap := state.Snapshot()
	if snap.Status != "ended" || snap.StopReason != "error" || snap.ExitCode == nil || *snap.ExitCode != 1 {
		t.Fatalf("settled status = %#v, want ended/error/1", snap)
	}
	history, _ := s.runtimeHistory("run_retry", 0, 10)
	last := history[len(history)-1]
	if last.Event != "agent.status" {
		t.Fatalf("last history event = %q, want the settle wake-up", last.Event)
	}
	if phase, _ := last.Fields["phase"].(string); phase != "done" {
		t.Fatalf("settle wake-up phase = %q, want done", phase)
	}
}

// A run the event stream already settled needs no wake event; a duplicate
// agent.status would land in subscriber streams and replay history.
func TestOneShotSettledRunPublishesNoWakeEvent(t *testing.T) {
	state := NewState("run_done", "done", 0)
	s, _ := NewTestServer(state)
	s.PublishEvent(events.Event{Event: "session.start"})
	s.PublishEvent(events.Event{Event: "session.end", Fields: map[string]any{"stop_reason": "end_turn", "exit_code": 0}})
	before, _ := s.runtimeHistory("run_done", 0, 10)

	s.SetRunSettled(0, "end_turn")
	after, _ := s.runtimeHistory("run_done", 0, 10)
	if len(after) != len(before) {
		t.Fatalf("history grew from %d to %d after settling an already-settled run", len(before), len(after))
	}
}

// A supervisor server ignores settlement: its outcomes belong to runtimes.
func TestSupervisorIgnoresRunSettlement(t *testing.T) {
	state := NewState("run_1", "demo", 0)
	s, _ := NewTestServer(state)
	s.SetStableHandler(&mockStableHandler{})
	s.SetRunSettled(1, "error")
	if snap := state.Snapshot(); snap.Status != "" || snap.ExitCode != nil {
		t.Fatalf("supervisor settlement wrote the server snapshot: %#v", snap)
	}
}

// A retryable attempt failure is not the run's outcome while another attempt is
// allowed: the failed attempt must not settle the status await reads.
func TestOneShotRetryableFailureKeepsRunning(t *testing.T) {
	state := NewState("run_retry", "retry", 1)
	s, _ := NewTestServer(state)

	state.Update(func(ss *Snapshot) { ss.RetryAttempt = 1 })
	s.PublishEvent(events.Event{Event: "session.start"})
	s.PublishEvent(events.Event{Event: "session.end", Fields: map[string]any{"stop_reason": "error", "exit_code": 1}})
	snap := state.Snapshot()
	if snap.Status != "running" {
		t.Fatalf("status after retryable failure = %q, want running", snap.Status)
	}
	if snap.ExitCode != nil {
		t.Fatalf("retryable failure recorded a terminal exit code: %v", *snap.ExitCode)
	}

	// The final allowed attempt settles the run.
	state.Update(func(ss *Snapshot) { ss.RetryAttempt = 2 })
	s.PublishEvent(events.Event{Event: "session.start"})
	s.PublishEvent(events.Event{Event: "session.end", Fields: map[string]any{"stop_reason": "error", "exit_code": 1}})
	snap = state.Snapshot()
	if snap.Status != "ended" || snap.StopReason != "error" || snap.ExitCode == nil || *snap.ExitCode != 1 {
		t.Fatalf("exhausted retry status = %#v, want ended/error/1", snap)
	}
}

// A non-retryable failure settles immediately even when retries remain.
func TestOneShotNonRetryableFailureSettles(t *testing.T) {
	state := NewState("run_refusal", "refusal", 2)
	s, _ := NewTestServer(state)

	state.Update(func(ss *Snapshot) { ss.RetryAttempt = 1 })
	s.PublishEvent(events.Event{Event: "session.start"})
	s.PublishEvent(events.Event{Event: "session.end", Fields: map[string]any{"stop_reason": "refusal", "exit_code": 2}})
	snap := state.Snapshot()
	if snap.Status != "ended" || snap.StopReason != "refusal" {
		t.Fatalf("non-retryable status = %#v, want ended/refusal", snap)
	}
}

// Events a one-shot server publishes carry no runtime_id, so a subscription
// scoped to its run must still match them.
func TestOneShotSubscriptionMatchesUnscopedEvents(t *testing.T) {
	sub := newSubscriber(nil, "run_1", 0)
	sub.scopeID = "run_1"
	if !sub.matches(events.Event{Event: "session.start"}) {
		t.Fatal("unscoped event did not match the one-shot run")
	}
	if sub.matches(events.Event{Event: "session.start", Fields: map[string]any{"runtime_id": "rt_other"}}) {
		t.Fatal("event from another runtime matched the one-shot run")
	}
}
