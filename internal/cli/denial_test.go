package cli

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

type recordingSink struct {
	mu     sync.Mutex
	events []events.Event
}

func (s *recordingSink) Write(ev events.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, ev)
	return nil
}

func (s *recordingSink) Close() error { return nil }

func (s *recordingSink) snapshot() []events.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]events.Event(nil), s.events...)
}

// deniedToolEvents reproduces the event stream OpenCode 1.15.x emits when a
// configured permission deny (permission.doom_loop=deny) aborts the tool call
// and the ACP agent still answers the prompt with stopReason end_turn.
func deniedToolEvents(sessionID string) []events.Event {
	return []events.Event{
		{Event: "session.start", SessionID: sessionID, Fields: map[string]any{"conversation_id": sessionID}},
		{Event: "agent.message_chunk", SessionID: sessionID, Fields: map[string]any{"delta": "working"}},
		{Event: "tool.call", SessionID: sessionID, Fields: map[string]any{"kind": "search", "title": "grep"}},
		{Event: "tool.call_update", SessionID: sessionID, Fields: map[string]any{
			"status": "failed",
			"rawOutput": map[string]any{
				"error":    "Tool execution aborted",
				"metadata": map[string]any{"interrupted": true},
			},
		}},
		{Event: "session.end", SessionID: sessionID, Fields: map[string]any{"stop_reason": "end_turn"}},
	}
}

// TestWaitForSessionDeniedToolCallIsNotCleanEndTurn proves a triggered
// doom-loop denial is not silently accepted as successful completion: the
// aborted tool call in the turn downgrades the authoritative end_turn to
// tool_denied.
func TestWaitForSessionDeniedToolCallIsNotCleanEndTurn(t *testing.T) {
	sink := &recordingSink{}
	eventCh := make(chan events.Event, 8)
	for _, ev := range deniedToolEvents("ses_deny") {
		eventCh <- ev
	}
	close(eventCh)
	promptDone := make(chan error, 1)
	promptDone <- nil

	result := WaitForSession(context.Background(), &cliFakeProvider{}, SessionWaitConfig{
		EventCh:    eventCh,
		PromptDone: promptDone,
		SessionID:  "ses_deny",
		RunID:      "run_deny",
	}, SessionWaitDeps{Writer: sink, Stderr: io.Discard})

	if result.ExitCode != 2 {
		t.Fatalf("ExitCode = %d, want 2 (tool_denied)", result.ExitCode)
	}
	if result.StopReason != "tool_denied" {
		t.Fatalf("StopReason = %q, want tool_denied", result.StopReason)
	}
	var diagnostic bool
	publishedReason := ""
	publishedExitCode := int64(-1)
	for _, ev := range sink.snapshot() {
		if ev.Event == "avenor.error" {
			if msg, _ := ev.Fields["message"].(string); strings.Contains(msg, "tool_denied") {
				diagnostic = true
			}
		}
		if ev.Event == "session.end" {
			publishedReason, _ = ev.Fields["stop_reason"].(string)
			publishedExitCode, _ = events.Int64(ev.Fields["exit_code"])
		}
	}
	if !diagnostic {
		t.Fatal("expected an avenor.error diagnostic explaining the tool_denied classification")
	}
	if publishedReason != runtime.ToolDeniedStopReason {
		t.Fatalf("published session.end stop_reason = %q, want %q", publishedReason, runtime.ToolDeniedStopReason)
	}
	if publishedExitCode != 2 {
		t.Fatalf("published session.end exit_code = %d, want 2", publishedExitCode)
	}
}

// TestWaitForSessionOrdinaryToolFailureStaysRecoverable proves the denial
// containment does not make ordinary failed tool calls fatal: a failed tool
// call without an abort, followed by a clean end_turn, still completes the
// turn successfully so agents can recover from normal tool errors.
func TestWaitForSessionOrdinaryToolFailureStaysRecoverable(t *testing.T) {
	sink := &recordingSink{}
	eventCh := make(chan events.Event, 8)
	eventCh <- events.Event{Event: "tool.call", SessionID: "ses_tool", Fields: map[string]any{"kind": "shell", "title": "grep"}}
	eventCh <- events.Event{Event: "tool.call_update", SessionID: "ses_tool", Fields: map[string]any{
		"status":    "failed",
		"rawOutput": map[string]any{"error": "grep: no match"},
	}}
	eventCh <- events.Event{Event: "agent.message_chunk", SessionID: "ses_tool", Fields: map[string]any{"delta": "handled the failure"}}
	eventCh <- events.Event{Event: "session.end", SessionID: "ses_tool", Fields: map[string]any{"stop_reason": "end_turn"}}
	close(eventCh)
	promptDone := make(chan error, 1)
	promptDone <- nil

	result := WaitForSession(context.Background(), &cliFakeProvider{}, SessionWaitConfig{
		EventCh:    eventCh,
		PromptDone: promptDone,
		SessionID:  "ses_tool",
		RunID:      "run_tool",
	}, SessionWaitDeps{Writer: sink, Stderr: io.Discard})

	if result.ExitCode != 0 {
		t.Fatalf("ExitCode = %d, want 0", result.ExitCode)
	}
	if result.StopReason != "end_turn" {
		t.Fatalf("StopReason = %q, want end_turn", result.StopReason)
	}
}

// TestWaitForSessionSharedDeadlineReachesEveryWaiter is the regression for the
// single shared timer tick: two concurrently active waiters bound by the same
// run deadline must both stop with timeout, instead of one consuming the only
// tick while the other waits forever.
func TestWaitForSessionSharedDeadlineReachesEveryWaiter(t *testing.T) {
	deadline := time.Now().Add(100 * time.Millisecond)
	results := make([]sessionResult, 2)
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			eventCh := make(chan events.Event) // never delivers: continuously active waiter
			results[slot] = WaitForSession(context.Background(), &cliFakeProvider{}, SessionWaitConfig{
				EventCh:   eventCh,
				SessionID: "ses_deadline",
				RunID:     "run_deadline",
				Deadline:  deadline,
			}, SessionWaitDeps{Writer: &recordingSink{}, Stderr: io.Discard})
		}(i)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a waiter did not stop at the shared deadline")
	}
	for i, result := range results {
		if result.ExitCode != 124 || result.StopReason != "timeout" {
			t.Fatalf("waiter %d = {%d %q}, want {124 timeout}", i, result.ExitCode, result.StopReason)
		}
	}
}
