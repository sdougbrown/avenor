package agy

import (
	"context"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
)

// sendEvents writes a series of events to the pipe writer and closes it.
func sendEvents(w io.Writer, events ...map[string]any) {
	for _, evt := range events {
		_ = writeJSONL(w, evt)
	}
	if cw, ok := w.(io.Closer); ok {
		_ = cw.Close()
	}
}

// --- 1. Compile-time interface check ---

func TestProviderImplementsInterface(t *testing.T) {
	var _ runtime.Provider = (*Provider)(nil)
}

// --- 2. Start returns session with correct fields ---

func TestStartReturnsSession(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{Dir: "/work"})

	c, wOut, _, _, _ := fakeClient()
	p.clientFactory = func() *client {
		return c
	}

	go func() {
		writeJSONL(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-abc123",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		})
	}()

	sess, err := p.Start(context.Background(), runtime.StartOptions{
		Model: "gemini-2.0-flash",
		Dir:   "/work",
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if sess.SessionID != "conv-abc123" {
		t.Errorf("SessionID = %q, want %q", sess.SessionID, "conv-abc123")
	}
	if sess.Backend != "agy" {
		t.Errorf("Backend = %q, want agy", sess.Backend)
	}
	if sess.Dir != "/work" {
		t.Errorf("Dir = %q, want /work", sess.Dir)
	}

	_ = c.Close()
	_ = wOut.Close()
}

// --- 3. Resume returns session with correct fields ---

func TestResumeReturnsSession(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{Dir: "/work"})

	sess, err := p.Resume(context.Background(), "conv-existing")
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if sess.SessionID != "conv-existing" {
		t.Errorf("SessionID = %q, want %q", sess.SessionID, "conv-existing")
	}
	if sess.Backend != "agy" {
		t.Errorf("Backend = %q, want agy", sess.Backend)
	}
	if sess.Dir != "/work" {
		t.Errorf("Dir = %q, want /work", sess.Dir)
	}
}

func TestResumeEmptySessionID(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	_, err := p.Resume(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty session id")
	}
}

// --- 4. First Prompt publishes session.start then reads to completion ---

func TestFirstPromptPublishesSessionStart(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{Dir: "/work"})

	c, wOut, _, _, _ := fakeClient()
	p.clientFactory = func() *client {
		return c
	}

	// Send init event so Start succeeds
	go func() {
		writeJSONL(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-abc",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		})
	}()

	sess, err := p.Start(context.Background(), runtime.StartOptions{
		Model: "gemini-2.0-flash",
		Dir:   "/work",
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Subscribe before Prompt
	evCh, err := p.Events(context.Background(), sess.SessionID)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}

	// Give time for subscription to register
	time.Sleep(50 * time.Millisecond)

	// Send init event for FirstPrompt and then result
	go func() {
		if err := writeJSONL(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-abc",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		}); err != nil {
			t.Logf("first prompt init write failed: %v", err)
		}
		if err := writeJSONL(wOut, map[string]any{
			"type":            "result",
			"conversation_id": "conv-abc",
			"response":        "hello world",
		}); err != nil {
			t.Logf("result write failed: %v", err)
		}
		wOut.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = p.Prompt(ctx, sess.SessionID, "test prompt")
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}

	// Check that session.start was received
	select {
	case evt := <-evCh:
		if evt.Event != "session.start" {
			t.Errorf("first event = %q, want session.start", evt.Event)
		}
		if evt.Fields["conversation_id"] != "conv-abc" {
			t.Errorf("session.start conversation_id = %v, want conv-abc", evt.Fields["conversation_id"])
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("timed out waiting for session.start event")
	}

	select {
	case evt := <-evCh:
		if evt.Event != "session.end" {
			t.Errorf("second event = %q, want session.end", evt.Event)
		}
		if evt.Fields["stop_reason"] != "end_turn" {
			t.Errorf("session.end stop_reason = %v, want end_turn", evt.Fields["stop_reason"])
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("timed out waiting for session.end event")
	}

	_ = c.Close()
	_ = wOut.Close()
}

// --- 5. Second Prompt on same session starts a resumed process ---

func TestSecondPromptResumedProcess(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{Dir: "/work"})

	c, wOut, _, _, _ := fakeClient()
	p.clientFactory = func() *client {
		return c
	}

	// Initial Start
	go func() {
		writeJSONL(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-abc",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		})
	}()

	sess, err := p.Start(context.Background(), runtime.StartOptions{
		Model: "gemini-2.0-flash",
		Dir:   "/work",
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Subscribe for events
	evCh, _ := p.Events(context.Background(), sess.SessionID)
	time.Sleep(50 * time.Millisecond)

	// First Prompt
	go func() {
		sendEvents(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-abc",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		}, map[string]any{
			"type":            "result",
			"conversation_id": "conv-abc",
			"response":        "first response",
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err = p.Prompt(ctx, sess.SessionID, "first prompt")
	cancel()
	if err != nil {
		t.Fatalf("first Prompt: %v", err)
	}

	// Check session.start was emitted
	select {
	case evt := <-evCh:
		if evt.Event != "session.start" {
			t.Errorf("first event = %q, want session.start", evt.Event)
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("timed out waiting for session.start")
	}

	// Second Prompt — uses resumed path (clientFactory creates new client)
	// The second prompt will create a new client via clientFactory, which in test
	// mode won't have a real subprocess. We verify the second turn flag works
	// by checking that session.start is NOT emitted again.
	// In test mode, the second client has no init event, so ResumePrompt will
	// fail with startup timeout. We expect this failure but verify firstTurn=false
	// was set.

	// Set up a second fake client for the resumed path
	c2, wOut2, _, _, _ := fakeClient()
	p.clientFactory = func() *client {
		return c2
	}

	// Send init + result for the resumed client
	go func() {
		sendEvents(wOut2, map[string]any{
			"type":            "init",
			"conversation_id": "conv-abc",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		}, map[string]any{
			"type":            "result",
			"conversation_id": "conv-abc",
			"response":        "second response",
		})
	}()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	err = p.Prompt(ctx2, sess.SessionID, "second prompt")
	cancel2()
	if err != nil {
		t.Fatalf("second Prompt: %v", err)
	}

	// session.start should NOT be emitted again (firstTurn=false)
	select {
	case evt := <-evCh:
		if evt.Event == "session.start" {
			t.Error("session.start should not be emitted on second turn")
		}
	case <-time.After(200 * time.Millisecond):
		// No event is fine — second turn doesn't emit session.start
	}

	_ = c.Close()
	_ = wOut.Close()
	_ = c2.Close()
	_ = wOut2.Close()
}

// --- 6. Same-session prompt rejection (concurrent) ---

func TestConcurrentPromptRejection(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{Dir: "/work"})

	c, wOut, _, _, _ := fakeClient()
	p.clientFactory = func() *client {
		return c
	}

	// Send init for Start
	go func() {
		writeJSONL(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-abc",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		})
	}()

	sess, err := p.Start(context.Background(), runtime.StartOptions{
		Model: "gemini-2.0-flash",
		Dir:   "/work",
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// First prompt starts and blocks on readUntilResult
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan struct{})
	go func() {
		// Send init for first prompt
		writeJSONL(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-abc",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		})
		// Don't send result — keep it blocked
		_ = p.Prompt(ctx1, sess.SessionID, "blocked prompt")
		close(done1)
	}()
	time.Sleep(100 * time.Millisecond)

	// Second prompt should fail
	err = p.Prompt(context.Background(), sess.SessionID, "second prompt")
	if err == nil {
		t.Fatal("expected error for concurrent prompt")
	}
	if !strings.Contains(err.Error(), "prompt already active") {
		t.Errorf("error = %v, want 'prompt already active'", err)
	}

	cancel1()
	<-done1

	_ = c.Close()
	_ = wOut.Close()
}

// --- 7. Concurrent independent sessions work ---

func TestConcurrentIndependentSessions(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{Dir: "/work"})

	c1, wOut1, _, _, _ := fakeClient()
	c2, wOut2, _, _, _ := fakeClient()

	factory := func() *client {
		return c1
	}
	p.clientFactory = factory

	// Start first session
	go func() {
		writeJSONL(wOut1, map[string]any{
			"type":            "init",
			"conversation_id": "conv-1",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		})
	}()

	s1, err := p.Start(context.Background(), runtime.StartOptions{
		Model: "gemini-2.0-flash",
		Dir:   "/work",
	})
	if err != nil {
		t.Fatalf("Start session 1: %v", err)
	}

	// Start second session — need a different client
	factory = func() *client {
		return c2
	}
	p.clientFactory = factory

	go func() {
		writeJSONL(wOut2, map[string]any{
			"type":            "init",
			"conversation_id": "conv-2",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		})
	}()

	s2, err := p.Start(context.Background(), runtime.StartOptions{
		Model: "gemini-2.0-flash",
		Dir:   "/work",
	})
	if err != nil {
		t.Fatalf("Start session 2: %v", err)
	}

	if s1.SessionID == s2.SessionID {
		t.Fatal("sessions should have different IDs")
	}

	// Both should accept prompts concurrently
	done := make(chan struct{}, 2)

	// Session 1 prompt
	go func() {
		sendEvents(wOut1, map[string]any{
			"type":            "init",
			"conversation_id": "conv-1",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		}, map[string]any{
			"type":            "result",
			"conversation_id": "conv-1",
			"response":        "response 1",
		})
		_ = p.Prompt(context.Background(), s1.SessionID, "prompt 1")
		done <- struct{}{}
	}()

	// Session 2 prompt
	go func() {
		sendEvents(wOut2, map[string]any{
			"type":            "init",
			"conversation_id": "conv-2",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		}, map[string]any{
			"type":            "result",
			"conversation_id": "conv-2",
			"response":        "response 2",
		})
		_ = p.Prompt(context.Background(), s2.SessionID, "prompt 2")
		done <- struct{}{}
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for prompt %d", i+1)
		}
	}

	_ = c1.Close()
	_ = wOut1.Close()
	_ = c2.Close()
	_ = wOut2.Close()
}

// --- 8. Cancel emits cancelled session.end ---

func TestCancelEmitsCancelledEnd(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{Dir: "/work"})

	c, wOut, _, _, _ := fakeClient()
	p.clientFactory = func() *client {
		return c
	}

	go func() {
		writeJSONL(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-cancel",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		})
	}()

	sess, err := p.Start(context.Background(), runtime.StartOptions{
		Model: "gemini-2.0-flash",
		Dir:   "/work",
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	evCh, err := p.Events(context.Background(), sess.SessionID)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}

	// Start a blocking prompt
	go func() {
		// Only send init — keep result unsent so the prompt blocks
		writeJSONL(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-cancel",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		})
		_ = p.Prompt(context.Background(), sess.SessionID, "blocked")
	}()
	time.Sleep(100 * time.Millisecond)

	// Cancel
	cancelCtx, cancelCancel := context.WithTimeout(context.Background(), 2*time.Second)
	err = p.Cancel(cancelCtx, sess.SessionID)
	cancelCancel()
	if err != nil {
		t.Logf("Cancel returned: %v", err)
	}

	// Drain the session.start that the Events forwarding goroutine picks up
	// from the client's translated init event.
	select {
	case <-evCh:
	case <-time.After(500 * time.Millisecond):
		t.Error("timed out draining session.start")
	}

	// Check for session.end with cancelled stop_reason
	select {
	case evt := <-evCh:
		if evt.Event != "session.end" {
			t.Errorf("event = %q, want session.end", evt.Event)
		}
		if evt.Fields["stop_reason"] != "cancelled" {
			t.Errorf("stop_reason = %v, want cancelled", evt.Fields["stop_reason"])
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("timed out waiting for session.end event after cancel")
	}

	_ = c.Close()
	_ = wOut.Close()
}

// --- 9. session.end for success (stop_reason=end_turn) ---

func TestSessionEndSuccess(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{Dir: "/work"})

	c, wOut, _, _, _ := fakeClient()
	p.clientFactory = func() *client {
		return c
	}

	go func() {
		writeJSONL(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-success",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		})
	}()

	sess, err := p.Start(context.Background(), runtime.StartOptions{
		Model: "gemini-2.0-flash",
		Dir:   "/work",
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	evCh, _ := p.Events(context.Background(), sess.SessionID)
	time.Sleep(50 * time.Millisecond)

	// Send init + result for the prompt
	go func() {
		sendEvents(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-success",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		}, map[string]any{
			"type":            "result",
			"conversation_id": "conv-success",
			"response":        "success response",
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err = p.Prompt(ctx, sess.SessionID, "test prompt")
	cancel()
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}

	// Check session.start
	select {
	case evt := <-evCh:
		if evt.Event != "session.start" {
			t.Errorf("first event = %q, want session.start", evt.Event)
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("timed out waiting for session.start")
	}

	// Check session.end with end_turn
	select {
	case evt := <-evCh:
		if evt.Event != "session.end" {
			t.Errorf("event = %q, want session.end", evt.Event)
		}
		if evt.Fields["stop_reason"] != "end_turn" {
			t.Errorf("stop_reason = %v, want end_turn", evt.Fields["stop_reason"])
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("timed out waiting for session.end event")
	}

	_ = c.Close()
	_ = wOut.Close()
}

// --- 10. session.end for error ---

func TestSessionEndError(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{Dir: "/work"})

	c, wOut, _, _, _ := fakeClient()
	p.clientFactory = func() *client {
		return c
	}

	go func() {
		writeJSONL(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-error",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		})
	}()

	sess, err := p.Start(context.Background(), runtime.StartOptions{
		Model: "gemini-2.0-flash",
		Dir:   "/work",
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	evCh, _ := p.Events(context.Background(), sess.SessionID)
	time.Sleep(50 * time.Millisecond)

	// Send init + error result
	go func() {
		writeJSONL(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-error",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		})
		writeJSONL(wOut, map[string]any{
			"type":            "result",
			"conversation_id": "conv-error",
			"error":           "model error",
		})
		wOut.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err = p.Prompt(ctx, sess.SessionID, "test prompt")
	cancel()
	if err == nil {
		t.Fatal("expected error from Prompt with error result")
	}

	// Drain session.start forwarded by the Events goroutine
	select {
	case <-evCh:
	case <-time.After(500 * time.Millisecond):
		t.Error("timed out draining session.start")
	}

	// Check session.end with error stop_reason
	select {
	case evt := <-evCh:
		if evt.Event != "session.end" {
			t.Errorf("event = %q, want session.end", evt.Event)
		}
		if evt.Fields["stop_reason"] != "error" {
			t.Errorf("stop_reason = %v, want error", evt.Fields["stop_reason"])
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("timed out waiting for session.end event")
	}

	_ = c.Close()
	_ = wOut.Close()
}

// --- 11. session.start not lost before subscription ---

func TestSessionStartNotLostBeforeSubscription(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{Dir: "/work"})

	c, wOut, _, _, _ := fakeClient()
	p.clientFactory = func() *client {
		return c
	}

	// Start the session first
	go func() {
		writeJSONL(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-noloss",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		})
	}()

	sess, err := p.Start(context.Background(), runtime.StartOptions{
		Model: "gemini-2.0-flash",
		Dir:   "/work",
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Then subscribe
	evCh, err := p.Events(context.Background(), sess.SessionID)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}

	// Then prompt — session.start should be emitted
	go func() {
		sendEvents(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-noloss",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		}, map[string]any{
			"type":            "result",
			"conversation_id": "conv-noloss",
			"response":        "noloss response",
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	err = p.Prompt(ctx, sess.SessionID, "test prompt")
	cancel()
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}

	// Should receive session.start first
	select {
	case evt := <-evCh:
		if evt.Event != "session.start" {
			t.Errorf("first event = %q, want session.start", evt.Event)
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("timed out waiting for session.start")
	}

	_ = c.Close()
	_ = wOut.Close()
}

// --- 12. session.start emitted once, not duplicated ---

func TestSessionStartEmittedOnce(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{Dir: "/work"})

	c, wOut, _, _, _ := fakeClient()
	p.clientFactory = func() *client {
		return c
	}

	go func() {
		writeJSONL(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-once",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		})
	}()

	sess, err := p.Start(context.Background(), runtime.StartOptions{
		Model: "gemini-2.0-flash",
		Dir:   "/work",
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Two subscribers
	evCh1, _ := p.Events(context.Background(), sess.SessionID)
	evCh2, _ := p.Events(context.Background(), sess.SessionID)
	time.Sleep(50 * time.Millisecond)

	// Prompt
	go func() {
		sendEvents(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-once",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		}, map[string]any{
			"type":            "result",
			"conversation_id": "conv-once",
			"response":        "once response",
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	err = p.Prompt(ctx, sess.SessionID, "test")
	cancel()
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}

	// Each subscriber should receive exactly one session.start
	startCount := 0
	for range 2 {
		select {
		case evt := <-evCh1:
			if evt.Event == "session.start" {
				startCount++
			}
		case <-time.After(500 * time.Millisecond):
		}
	}
	if startCount != 1 {
		t.Errorf("subscriber 1 got %d session.start events, want 1", startCount)
	}

	startCount = 0
	for range 2 {
		select {
		case evt := <-evCh2:
			if evt.Event == "session.start" {
				startCount++
			}
		case <-time.After(500 * time.Millisecond):
		}
	}
	if startCount != 1 {
		t.Errorf("subscriber 2 got %d session.start events, want 1", startCount)
	}

	_ = c.Close()
	_ = wOut.Close()
}

// --- 13. Capabilities returns expected values ---

func TestCapabilities(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	caps, err := p.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	if caps.Backend != "agy" {
		t.Errorf("Backend = %q, want agy", caps.Backend)
	}
	if caps.Permissions {
		t.Error("Permissions should be false")
	}
	if !caps.Resume {
		t.Error("Resume should be true")
	}
	if caps.ExternalServerURL {
		t.Error("ExternalServerURL should be false")
	}
	if caps.SubprocessDiscovery {
		t.Error("SubprocessDiscovery should be false")
	}
	if !caps.ModelSelection {
		t.Error("ModelSelection should be true")
	}
}

// --- 14. AnswerPermission returns error ---

func TestAnswerPermissionError(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	err := p.AnswerPermission(context.Background(), "sess", "req", runtime.PermissionResponse{Allow: true})
	if err == nil {
		t.Fatal("expected error from AnswerPermission")
	}
	if !strings.Contains(err.Error(), "not supported") {
		t.Errorf("error = %v, want 'not supported'", err)
	}
}

// --- 15. Close during active prompts ---

func TestCloseDuringActivePrompt(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{Dir: "/work"})

	c, wOut, _, _, _ := fakeClient()
	p.clientFactory = func() *client {
		return c
	}

	go func() {
		writeJSONL(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-close",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		})
	}()

	sess, err := p.Start(context.Background(), runtime.StartOptions{
		Model: "gemini-2.0-flash",
		Dir:   "/work",
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Start a blocking prompt
	done := make(chan struct{})
	go func() {
		writeJSONL(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-close",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		})
		// Don't send result — keep blocked
		_ = p.Prompt(context.Background(), sess.SessionID, "blocked")
		close(done)
	}()
	time.Sleep(100 * time.Millisecond)

	// Close should succeed without blocking
	err = p.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The prompt goroutine should eventually return
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("prompt goroutine did not return after Close")
	}

	_ = wOut.Close()
}

// --- 16. Close before any prompt ---

func TestCloseBeforePrompt(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	err := p.Close()
	if err != nil {
		t.Fatalf("Close with no sessions: %v", err)
	}
}

// --- 17. Close cleanup is idempotent ---

func TestCloseIdempotent(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	err := p.Close()
	if err != nil {
		t.Fatalf("first Close: %v", err)
	}
	err = p.Close()
	if err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// --- 18. Events subscriber cancellation ---

func TestEventsSubscriberCancellation(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{Dir: "/work"})

	c, wOut, _, _, _ := fakeClient()
	p.clientFactory = func() *client {
		return c
	}

	go func() {
		writeJSONL(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-subcancel",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		})
	}()

	sess, err := p.Start(context.Background(), runtime.StartOptions{
		Model: "gemini-2.0-flash",
		Dir:   "/work",
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	evCh, err := p.Events(ctx, sess.SessionID)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}

	// Send init event for the prompt
	go func() {
		writeJSONL(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-subcancel",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		})
	}()
	time.Sleep(50 * time.Millisecond)

	// Cancel the context
	cancel()

	// Channel should be closed
	select {
	case _, ok := <-evCh:
		if ok {
			t.Error("events channel should be closed after context cancellation")
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("timed out waiting for events channel to close")
	}

	_ = c.Close()
	_ = wOut.Close()
}

// --- 19. Events channel buffered to 128 ---

func TestEventsChannelBuffered(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{Dir: "/work"})

	c, wOut, _, _, _ := fakeClient()
	p.clientFactory = func() *client {
		return c
	}

	go func() {
		writeJSONL(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-buf",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		})
	}()

	sess, err := p.Start(context.Background(), runtime.StartOptions{
		Model: "gemini-2.0-flash",
		Dir:   "/work",
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	evCh, err := p.Events(context.Background(), sess.SessionID)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}

	// Use reflect to check the channel buffer capacity.
	v := reflect.ValueOf(evCh)
	if v.Kind() != reflect.Chan {
		t.Fatal("evCh is not a channel")
	}
	if cap := v.Cap(); cap < 128 {
		t.Errorf("events channel capacity = %d, want at least 128", cap)
	}

	_ = c.Close()
	_ = wOut.Close()
}

// --- 20. Unknown sessions return errors ---

func TestUnknownSession(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})

	_, err := p.Events(context.Background(), "unknown-session")
	if err == nil {
		t.Fatal("expected error for unknown session")
	}

	err = p.Prompt(context.Background(), "unknown-session", "test")
	if err == nil {
		t.Fatal("expected error for prompt on unknown session")
	}

	err = p.Cancel(context.Background(), "unknown-session")
	if err == nil {
		t.Fatal("expected error for cancel on unknown session")
	}
}

// --- 21. 5-minute idle timer transition (use short timer) ---

func TestIdleTimerTransition(t *testing.T) {
	// This test verifies the lifecycle:
	// 1. Start creates a session with a client
	// 2. After the first prompt completes, the client is closed and nil
	// 3. Resume still works (creates logical session)
	// 4. A later Prompt creates a new client via the factory

	p := NewWithOptions(runtime.StartOptions{Dir: "/work"})

	c, wOut, _, _, _ := fakeClient()
	p.clientFactory = func() *client {
		return c
	}

	// Start the session
	go func() {
		writeJSONL(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-idle",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		})
	}()

	sess, err := p.Start(context.Background(), runtime.StartOptions{
		Model: "gemini-2.0-flash",
		Dir:   "/work",
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// First prompt — completes normally
	go func() {
		sendEvents(wOut, map[string]any{
			"type":            "init",
			"conversation_id": "conv-idle",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		}, map[string]any{
			"type":            "result",
			"conversation_id": "conv-idle",
			"response":        "first response",
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = p.Prompt(ctx, sess.SessionID, "first prompt")
	cancel()
	if err != nil {
		t.Fatalf("first Prompt: %v", err)
	}

	// After Prompt, client should be closed and nil
	s := p.getSession(sess.SessionID)
	if s == nil {
		t.Fatal("session should still exist after Prompt")
	}
	if s.client != nil {
		t.Fatal("client should be nil after Prompt completes")
	}
	if s.firstTurn {
		t.Fatal("firstTurn should be false after first Prompt")
	}

	// Resume the session (creates a logical session, no client)
	_, err = p.Resume(context.Background(), sess.SessionID)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}

	// Second prompt — creates a new client via factory
	c2, wOut2, _, _, _ := fakeClient()
	p.clientFactory = func() *client {
		return c2
	}

	go func() {
		sendEvents(wOut2, map[string]any{
			"type":            "init",
			"conversation_id": "conv-idle",
			"model":           "gemini-2.0-flash",
			"agy_version":     "1.2.0",
		}, map[string]any{
			"type":            "result",
			"conversation_id": "conv-idle",
			"response":        "second response",
		})
	}()

	evCh, _ := p.Events(context.Background(), sess.SessionID)
	time.Sleep(50 * time.Millisecond)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	err = p.Prompt(ctx2, sess.SessionID, "second prompt")
	cancel2()
	if err != nil {
		t.Fatalf("second Prompt: %v", err)
	}

	// session.start should NOT be emitted — this is the resumed path
	select {
	case evt := <-evCh:
		if evt.Event == "session.start" {
			t.Error("session.start should not be emitted on resumed process")
		}
	case <-time.After(200 * time.Millisecond):
		// Expected — no session.start on resumed path
	}

	_ = c.Close()
	_ = wOut.Close()
	_ = c2.Close()
	_ = wOut2.Close()
}

// --- 22. Unknown session Resume ---

func TestResumeUnknownSessionCreatesSession(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{Dir: "/work"})

	sess, err := p.Resume(context.Background(), "conv-new")
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if sess.SessionID != "conv-new" {
		t.Errorf("SessionID = %q, want conv-new", sess.SessionID)
	}

	// Verify session exists for later Prompt
	s := p.getSession("conv-new")
	if s == nil {
		t.Fatal("session should exist after Resume")
	}
}
