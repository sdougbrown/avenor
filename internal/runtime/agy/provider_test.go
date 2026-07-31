package agy

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

func testProvider(opts runtime.StartOptions) *Provider {
	p := NewWithOptions(opts)
	p.getenv = func(string) string { return "headless" }
	p.version = "1.1.5"
	p.versionOnce.Do(func() {})
	return p
}

func eventWithin(t *testing.T, ch <-chan events.Event) events.Event {
	t.Helper()
	select {
	case evt, ok := <-ch:
		if !ok {
			t.Fatal("event channel closed")
		}
		return evt
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
		return events.Event{}
	}
}

func TestProviderStartReturnsResourceFreeProvisionalSession(t *testing.T) {
	p := testProvider(runtime.StartOptions{Dir: "/default", Model: "default-model"})
	p.ptyRPCHostFactory = func(context.Context, runtime.StartOptions, string, string) (*ptyRPCHost, error) {
		t.Fatal("Phase 1 Start must not select or launch the RPC PTY host")
		return nil, nil
	}

	sess, err := p.Start(context.Background(), runtime.StartOptions{Dir: "/work", Model: "chosen-model"})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !strings.HasPrefix(sess.SessionID, "agy-pending-") {
		t.Fatalf("SessionID = %q, want agy-pending-*", sess.SessionID)
	}
	if sess.Backend != BackendID || sess.Dir != "/work" {
		t.Fatalf("session = %#v", sess)
	}

	s := p.getSession(sess.SessionID)
	if s == nil {
		t.Fatal("provisional session not registered")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client != nil {
		t.Fatal("Start must not launch a process")
	}
	if s.externalID != "" {
		t.Fatalf("externalID = %q before Prompt", s.externalID)
	}
	if s.startOpts.Model != "chosen-model" {
		t.Fatalf("model = %q", s.startOpts.Model)
	}
}

func TestProviderFirstPromptAdoptsExternalIDAndForwardsEvents(t *testing.T) {
	p := testProvider(runtime.StartOptions{Dir: "/work"})
	c, out := fakeClient()
	p.clientFactory = func() *client { return c }

	sess, err := p.Start(context.Background(), runtime.StartOptions{Model: "model-x"})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	eventCtx, cancelEvents := context.WithCancel(context.Background())
	defer cancelEvents()
	evCh, err := p.Events(eventCtx, sess.SessionID)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}

	go sendEvents(out,
		newInit("conv-external", "", ""),
		newAgentActive(1, "hello"),
		newToolActive(2, "view_file", map[string]any{"path": "README.md"}),
		newToolDone(2, "view_file", "contents", 0.1),
		newResult("conv-external", "hello", map[string]any{"total_tokens": 5}),
	)
	if err := p.Prompt(context.Background(), sess.SessionID, "say hello"); err != nil {
		t.Fatalf("Prompt: %v", err)
	}

	want := []string{"session.start", "agent.message_chunk", "avenor.message.delta", "tool.call", "tool.call_update", "session.end"}
	for i, name := range want {
		evt := eventWithin(t, evCh)
		if evt.Event != name {
			t.Fatalf("event %d = %q, want %q", i, evt.Event, name)
		}
		if evt.SessionID != "conv-external" {
			t.Fatalf("event %d session = %q", i, evt.SessionID)
		}
		if evt.Event == "session.start" {
			if evt.Fields["conversation_id"] != "conv-external" || evt.Fields["agy_version"] != "1.1.5" || evt.Fields["model"] != "model-x" {
				t.Fatalf("session.start fields = %#v", evt.Fields)
			}
		}
	}
	if p.getSession("conv-external") == nil {
		t.Fatal("external session ID was not registered")
	}
	if p.getSession(sess.SessionID) != nil {
		t.Fatal("provisional alias was not removed after Prompt")
	}
}

func TestProviderProvisionalAliasResolvesUntilPromptReturns(t *testing.T) {
	p := testProvider(runtime.StartOptions{Dir: "/work"})
	c, out := fakeClient()
	p.clientFactory = func() *client { return c }

	sess, err := p.Start(context.Background(), runtime.StartOptions{Model: "model-x"})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	provisional := sess.SessionID
	if !strings.HasPrefix(provisional, "agy-pending-") {
		t.Fatalf("provisional SessionID = %q, want agy-pending-*", provisional)
	}

	eventCtx, cancelEvents := context.WithCancel(context.Background())
	defer cancelEvents()
	evCh, err := p.Events(eventCtx, provisional)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}

	// Deliver init with the real conversation id but keep the stream open so
	// the first Prompt stays in flight after adoption fires.
	sendEventsNoClose(out, newInit("conv-alias-resolve", "", ""))

	promptDone := make(chan error, 1)
	go func() { promptDone <- p.Prompt(context.Background(), provisional, "block") }()

	// session.start is emitted after adoptSessionID registers the real id and
	// re-binds the provisional alias to the same session state.
	startEvt := eventWithin(t, evCh)
	if startEvt.Event != "session.start" || startEvt.SessionID != "conv-alias-resolve" {
		t.Fatalf("session.start = %#v", startEvt)
	}

	// The provisional alias must still resolve while the original Prompt is
	// in flight so cancellation and timeout paths that captured the old id
	// keep working.
	if p.getSession(provisional) == nil {
		t.Fatal("provisional alias was removed before Prompt returned")
	}
	if p.getSession("conv-alias-resolve") == nil {
		t.Fatal("adopted conversation id was not registered")
	}

	// Cancellation through the provisional id must resolve against the
	// in-flight prompt, not report a missing session.
	if err := p.Cancel(context.Background(), provisional); err != nil {
		t.Fatalf("Cancel through provisional id: %v", err)
	}

	endEvt := eventWithin(t, evCh)
	if endEvt.Event != "session.end" || endEvt.Fields["stop_reason"] != "cancelled" {
		t.Fatalf("cancel terminal = %#v", endEvt)
	}

	if err := <-promptDone; err == nil {
		t.Fatal("Prompt returned nil error after cancellation")
	}

	// Only after the original Prompt returns may the provisional alias be
	// removed; the adopted conversation id remains registered for resume.
	if p.getSession(provisional) != nil {
		t.Fatal("provisional alias was not removed after Prompt returned")
	}
	if p.getSession("conv-alias-resolve") == nil {
		t.Fatal("adopted conversation id was removed after Prompt returned")
	}
	_ = out.Close()
}

func TestProviderResumeFirstPromptEmitsLogicalStart(t *testing.T) {
	p := testProvider(runtime.StartOptions{Dir: "/work", Model: "model-r"})
	p.ptyRPCHostFactory = func(context.Context, runtime.StartOptions, string, string) (*ptyRPCHost, error) {
		t.Fatal("Phase 1 Resume must not select or launch the RPC PTY host")
		return nil, nil
	}
	c, out := fakeClient()
	p.clientFactory = func() *client { return c }

	sess, err := p.Resume(context.Background(), "conv-resume")
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	eventCtx, cancelEvents := context.WithCancel(context.Background())
	defer cancelEvents()
	evCh, _ := p.Events(eventCtx, sess.SessionID)
	go sendEvents(out,
		newInit("conv-resume", "", ""),
		newAgentActive(2, "resumed"),
		newResult("conv-resume", "resumed", nil),
	)
	if err := p.Prompt(context.Background(), sess.SessionID, "continue"); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if evt := eventWithin(t, evCh); evt.Event != "session.start" || evt.SessionID != "conv-resume" {
		t.Fatalf("first event = %#v", evt)
	}
}

func TestProviderSecondTurnUsesExternalIDWithoutDuplicateStart(t *testing.T) {
	p := testProvider(runtime.StartOptions{Dir: "/work"})
	c1, out1 := fakeClient()
	c2, out2 := fakeClient()
	clients := []*client{c1, c2}
	var factoryMu sync.Mutex
	p.clientFactory = func() *client {
		factoryMu.Lock()
		defer factoryMu.Unlock()
		c := clients[0]
		clients = clients[1:]
		return c
	}

	sess, _ := p.Start(context.Background(), runtime.StartOptions{})
	ctx1, cancel1 := context.WithCancel(context.Background())
	ev1, _ := p.Events(ctx1, sess.SessionID)
	go sendEvents(out1, newInit("conv-two-turns", "", ""), newResult("conv-two-turns", "one", nil))
	if err := p.Prompt(context.Background(), sess.SessionID, "one"); err != nil {
		t.Fatalf("first Prompt: %v", err)
	}
	if evt := eventWithin(t, ev1); evt.Event != "session.start" {
		t.Fatalf("first event = %q", evt.Event)
	}
	_ = eventWithin(t, ev1) // session.end
	cancel1()

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	ev2, _ := p.Events(ctx2, "conv-two-turns")
	go sendEvents(out2,
		newInit("conv-two-turns", "", ""),
		newAgentActive(3, "two"),
		newResult("conv-two-turns", "two", nil),
	)
	if err := p.Prompt(context.Background(), "conv-two-turns", "two"); err != nil {
		t.Fatalf("second Prompt: %v", err)
	}
	for {
		evt := eventWithin(t, ev2)
		if evt.Event == "session.start" {
			t.Fatal("second turn duplicated session.start")
		}
		if evt.Event == "session.end" {
			break
		}
	}
}

func TestProviderRejectsConcurrentPrompt(t *testing.T) {
	p := testProvider(runtime.StartOptions{})
	c, out := fakeClient()
	p.clientFactory = func() *client { return c }
	sess, _ := p.Start(context.Background(), runtime.StartOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	evCh, _ := p.Events(ctx, sess.SessionID)
	sendEventsNoClose(out, newInit("conv-blocked", "", ""))
	defer out.Close()

	promptCtx, cancelPrompt := context.WithCancel(context.Background())
	promptDone := make(chan error, 1)
	go func() { promptDone <- p.Prompt(promptCtx, sess.SessionID, "block") }()
	if evt := eventWithin(t, evCh); evt.Event != "session.start" {
		t.Fatalf("first event = %q", evt.Event)
	}
	if err := p.Prompt(context.Background(), "conv-blocked", "second"); err == nil || !strings.Contains(err.Error(), "prompt already active") {
		t.Fatalf("second Prompt error = %v", err)
	}
	cancelPrompt()
	<-promptDone
}

func TestProviderConcurrentSessionsAreIsolated(t *testing.T) {
	p := testProvider(runtime.StartOptions{})
	c1, out1 := fakeClient()
	c2, out2 := fakeClient()
	clients := []*client{c1, c2}
	var mu sync.Mutex
	p.clientFactory = func() *client {
		mu.Lock()
		defer mu.Unlock()
		c := clients[0]
		clients = clients[1:]
		return c
	}

	s1, _ := p.Start(context.Background(), runtime.StartOptions{})
	s2, _ := p.Start(context.Background(), runtime.StartOptions{})
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	e1, _ := p.Events(ctx1, s1.SessionID)
	e2, _ := p.Events(ctx2, s2.SessionID)

	sendEventsNoClose(out1, newInit("conv-one", "", ""))
	sendEventsNoClose(out2, newInit("conv-two", "", ""))
	done1 := make(chan error, 1)
	done2 := make(chan error, 1)
	go func() { done1 <- p.Prompt(context.Background(), s1.SessionID, "one") }()
	if evt := eventWithin(t, e1); evt.SessionID != "conv-one" {
		t.Fatalf("session one event = %#v", evt)
	}
	go func() { done2 <- p.Prompt(context.Background(), s2.SessionID, "two") }()
	if evt := eventWithin(t, e2); evt.SessionID != "conv-two" {
		t.Fatalf("session two event = %#v", evt)
	}

	sendEvents(out1, newResult("conv-one", "one", nil))
	sendEvents(out2, newResult("conv-two", "two", nil))
	if err := <-done1; err != nil {
		t.Fatalf("session one: %v", err)
	}
	if err := <-done2; err != nil {
		t.Fatalf("session two: %v", err)
	}
}

func TestProviderCancelEmitsOneTerminalEvent(t *testing.T) {
	p := testProvider(runtime.StartOptions{})
	c, out := fakeClient()
	p.clientFactory = func() *client { return c }
	sess, _ := p.Start(context.Background(), runtime.StartOptions{})
	eventCtx, cancelEvents := context.WithCancel(context.Background())
	defer cancelEvents()
	evCh, _ := p.Events(eventCtx, sess.SessionID)
	sendEventsNoClose(out, newInit("conv-cancel", "", ""))

	promptDone := make(chan error, 1)
	go func() { promptDone <- p.Prompt(context.Background(), sess.SessionID, "block") }()
	_ = eventWithin(t, evCh) // session.start
	if err := p.Cancel(context.Background(), "conv-cancel"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	end := eventWithin(t, evCh)
	if end.Event != "session.end" || end.Fields["stop_reason"] != "cancelled" {
		t.Fatalf("cancel event = %#v", end)
	}
	if err := p.Prompt(context.Background(), "conv-cancel", "too soon"); err == nil || !strings.Contains(err.Error(), "prompt already active") {
		t.Fatalf("Prompt during cancellation cleanup = %v", err)
	}
	_ = out.Close()
	<-promptDone
	select {
	case evt := <-evCh:
		if evt.Event == "session.end" {
			t.Fatal("duplicate session.end")
		}
	case <-time.After(100 * time.Millisecond):
	}
}

func TestProviderProcessDeathEmitsErrorTerminal(t *testing.T) {
	p := testProvider(runtime.StartOptions{})
	c, out := fakeClient()
	p.clientFactory = func() *client { return c }
	sess, _ := p.Start(context.Background(), runtime.StartOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	evCh, _ := p.Events(ctx, sess.SessionID)
	go sendEvents(out, newInit("conv-dead", "", ""))
	err := p.Prompt(context.Background(), sess.SessionID, "crash")
	if !errors.Is(err, ErrProcessDied) {
		t.Fatalf("Prompt error = %v", err)
	}
	_ = eventWithin(t, evCh) // session.start
	end := eventWithin(t, evCh)
	if end.Event != "session.end" || end.Fields["stop_reason"] != "error" {
		t.Fatalf("terminal = %#v", end)
	}
}

func TestProviderEventsCancellationAndCloseAreSafe(t *testing.T) {
	p := testProvider(runtime.StartOptions{})
	sess, _ := p.Start(context.Background(), runtime.StartOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := p.Events(ctx, sess.SessionID)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	cancel()
	if _, ok := <-ch; ok {
		t.Fatal("subscriber channel remained open")
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestProviderVersionFailure(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{})
	p.versionErr = errors.New("agy --version: missing")
	p.versionOnce.Do(func() {})
	if _, err := p.Start(context.Background(), runtime.StartOptions{}); err == nil {
		t.Fatal("Start succeeded with version error")
	}
	if _, err := p.Resume(context.Background(), "conv"); err == nil {
		t.Fatal("Resume succeeded with version error")
	}
}

func TestProviderCapabilitiesAndPermissionError(t *testing.T) {
	p := testProvider(runtime.StartOptions{})
	caps, err := p.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	if caps.Backend != BackendID || caps.Permissions || !caps.Resume || !caps.ModelSelection {
		t.Fatalf("capabilities = %#v", caps)
	}
	err = p.AnswerPermission(context.Background(), "session", "request", runtime.PermissionResponse{})
	if err == nil || err.Error() != "AnswerPermission is not supported in agy headless mode" {
		t.Fatalf("AnswerPermission = %v", err)
	}
}

func TestProviderUnknownSessions(t *testing.T) {
	p := testProvider(runtime.StartOptions{})
	if _, err := p.Events(context.Background(), "missing"); err == nil {
		t.Fatal("Events accepted unknown session")
	}
	if err := p.Prompt(context.Background(), "missing", "prompt"); err == nil {
		t.Fatal("Prompt accepted unknown session")
	}
	if err := p.Cancel(context.Background(), "missing"); err == nil {
		t.Fatal("Cancel accepted unknown session")
	}
	if _, err := p.Resume(context.Background(), ""); err == nil {
		t.Fatal("Resume accepted empty session ID")
	}
}
