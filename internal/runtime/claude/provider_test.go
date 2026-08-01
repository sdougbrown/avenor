package claude

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"
	"github.com/sdougbrown/avenor/internal/runtime/claudeutil"
)

func TestThinkingEffortArguments(t *testing.T) {
	for _, level := range []string{"low", "medium", "high", "xhigh", "max"} {
		args := claudeutil.BuildArgs("session", "", runtime.StartOptions{Thinking: level})
		if got := strings.Join(args, " "); !strings.Contains(got, "--effort "+level) {
			t.Fatalf("args for %s = %q", level, got)
		}
	}
	if got := strings.Join(claudeutil.BuildArgs("session", "", runtime.StartOptions{}), " "); strings.Contains(got, "--effort") {
		t.Fatalf("empty thinking args = %q", got)
	}
}

func TestNewWithOptions(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{
		Agent: "jockey",
		Model: "claude-sonnet-4",
	})
	if p == nil {
		t.Fatal("NewWithOptions returned nil provider")
	}
	cp, ok := p.(*Provider)
	if !ok {
		t.Fatalf("expected *Provider, got %T", p)
	}
	if cp.opts.Agent != "jockey" || cp.opts.Model != "claude-sonnet-4" {
		t.Fatalf("opts not stored: got %+v", cp.opts)
	}
}

func TestDefaultLauncherIsPTY(t *testing.T) {
	t.Setenv("AVENOR_CLAUDE_TERMINAL", "")
	l := defaultLauncher()
	if _, ok := l.(terminal.PTYLauncher); !ok {
		t.Fatalf("expected PTYLauncher, got %T", l)
	}
}

func TestTmuxEnvSelectsTmux(t *testing.T) {
	t.Setenv("AVENOR_CLAUDE_TERMINAL", "tmux")
	l := defaultLauncher()
	if _, ok := l.(terminal.TmuxLauncher); !ok {
		t.Fatalf("expected TmuxLauncher, got %T", l)
	}
}

func TestCapabilities(t *testing.T) {
	t.Setenv("AVENOR_CLAUDE_TERMINAL", "tmux")
	p := New()
	caps, err := p.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	if caps.Backend != backendID {
		t.Errorf("Backend = %q, want %q", caps.Backend, backendID)
	}
	if !caps.Permissions {
		t.Error("Permissions should be true")
	}
	if !caps.Resume {
		t.Error("Resume should be true")
	}
	if !caps.SubprocessDiscovery {
		t.Error("SubprocessDiscovery should be true")
	}
	if !caps.ModelSelection {
		t.Error("ModelSelection should be true")
	}
	if caps.ExternalServerURL {
		t.Error("ExternalServerURL should be false")
	}
}

func TestCapabilitiesPTYReportsResume(t *testing.T) {
	t.Setenv("AVENOR_CLAUDE_TERMINAL", "")
	p := New()
	caps, err := p.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	if !caps.Resume {
		t.Error("Resume should be true for PTY launcher (in-memory resume in stable mode)")
	}
}

func TestResumePTYInMemory(t *testing.T) {
	p := &Provider{
		sessions: make(map[string]*claudecore.Session),
		launcher: terminal.PTYLauncher{},
	}
	term := terminal.NewFakeSession("avenor-pty-live", 4243, "ready")
	p.sessions["ses-pty-live"] = &claudecore.Session{
		SessionID: "ses-pty-live",
		Dir:       "/tmp/work",
		Term:      term,
	}
	got, err := p.Resume(context.Background(), "ses-pty-live")
	if err != nil {
		t.Fatalf("Resume on PTY launcher: %v", err)
	}
	if got.SessionID != "ses-pty-live" || got.PID != 4243 {
		t.Fatalf("Resume = %+v, want SessionID=ses-pty-live PID=4243", got)
	}
}

func TestResumeMissingSession(t *testing.T) {
	p := &Provider{
		sessions: make(map[string]*claudecore.Session),
		launcher: terminal.TmuxLauncher{},
	}
	_, err := p.Resume(context.Background(), "ses-unknown")
	if err == nil {
		t.Fatal("expected error for unknown session")
	}
	if !strings.Contains(err.Error(), "session not found") {
		t.Fatalf("error = %q, want 'session not found'", err)
	}
}

func TestResumeReturnsLiveSession(t *testing.T) {
	p := &Provider{
		sessions: make(map[string]*claudecore.Session),
		launcher: terminal.TmuxLauncher{},
	}
	term := terminal.NewFakeSession("avenor-ses-live", 4242, "ready")
	p.sessions["ses-live"] = &claudecore.Session{
		SessionID: "ses-live",
		Dir:       "/tmp/work",
		Term:      term,
	}
	got, err := p.Resume(context.Background(), "ses-live")
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	want := runtime.Session{
		SessionID: "ses-live",
		Backend:   backendID,
		Dir:       "/tmp/work",
		PID:       4242,
	}
	if got != want {
		t.Fatalf("Resume = %+v, want %+v", got, want)
	}
}

func TestResumeAfterTerminalDeath(t *testing.T) {
	p := &Provider{
		sessions: make(map[string]*claudecore.Session),
		launcher: terminal.TmuxLauncher{},
	}
	term := terminal.NewFakeSession("avenor-ses-dead", 1, "")
	term.SetAlive(false)
	p.sessions["ses-dead"] = &claudecore.Session{
		SessionID: "ses-dead",
		Dir:       "/tmp/work",
		Term:      term,
	}
	_, err := p.Resume(context.Background(), "ses-dead")
	if err == nil {
		t.Fatal("expected error when terminal has exited")
	}
	if !strings.Contains(err.Error(), "terminal exited") {
		t.Fatalf("error = %q, want 'terminal exited'", err)
	}
}

func TestAnswerPermissionNotStarted(t *testing.T) {
	p := New()
	err := p.AnswerPermission(context.Background(), "ses", "req", runtime.PermissionResponse{Allow: true})
	if err == nil {
		t.Fatal("expected error for nonexistent session")
	}
	if !strings.Contains(err.Error(), "session not found") {
		t.Fatalf("error = %q, want session not found", err)
	}
}

func TestProviderInterface(t *testing.T) {
	var _ runtime.Provider = (*Provider)(nil)
}

func TestEventsNotFound(t *testing.T) {
	p := New()
	_, err := p.Events(context.Background(), "no-such-session")
	if err == nil {
		t.Fatal("expected error for nonexistent session")
	}
	if !strings.Contains(err.Error(), "session not found") {
		t.Fatalf("error = %q, want 'session not found'", err)
	}
}

func TestEventsProxySemantics(t *testing.T) {
	p := &Provider{sessions: make(map[string]*claudecore.Session)}
	sess := claudecore.NewSession(context.Background(), claudecore.SessionOptions{
		SessionID: "ses-events-proxy",
		EventsBuf: 4,
	})
	p.sessions[sess.SessionID] = sess

	// Proxy: event sent to session channel should arrive on the proxy channel.
	ch, err := p.Events(context.Background(), sess.SessionID)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	sess.Emit(events.Event{Event: "test.proxy", SessionID: sess.SessionID})
	select {
	case ev := <-ch:
		if ev.Event != "test.proxy" {
			t.Fatalf("event = %q, want test.proxy", ev.Event)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for proxied event")
	}

	// Context cancellation: proxy channel should close when the supplied ctx is done.
	childCtx, cancel := context.WithCancel(context.Background())
	ch2, err := p.Events(childCtx, sess.SessionID)
	if err != nil {
		t.Fatalf("Events (2): %v", err)
	}
	cancel()
	select {
	case _, ok := <-ch2:
		if ok {
			t.Fatal("expected proxy channel to be closed after context cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for proxy channel to close")
	}
}

func TestPromptNotFound(t *testing.T) {
	p := New()
	err := p.Prompt(context.Background(), "no-such-session", "hello")
	if err == nil {
		t.Fatal("expected error for nonexistent session")
	}
	if !strings.Contains(err.Error(), "session not found") {
		t.Fatalf("error = %q, want 'session not found'", err)
	}
}

func TestPromptOnFinishedSessionFails(t *testing.T) {
	p := &Provider{sessions: make(map[string]*claudecore.Session)}
	s := &claudecore.Session{
		SessionID: "ses-finished",
		Term:      terminal.NewFakeSession("test-term", 1, "ready"),
		Events:    make(chan events.Event, 8),
	}
	s.Finished = true
	p.sessions[s.SessionID] = s

	err := p.Prompt(context.Background(), s.SessionID, "hello")
	if err == nil {
		t.Fatal("expected error for finished session")
	}
	if !strings.Contains(err.Error(), "session already finished") {
		t.Fatalf("error = %q, want 'session already finished'", err)
	}
}

func TestPromptReturnsNilForLiveSession(t *testing.T) {
	p := &Provider{sessions: make(map[string]*claudecore.Session)}
	s := claudecore.NewSession(context.Background(), claudecore.SessionOptions{
		SessionID: "ses-live-prompt",
		EventsBuf: 8,
	})
	s.Term = terminal.NewFakeSession("test-term", 1, "ready")
	p.sessions[s.SessionID] = s
	defer s.CancelFn()

	if err := p.Prompt(context.Background(), s.SessionID, "hello"); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
}

func TestCancelNotFound(t *testing.T) {
	p := New()
	err := p.Cancel(context.Background(), "no-such-session")
	if err == nil {
		t.Fatal("expected error for nonexistent session")
	}
	if !strings.Contains(err.Error(), "session not found") {
		t.Fatalf("error = %q, want 'session not found'", err)
	}
}

func TestCancelSendsEscThenWaitsForIdle(t *testing.T) {
	p := &Provider{
		sessions:    make(map[string]*claudecore.Session),
		cancelGrace: 500 * time.Millisecond,
	}
	// First poll sees active; subsequent polls see idle (queue sticks on last).
	term := terminal.NewFakeSessionQueue("test-term", 1, []string{
		"✻ Churning… (2m 17s · ↓ 7.2k tokens)",
		"❯ ready",
	})

	s := claudecore.NewSession(context.Background(), claudecore.SessionOptions{
		SessionID: "ses-cancel-idle",
		EventsBuf: 8,
	})
	s.Term = term
	p.sessions[s.SessionID] = s
	defer s.CancelFn()

	if err := p.Cancel(context.Background(), s.SessionID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	// Verify Esc Esc was sent.
	sends := term.SendCalls()
	if len(sends) != 1 {
		t.Fatalf("SendCalls() len = %d, want 1", len(sends))
	}
	if len(sends[0]) != 2 || sends[0][0] != string(terminal.KeyEsc) || sends[0][1] != string(terminal.KeyEsc) {
		t.Fatalf("keys sent = %v, want [Esc Esc]", sends[0])
	}

	// Verify session.end with stop_reason=cancelled was emitted.
	var found bool
	for len(s.Events) > 0 {
		ev := <-s.Events
		if ev.Event == "session.end" {
			if ev.Fields["stop_reason"] != "cancelled" {
				t.Fatalf("stop_reason = %v, want cancelled", ev.Fields["stop_reason"])
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatal("no session.end event with stop_reason=cancelled emitted")
	}

	s.Mu.Lock()
	finished := s.Finished
	s.Mu.Unlock()
	if !finished {
		t.Fatal("session.Finished should be true after Cancel")
	}
}

func TestCancelForcesKillOnTimeout(t *testing.T) {
	t.Parallel()
	p := &Provider{
		sessions:    make(map[string]*claudecore.Session),
		cancelGrace: 200 * time.Millisecond,
	}
	// Stays active forever — never returns idle.
	term := terminal.NewFakeSession("test-term", 1, "✻ Churning… (2m 17s · ↓ 7.2k tokens)")
	s := claudecore.NewSession(context.Background(), claudecore.SessionOptions{
		SessionID: "ses-cancel-forced",
		EventsBuf: 8,
	})
	s.Term = term
	p.sessions[s.SessionID] = s
	defer s.CancelFn()

	start := time.Now()
	if err := p.Cancel(context.Background(), s.SessionID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	elapsed := time.Since(start)

	// Should complete close to cancelGrace (200ms), well under 1s.
	if elapsed > time.Second {
		t.Fatalf("Cancel took %v, want ≤1s", elapsed)
	}

	// Drain events to find session.end.
	var foundForced bool
drain:
	for {
		select {
		case ev, ok := <-s.Events:
			if !ok {
				break drain
			}
			if ev.Event == "session.end" && ev.Fields["stop_reason"] == "cancelled_forced" {
				foundForced = true
			}
		default:
			break drain
		}
	}
	if !foundForced {
		t.Fatal("no session.end with stop_reason=cancelled_forced emitted")
	}

	s.Mu.Lock()
	finished := s.Finished
	s.Mu.Unlock()
	if !finished {
		t.Fatal("session.Finished should be true after forced cancel")
	}
}

func TestCancelOnCanceledCtx(t *testing.T) {
	t.Parallel()
	p := &Provider{
		sessions:    make(map[string]*claudecore.Session),
		cancelGrace: 5 * time.Second,
	}
	term := terminal.NewFakeSession("test-term", 1, "✻ Churning…")
	s := claudecore.NewSession(context.Background(), claudecore.SessionOptions{
		SessionID: "ses-ctx-cancelled",
		EventsBuf: 8,
	})
	s.Term = term
	p.sessions[s.SessionID] = s
	defer s.CancelFn()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	if err := p.Cancel(ctx, s.SessionID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("Cancel with cancelled ctx took %v, want <500ms", elapsed)
	}

	var foundForced bool
drain:
	for {
		select {
		case ev, ok := <-s.Events:
			if !ok {
				break drain
			}
			if ev.Event == "session.end" && ev.Fields["stop_reason"] == "cancelled_forced" {
				foundForced = true
			}
		default:
			break drain
		}
	}
	if !foundForced {
		t.Fatal("no session.end with stop_reason=cancelled_forced emitted")
	}
}

func TestCancelSendKeysErrorStillForcedKills(t *testing.T) {
	t.Parallel()
	p := &Provider{
		sessions:    make(map[string]*claudecore.Session),
		cancelGrace: 200 * time.Millisecond,
	}
	term := terminal.NewFakeSession("test-term", 1, "✻ Churning…")
	term.SetSendErr(fmt.Errorf("terminal dead"))
	s := claudecore.NewSession(context.Background(), claudecore.SessionOptions{
		SessionID: "ses-sendkeys-err",
		EventsBuf: 8,
	})
	s.Term = term
	p.sessions[s.SessionID] = s
	defer s.CancelFn()

	if err := p.Cancel(context.Background(), s.SessionID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	var foundForced bool
drain:
	for {
		select {
		case ev, ok := <-s.Events:
			if !ok {
				break drain
			}
			if ev.Event == "session.end" && ev.Fields["stop_reason"] == "cancelled_forced" {
				foundForced = true
			}
		default:
			break drain
		}
	}
	if !foundForced {
		t.Fatal("no session.end with stop_reason=cancelled_forced emitted")
	}
}

func TestCancelSkipsEmitIfAlreadyFinished(t *testing.T) {
	p := &Provider{
		sessions:    make(map[string]*claudecore.Session),
		cancelGrace: 200 * time.Millisecond,
	}
	// Idle immediately — would trigger the success path, but Finished is pre-set.
	term := terminal.NewFakeSession("test-term", 1, "❯ ready")
	s := claudecore.NewSession(context.Background(), claudecore.SessionOptions{
		SessionID: "ses-already-done",
		EventsBuf: 8,
	})
	s.Term = term
	s.Mu.Lock()
	s.Finished = true
	s.Mu.Unlock()
	p.sessions[s.SessionID] = s
	defer s.CancelFn()

	if err := p.Cancel(context.Background(), s.SessionID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	// Events channel should be empty — no session.end emitted.
	select {
	case ev := <-s.Events:
		t.Fatalf("unexpected event emitted: %+v", ev)
	default:
	}
}

func TestAnswerPermissionTerminalRoute(t *testing.T) {
	p := &Provider{sessions: make(map[string]*claudecore.Session)}

	terminalPerm := &claudecore.TerminalPermission{
		RequestID: "term-req-1",
		Prompt:    "Do you want to proceed?",
		Options:   []claudecore.PermissionOption{{ID: "1", Label: "Yes"}, {ID: "2", Label: "No"}},
	}

	s := &claudecore.Session{
		SessionID:           "ses-perm",
		PendingTerminalPerm: terminalPerm,
		Term:                terminal.NewFakeSession("test-term", 1, "ready"),
	}
	p.sessions["ses-perm"] = s

	err := p.AnswerPermission(context.Background(), "ses-perm", "term-req-1", runtime.PermissionResponse{Allow: true, OptionID: "1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	s.Mu.Lock()
	cleared := s.PendingTerminalPerm
	s.Mu.Unlock()
	if cleared != nil {
		t.Fatal("PendingTerminalPerm should be cleared after AnswerPermission")
	}

	sends := s.Term.(*terminal.FakeSession).SendCalls()
	if len(sends) != 1 || len(sends[0]) != 2 || sends[0][0] != "1" || sends[0][1] != string(terminal.KeyEnter) {
		t.Fatalf("keys sent = %v, want [[1 Enter]]", sends)
	}
}

func TestAnswerPermissionRejectsUnknownRequest(t *testing.T) {
	p := &Provider{sessions: make(map[string]*claudecore.Session)}

	s := &claudecore.Session{
		SessionID: "ses-noperm",
		Term:      terminal.NewFakeSession("test-term", 1, "ready"),
	}
	p.sessions["ses-noperm"] = s

	err := p.AnswerPermission(context.Background(), "ses-noperm", "unknown-req-id", runtime.PermissionResponse{Allow: true})
	if err == nil {
		t.Fatal("expected error for unknown request_id with no broker fallback")
	}
	if !strings.Contains(err.Error(), "no pending permission request") {
		t.Fatalf("error = %q, want 'no pending permission request'", err)
	}
}

func TestSessionGoneEmitsEndEvent(t *testing.T) {
	p := &Provider{
		sessions: make(map[string]*claudecore.Session),
		launcher: terminal.PTYLauncher{},
	}
	s := claudecore.NewSession(context.Background(), claudecore.SessionOptions{
		SessionID: "ses-gone",
		EventsBuf: 8,
	})
	// Prompted=false keeps the pane/transcript tickers as no-ops so the only
	// event we can observe is the session.end from the sessionGone branch.
	s.Term = terminal.NewFakeSession("test-term", 1, "ready")
	s.Term.(*terminal.FakeSession).SetAlive(false)
	p.sessions[s.SessionID] = s

	go p.runSession(s.Ctx, s)

	select {
	case ev := <-s.Events:
		if ev.Event != "session.end" {
			t.Fatalf("event = %q, want session.end", ev.Event)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for session.end event")
	}

	select {
	case <-s.Done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for runSession to exit")
	}
}
