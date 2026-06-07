package claude

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"
)

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
		t.Error("Resume should be true for tmux launcher")
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

func TestCapabilitiesPTYResumeFalse(t *testing.T) {
	t.Setenv("AVENOR_CLAUDE_TERMINAL", "")
	p := New()
	caps, err := p.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	if caps.Resume {
		t.Error("Resume should be false for PTY launcher")
	}
}

func TestResumeRequiresSupportingLauncher(t *testing.T) {
	p := &Provider{
		sessions: make(map[string]*claudecore.Session),
		launcher: terminal.PTYLauncher{},
	}
	_, err := p.Resume(context.Background(), "ses-1")
	if err == nil {
		t.Fatal("expected error from Resume on PTY launcher")
	}
	if !strings.Contains(err.Error(), "resume not supported") {
		t.Fatalf("error = %q, want 'resume not supported'", err)
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

func TestCancelReturnsNilForLiveSession(t *testing.T) {
	p := &Provider{sessions: make(map[string]*claudecore.Session)}
	s := claudecore.NewSession(context.Background(), claudecore.SessionOptions{
		SessionID: "ses-live-cancel",
		EventsBuf: 8,
	})
	s.Term = terminal.NewFakeSession("test-term", 1, "ready")
	p.sessions[s.SessionID] = s
	defer s.CancelFn()

	if err := p.Cancel(context.Background(), s.SessionID); err != nil {
		t.Fatalf("Cancel: %v", err)
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
