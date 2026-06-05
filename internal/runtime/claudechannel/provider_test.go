package claudechannel

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
	"github.com/sdougbrown/avenor/internal/runtime/claudechannel/terminal"
)

func TestNewWithOptions(t *testing.T) {
	p := NewWithOptions(runtime.StartOptions{
		Agent: "jockey",
		Model: "claude-sonnet-4",
	})
	if p == nil {
		t.Fatal("NewWithOptions returned nil provider")
	}
}

func TestDefaultLauncherIsTmux(t *testing.T) {
	// Unset env var.
	t.Setenv("AVENOR_CLAUDE_CHANNEL_TERMINAL", "")
	l := defaultLauncher()
	if _, ok := l.(terminal.TmuxLauncher); !ok {
		t.Fatalf("expected TmuxLauncher, got %T", l)
	}
}

func TestPTYEnvSelectsPTY(t *testing.T) {
	t.Setenv("AVENOR_CLAUDE_CHANNEL_TERMINAL", "pty")
	l := defaultLauncher()
	if _, ok := l.(terminal.PTYLauncher); !ok {
		t.Fatalf("expected PTYLauncher, got %T", l)
	}
}

func TestInvalidValueDefaultsToTmux(t *testing.T) {
	t.Setenv("AVENOR_CLAUDE_CHANNEL_TERMINAL", "bogus")
	l := defaultLauncher()
	if _, ok := l.(terminal.TmuxLauncher); !ok {
		t.Fatalf("expected TmuxLauncher for invalid value, got %T", l)
	}
}

func TestCapabilities(t *testing.T) {
	t.Setenv("AVENOR_CLAUDE_CHANNEL_TERMINAL", "tmux")
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
}

func TestCapabilitiesPTYResumeFalse(t *testing.T) {
	t.Setenv("AVENOR_CLAUDE_CHANNEL_TERMINAL", "pty")
	p := New()
	caps, err := p.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	if caps.Resume {
		t.Error("Resume should be false for pty launcher")
	}
}

func TestResumeRequiresSupportingLauncher(t *testing.T) {
	p := &Provider{
		sessions: make(map[string]*session),
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
		sessions: make(map[string]*session),
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
		sessions: make(map[string]*session),
		launcher: terminal.TmuxLauncher{},
	}
	term := terminal.NewFakeSession("avenor-claude-ses-live", 4242, "ready")
	p.sessions["ses-live"] = &session{
		sessionID: "ses-live",
		dir:       "/tmp/work",
		term:      term,
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
		sessions: make(map[string]*session),
		launcher: terminal.TmuxLauncher{},
	}
	term := terminal.NewFakeSession("avenor-claude-ses-dead", 1, "")
	term.SetAlive(false)
	p.sessions["ses-dead"] = &session{
		sessionID: "ses-dead",
		dir:       "/tmp/work",
		term:      term,
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

func TestBrokerEventMapping(t *testing.T) {
	cases := []struct {
		state     string
		wantEv    string
		wantPhase string
	}{
		{"thinking", "agent.status", "thinking"},
		{"working", "agent.status", "working"},
		{"checkpoint", "agent.message_chunk", ""},
		{"blocked", "agent.status", "waiting"},
		{"permission_requested", "permission.request", ""},
		{"started", "agent.status", "started"},
		{"unknown", "agent.status", "unknown"},
	}

	for _, c := range cases {
		payload := []byte(`{"summary":"ok"}`)
		ev := brokerEvent("ses_1", c.state, payload)
		if ev.Event != c.wantEv {
			t.Errorf("brokerEvent(%q) event = %q, want %q", c.state, ev.Event, c.wantEv)
		}
		if c.wantPhase != "" && ev.Fields["phase"] != c.wantPhase {
			t.Errorf("brokerEvent(%q) phase = %v, want %v", c.state, ev.Fields["phase"], c.wantPhase)
		}
		if ev.SessionID != "ses_1" {
			t.Errorf("brokerEvent(%q) sessionID = %q, want ses_1", c.state, ev.SessionID)
		}
	}
}

func TestShortRunID(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"1234567890", "12345678"},
		{"abc", "abc00000"},
		{"", "00000000"},
	}
	for _, tc := range cases {
		if got := shortRunID(tc.input); got != tc.want {
			t.Fatalf("shortRunID(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestMapFinishStatus(t *testing.T) {
	cases := []struct {
		input, want string
	}{
		{"done", "end_turn"},
		{"failed", "error"},
		{"blocked", "cancelled"},
		{"unknown", "end_turn"},
	}
	for _, c := range cases {
		got := mapFinishStatus(c.input)
		if got != c.want {
			t.Errorf("mapFinishStatus(%q) = %q, want %q", c.input, got, c.want)
		}
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
}

func TestPromptNotFound(t *testing.T) {
	p := New()
	err := p.Prompt(context.Background(), "no-such-session", "hello")
	if err == nil {
		t.Fatal("expected error for nonexistent session")
	}
}

func TestCancelNotFound(t *testing.T) {
	p := New()
	err := p.Cancel(context.Background(), "no-such-session")
	if err == nil {
		t.Fatal("expected error for nonexistent session")
	}
}

func TestPromptQueuesChannelEvent(t *testing.T) {
	runID := "run-prompt"
	b := broker.New("")
	if _, err := b.CreateRun(runID); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	p := &Provider{broker: b, sessions: make(map[string]*session)}
	s := &session{
		sessionID: "session-prompt",
		runID:     runID,
		tmuxName:  "missing",
		ctx:       context.Background(),
		events:    make(chan events.Event, 8),
		term: func() terminal.Session {
			term := terminal.NewFakeSession("missing", 0, "")
			term.SetAlive(false)
			return term
		}(),
	}
	p.sessions[s.sessionID] = s

	if err := p.Prompt(context.Background(), s.sessionID, "hello from test"); err != nil {
		t.Fatalf("Prompt: %v", err)
	}

	select {
	case ev := <-s.events:
		if ev.Event != "agent.prompt_queued" {
			t.Fatalf("event = %q, want agent.prompt_queued", ev.Event)
		}
		if ev.Fields["delivery"] != "channel" {
			t.Fatalf("delivery = %v, want channel", ev.Fields["delivery"])
		}
		if ev.Fields["message_type"] != "continue" {
			t.Fatalf("message_type = %v, want continue", ev.Fields["message_type"])
		}
	default:
		t.Fatal("expected agent.prompt_queued event")
	}
}

func TestPollBrokerEventsSkipsChannelReadyBeforeSidecarRegister(t *testing.T) {
	runID := "run-not-ready"
	b := broker.New("")
	if _, err := b.CreateRun(runID); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	p := &Provider{broker: b}
	s := &session{sessionID: "session-not-ready", runID: runID, mcpServer: "avenor-channel-test", events: make(chan events.Event, 4)}

	p.pollBrokerEvents(s)

	select {
	case ev := <-s.events:
		t.Fatalf("expected no events before sidecar register, got %q", ev.Event)
	default:
	}

	s.mu.Lock()
	readyEmitted := s.channelReadyEmitted
	s.mu.Unlock()
	if readyEmitted {
		t.Fatal("channelReadyEmitted should remain false before sidecar register")
	}
}

func TestPollBrokerEventsEmitsChannelLifecycle(t *testing.T) {
	runID := "run-lifecycle"
	b := broker.New("")
	if _, err := b.CreateRun(runID); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	st := b.GetRun(runID)
	st.Lock()
	st.RegisteredAt = st.LastSeen
	st.Reports = append(st.Reports, broker.Report{RunID: runID, State: "checkpoint", Payload: []byte(`{"content":{"text":"progress"}}`)})
	st.Replies = append(st.Replies, broker.Reply{RunID: runID, To: "controller", Payload: []byte(`{"summary":"done"}`)})
	st.Finishes = append(st.Finishes, broker.Finish{RunID: runID, Status: "done", Summary: "ok", FilesChanged: []string{"docs/events.md"}, Payload: []byte(`{"result":"ok"}`)})
	st.Unlock()

	p := &Provider{broker: b}
	s := &session{sessionID: "session-lifecycle", runID: runID, mcpServer: "avenor-channel-test", events: make(chan events.Event, 8)}

	p.pollBrokerEvents(s)

	got := make([]string, 0, 6)
	for i := 0; i < 6; i++ {
		select {
		case ev := <-s.events:
			got = append(got, ev.Event)
		default:
			t.Fatalf("expected 6 events, got %d", len(got))
		}
	}
	want := []string{"agent.channel_ready", "agent.report", "agent.message_chunk", "agent.finish", "session.end", "agent.reply"}
	for i, ev := range got {
		if ev != want[i] {
			t.Fatalf("event[%d] = %q, want %q", i, ev, want[i])
		}
	}

	s.mu.Lock()
	readyEmitted := s.channelReadyEmitted
	finished := s.finished
	s.mu.Unlock()
	if !readyEmitted {
		t.Fatal("expected channelReadyEmitted to be set")
	}
	if !finished {
		t.Fatal("expected session to be marked finished")
	}

	p.pollBrokerEvents(s)
	select {
	case ev := <-s.events:
		t.Fatalf("expected no duplicate events after drain, got %q", ev.Event)
	default:
	}
}

func TestPollBrokerEventsMarksFinishedBeforeSendingEnd(t *testing.T) {
	runID := "run-1"
	b := broker.New("")
	if _, err := b.CreateRun(runID); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	st := b.GetRun(runID)
	st.Lock()
	st.Finishes = append(st.Finishes, broker.Finish{Status: "done", Summary: "ok", FilesChanged: []string{}})
	st.Unlock()

	p := &Provider{broker: b}
	s := &session{sessionID: "session-1", runID: runID, events: make(chan events.Event, 2)}

	p.pollBrokerEvents(s)

	s.mu.Lock()
	finished := s.finished
	s.mu.Unlock()
	if !finished {
		t.Fatal("session should be marked finished before session.end is consumed")
	}
	for _, want := range []string{"agent.finish", "session.end"} {
		select {
		case ev := <-s.events:
			if ev.Event != want {
				t.Fatalf("event = %q, want %q", ev.Event, want)
			}
		default:
			t.Fatalf("expected %s event", want)
		}
	}
}

func TestWaitForPaneReadyStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	term := terminal.NewFakeSession("missing", 0, "")
	if waitForPaneReady(ctx, term) {
		t.Fatal("waitForPaneReady should stop when context is canceled")
	}
}

func TestClassifyPane(t *testing.T) {
	cases := []struct {
		name string
		pane string
		want paneState
	}{
		{
			name: "edit permission",
			pane: "Do you want to make this edit to README.md?\n ❯ 1. Yes\n   2. Yes, allow all edits during this session",
			want: paneStatePermission,
		},
		{
			name: "proceed permission",
			pane: "Do you want to proceed?\n ❯ 1. Yes\n   2. No",
			want: paneStatePermission,
		},
		{
			name: "ruminating activity",
			pane: "✻ Ruminating… (13s · ↓ 557 tokens · thinking)",
			want: paneStateActive,
		},
		{
			name: "generic ing activity",
			pane: "✢ Churning… (2m 17s · ↓ 7.2k tokens)",
			want: paneStateActive,
		},
		{
			name: "token activity fallback",
			pane: "✻ Working · ↓ 557 tokens",
			want: paneStateActive,
		},
		{
			name: "idle prompt",
			pane: "────────────────\n❯ \n────────────────",
			want: paneStateIdle,
		},
		{
			name: "unknown noise",
			pane: "\n────────────────\nClaude Code v2.1.161\n────────────────\n",
			want: paneStateUnknown,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyPane(tc.pane); got != tc.want {
				t.Fatalf("classifyPane() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestScanTerminalTickActiveEmitsNothing pins the Stage-2 behavior: an
// "active" pane state must NOT emit agent.status working — that's the
// transcript tick's job. The pane scan only contributes idle/permission.
func TestScanTerminalTickActiveEmitsNothing(t *testing.T) {
	p := &Provider{
		sessions: make(map[string]*session),
		launcher: terminal.TmuxLauncher{},
	}
	// "Churning" classifies as paneStateActive.
	term := terminal.NewFakeSession("test-term", 1, "✢ Churning… (2m 17s · ↓ 7.2k tokens)")
	s := &session{
		sessionID: "ses-active",
		term:      term,
		events:    make(chan events.Event, 4),
		prompted:  true,
		ctx:       context.Background(),
	}
	p.sessions[s.sessionID] = s

	p.scanTerminalTick(s)

	select {
	case ev := <-s.events:
		t.Fatalf("scanTerminalTick on active pane should not emit; got %+v", ev)
	default:
	}
	s.mu.Lock()
	active := s.active
	s.mu.Unlock()
	if !active {
		t.Fatal("scanTerminalTick on active pane should still markActive")
	}
}

// TestScanTranscriptTickIsAuthoritativeWorkingSignal verifies that, post-
// Stage-2, agent.status working events come exclusively from the transcript
// scanner when records arrive.
func TestScanTranscriptTickIsAuthoritativeWorkingSignal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	if err := os.WriteFile(path, []byte(`{"type":"assistant","timestamp":"t1"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	p := &Provider{
		sessions: make(map[string]*session),
		launcher: terminal.TmuxLauncher{},
	}
	s := &session{
		sessionID:  "ses-trans",
		term:       terminal.NewFakeSession("test-term", 1, "ready"),
		transcript: newTranscriptReader(path),
		events:     make(chan events.Event, 4),
		prompted:   true,
		ctx:        context.Background(),
	}
	p.sessions[s.sessionID] = s

	p.scanTranscriptTick(s)

	select {
	case ev := <-s.events:
		if ev.Event != "agent.status" {
			t.Fatalf("event = %q, want agent.status", ev.Event)
		}
		if ev.Fields["phase"] != "working" {
			t.Fatalf("phase = %v, want working", ev.Fields["phase"])
		}
		if ev.Fields["source"] != "transcript" {
			t.Fatalf("source = %v, want transcript", ev.Fields["source"])
		}
		if ev.Fields["records"] != 1 {
			t.Fatalf("records = %v, want 1", ev.Fields["records"])
		}
	default:
		t.Fatal("expected agent.status working from scanTranscriptTick")
	}
}

// Idle pane must emit waiting with source = backend Kind() (tmux or pty),
// not a hardcoded string. Covers both backends so a regression that
// reverts to a constant is caught.
func TestScanTerminalTickIdleEmitsWaitingWithKind(t *testing.T) {
	cases := []struct {
		name string
		kind string
	}{
		{"tmux", "tmux"},
		{"pty", "pty"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := &Provider{
				sessions: make(map[string]*session),
				launcher: terminal.TmuxLauncher{},
			}
			term := terminal.NewFakeSession("test-term", 1, "❯ ready for input")
			term.SetKind(tc.kind)
			s := &session{
				sessionID: "ses-idle-" + tc.kind,
				term:      term,
				events:    make(chan events.Event, 4),
				prompted:  true,
				ctx:       context.Background(),
			}
			p.sessions[s.sessionID] = s

			p.scanTerminalTick(s)

			select {
			case ev := <-s.events:
				if ev.Event != "agent.status" {
					t.Fatalf("event = %q, want agent.status", ev.Event)
				}
				if ev.Fields["phase"] != "waiting" {
					t.Fatalf("phase = %v, want waiting", ev.Fields["phase"])
				}
				if ev.Fields["source"] != tc.kind {
					t.Fatalf("source = %v, want %q", ev.Fields["source"], tc.kind)
				}
			default:
				t.Fatalf("expected agent.status waiting; got nothing")
			}
		})
	}
}

// scanTranscriptTick and scanTerminalTick both short-circuit when the
// session is finished or has never been prompted. These guards prevent
// stale or premature status emissions; regressions would silently re-emit.
func TestScanTicksRespectFinishedAndPromptedGuards(t *testing.T) {
	writeTranscript := func(t *testing.T) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "session.jsonl")
		if err := os.WriteFile(path, []byte(`{"type":"assistant","timestamp":"t1"}`+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		return path
	}
	newSession := func(t *testing.T) *session {
		t.Helper()
		return &session{
			sessionID:  "ses-guard",
			term:       terminal.NewFakeSession("test-term", 1, "❯ ready for input"),
			transcript: newTranscriptReader(writeTranscript(t)),
			events:     make(chan events.Event, 4),
			ctx:        context.Background(),
		}
	}
	assertNoEmit := func(t *testing.T, s *session) {
		t.Helper()
		select {
		case ev := <-s.events:
			t.Fatalf("expected no emit; got %+v", ev)
		default:
		}
	}

	t.Run("transcript skip when finished", func(t *testing.T) {
		p := &Provider{sessions: make(map[string]*session), launcher: terminal.TmuxLauncher{}}
		s := newSession(t)
		s.prompted = true
		s.finished = true
		p.sessions[s.sessionID] = s
		p.scanTranscriptTick(s)
		assertNoEmit(t, s)
	})

	t.Run("transcript skip when not prompted", func(t *testing.T) {
		p := &Provider{sessions: make(map[string]*session), launcher: terminal.TmuxLauncher{}}
		s := newSession(t)
		s.prompted = false
		p.sessions[s.sessionID] = s
		p.scanTranscriptTick(s)
		assertNoEmit(t, s)
	})

	t.Run("terminal skip when finished", func(t *testing.T) {
		p := &Provider{sessions: make(map[string]*session), launcher: terminal.TmuxLauncher{}}
		s := newSession(t)
		s.prompted = true
		s.finished = true
		p.sessions[s.sessionID] = s
		p.scanTerminalTick(s)
		assertNoEmit(t, s)
	})

	t.Run("terminal skip when not prompted", func(t *testing.T) {
		p := &Provider{sessions: make(map[string]*session), launcher: terminal.TmuxLauncher{}}
		s := newSession(t)
		s.prompted = false
		p.sessions[s.sessionID] = s
		p.scanTerminalTick(s)
		assertNoEmit(t, s)
	})
}

func TestMarkActive(t *testing.T) {
	s := &session{}
	markActive(s)

	s.mu.Lock()
	active := s.active
	s.mu.Unlock()
	if !active {
		t.Fatal("session should be marked active")
	}
}

func TestParseTerminalPermission(t *testing.T) {
	cases := []struct {
		name        string
		text        string
		wantPrompt  string
		wantOpts    int
		wantFirstID string
	}{
		{
			name:        "edit permission dialog",
			text:        "Do you want to make this edit to README.md?\n ❯ 1. Yes\n   2. Yes, allow all edits during this session\n   3. No\n\n Esc to cancel · Tab to amend",
			wantPrompt:  "Do you want to make this edit to README.md?",
			wantOpts:    3,
			wantFirstID: "1",
		},
		{
			name:        "proceed permission dialog",
			text:        "Do you want to proceed?\n ❯ 1. Yes\n   2. No\n\n Esc to cancel · Tab to amend",
			wantPrompt:  "Do you want to proceed?",
			wantOpts:    2,
			wantFirstID: "1",
		},
		{
			name:        "no permission prompt",
			text:        "────────────────\n❯ \n────────────────",
			wantPrompt:  "",
			wantOpts:    0,
			wantFirstID: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseTerminalPermission(tc.text)
			if tc.wantOpts == 0 {
				if got != nil {
					t.Fatalf("parseTerminalPermission should return nil, got %+v", got)
				}
				return
			}
			if got == nil {
				t.Fatal("parseTerminalPermission returned nil")
			}
			if got.prompt != tc.wantPrompt {
				t.Fatalf("prompt = %q, want %q", got.prompt, tc.wantPrompt)
			}
			if len(got.options) != tc.wantOpts {
				t.Fatalf("options count = %d, want %d", len(got.options), tc.wantOpts)
			}
			if got.options[0].ID != tc.wantFirstID {
				t.Fatalf("first option id = %q, want %q", got.options[0].ID, tc.wantFirstID)
			}
		})
	}
}

func TestAnswerPermissionTerminalRoute(t *testing.T) {
	p := &Provider{sessions: make(map[string]*session)}

	terminalPerm := &terminalPermission{
		requestID: "term-req-1",
		prompt:    "Do you want to proceed?",
		options:   []permissionOption{{ID: "1", Label: "Yes"}, {ID: "2", Label: "No"}},
	}

	s := &session{
		sessionID:           "ses-tmux",
		runID:               "run-tmux",
		pendingTerminalPerm: terminalPerm,
		events:              make(chan events.Event, 64),
		term:                terminal.NewFakeSession("test-term", 1, "ready"),
	}

	p.sessions["ses-tmux"] = s

	err := p.AnswerPermission(context.Background(), "ses-tmux", "term-req-1", runtime.PermissionResponse{Allow: true, OptionID: "1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	s.mu.Lock()
	cleared := s.pendingTerminalPerm
	s.mu.Unlock()
	if cleared != nil {
		t.Fatal("pendingTerminalPerm should be cleared after AnswerPermission")
	}

	sends := s.term.(*terminal.FakeSession).SendCalls()
	if len(sends) != 1 || len(sends[0]) != 2 || sends[0][0] != "1" || sends[0][1] != string(terminal.KeyEnter) {
		t.Fatalf("keys sent = %v, want [[1 Enter]]", sends)
	}
}

func TestAnswerPermissionFallthroughToBroker(t *testing.T) {
	b := broker.New("")
	if _, err := b.CreateRun("run-broker"); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	p := &Provider{sessions: make(map[string]*session), broker: b}
	s := &session{
		sessionID: "ses-broker",
		runID:     "run-broker",
		events:    make(chan events.Event, 64),
	}
	p.sessions["ses-broker"] = s

	// Answer with a request ID not matching any tmux permission.
	err := p.AnswerPermission(context.Background(), "ses-broker", "sidecar-req-1", runtime.PermissionResponse{Allow: true})
	if err != nil {
		t.Fatalf("broker AnswerPermission: %v", err)
	}
}

func TestValidTmuxKey(t *testing.T) {
	cases := []struct {
		key  string
		want bool
	}{
		{"1", true},
		{"9", true},
		{"Esc", true},
		{"a", true},
		{"Z", true},
		{"C-c", false},
		{"1\nrm -rf /", false},
		{"\n", false},
		{" 1", false},
	}
	for _, tc := range cases {
		if got := validTmuxKey(tc.key); got != tc.want {
			t.Errorf("validTmuxKey(%q) = %v, want %v", tc.key, got, tc.want)
		}
	}
}

func TestEmitNonBlockingFullChannel(t *testing.T) {
	s := &session{events: make(chan events.Event, 1)}
	s.events <- events.Event{Event: "test"}
	// Channel is full; emitNonBlocking should not block.
	emitNonBlocking(s, events.Event{Event: "dropped"})
	if len(s.events) != 1 {
		t.Fatalf("channel should still have 1 event, got %d", len(s.events))
	}
}

func TestProjectMCPConfigRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".mcp.json")

	if err := upsertProjectMCPServer(path, "avenor-channel-a", map[string]any{"command": "avenor", "args": []string{"claude-channel"}}); err != nil {
		t.Fatalf("upsert first server: %v", err)
	}
	if err := upsertProjectMCPServer(path, "avenor-channel-b", map[string]any{"command": "avenor", "args": []string{"claude-channel", "--run-id", "b"}}); err != nil {
		t.Fatalf("upsert second server: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	var cfg map[string]any
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("decode config: %v", err)
	}
	servers, ok := cfg["mcpServers"].(map[string]any)
	if !ok {
		t.Fatalf("mcpServers has type %T, want map[string]any", cfg["mcpServers"])
	}
	if len(servers) != 2 {
		t.Fatalf("mcpServers len = %d, want 2", len(servers))
	}
	if _, ok := servers["avenor-channel-a"]; !ok {
		t.Fatal("expected avenor-channel-a to be present in config")
	}
	if _, ok := servers["avenor-channel-b"]; !ok {
		t.Fatal("expected avenor-channel-b to be present in config")
	}

	if err := removeProjectMCPServer(path, "avenor-channel-a"); err != nil {
		t.Fatalf("remove first server: %v", err)
	}
	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config after first remove: %v", err)
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("decode config after first remove: %v", err)
	}
	servers, ok = cfg["mcpServers"].(map[string]any)
	if !ok {
		t.Fatalf("mcpServers after first remove has type %T, want map[string]any", cfg["mcpServers"])
	}
	if len(servers) != 1 {
		t.Fatalf("mcpServers len after first remove = %d, want 1", len(servers))
	}
	if _, ok := servers["avenor-channel-b"]; !ok {
		t.Fatal("expected avenor-channel-b to remain in config")
	}

	if err := removeProjectMCPServer(path, "avenor-channel-b"); err != nil {
		t.Fatalf("remove second server: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected config file to be removed, stat err = %v", err)
	}
}

func TestProjectMCPConfigPreservesUnrelatedServers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".mcp.json")
	seed := map[string]any{
		"mcpServers": map[string]any{
			"context7": map[string]any{"type": "http", "url": "https://mcp.context7.com/mcp"},
		},
	}
	data, err := json.MarshalIndent(seed, "", "  ")
	if err != nil {
		t.Fatalf("marshal seed config: %v", err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		t.Fatalf("write seed config: %v", err)
	}

	if err := upsertProjectMCPServer(path, "avenor-channel-a", map[string]any{"command": "avenor"}); err != nil {
		t.Fatalf("upsert avenor server: %v", err)
	}
	if err := removeProjectMCPServer(path, "avenor-channel-a"); err != nil {
		t.Fatalf("remove avenor server: %v", err)
	}

	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatalf("read preserved config: %v", err)
	}
	var cfg map[string]any
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("decode preserved config: %v", err)
	}
	servers, ok := cfg["mcpServers"].(map[string]any)
	if !ok {
		t.Fatalf("mcpServers has type %T, want map[string]any", cfg["mcpServers"])
	}
	if len(servers) != 1 {
		t.Fatalf("mcpServers len = %d, want 1", len(servers))
	}
	if _, ok := servers["context7"]; !ok {
		t.Fatal("expected context7 to remain in config")
	}
}

func TestCleanupProjectMCPRemovesOnlyAvenorChannelEntries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".mcp.json")
	seed := map[string]any{
		"mcpServers": map[string]any{
			"avenor-channel-old": map[string]any{"command": "avenor", "args": []string{"claude-channel", "--run-id", "old"}},
			"context7":           map[string]any{"type": "http", "url": "https://mcp.context7.com/mcp"},
		},
	}
	data, err := json.MarshalIndent(seed, "", "  ")
	if err != nil {
		t.Fatalf("marshal seed config: %v", err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		t.Fatalf("write seed config: %v", err)
	}

	removed, err := CleanupProjectMCP(path)
	if err != nil {
		t.Fatalf("cleanup project mcp: %v", err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}

	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatalf("read cleaned config: %v", err)
	}
	var cfg map[string]any
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("decode cleaned config: %v", err)
	}
	servers, ok := cfg["mcpServers"].(map[string]any)
	if !ok {
		t.Fatalf("mcpServers has type %T, want map[string]any", cfg["mcpServers"])
	}
	if _, ok := servers["avenor-channel-old"]; ok {
		t.Fatal("expected avenor-channel-old entry to be removed")
	}
	if _, ok := servers["context7"]; !ok {
		t.Fatal("expected context7 entry to remain")
	}
}

// ---- Stage 3: Adapter-backed provider tests ----

func TestPromptWaitsForStartupClearBeforePaste(t *testing.T) {
	// Test that waitForPaneReady returns true when capture doesn't contain needle.
	term := terminal.NewFakeSession("test-term", 1, "ready")
	if !waitForPaneReady(context.Background(), term) {
		t.Fatal("waitForPaneReady should return true when capture doesn't contain needle")
	}

	// Test that waitForPaneReady waits when capture contains needle, then returns true.
	term2 := terminal.NewFakeSessionQueue("test-term2", 2, []string{
		"Loading development channels\n",
		"ready\n",
	})
	if !waitForPaneReady(context.Background(), term2) {
		t.Fatal("waitForPaneReady should return true after needle disappears")
	}
}

func TestPromptQueuesBrokerControlEvenWhenTerminalInjectionAsync(t *testing.T) {
	runID := "run-queue"
	b := broker.New("")
	if _, err := b.CreateRun(runID); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	p := &Provider{
		broker:   b,
		sessions: make(map[string]*session),
		launcher: terminal.TmuxLauncher{},
	}
	term := terminal.NewFakeSession("test-term", 1, "ready")
	sessCtx, sessCancel := context.WithCancel(context.Background())
	t.Cleanup(sessCancel)
	s := &session{
		sessionID: "ses-queue",
		runID:     runID,
		ctx:       sessCtx,
		term:      term,
		events:    make(chan events.Event, 8),
	}
	p.sessions[s.sessionID] = s

	if err := p.Prompt(context.Background(), s.sessionID, "hello"); err != nil {
		t.Fatalf("Prompt: %v", err)
	}

	// The broker control message should be pushed synchronously.
	select {
	case ev := <-s.events:
		if ev.Event != "agent.prompt_queued" {
			t.Fatalf("event = %q, want agent.prompt_queued", ev.Event)
		}
		if ev.Fields["delivery"] != "channel" {
			t.Fatalf("delivery = %v, want channel", ev.Fields["delivery"])
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected agent.prompt_queued event")
	}
}

func TestDevelopmentChannelPromptsSendRightKeys(t *testing.T) {
	cases := []struct {
		name    string
		search  string
		wantSeq []string
	}{
		{"mcp found", "New MCP server found in this project", []string{"1", string(terminal.KeyEnter)}},
		{"loading channels", "Loading development channels", []string{string(terminal.KeyEnter)}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runID := "run-" + tc.name
			b := broker.New("")
			if _, err := b.CreateRun(runID); err != nil {
				t.Fatalf("CreateRun: %v", err)
			}
			p := &Provider{
				broker:   b,
				sessions: make(map[string]*session),
				launcher: terminal.TmuxLauncher{},
			}
			term := terminal.NewFakeSessionQueue("test-term", 1, []string{
				tc.search + "\n",
				"ready\n",
			})
			sendNotify := make(chan []string, 1)
			term.SetSendNotify(sendNotify)
			s := &session{
				sessionID: "ses-dev",
				runID:     runID,
				ctx:       context.Background(),
				term:      term,
				events:    make(chan events.Event, 8),
				done:      make(chan struct{}),
			}
			p.sessions[s.sessionID] = s

			// Start autoConfirmDevelopmentChannelPrompt directly since we're not
			// calling Start() which would normally start it.
			go autoConfirmDevelopmentChannelPrompt(s.ctx, s)
			defer close(s.done)

			select {
			case send := <-sendNotify:
				if len(send) != len(tc.wantSeq) {
					t.Fatalf("key count = %d, want %d (got %v)", len(send), len(tc.wantSeq), send)
				}
				for i, want := range tc.wantSeq {
					if send[i] != want {
						t.Fatalf("key[%d] = %q, want %q (full sequence: %v)", i, send[i], want, send)
					}
				}
			case <-time.After(2 * time.Second):
				t.Fatal("expected SendKeys to be called by autoConfirmDevelopmentChannelPrompt")
			}
		})
	}
}

func TestPermissionAllowedSendsDigitAndEnter(t *testing.T) {
	runID := "run-allow"
	b := broker.New("")
	if _, err := b.CreateRun(runID); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	p := &Provider{
		broker:   b,
		sessions: make(map[string]*session),
		launcher: terminal.TmuxLauncher{},
	}
	term := terminal.NewFakeSession("test-term", 1, "ready")
	sessCtx, sessCancel := context.WithCancel(context.Background())
	s := &session{
		sessionID: "ses-allow",
		runID:     runID,
		ctx:       sessCtx,
		cancelFn:  sessCancel,
		term:      term,
		pendingTerminalPerm: &terminalPermission{
			requestID: "perm-1",
		},
		events: make(chan events.Event, 8),
		done:   make(chan struct{}),
	}
	p.sessions[s.sessionID] = s

	err := p.AnswerPermission(context.Background(), s.sessionID, "perm-1", runtime.PermissionResponse{Allow: true, OptionID: "1"})
	if err != nil {
		t.Fatalf("AnswerPermission: %v", err)
	}

	sends := term.SendCalls()
	if len(sends) != 1 {
		t.Fatalf("SendKeys call count = %d, want 1 (calls: %v)", len(sends), sends)
	}
	if len(sends[0]) != 2 {
		t.Fatalf("keys sent = %v, want [1 Enter]", sends[0])
	}
	if sends[0][0] != "1" || sends[0][1] != string(terminal.KeyEnter) {
		t.Fatalf("keys sent = %v, want [1 Enter]", sends[0])
	}
}

func TestPermissionDeniedSendsEscAndEnter(t *testing.T) {
	runID := "run-deny"
	b := broker.New("")
	if _, err := b.CreateRun(runID); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	p := &Provider{
		broker:   b,
		sessions: make(map[string]*session),
		launcher: terminal.TmuxLauncher{},
	}
	term := terminal.NewFakeSession("test-term", 1, "ready")
	sessCtx, sessCancel := context.WithCancel(context.Background())
	s := &session{
		sessionID: "ses-deny",
		runID:     runID,
		ctx:       sessCtx,
		cancelFn:  sessCancel,
		term:      term,
		pendingTerminalPerm: &terminalPermission{
			requestID: "perm-2",
		},
		events: make(chan events.Event, 8),
		done:   make(chan struct{}),
	}
	p.sessions[s.sessionID] = s

	err := p.AnswerPermission(context.Background(), s.sessionID, "perm-2", runtime.PermissionResponse{Allow: false})
	if err != nil {
		t.Fatalf("AnswerPermission: %v", err)
	}

	sends := term.SendCalls()
	if len(sends) != 1 {
		t.Fatalf("SendKeys call count = %d, want 1 (calls: %v)", len(sends), sends)
	}
	if len(sends[0]) != 2 {
		t.Fatalf("keys sent = %v, want [Esc Enter]", sends[0])
	}
	if sends[0][0] != string(terminal.KeyEsc) || sends[0][1] != string(terminal.KeyEnter) {
		t.Fatalf("keys sent = %v, want [Esc Enter]", sends[0])
	}
}

func TestSessionGoneEmitsFallbackEnd(t *testing.T) {
	runID := "run-gone"
	b := broker.New("")
	if _, err := b.CreateRun(runID); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	p := &Provider{
		broker:   b,
		sessions: make(map[string]*session),
		launcher: terminal.TmuxLauncher{},
	}
	term := terminal.NewFakeSession("test-term", 1, "ready")
	sessCtx, sessCancel := context.WithCancel(context.Background())
	mcpDir := t.TempDir()
	mcpProject := filepath.Join(t.TempDir(), ".mcp.json")
	s := &session{
		sessionID:  "ses-gone",
		runID:      runID,
		ctx:        sessCtx,
		cancelFn:   sessCancel,
		term:       term,
		mcpDir:     mcpDir,
		mcpProject: mcpProject,
		mcpServer:  "test-server",
		events:     make(chan events.Event, 8),
		done:       make(chan struct{}),
	}
	p.sessions[s.sessionID] = s

	term.SetAlive(false)
	go p.runSession(s.ctx, s)

	// Wait for session.end event.
	select {
	case ev := <-s.events:
		if ev.Event != "session.end" {
			t.Fatalf("event = %q, want session.end", ev.Event)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for session.end event after session went away")
	}

	select {
	case <-s.done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for runSession to exit")
	}
}
