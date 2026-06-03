package claudechannel

import (
	"context"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/claudechannel/broker"
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

func TestCapabilities(t *testing.T) {
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
	if caps.Resume {
		t.Error("Resume should be false")
	}
	if !caps.SubprocessDiscovery {
		t.Error("SubprocessDiscovery should be true")
	}
}

func TestResumeFails(t *testing.T) {
	p := New()
	_, err := p.Resume(context.Background(), "ses_1")
	if err == nil {
		t.Fatal("expected error for resume")
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
	s := &session{sessionID: "session-1", runID: runID, events: make(chan events.Event, 1)}

	p.pollBrokerEvents(s)

	s.mu.Lock()
	finished := s.finished
	s.mu.Unlock()
	if !finished {
		t.Fatal("session should be marked finished before session.end is consumed")
	}
	select {
	case ev := <-s.events:
		if ev.Event != "session.end" {
			t.Fatalf("event = %q, want session.end", ev.Event)
		}
	default:
		t.Fatal("expected session.end event")
	}
}

func TestWaitForPaneReadyStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if waitForPaneReady(ctx, "missing-session") {
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

func TestParseTmuxPermission(t *testing.T) {
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
			got := parseTmuxPermission(tc.text)
			if tc.wantOpts == 0 {
				if got != nil {
					t.Fatalf("parseTmuxPermission should return nil, got %+v", got)
				}
				return
			}
			if got == nil {
				t.Fatal("parseTmuxPermission returned nil")
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

func TestAnswerPermissionTmuxRoute(t *testing.T) {
	p := &Provider{sessions: make(map[string]*session)}

	tmuxPerm := &tmuxPermission{
		requestID: "tmux-req-1",
		prompt:    "Do you want to proceed?",
		options:   []permissionOption{{ID: "1", Label: "Yes"}, {ID: "2", Label: "No"}},
		tmuxName:  "avenor-test",
	}

	s := &session{
		sessionID:       "ses-tmux",
		runID:           "run-tmux",
		pendingTmuxPerm: tmuxPerm,
		events:          make(chan events.Event, 64),
	}

	p.sessions["ses-tmux"] = s

	// Answering a tmux permission with allow (sends keys via tmux, which won't
	// exist in tests — we check that it clears the pending state and doesn't
	// return a broker routing error).
	err := p.AnswerPermission(context.Background(), "ses-tmux", "tmux-req-1", runtime.PermissionResponse{Allow: true, OptionID: "1"})
	// Expect tmux error since no tmux session exists, but permission state cleared.
	if err == nil {
		t.Fatal("expected tmux error (no session) but got nil")
	}

	s.mu.Lock()
	cleared := s.pendingTmuxPerm
	s.mu.Unlock()
	if cleared != nil {
		t.Fatal("pendingTmuxPerm should be cleared after AnswerPermission")
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
