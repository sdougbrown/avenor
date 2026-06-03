package claudechannel

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
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
	s := &session{sessionID: "session-prompt", runID: runID, tmuxName: "missing", ctx: context.Background(), events: make(chan events.Event, 8)}
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
