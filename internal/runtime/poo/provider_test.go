package poo

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/backendlaunch"
	"github.com/sdougbrown/avenor/internal/events"
	avruntime "github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/poo/wire/protocol"
)

// helperEnv selects the fake-server mode when the test binary is re-executed.
// The name is not one the launch scrub removes, so the child inherits it.
const helperEnv = "AVENOR_POO_HELPER"

var (
	pooBinaryOnce sync.Once
	pooBinaryPath string
	pooBinaryErr  error
	pooBinaryDir  string
)

func TestMain(m *testing.M) {
	if mode := os.Getenv(helperEnv); mode != "" {
		os.Exit(runHelperServer(mode))
	}
	code := m.Run()
	if pooBinaryDir != "" {
		_ = os.RemoveAll(pooBinaryDir)
	}
	os.Exit(code)
}

// buildPoo builds the pinned poo binary once per test process. The whole
// package shares it: the build is the only expensive step in these tests.
func buildPoo(t *testing.T) string {
	t.Helper()
	pooBinaryOnce.Do(func() {
		pooBinaryDir, pooBinaryErr = os.MkdirTemp("", "avenor-poo-bin")
		if pooBinaryErr != nil {
			return
		}
		pooBinaryPath = filepath.Join(pooBinaryDir, "poo")
		goTool := filepath.Join(runtime.GOROOT(), "bin", "go")
		if _, err := os.Stat(goTool); err != nil {
			goTool = "go"
		}
		cmd := exec.Command(goTool, "build", "-o", pooBinaryPath, "github.com/sdougbrown/poo/cmd/poo")
		output, err := cmd.CombinedOutput()
		if err != nil {
			pooBinaryErr = fmt.Errorf("build poo: %w\n%s", err, output)
		}
	})
	if pooBinaryErr != nil {
		t.Fatalf("%v", pooBinaryErr)
	}
	return pooBinaryPath
}

// scriptedToolCall is one tool call a scripted turn answers with.
type scriptedToolCall struct {
	Name      string
	Arguments map[string]any
}

// scriptedTurn is one chat-completions answer the fake endpoint serves.
type scriptedTurn struct {
	Text      string
	ToolCalls []scriptedToolCall
	Delay     time.Duration
}

// fakeEndpoint is a scripted OpenAI-compatible chat-completions endpoint. It
// serves the turns in order and keeps every request body, so a test can read
// what the model was asked.
type fakeEndpoint struct {
	server *httptest.Server

	mu       sync.Mutex
	turns    []scriptedTurn
	index    int
	requests []map[string]any
}

func newFakeEndpoint(t *testing.T, turns ...scriptedTurn) *fakeEndpoint {
	t.Helper()
	endpoint := &fakeEndpoint{turns: turns}
	endpoint.server = httptest.NewServer(http.HandlerFunc(endpoint.serve))
	t.Cleanup(endpoint.server.Close)
	return endpoint
}

func (e *fakeEndpoint) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var decoded map[string]any
	_ = json.Unmarshal(body, &decoded)

	e.mu.Lock()
	e.requests = append(e.requests, decoded)
	turn := e.nextLocked()
	e.mu.Unlock()

	if turn.Delay > 0 {
		timer := time.NewTimer(turn.Delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
			return
		}
	}
	w.Header().Set("Content-Type", "text/event-stream")
	flusher, _ := w.(http.Flusher)
	write := func(payload string) {
		_, _ = io.WriteString(w, "data: "+payload+"\n\n")
		if flusher != nil {
			flusher.Flush()
		}
	}
	quote := func(text string) string {
		encoded, _ := json.Marshal(text)
		return string(encoded)
	}
	if turn.Text != "" || len(turn.ToolCalls) == 0 {
		write(`{"choices":[{"index":0,"delta":{"role":"assistant","content":` + quote(turn.Text) + `}}]}`)
	}
	for i, call := range turn.ToolCalls {
		arguments, _ := json.Marshal(call.Arguments)
		write(fmt.Sprintf(
			`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":%d,"id":"call_%d","type":"function","function":{"name":%s,"arguments":%s}}]}}]}`,
			i, i+1, quote(call.Name), quote(string(arguments))))
	}
	finish := "stop"
	if len(turn.ToolCalls) > 0 {
		finish = "tool_calls"
	}
	write(`{"choices":[{"index":0,"delta":{},"finish_reason":"` + finish + `"}]}`)
	_, _ = io.WriteString(w, "data: [DONE]\n\n")
	if flusher != nil {
		flusher.Flush()
	}
}

func (e *fakeEndpoint) nextLocked() scriptedTurn {
	if len(e.turns) == 0 {
		return scriptedTurn{Text: "the answer"}
	}
	if e.index < len(e.turns) {
		turn := e.turns[e.index]
		e.index++
		return turn
	}
	// The last turn is served again, so a test that does not script every
	// request still gets an answer.
	return e.turns[len(e.turns)-1]
}

// requestBodies returns the bodies the endpoint received.
func (e *fakeEndpoint) requestBodies() []map[string]any {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]map[string]any(nil), e.requests...)
}

// promptTexts returns the user-role text of every request, in order.
func (e *fakeEndpoint) promptTexts() []string {
	var texts []string
	for _, body := range e.requestBodies() {
		messages, _ := body["messages"].([]any)
		var parts []string
		for _, message := range messages {
			entry, _ := message.(map[string]any)
			if entry == nil {
				continue
			}
			if role, _ := entry["role"].(string); role != "user" {
				continue
			}
			if text, _ := entry["content"].(string); text != "" {
				parts = append(parts, text)
			}
		}
		texts = append(texts, strings.Join(parts, "\n"))
	}
	return texts
}

// pooFixture is one running binary with its settings, working directory and
// session directory.
type pooFixture struct {
	dir      string
	settings string
	workDir  string
}

func newPooFixture(t *testing.T, endpoint *fakeEndpoint) pooFixture {
	t.Helper()
	return newPooFixtureWith(t, endpoint, nil)
}

// newPooFixtureWith adds settings blocks to the fixture's document, so a test
// can configure the permission policy a turn runs under.
func newPooFixtureWith(t *testing.T, endpoint *fakeEndpoint, extra map[string]any) pooFixture {
	t.Helper()
	dir := t.TempDir()
	workDir := filepath.Join(dir, "work")
	sessionDir := filepath.Join(dir, "sessions")
	for _, path := range []string{workDir, sessionDir} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	document := map[string]any{
		"version": 1,
		"endpoints": map[string]any{
			"fake": map[string]any{"endpoint": endpoint.server.URL + "/v1"},
		},
		"models": map[string]any{
			"test-model": map[string]any{
				"endpoint":      "fake",
				"model_id":      "test-model",
				"api":           "chat_completions",
				"tools_mode":    "native",
				"ops":           []string{"chat"},
				"context_limit": 200000,
			},
		},
		"roles":   map[string]any{"chat": "test-model"},
		"session": map[string]any{"mode": "logged", "dir": sessionDir},
	}
	for key, value := range extra {
		document[key] = value
	}
	encoded, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	settings := filepath.Join(dir, "settings.json")
	if err := os.WriteFile(settings, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	return pooFixture{dir: dir, settings: settings, workDir: workDir}
}

func (f pooFixture) startOptions(t *testing.T) avruntime.StartOptions {
	t.Helper()
	return avruntime.StartOptions{
		Dir:   f.workDir,
		Agent: "code",
		Model: "test-model",
		Launch: backendlaunch.Spec{
			Command:  buildPoo(t),
			Args:     []string{"serve"},
			Settings: f.settings,
		},
	}
}

// eventLog collects a session's events so a test can wait for one.
type eventLog struct {
	mu     sync.Mutex
	events []events.Event
}

func collect(ctx context.Context, ch <-chan events.Event) *eventLog {
	log := &eventLog{}
	go func() {
		for {
			select {
			case ev, ok := <-ch:
				if !ok {
					return
				}
				log.mu.Lock()
				log.events = append(log.events, ev)
				log.mu.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}()
	return log
}

func (l *eventLog) snapshot() []events.Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]events.Event(nil), l.events...)
}

// waitFor waits for the first event with the name and returns it.
func (l *eventLog) waitFor(t *testing.T, name string, timeout time.Duration) events.Event {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, ev := range l.snapshot() {
			if ev.Event == name {
				return ev
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no %q event within %s; saw %v", name, timeout, l.names())
	return events.Event{}
}

func (l *eventLog) names() []string {
	var names []string
	for _, ev := range l.snapshot() {
		names = append(names, ev.Event)
	}
	return names
}

// startProvider starts one provider, subscribes to its events and returns all
// three. The provider is closed when the test ends.
func startProvider(t *testing.T, opts avruntime.StartOptions) (*Provider, avruntime.Session, *eventLog, context.CancelFunc) {
	t.Helper()
	provider := NewWithOptions(avruntime.StartOptions{})
	session, err := provider.Start(context.Background(), opts)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = provider.Close()
	})
	ch, err := provider.Events(ctx, session.SessionID)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	return provider, session, collect(ctx, ch), cancel
}

// TestPromptRunsToSettle drives one turn to a natural end: the model answers,
// the run settles, and the published events carry avenor's own stop reason.
func TestPromptRunsToSettle(t *testing.T) {
	endpoint := newFakeEndpoint(t, scriptedTurn{Text: "hello from poo"})
	fixture := newPooFixture(t, endpoint)
	provider, session, log, _ := startProvider(t, fixture.startOptions(t))

	if session.PID == 0 {
		t.Fatal("Start reported no pid")
	}
	state, err := provider.session(session.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if state.sessionDir == "" {
		t.Fatal("Start kept no session dir for a logged session")
	}
	if state.mode != "logged" {
		t.Fatalf("kept mode = %q, want logged", state.mode)
	}

	promptDone := make(chan error, 1)
	go func() { promptDone <- provider.Prompt(context.Background(), session.SessionID, "say hello") }()

	end := log.waitFor(t, "session.end", 60*time.Second)
	if err := <-promptDone; err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if reason, _ := end.Fields["stop_reason"].(string); reason != "end_turn" {
		t.Fatalf("stop_reason = %q, want end_turn", reason)
	}
	if code, _ := events.Int64(end.Fields["exit_code"]); code != 0 {
		t.Fatalf("exit_code = %d, want 0", code)
	}

	var sawSubmitted, sawChunk bool
	for _, ev := range log.snapshot() {
		switch ev.Event {
		case "agent.prompt_submitted":
			sawSubmitted = true
		case "agent.message_chunk":
			if text, _ := ev.Fields["delta"].(string); text == "hello from poo" {
				sawChunk = true
			}
		}
	}
	if !sawSubmitted {
		t.Error("no agent.prompt_submitted event")
	}
	if !sawChunk {
		t.Error("no agent.message_chunk carrying the answer")
	}
}

// TestPromptSteersAnActiveRun proves a second prompt does not start a second
// run: it steers the running one, and the model sees the text at its next
// request.
func TestPromptSteersAnActiveRun(t *testing.T) {
	endpoint := newFakeEndpoint(t,
		scriptedTurn{ToolCalls: []scriptedToolCall{{Name: "bash", Arguments: map[string]any{"command": "sleep 0.5"}}}},
		scriptedTurn{Text: "done"},
	)
	fixture := newPooFixture(t, endpoint)
	provider, session, log, _ := startProvider(t, fixture.startOptions(t))

	firstDone := make(chan error, 1)
	go func() { firstDone <- provider.Prompt(context.Background(), session.SessionID, "first") }()
	log.waitFor(t, "tool.call", 60*time.Second)

	secondDone := make(chan error, 1)
	go func() { secondDone <- provider.Prompt(context.Background(), session.SessionID, "STEER-MARKER") }()

	for _, done := range []chan error{firstDone, secondDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Prompt: %v", err)
			}
		case <-time.After(60 * time.Second):
			t.Fatal("a prompt did not return")
		}
	}

	steered := false
	for _, text := range endpoint.promptTexts() {
		if strings.Contains(text, "STEER-MARKER") {
			steered = true
		}
	}
	if !steered {
		t.Fatalf("the model never saw the steer text; requests were %#v", endpoint.promptTexts())
	}
}

// TestAnUnansweredAskIsNotRelayedAndTheRunCarriesOn proves the Stage 2 boundary
// for poo's permission asks: the ask is not published as a permission.request,
// so no resolver tries to answer it, and the run continues once poo's own ask
// times out and the chain falls back.
func TestAnUnansweredAskIsNotRelayedAndTheRunCarriesOn(t *testing.T) {
	endpoint := newFakeEndpoint(t,
		scriptedTurn{ToolCalls: []scriptedToolCall{{Name: "bash", Arguments: map[string]any{"command": "true"}}}},
		scriptedTurn{Text: "carried on"},
	)
	// The chain timeout stands in for the wire asker's own 60 s budget: the
	// ask is left unanswered either way, and poo resolves it by itself.
	fixture := newPooFixtureWith(t, endpoint, map[string]any{
		"permission": map[string]any{
			"unmatched":       "ask",
			"askers":          []string{"wire"},
			"chain_timeout_s": 2,
		},
	})
	provider, session, log, _ := startProvider(t, fixture.startOptions(t))

	promptDone := make(chan error, 1)
	go func() { promptDone <- provider.Prompt(context.Background(), session.SessionID, "run it") }()

	// The ask reaches the client — the backend is asked because the handshake
	// declares the capability — and is echoed as the session's own record.
	log.waitFor(t, "avenor.permission.asked", 60*time.Second)
	select {
	case err := <-promptDone:
		if err != nil {
			t.Fatalf("Prompt: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("Prompt did not return after the ask timed out")
	}

	for _, ev := range log.snapshot() {
		if ev.Event == "permission.request" {
			t.Fatal("the ask was relayed as a permission.request, which Stage 2 cannot answer")
		}
	}
	end := log.waitFor(t, "session.end", 10*time.Second)
	if reason, _ := end.Fields["stop_reason"].(string); reason != "end_turn" {
		t.Fatalf("stop_reason = %q, want end_turn: the run did not carry on", reason)
	}
	if code, _ := events.Int64(end.Fields["exit_code"]); code != 0 {
		t.Fatalf("exit_code = %d, want 0", code)
	}
	// The model saw the fallback's denial as a tool result and answered again.
	if requests := endpoint.requestBodies(); len(requests) < 2 {
		t.Fatalf("the model sent %d requests, want a second turn after the denial", len(requests))
	}
}

// TestPromptRetriesStartWhenTheRunSettlesBeforeTheSessionReportsIt proves the
// busy/session-get race is closed: when the run that refused the prompt is gone
// before session/get can name it, the prompt starts its own turn instead of
// failing.
func TestPromptRetriesStartWhenTheRunSettlesBeforeTheSessionReportsIt(t *testing.T) {
	t.Setenv(helperEnv, "busysettle")
	provider := NewWithOptions(avruntime.StartOptions{})
	t.Cleanup(func() { _ = provider.Close() })

	session, err := provider.Start(context.Background(), helperStartOptions())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := provider.Prompt(context.Background(), session.SessionID, "go"); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
}

// TestCancelStopsARunInATool proves cancel reaches the run id the prompt
// started, and the run settles cancelled rather than as a generic failure.
func TestCancelStopsARunInATool(t *testing.T) {
	endpoint := newFakeEndpoint(t,
		scriptedTurn{ToolCalls: []scriptedToolCall{{Name: "bash", Arguments: map[string]any{"command": "sleep 10"}}}},
	)
	fixture := newPooFixture(t, endpoint)
	provider, session, log, _ := startProvider(t, fixture.startOptions(t))

	promptDone := make(chan error, 1)
	go func() { promptDone <- provider.Prompt(context.Background(), session.SessionID, "run it") }()
	log.waitFor(t, "tool.call", 60*time.Second)

	if err := provider.Cancel(context.Background(), session.SessionID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	select {
	case err := <-promptDone:
		if err == nil {
			t.Fatal("Prompt returned nil after a cancel")
		}
		if !strings.Contains(err.Error(), "cancelled") {
			t.Fatalf("Prompt error = %q, want a cancellation", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("Prompt did not return after cancel")
	}

	end := log.waitFor(t, "session.end", 30*time.Second)
	if reason, _ := end.Fields["stop_reason"].(string); reason != "cancelled" {
		t.Fatalf("stop_reason = %q, want cancelled", reason)
	}
	if code, _ := events.Int64(end.Fields["exit_code"]); code != 130 {
		t.Fatalf("exit_code = %d, want 130", code)
	}
}

// TestPooCrashFailsTheSessionCleanly proves a dead child surfaces as a prompt
// failure with its own stderr or exit status, and that teardown does not wait
// for a process that is already gone.
func TestPooCrashFailsTheSessionCleanly(t *testing.T) {
	endpoint := newFakeEndpoint(t, scriptedTurn{Text: "slow", Delay: 3 * time.Second})
	fixture := newPooFixture(t, endpoint)
	provider, session, _, _ := startProvider(t, fixture.startOptions(t))

	promptDone := make(chan error, 1)
	go func() { promptDone <- provider.Prompt(context.Background(), session.SessionID, "go") }()

	// Kill the child while the endpoint is still holding the request.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if len(endpoint.requestBodies()) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	proc, err := os.FindProcess(session.PID)
	if err != nil {
		t.Fatalf("find poo process: %v", err)
	}
	if err := proc.Kill(); err != nil {
		t.Fatalf("kill poo process: %v", err)
	}

	select {
	case err := <-promptDone:
		if err == nil {
			t.Fatal("Prompt returned nil after the process was killed")
		}
		if !strings.Contains(err.Error(), "exited") && !strings.Contains(err.Error(), "signal") {
			t.Fatalf("Prompt error = %q, want the process exit named", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Prompt did not fail after the process was killed")
	}

	closed := make(chan error, 1)
	go func() { closed <- provider.Close() }()
	select {
	case <-closed:
	case <-time.After(15 * time.Second):
		t.Fatal("Close blocked on a process that had already exited")
	}
}

// TestCloseAnswersWhileARunIsLive proves a live run does not hold teardown: the
// session closes, the process exits and nothing is left behind.
func TestCloseAnswersWhileARunIsLive(t *testing.T) {
	endpoint := newFakeEndpoint(t,
		scriptedTurn{ToolCalls: []scriptedToolCall{{Name: "bash", Arguments: map[string]any{"command": "sleep 20"}}}},
	)
	fixture := newPooFixture(t, endpoint)
	provider, session, log, _ := startProvider(t, fixture.startOptions(t))

	promptDone := make(chan error, 1)
	go func() { promptDone <- provider.Prompt(context.Background(), session.SessionID, "run it") }()
	log.waitFor(t, "tool.call", 60*time.Second)

	start := time.Now()
	closed := make(chan error, 1)
	go func() { closed <- provider.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(25 * time.Second):
		t.Fatal("Close did not answer while a run was live")
	}
	if elapsed := time.Since(start); elapsed > 25*time.Second {
		t.Fatalf("Close took %s", elapsed)
	}
	select {
	case <-promptDone:
	case <-time.After(15 * time.Second):
		t.Fatal("the live prompt never returned after Close")
	}
	if err := syscall.Kill(session.PID, 0); err == nil {
		t.Fatalf("poo process %d is still alive after Close", session.PID)
	}
}

// TestStartRefusesAnUnsupportedProtocolVersion proves the handshake checks the
// agreed version instead of assuming the one it offered.
func TestStartRefusesAnUnsupportedProtocolVersion(t *testing.T) {
	t.Setenv(helperEnv, "version")
	provider := NewWithOptions(avruntime.StartOptions{})
	t.Cleanup(func() { _ = provider.Close() })
	provider.protocolVersions = []int{999}

	_, err := provider.Start(context.Background(), helperStartOptions())
	if err == nil {
		t.Fatal("Start accepted a server that speaks no offered version")
	}
	if !strings.Contains(err.Error(), "protocol version") || !strings.Contains(err.Error(), "protocol_version_unsupported") {
		t.Fatalf("Start error = %q, want the refused version named", err)
	}
}

// TestStartRefusesAnUnknownProfileAndModel proves both create refusals surface
// at Start, named by the field the operator has to fix.
func TestStartRefusesAnUnknownProfileAndModel(t *testing.T) {
	endpoint := newFakeEndpoint(t, scriptedTurn{Text: "unused"})
	fixture := newPooFixture(t, endpoint)

	t.Run("profile", func(t *testing.T) {
		opts := fixture.startOptions(t)
		opts.Agent = "no-such-profile"
		provider := NewWithOptions(avruntime.StartOptions{})
		t.Cleanup(func() { _ = provider.Close() })
		_, err := provider.Start(context.Background(), opts)
		if err == nil {
			t.Fatal("Start accepted an unknown profile")
		}
		for _, want := range []string{"profile", "no-such-profile", "unknown_profile"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("Start error = %q, want %q", err, want)
			}
		}
	})

	t.Run("model", func(t *testing.T) {
		opts := fixture.startOptions(t)
		opts.Model = "no-such-model"
		provider := NewWithOptions(avruntime.StartOptions{})
		t.Cleanup(func() { _ = provider.Close() })
		_, err := provider.Start(context.Background(), opts)
		if err == nil {
			t.Fatal("Start accepted an unknown model")
		}
		for _, want := range []string{"model", "no-such-model", "unknown_model"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("Start error = %q, want %q", err, want)
			}
		}
	})
}

// TestEventsBeforeTheCreateResponseAreKept proves the backend routes by the
// notification's own session id and buffers until a subscriber attaches. The
// server emits the run's first events before it answers session/create.
func TestEventsBeforeTheCreateResponseAreKept(t *testing.T) {
	t.Setenv(helperEnv, "early")
	provider := NewWithOptions(avruntime.StartOptions{})
	t.Cleanup(func() { _ = provider.Close() })

	session, err := provider.Start(context.Background(), helperStartOptions())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if session.SessionID != "poo-early" {
		t.Fatalf("SessionID = %q, want the id from the create result", session.SessionID)
	}
	state, err := provider.session(session.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if state.mode != "ephemeral" {
		t.Fatalf("kept mode = %q, want the mode the create result reported", state.mode)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := provider.Events(ctx, session.SessionID)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	log := collect(ctx, ch)
	log.waitFor(t, "avenor.turn.start", 10*time.Second)
	log.waitFor(t, "agent.message_chunk", 10*time.Second)
}

// TestPromptReAsksWhenTheInspectWaitTimesOut proves a wait that runs out is
// not a failure: the client asks again and returns the outcome the second wait
// reports, not the empty first answer.
func TestPromptReAsksWhenTheInspectWaitTimesOut(t *testing.T) {
	t.Setenv(helperEnv, "inspecttimeout")
	provider := NewWithOptions(avruntime.StartOptions{})
	t.Cleanup(func() { _ = provider.Close() })

	session, err := provider.Start(context.Background(), helperStartOptions())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	err = provider.Prompt(context.Background(), session.SessionID, "go")
	if err == nil {
		t.Fatal("Prompt returned nil, want the failure the second wait settled")
	}
	if !strings.Contains(err.Error(), "the second wait settled") {
		t.Fatalf("Prompt error = %q, want the second wait's outcome", err)
	}
}

// TestCloseEscalatesFromSIGTERMToSIGKILL proves teardown does not give up on a
// child that ignores both stdin EOF and SIGTERM: Close spends the exit grace,
// signals, spends the kill grace and kills, and the process is gone.
func TestCloseEscalatesFromSIGTERMToSIGKILL(t *testing.T) {
	t.Setenv(helperEnv, "ignoreterm")
	oldExit, oldKill := exitGrace, killGrace
	exitGrace, killGrace = 500*time.Millisecond, 500*time.Millisecond
	t.Cleanup(func() { exitGrace, killGrace = oldExit, oldKill })
	provider := NewWithOptions(avruntime.StartOptions{})

	session, err := provider.Start(context.Background(), helperStartOptions())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	start := time.Now()
	if err := provider.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	elapsed := time.Since(start)
	if budget := exitGrace + killGrace + 5*time.Second; elapsed > budget {
		t.Fatalf("Close took %s, want it to return within %s", elapsed, budget)
	}
	if err := syscall.Kill(session.PID, 0); err == nil {
		t.Fatalf("poo process %d survived Close", session.PID)
	}
}

// TestStartRefusesASecondSession proves one provider owns one process's one
// session: a second Start is refused rather than opening a session the client
// cannot serve.
func TestStartRefusesASecondSession(t *testing.T) {
	t.Setenv(helperEnv, "early")
	provider := NewWithOptions(avruntime.StartOptions{})
	t.Cleanup(func() { _ = provider.Close() })

	if _, err := provider.Start(context.Background(), helperStartOptions()); err != nil {
		t.Fatalf("the first Start: %v", err)
	}
	second, err := provider.Start(context.Background(), helperStartOptions())
	if err == nil {
		t.Fatal("the second Start opened a session, want the one-session refusal")
	}
	if !strings.Contains(err.Error(), "poo runs one session per process") {
		t.Fatalf("the second Start error = %q, want the one-session refusal", err)
	}
	if second.SessionID != "" {
		t.Fatalf("the second Start returned session %q, want none", second.SessionID)
	}
}

// TestClassifySettleFollowsTheStopReasonTable pins the classification,
// including the reasons a newer binary adds and a reason nobody has thought of.
func TestClassifySettleFollowsTheStopReasonTable(t *testing.T) {
	cases := []struct {
		name        string
		reason      string
		detail      protocol.SettleDetail
		stopReason  string
		from        string
		retryAfter  int64
		exit        int
		errorSuffix string
	}{
		{name: "end turn", reason: "end_turn", stopReason: "end_turn", exit: 0},
		{name: "finished", reason: "finished", stopReason: "end_turn", exit: 0},
		{name: "stop requested", reason: "stop_requested", stopReason: "end_turn", exit: 0},
		{name: "truncated", reason: "output_truncated", stopReason: "max_tokens", from: "output_truncated", exit: 3},
		{name: "max turns", reason: "max_turns", stopReason: "max_turn_requests", from: "max_turns", exit: 4},
		{name: "cancelled", reason: "cancelled", stopReason: "cancelled", exit: 130},
		{
			name:       "rate limited",
			reason:     "provider_limit",
			detail:     protocol.SettleDetail{Provider: &protocol.ProviderDetail{Kind: "rate_limited", RetryAfterS: 12}},
			stopReason: avruntime.RateLimitedStopReason,
			from:       "provider_limit",
			retryAfter: 12,
			exit:       7,
		},
		{
			name:       "usage limit",
			reason:     "provider_limit",
			detail:     protocol.SettleDetail{Provider: &protocol.ProviderDetail{Kind: "usage_limit"}},
			stopReason: avruntime.QuotaExhaustedStopReason,
			from:       "provider_limit",
			exit:       6,
		},
		{
			name:       "plan inactive",
			reason:     "provider_limit",
			detail:     protocol.SettleDetail{Provider: &protocol.ProviderDetail{Kind: "plan_inactive"}},
			stopReason: avruntime.QuotaExhaustedStopReason,
			from:       "provider_limit",
			exit:       6,
		},
		{
			name:        "provider limit of another kind",
			reason:      "provider_limit",
			detail:      protocol.SettleDetail{Provider: &protocol.ProviderDetail{Kind: "auth_failed"}, Text: "the key is wrong"},
			stopReason:  "error",
			from:        "provider_limit",
			exit:        1,
			errorSuffix: "auth_failed",
		},
		{name: "provider error", reason: "provider_error", detail: protocol.SettleDetail{Text: "boom"}, stopReason: "error", from: "provider_error", exit: 1, errorSuffix: "boom"},
		{name: "hook error", reason: "hook_error", detail: protocol.SettleDetail{Text: "hook"}, stopReason: "error", from: "hook_error", exit: 1},
		{name: "tool fatal", reason: "tool_fatal", detail: protocol.SettleDetail{Text: "fatal"}, stopReason: "error", from: "tool_fatal", exit: 1},
		{name: "store error", reason: "store_error", stopReason: "error", from: "store_error", exit: 1},
		{name: "context exhausted", reason: "context_exhausted", detail: protocol.SettleDetail{Text: "full", Handoff: "the note"}, stopReason: "error", from: "context_exhausted", exit: 1},
		{name: "interrupted", reason: "interrupted", stopReason: "error", from: "interrupted", exit: 1},
		{name: "invalid turns", reason: "invalid_turns", detail: protocol.SettleDetail{Text: "the model answered nonsense"}, stopReason: "error", from: "invalid_turns", exit: 1, errorSuffix: "nonsense"},
		{name: "a reason nobody knows", reason: "brand_new_reason", detail: protocol.SettleDetail{Text: "who knows"}, stopReason: "error", from: "brand_new_reason", exit: 1, errorSuffix: "who knows"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			outcome := classifySettle(tc.reason, tc.detail)
			if outcome.StopReason != tc.stopReason {
				t.Fatalf("stop reason = %q, want %q", outcome.StopReason, tc.stopReason)
			}
			if outcome.ClassifiedFrom != tc.from {
				t.Fatalf("classified from = %q, want %q", outcome.ClassifiedFrom, tc.from)
			}
			if outcome.RetryAfter != tc.retryAfter {
				t.Fatalf("retry after = %d, want %d", outcome.RetryAfter, tc.retryAfter)
			}
			if tc.errorSuffix != "" && !strings.Contains(outcome.ErrorMessage, tc.errorSuffix) {
				t.Fatalf("error message = %q, want it to mention %q", outcome.ErrorMessage, tc.errorSuffix)
			}
			if got := avruntime.ExitCodeForStopReason(outcome.StopReason); got != tc.exit {
				t.Fatalf("exit code for %q = %d, want %d", outcome.StopReason, got, tc.exit)
			}
		})
	}
}

// TestSettledEventCarriesTheLoopRunnerFields proves the terminal record surfaces
// what the loop reads besides the reason: the handoff, the jobs that outlive the
// run and the finish result.
func TestSettledEventCarriesTheLoopRunnerFields(t *testing.T) {
	exhausted := settledEvent(sessionEvent{
		SessionID: "ses_1",
		Type:      "run.settled",
		Fields: map[string]any{
			"reason": "context_exhausted",
			"detail": map[string]any{
				"text":         "the window is full",
				"handoff":      "continue from the plan",
				"jobs_running": []any{"j1"},
			},
		},
	})
	if exhausted.Event != "session.end" {
		t.Fatalf("event = %q, want session.end", exhausted.Event)
	}
	for key, want := range map[string]any{
		"stop_reason":     "error",
		"error_message":   "the window is full",
		"handoff":         "continue from the plan",
		"classified_from": "context_exhausted",
	} {
		if got := exhausted.Fields[key]; got != want {
			t.Errorf("%s = %#v, want %#v", key, got, want)
		}
	}
	if jobs, _ := exhausted.Fields["jobs_running"].([]string); len(jobs) != 1 || jobs[0] != "j1" {
		t.Errorf("jobs_running = %#v, want [j1]", exhausted.Fields["jobs_running"])
	}

	finished := settledEvent(sessionEvent{
		SessionID: "ses_1",
		Type:      "run.settled",
		Fields: map[string]any{
			"reason": "finished",
			"detail": map[string]any{"result": map[string]any{"verdict": "ok"}},
		},
	})
	if reason, _ := finished.Fields["stop_reason"].(string); reason != "end_turn" {
		t.Fatalf("finished stop_reason = %q, want end_turn", reason)
	}
	if result, ok := finished.Fields["result"].(json.RawMessage); !ok || !strings.Contains(string(result), "verdict") {
		t.Fatalf("result = %#v, want the finish payload", finished.Fields["result"])
	}
}

// TestProviderRecoversFromDenial pins the tool_denied override: the poo backend
// declares that a denial the model recovered from does not end the run.
func TestProviderRecoversFromDenial(t *testing.T) {
	var provider avruntime.Provider = NewWithOptions(avruntime.StartOptions{})
	recovery, ok := provider.(avruntime.DenialRecovery)
	if !ok {
		t.Fatal("the poo provider does not declare denial recovery")
	}
	if !recovery.RecoveredDenialIsCompletion() {
		t.Fatal("RecoveredDenialIsCompletion() = false, want true")
	}
}

// helperStartOptions is the launch identity that starts the test binary as a
// fake poo server.
func helperStartOptions() avruntime.StartOptions {
	return avruntime.StartOptions{
		Dir:    os.TempDir(),
		Agent:  "code",
		Launch: backendlaunch.Spec{Command: os.Args[0], Args: []string{"serve"}},
	}
}

// runHelperServer answers the wire protocol as the scenario mode asks. It runs
// in a re-executed test binary, so it only knows the bytes on stdin.
func runHelperServer(mode string) int {
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 64*1024), 1<<20)
	out := bufio.NewWriter(os.Stdout)
	if mode == "ignoreterm" {
		// The SIGKILL leg of teardown is only reached by a child that survives
		// SIGTERM, so this mode refuses to die until it is killed.
		signal.Ignore(syscall.SIGTERM)
	}
	runPrompts := 0
	inspectWaits := 0
	write := func(value any) {
		encoded, err := json.Marshal(value)
		if err != nil {
			return
		}
		_, _ = out.Write(encoded)
		_ = out.WriteByte('\n')
		_ = out.Flush()
	}
	for in.Scan() {
		line := bytes.TrimSpace(in.Bytes())
		if len(line) == 0 {
			continue
		}
		var request struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.Unmarshal(line, &request); err != nil {
			continue
		}
		switch request.Method {
		case protocol.MethodSessionGet:
			if mode == "busysettle" {
				// The run that refused the prompt has settled, so the
				// session names none.
				write(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": protocol.GetResult{State: "idle"}})
				continue
			}
			write(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": map[string]any{}})
		case protocol.MethodRunPrompt:
			if mode == "busysettle" && runPrompts == 0 {
				runPrompts++
				write(map[string]any{
					"jsonrpc": "2.0",
					"id":      request.ID,
					"error": map[string]any{
						"code":    protocol.CodeServerError,
						"message": "the session already has a run",
						"data":    map[string]any{"code": protocol.DataBusy},
					},
				})
				continue
			}
			runPrompts++
			write(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": protocol.PromptResult{RunID: "run-busy"}})
		case protocol.MethodRunInspect:
			if mode == "inspecttimeout" {
				// The first wait runs out, which is not a failure: the client has
				// to ask again and read the outcome off the second answer.
				inspectWaits++
				if inspectWaits == 1 {
					write(map[string]any{
						"jsonrpc": "2.0",
						"id":      request.ID,
						"result":  protocol.InspectResult{TimedOut: true},
					})
					continue
				}
				write(map[string]any{
					"jsonrpc": "2.0",
					"id":      request.ID,
					"result": protocol.InspectResult{Settled: &protocol.Settled{
						RunID:  "run-busy",
						Reason: "provider_error",
						Detail: protocol.SettleDetail{Text: "the second wait settled"},
					}},
				})
				continue
			}
			write(map[string]any{
				"jsonrpc": "2.0",
				"id":      request.ID,
				"result":  protocol.InspectResult{Settled: &protocol.Settled{RunID: "run-busy", Reason: "end_turn"}},
			})
		case protocol.MethodInitialize:
			if mode == "version" {
				write(map[string]any{
					"jsonrpc": "2.0",
					"id":      request.ID,
					"error": map[string]any{
						"code":    protocol.CodeVersionUnsupported,
						"message": "the client speaks no version this server speaks",
						"data":    map[string]any{"code": protocol.DataProtocolVersionUnsupported},
					},
				})
				return 0
			}
			write(map[string]any{
				"jsonrpc": "2.0",
				"id":      request.ID,
				"result": protocol.InitializeResult{
					ProtocolVersion: protocol.Version,
					Build:           protocol.Build{Name: "poo", Version: "test"},
					Profiles:        []protocol.ProfileInfo{{Name: "code"}},
					Models:          protocol.ModelRoles{Chat: "test-model"},
				},
			})
		case protocol.MethodSessionCreate:
			if mode == "early" {
				write(map[string]any{
					"jsonrpc": "2.0",
					"method":  protocol.MethodEvent,
					"params":  map[string]any{"session_id": "poo-early", "seq": 1, "type": "run.started", "run_id": "run-early"},
				})
				write(map[string]any{
					"jsonrpc": "2.0",
					"method":  protocol.MethodEvent,
					"params":  map[string]any{"session_id": "poo-early", "seq": 2, "type": "message.delta", "run_id": "run-early", "kind": "text", "text": "early"},
				})
			}
			write(map[string]any{
				"jsonrpc": "2.0",
				"id":      request.ID,
				"result":  protocol.SessionInfo{SessionID: "poo-early", Mode: "ephemeral"},
			})
		default:
			write(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": map[string]any{}})
		}
	}
	if mode == "ignoreterm" {
		// Stdin closed and the scanner ended; a child that ignores SIGTERM has
		// to be killed, so stay alive here instead of returning.
		select {}
	}
	return 0
}
