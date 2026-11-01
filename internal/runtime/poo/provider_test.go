package poo

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
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

// toolTexts returns the text of every tool-role message the endpoint was sent:
// what the model read as a tool's result.
func (e *fakeEndpoint) toolTexts() []string {
	var texts []string
	for _, body := range e.requestBodies() {
		messages, _ := body["messages"].([]any)
		for _, message := range messages {
			entry, _ := message.(map[string]any)
			if entry == nil {
				continue
			}
			if role, _ := entry["role"].(string); role != "tool" {
				continue
			}
			if text, _ := entry["content"].(string); text != "" {
				texts = append(texts, text)
			}
		}
	}
	return texts
}

// sawToolText reports whether a tool's result reached the model with the text
// in it.
func (e *fakeEndpoint) sawToolText(text string) bool {
	for _, got := range e.toolTexts() {
		if strings.Contains(got, text) {
			return true
		}
	}
	return false
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

// count returns how many events with the name the log has seen.
func (l *eventLog) count(name string) int {
	seen := 0
	for _, ev := range l.snapshot() {
		if ev.Event == name {
			seen++
		}
	}
	return seen
}

// waitForCount waits until the log has seen want events with the name.
func (l *eventLog) waitForCount(t *testing.T, name string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l.count(name) >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("saw %d %q events within %s, want %d; saw %v", l.count(name), name, timeout, want, l.names())
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

// permissionRelaySettings is the permission block a relay test runs under:
// every side-effecting call is asked about, the wire asker is the whole chain,
// and the chain's own budget is the timeout the test wants.
func permissionRelaySettings(chainTimeoutS int) map[string]any {
	return map[string]any{
		"unmatched":       "ask",
		"fallback":        "deny",
		"askers":          []string{"wire"},
		"chain_timeout_s": chainTimeoutS,
	}
}

// startPrompt runs one prompt in the background and returns its result
// channel.
func startPrompt(provider *Provider, sessionID, text string) <-chan error {
	done := make(chan error, 1)
	go func() { done <- provider.Prompt(context.Background(), sessionID, text) }()
	return done
}

// awaitPrompt waits for a prompt's result and fails on an error.
func awaitPrompt(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Prompt: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the prompt did not return")
	}
}

// askRequestID returns the ask id a permission.request event carries.
func askRequestID(t *testing.T, ask events.Event) string {
	t.Helper()
	id, _ := ask.Fields["request_id"].(string)
	if id == "" {
		t.Fatalf("the permission request has no request_id: %#v", ask.Fields)
	}
	return id
}

// hasOptionKind reports whether the request offers an option of the kind.
func hasOptionKind(options []any, kind string) bool {
	for _, raw := range options {
		option, _ := raw.(map[string]any)
		if option == nil {
			continue
		}
		if got, _ := option["kind"].(string); got == kind {
			return true
		}
	}
	return false
}

// TestPermissionRelayAllowsTheCall proves an allow answer reaches poo: the
// allowed call runs, the model reads its output, and the run settles end_turn.
func TestPermissionRelayAllowsTheCall(t *testing.T) {
	endpoint := newFakeEndpoint(t,
		scriptedTurn{ToolCalls: []scriptedToolCall{{Name: "bash", Arguments: map[string]any{"command": "echo relay-allowed"}}}},
		scriptedTurn{Text: "carried on"},
	)
	fixture := newPooFixtureWith(t, endpoint, map[string]any{"permission": permissionRelaySettings(60)})
	provider, session, log, _ := startProvider(t, fixture.startOptions(t))

	done := startPrompt(provider, session.SessionID, "run it")
	ask := log.waitFor(t, "permission.request", 30*time.Second)
	id := askRequestID(t, ask)
	if tool, _ := ask.Fields["tool"].(string); tool != "bash" {
		t.Fatalf("the permission request names tool %q, want bash", tool)
	}
	if kind, _ := ask.Fields["kind"].(string); kind != "confirm" {
		t.Fatalf("the permission request kind is %q, want confirm", kind)
	}
	options, _ := ask.Fields["options"].([]any)
	if !hasOptionKind(options, "allow") || !hasOptionKind(options, "reject") {
		t.Fatalf("the permission request offers %#v, want an allow and a reject option", options)
	}

	if err := provider.AnswerPermission(context.Background(), session.SessionID, id, avruntime.PermissionResponse{Allow: true, OptionID: "allow"}); err != nil {
		t.Fatalf("AnswerPermission: %v", err)
	}
	awaitPrompt(t, done)
	end := log.waitFor(t, "session.end", 30*time.Second)
	if reason, _ := end.Fields["stop_reason"].(string); reason != "end_turn" {
		t.Fatalf("stop_reason = %q, want end_turn: the run did not carry on", reason)
	}
	if code, _ := events.Int64(end.Fields["exit_code"]); code != 0 {
		t.Fatalf("exit_code = %d, want 0", code)
	}
	answered := log.waitFor(t, "avenor.permission.answered", 30*time.Second)
	if by, _ := answered.Fields["answered_by"].(string); by != "wire" {
		t.Fatalf("the ask was answered by %q, want wire", by)
	}
	if decision, _ := answered.Fields["decision"].(string); decision != "allow" {
		t.Fatalf("the ask was answered %q, want allow", decision)
	}
	if !endpoint.sawToolText("relay-allowed") {
		t.Fatalf("the model never read the allowed call's output; tool messages were %#v", endpoint.toolTexts())
	}
}

// TestPermissionRelayDeniesTheCall proves a deny answer reaches the model as a
// tool result and the run still settles cleanly. A denial is not a failed run:
// the backend reports end_turn, and its DenialRecovery declaration keeps the
// run loop from downgrading that to tool_denied.
func TestPermissionRelayDeniesTheCall(t *testing.T) {
	endpoint := newFakeEndpoint(t,
		scriptedTurn{ToolCalls: []scriptedToolCall{{Name: "bash", Arguments: map[string]any{"command": "echo relay-denied"}}}},
		scriptedTurn{Text: "the call was refused"},
	)
	fixture := newPooFixtureWith(t, endpoint, map[string]any{"permission": permissionRelaySettings(60)})
	provider, session, log, _ := startProvider(t, fixture.startOptions(t))

	done := startPrompt(provider, session.SessionID, "run it")
	id := askRequestID(t, log.waitFor(t, "permission.request", 30*time.Second))
	if err := provider.AnswerPermission(context.Background(), session.SessionID, id, avruntime.PermissionResponse{OptionID: "deny"}); err != nil {
		t.Fatalf("AnswerPermission: %v", err)
	}
	awaitPrompt(t, done)
	end := log.waitFor(t, "session.end", 30*time.Second)
	if reason, _ := end.Fields["stop_reason"].(string); reason != "end_turn" {
		t.Fatalf("stop_reason = %q, want end_turn: a refused call is not a failed run", reason)
	}
	answered := log.waitFor(t, "avenor.permission.answered", 30*time.Second)
	if by, _ := answered.Fields["answered_by"].(string); by != "wire" {
		t.Fatalf("the ask was answered by %q, want wire", by)
	}
	if decision, _ := answered.Fields["decision"].(string); decision != "deny" {
		t.Fatalf("the ask was answered %q, want deny", decision)
	}
	if !endpoint.sawToolText("denied by wire") {
		t.Fatalf("the model never read the denial; tool messages were %#v", endpoint.toolTexts())
	}
}

// TestPermissionRelayFallsBackWhenNoAnswerArrives proves the backend does not
// hold a run hostage to an unanswered ask: poo's own chain budget runs out, the
// fallback decides, and the run continues.
func TestPermissionRelayFallsBackWhenNoAnswerArrives(t *testing.T) {
	endpoint := newFakeEndpoint(t,
		scriptedTurn{ToolCalls: []scriptedToolCall{{Name: "bash", Arguments: map[string]any{"command": "echo relay-unanswered"}}}},
		scriptedTurn{Text: "carried on"},
	)
	fixture := newPooFixtureWith(t, endpoint, map[string]any{"permission": permissionRelaySettings(2)})
	provider, session, log, _ := startProvider(t, fixture.startOptions(t))

	done := startPrompt(provider, session.SessionID, "run it")
	askRequestID(t, log.waitFor(t, "permission.request", 30*time.Second))
	answered := log.waitFor(t, "avenor.permission.answered", 60*time.Second)
	if provenance, _ := answered.Fields["provenance"].(string); provenance != "timeout" {
		t.Fatalf("the ask was resolved with provenance %q, want timeout", provenance)
	}
	if by, _ := answered.Fields["answered_by"].(string); by != "fallback" {
		t.Fatalf("the ask was answered by %q, want fallback", by)
	}
	if decision, _ := answered.Fields["decision"].(string); decision != "deny" {
		t.Fatalf("the fallback answered %q, want deny", decision)
	}
	awaitPrompt(t, done)
	end := log.waitFor(t, "session.end", 30*time.Second)
	if reason, _ := end.Fields["stop_reason"].(string); reason != "end_turn" {
		t.Fatalf("stop_reason = %q, want end_turn: the run did not carry on", reason)
	}
	if !endpoint.sawToolText("denied by fallback") {
		t.Fatalf("the model never read the fallback's denial; tool messages were %#v", endpoint.toolTexts())
	}
}

// TestPermissionRelayRejectsALateAnswer proves an answer for an ask poo already
// resolved writes nothing, and that the session takes another prompt.
func TestPermissionRelayRejectsALateAnswer(t *testing.T) {
	endpoint := newFakeEndpoint(t,
		scriptedTurn{ToolCalls: []scriptedToolCall{{Name: "bash", Arguments: map[string]any{"command": "echo relay-late"}}}},
		scriptedTurn{Text: "carried on"},
	)
	fixture := newPooFixtureWith(t, endpoint, map[string]any{"permission": permissionRelaySettings(2)})
	provider, session, log, _ := startProvider(t, fixture.startOptions(t))

	done := startPrompt(provider, session.SessionID, "run it")
	id := askRequestID(t, log.waitFor(t, "permission.request", 30*time.Second))
	log.waitFor(t, "avenor.permission.answered", 60*time.Second)

	err := provider.AnswerPermission(context.Background(), session.SessionID, id, avruntime.PermissionResponse{Allow: true})
	if err == nil {
		t.Fatal("AnswerPermission for an ask poo already resolved returned no error")
	}
	if !strings.Contains(err.Error(), "not pending") {
		t.Fatalf("AnswerPermission error = %q, want the ask reported as not pending", err)
	}
	awaitPrompt(t, done)
	if reason := log.waitFor(t, "session.end", 30*time.Second).Fields["stop_reason"]; reason != "end_turn" {
		t.Fatalf("stop_reason = %#v, want end_turn", reason)
	}

	// A late answer must not wedge the session: the next prompt runs.
	awaitPrompt(t, startPrompt(provider, session.SessionID, "again"))
	log.waitForCount(t, "session.end", 2, 60*time.Second)
}

// TestPermissionRelayRejectsAnUnknownAsk proves an answer for an ask that never
// existed writes nothing and leaves the real ask answerable.
func TestPermissionRelayRejectsAnUnknownAsk(t *testing.T) {
	endpoint := newFakeEndpoint(t,
		scriptedTurn{ToolCalls: []scriptedToolCall{{Name: "bash", Arguments: map[string]any{"command": "echo relay-known"}}}},
		scriptedTurn{Text: "carried on"},
	)
	fixture := newPooFixtureWith(t, endpoint, map[string]any{"permission": permissionRelaySettings(60)})
	provider, session, log, _ := startProvider(t, fixture.startOptions(t))

	done := startPrompt(provider, session.SessionID, "run it")
	id := askRequestID(t, log.waitFor(t, "permission.request", 30*time.Second))

	err := provider.AnswerPermission(context.Background(), session.SessionID, "ask-that-never-existed", avruntime.PermissionResponse{Allow: true})
	if err == nil {
		t.Fatal("AnswerPermission for an unknown ask returned no error")
	}
	if !strings.Contains(err.Error(), "ask-that-never-existed") {
		t.Fatalf("AnswerPermission error = %q, want the unknown ask named", err)
	}
	if err := provider.AnswerPermission(context.Background(), session.SessionID, id, avruntime.PermissionResponse{Allow: true}); err != nil {
		t.Fatalf("AnswerPermission for the pending ask: %v", err)
	}
	awaitPrompt(t, done)
	if reason := log.waitFor(t, "session.end", 30*time.Second).Fields["stop_reason"]; reason != "end_turn" {
		t.Fatalf("stop_reason = %#v, want end_turn", reason)
	}
	if !endpoint.sawToolText("relay-known") {
		t.Fatalf("the model never read the allowed call's output; tool messages were %#v", endpoint.toolTexts())
	}
}

// TestPermissionRelayRejectsAnAskAnsweredUnderAnotherSession proves an answer
// sent under a session id that does not own the ask writes nothing: the ask
// stays pending for the session that owns it, and the run completes once that
// session answers.
func TestPermissionRelayRejectsAnAskAnsweredUnderAnotherSession(t *testing.T) {
	endpoint := newFakeEndpoint(t,
		scriptedTurn{ToolCalls: []scriptedToolCall{{Name: "bash", Arguments: map[string]any{"command": "echo relay-foreign"}}}},
		scriptedTurn{Text: "carried on"},
	)
	fixture := newPooFixtureWith(t, endpoint, map[string]any{"permission": permissionRelaySettings(60)})
	provider, session, log, _ := startProvider(t, fixture.startOptions(t))

	done := startPrompt(provider, session.SessionID, "run it")
	id := askRequestID(t, log.waitFor(t, "permission.request", 30*time.Second))

	err := provider.AnswerPermission(context.Background(), session.SessionID+"-other", id, avruntime.PermissionResponse{Allow: true})
	if err == nil {
		t.Fatal("AnswerPermission under a foreign session id returned no error")
	}
	if !strings.Contains(err.Error(), "not pending") {
		t.Fatalf("AnswerPermission error = %q, want the ask reported as not pending", err)
	}
	if err := provider.AnswerPermission(context.Background(), session.SessionID, id, avruntime.PermissionResponse{Allow: true}); err != nil {
		t.Fatalf("AnswerPermission for the owning session: %v", err)
	}
	awaitPrompt(t, done)
	end := log.waitFor(t, "session.end", 30*time.Second)
	if reason, _ := end.Fields["stop_reason"].(string); reason != "end_turn" {
		t.Fatalf("stop_reason = %q, want end_turn: the run did not carry on", reason)
	}
	if !endpoint.sawToolText("relay-foreign") {
		t.Fatalf("the model never read the allowed call's output; tool messages were %#v", endpoint.toolTexts())
	}
}

// rewriteSettings replaces the fixture's settings document, so a test can
// change what the next process reads without moving the launch.
func (f pooFixture) rewriteSettings(t *testing.T, mutate func(map[string]any)) {
	t.Helper()
	raw, err := os.ReadFile(f.settings)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	mutate(document)
	encoded, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.settings, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
}

// sessionHistory reads a session's recorded history from the provider's own
// process, so a test can see what a resumed session restored.
func sessionHistory(t *testing.T, provider *Provider, sessionID string) []protocol.HistoryItem {
	t.Helper()
	c, err := provider.getClient()
	if err != nil {
		t.Fatalf("the resumed provider has no process: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	raw, rpcErr, err := c.call(ctx, protocol.MethodSessionHistory, protocol.HistoryParams{SessionID: sessionID, Limit: 500})
	if err != nil {
		t.Fatalf("session/history: %v", err)
	}
	if rpcErr != nil {
		t.Fatalf("session/history: %s", describeRefusal(rpcErr))
	}
	var page protocol.HistoryResult
	if err := json.Unmarshal(raw, &page); err != nil {
		t.Fatalf("decode session/history result: %v", err)
	}
	return page.Items
}

// livePooProcesses returns the pids whose command line names a fixture's
// settings file. The path is unique per fixture, so this finds only the
// processes the test itself started.
func livePooProcesses(t *testing.T, settings string) []int {
	t.Helper()
	entries, err := os.ReadDir("/proc")
	if err != nil {
		t.Skipf("cannot read /proc: %v", err)
	}
	var pids []int
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		cmdline, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "cmdline"))
		if err != nil {
			continue
		}
		if strings.Contains(string(cmdline), settings) {
			pids = append(pids, pid)
		}
	}
	return pids
}

// awaitNoPooProcesses fails when a poo process the fixture started still runs.
// Teardown reaps the process before it returns, so the wait only covers the
// window between the signal and the kernel dropping the entry.
func awaitNoPooProcesses(t *testing.T, settings string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		pids := livePooProcesses(t, settings)
		if len(pids) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("poo processes still running for this fixture: %v", pids)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestResumeContinuesAnInterruptedRun proves a resume after a crash continues
// the session the dead process left: the interrupted call is in the restored
// history, and a follow-up prompt carries the prior turn to the model.
func TestResumeContinuesAnInterruptedRun(t *testing.T) {
	endpoint := newFakeEndpoint(t,
		scriptedTurn{ToolCalls: []scriptedToolCall{{Name: "bash", Arguments: map[string]any{"command": "sleep 5"}}}},
		scriptedTurn{Text: "the answer after the resume"},
	)
	fixture := newPooFixture(t, endpoint)
	opts := fixture.startOptions(t)

	first, session, log, _ := startProvider(t, opts)
	done := startPrompt(first, session.SessionID, "do the thing")
	log.waitFor(t, "tool.call", 60*time.Second)

	proc, err := os.FindProcess(session.PID)
	if err != nil {
		t.Fatalf("find poo process: %v", err)
	}
	if err := proc.Kill(); err != nil {
		t.Fatalf("kill poo process: %v", err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Prompt returned nil after the process was killed")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Prompt did not fail after the process was killed")
	}
	// The dead child's stdin is already closed, so teardown reports the write
	// it could not make; the process itself is gone.
	_ = first.Close()

	second := NewWithOptions(opts)
	t.Cleanup(func() { _ = second.Close() })
	resumed, err := second.Resume(context.Background(), session.SessionID)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if resumed.SessionID != session.SessionID {
		t.Fatalf("Resume returned session %q, want %q", resumed.SessionID, session.SessionID)
	}
	if resumed.Backend != backendID {
		t.Fatalf("Resume reported backend %q, want %q", resumed.Backend, backendID)
	}
	if resumed.PID == 0 {
		t.Fatal("Resume reported no pid")
	}
	// The dead process is gone and the resumed one is the only poo this
	// fixture runs, which is what makes the teardown check below meaningful.
	if live := livePooProcesses(t, fixture.settings); len(live) != 1 || live[0] != resumed.PID {
		t.Fatalf("running poo processes = %v, want only the resumed %d", live, resumed.PID)
	}

	history := sessionHistory(t, second, session.SessionID)
	var sawInterrupted, sawPrompt bool
	for _, item := range history {
		if item.Type == "tool_result" && item.Outcome == "interrupted" {
			sawInterrupted = true
		}
		if item.Type == "message" && strings.Contains(item.Text, "do the thing") {
			sawPrompt = true
		}
	}
	if !sawInterrupted {
		t.Fatalf("the restored history holds no interrupted call: %#v", history)
	}
	if !sawPrompt {
		t.Fatalf("the restored history holds no record of the interrupted run's prompt: %#v", history)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := second.Events(ctx, session.SessionID)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	after := collect(ctx, ch)
	awaitPrompt(t, startPrompt(second, session.SessionID, "carry on"))
	end := after.waitFor(t, "session.end", 60*time.Second)
	if reason, _ := end.Fields["stop_reason"].(string); reason != "end_turn" {
		t.Fatalf("stop_reason = %q, want end_turn: the resumed session did not carry on", reason)
	}
	if code, _ := events.Int64(end.Fields["exit_code"]); code != 0 {
		t.Fatalf("exit_code = %d, want 0", code)
	}

	texts := endpoint.promptTexts()
	carried := texts[len(texts)-1]
	if !strings.Contains(carried, "do the thing") || !strings.Contains(carried, "carry on") {
		t.Fatalf("the resumed request carried %q, want the interrupted prompt and the new one", carried)
	}

	if err := second.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	awaitNoPooProcesses(t, fixture.settings)
}

// TestResumeRefusesASessionTheSettingsNoLongerFit proves the session's own
// model is what decides: when the settings stop naming it, the resume fails as
// an unfitting session instead of continuing under another model, and the
// process the attempt started is reaped.
func TestResumeRefusesASessionTheSettingsNoLongerFit(t *testing.T) {
	endpoint := newFakeEndpoint(t, scriptedTurn{Text: "hello from poo"})
	fixture := newPooFixture(t, endpoint)

	first, session, _, _ := startProvider(t, fixture.startOptions(t))
	awaitPrompt(t, startPrompt(first, session.SessionID, "say hello"))
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	fixture.rewriteSettings(t, func(document map[string]any) {
		document["models"] = map[string]any{
			"other-model": map[string]any{
				"endpoint":      "fake",
				"model_id":      "other-model",
				"api":           "chat_completions",
				"tools_mode":    "native",
				"ops":           []string{"chat"},
				"context_limit": 200000,
			},
		}
		document["roles"] = map[string]any{"chat": "other-model"}
	})

	provider := NewWithOptions(fixture.startOptions(t))
	t.Cleanup(func() { _ = provider.Close() })
	_, err := provider.Resume(context.Background(), session.SessionID)
	if err == nil {
		t.Fatal("Resume accepted a session whose recorded model the settings no longer name")
	}
	for _, want := range []string{"cannot resume session", session.SessionID, "does not fit this build and settings", "unknown_model"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Resume error = %q, want %q", err, want)
		}
	}
	if _, err := provider.getClient(); err == nil {
		t.Fatal("a refused resume left its process running")
	}
	awaitNoPooProcesses(t, fixture.settings)
}

// TestResumeRefusesASessionWithNoRecords proves both ways a session cannot be
// continued: one created ephemeral, which keeps no records, and one this poo
// has never seen. Both are the unresumable-session failure, and neither leaves
// a process behind.
func TestResumeRefusesASessionWithNoRecords(t *testing.T) {
	t.Run("ephemeral", func(t *testing.T) {
		endpoint := newFakeEndpoint(t, scriptedTurn{Text: "hello from poo"})
		fixture := newPooFixture(t, endpoint)
		fixture.rewriteSettings(t, func(document map[string]any) {
			document["session"].(map[string]any)["mode"] = "ephemeral"
		})

		first, session, _, _ := startProvider(t, fixture.startOptions(t))
		awaitPrompt(t, startPrompt(first, session.SessionID, "say hello"))
		if err := first.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		provider := NewWithOptions(fixture.startOptions(t))
		t.Cleanup(func() { _ = provider.Close() })
		_, err := provider.Resume(context.Background(), session.SessionID)
		assertSessionNotFound(t, err, session.SessionID)
		if _, err := provider.getClient(); err == nil {
			t.Fatal("a refused resume left its process running")
		}
		awaitNoPooProcesses(t, fixture.settings)
	})

	t.Run("unknown", func(t *testing.T) {
		endpoint := newFakeEndpoint(t, scriptedTurn{Text: "unused"})
		fixture := newPooFixture(t, endpoint)

		provider := NewWithOptions(fixture.startOptions(t))
		t.Cleanup(func() { _ = provider.Close() })
		const sessionID = "ses-01J0000000000000000000000"
		if _, err := provider.Resume(context.Background(), sessionID); err == nil {
			t.Fatalf("Resume(%q) returned no error", sessionID)
		} else {
			assertSessionNotFound(t, err, sessionID)
		}
		if _, err := provider.getClient(); err == nil {
			t.Fatal("a refused resume left its process running")
		}
		awaitNoPooProcesses(t, fixture.settings)
	})
}

// TestResumeReturnsTheLiveSessionItAlreadyServes proves the supervisor's retry
// path works: Resume on a provider that already serves the session hands that
// live session back instead of refusing it, with no second process. A process
// that has died falls through to the cross-process resume a fresh provider
// would take, and the session continues in a new process.
func TestResumeReturnsTheLiveSessionItAlreadyServes(t *testing.T) {
	endpoint := newFakeEndpoint(t,
		scriptedTurn{Text: "hello from poo"},
		scriptedTurn{Text: "carried on"},
	)
	fixture := newPooFixture(t, endpoint)
	provider := NewWithOptions(fixture.startOptions(t))
	t.Cleanup(func() { _ = provider.Close() })
	session, err := provider.Start(context.Background(), avruntime.StartOptions{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	awaitPrompt(t, startPrompt(provider, session.SessionID, "say hello"))

	resumed, err := provider.Resume(context.Background(), session.SessionID)
	if err != nil {
		t.Fatalf("Resume of the live session: %v", err)
	}
	if resumed.SessionID != session.SessionID {
		t.Fatalf("Resume returned session %q, want %q", resumed.SessionID, session.SessionID)
	}
	if resumed.PID != session.PID {
		t.Fatalf("Resume reported pid %d, want the live session's %d", resumed.PID, session.PID)
	}
	if live := livePooProcesses(t, fixture.settings); len(live) != 1 || live[0] != session.PID {
		t.Fatalf("running poo processes = %v, want only the original %d", live, session.PID)
	}

	// The live session still serves prompts after the resume.
	awaitPrompt(t, startPrompt(provider, session.SessionID, "again"))

	proc, err := os.FindProcess(session.PID)
	if err != nil {
		t.Fatalf("find poo process: %v", err)
	}
	if err := proc.Kill(); err != nil {
		t.Fatalf("kill poo process: %v", err)
	}
	// The dead process is reaped before the resume, so the provider sees the
	// death instead of racing the reap.
	awaitNoPooProcesses(t, fixture.settings)
	resumed, err = provider.Resume(context.Background(), session.SessionID)
	if err != nil {
		t.Fatalf("Resume after the process died: %v", err)
	}
	if resumed.SessionID != session.SessionID {
		t.Fatalf("the resumed session is %q, want %q", resumed.SessionID, session.SessionID)
	}
	if resumed.PID == 0 || resumed.PID == session.PID {
		t.Fatalf("Resume reported pid %d, want a fresh live process", resumed.PID)
	}
	if live := livePooProcesses(t, fixture.settings); len(live) != 1 || live[0] != resumed.PID {
		t.Fatalf("running poo processes = %v, want only the resumed %d", live, resumed.PID)
	}
	awaitPrompt(t, startPrompt(provider, session.SessionID, "carry on"))
	texts := endpoint.promptTexts()
	carried := texts[len(texts)-1]
	if !strings.Contains(carried, "say hello") || !strings.Contains(carried, "carry on") {
		t.Fatalf("the resumed request carried %q, want the earlier turn and the new prompt", carried)
	}

	if err := provider.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	awaitNoPooProcesses(t, fixture.settings)
}

// TestResumeRefusesADifferentSessionWhileOneIsLive proves the one-session
// refusal keeps its meaning: a resume naming a session this provider does not
// serve is refused, and the live session keeps its process.
func TestResumeRefusesADifferentSessionWhileOneIsLive(t *testing.T) {
	endpoint := newFakeEndpoint(t, scriptedTurn{Text: "hello from poo"})
	fixture := newPooFixture(t, endpoint)
	provider, session, _, _ := startProvider(t, fixture.startOptions(t))
	awaitPrompt(t, startPrompt(provider, session.SessionID, "say hello"))

	resumed, err := provider.Resume(context.Background(), "ses-01J0000000000000000000000")
	if err == nil {
		t.Fatal("Resume of a different session returned no error")
	}
	if !strings.Contains(err.Error(), "poo runs one session per process") {
		t.Fatalf("Resume error = %q, want the one-session refusal", err)
	}
	if resumed.SessionID != "" {
		t.Fatalf("the refused resume returned session %q, want none", resumed.SessionID)
	}
	if live := livePooProcesses(t, fixture.settings); len(live) != 1 || live[0] != session.PID {
		t.Fatalf("running poo processes = %v, want only the original %d", live, session.PID)
	}
}

// assertSessionNotFound checks the failure is the unresumable-session class,
// which names the session it could not continue.
func assertSessionNotFound(t *testing.T, err error, sessionID string) {
	t.Helper()
	if err == nil {
		t.Fatalf("resume of %q returned no error", sessionID)
	}
	if !strings.Contains(err.Error(), "session not found: "+sessionID) {
		t.Fatalf("resume error = %q, want the session reported as not found", err)
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

// TestPromptReturnsWhenThePromptContextIsCanceled proves a prompt parked in a
// run/inspect wait ends when its own context does: Prompt returns the
// cancellation quickly, and teardown leaves no process behind.
func TestPromptReturnsWhenThePromptContextIsCanceled(t *testing.T) {
	t.Setenv(helperEnv, "noinspect")
	provider := NewWithOptions(avruntime.StartOptions{})
	t.Cleanup(func() { _ = provider.Close() })

	session, err := provider.Start(context.Background(), helperStartOptions())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	promptDone := make(chan error, 1)
	go func() { promptDone <- provider.Prompt(ctx, session.SessionID, "go") }()
	// The run/prompt answer lands at once and the inspect wait never does, so
	// a prompt that is still running after this has to be parked in the wait.
	time.Sleep(500 * time.Millisecond)
	cancel()

	select {
	case err := <-promptDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Prompt error = %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Prompt did not return after its context was canceled")
	}

	if err := provider.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := syscall.Kill(session.PID, 0); err == nil {
		t.Fatalf("poo process %d survived Close", session.PID)
	}
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

	rateLimited := settledEvent(sessionEvent{
		SessionID: "ses_1",
		Type:      "run.settled",
		Fields: map[string]any{
			"reason": "provider_limit",
			"detail": map[string]any{
				"provider": map[string]any{"kind": "rate_limited", "retry_after_s": 12},
			},
		},
	})
	if reason, _ := rateLimited.Fields["stop_reason"].(string); reason != avruntime.RateLimitedStopReason {
		t.Fatalf("rate limited stop_reason = %q, want %q", reason, avruntime.RateLimitedStopReason)
	}
	if retry, _ := events.Int64(rateLimited.Fields["retry_after"]); retry != 12 {
		t.Fatalf("retry_after = %#v, want 12", rateLimited.Fields["retry_after"])
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
			if mode == "noinspect" {
				// The wait is never answered, so only the prompt's own
				// context can end a prompt parked here.
				continue
			}
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
