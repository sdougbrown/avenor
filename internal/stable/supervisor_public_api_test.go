package stable

// Public-API tests for the stable Supervisor. Every scenario builds the
// supervisor with NewSupervisor, starts its control server, and drives it only
// through the caller-facing surface: Spawn, RuntimeStatus, RuntimeWaitTurn,
// RuntimeResult, RuntimeCancel, RuntimePrompt, RuntimeAnswerPermission, List,
// Shutdown, a connected control client, and the control event stream. The one
// seam is the provider factory (newProviderFunc), which stands in for the
// backend process; scripted providers emit the same events a backend would,
// including permission.request.
//
// Assertions are limited to what a caller can see: status and phase after
// runstate.Translate, the completion sentinel, RuntimeResult, and emitted
// events.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runstate"
	"github.com/sdougbrown/avenor/internal/runtime"
)

const (
	itestTimeout = 10 * time.Second
	// itestQuietWindow bounds how long a negative assertion (something must
	// not happen) observes the supervisor.
	itestQuietWindow = 200 * time.Millisecond
	// Decider limits as callers experience them.
	itestDeciderSpawnBound = 3
	itestDeciderCap        = 3
)

// itestEventLog records every event published on the control stream so tests
// can wait for a specific event without sleeping.
type itestEventLog struct {
	mu      sync.Mutex
	events  []events.Event
	changed chan struct{}
}

func newItestEventLog(ch <-chan events.Event) *itestEventLog {
	l := &itestEventLog{changed: make(chan struct{})}
	go func() {
		for ev := range ch {
			l.mu.Lock()
			l.events = append(l.events, ev)
			close(l.changed)
			l.changed = make(chan struct{})
			l.mu.Unlock()
		}
	}()
	return l
}

func (l *itestEventLog) all() []events.Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]events.Event(nil), l.events...)
}

// find returns the first recorded event matching pred.
func (l *itestEventLog) find(pred func(events.Event) bool) (events.Event, bool) {
	for _, ev := range l.all() {
		if pred(ev) {
			return ev, true
		}
	}
	return events.Event{}, false
}

// waitFor blocks until an event matching pred has been recorded.
func (l *itestEventLog) waitFor(t *testing.T, what string, pred func(events.Event) bool) events.Event {
	t.Helper()
	deadline := time.After(itestTimeout)
	for {
		l.mu.Lock()
		changed := l.changed
		var found *events.Event
		for i := range l.events {
			if pred(l.events[i]) {
				ev := l.events[i]
				found = &ev
				break
			}
		}
		l.mu.Unlock()
		if found != nil {
			return *found
		}
		select {
		case <-changed:
		case <-deadline:
			t.Fatalf("timed out waiting for event: %s", what)
		}
	}
}

func itestEventIs(name, runtimeID string) func(events.Event) bool {
	return func(ev events.Event) bool {
		if ev.Event != name {
			return false
		}
		id, _ := ev.Fields["runtime_id"].(string)
		return id == runtimeID
	}
}

func itestStopReasonIs(runtimeID, stopReason string) func(events.Event) bool {
	isEnd := itestEventIs("session.end", runtimeID)
	return func(ev events.Event) bool {
		got, _ := ev.Fields["stop_reason"].(string)
		return isEnd(ev) && got == stopReason
	}
}

func (l *itestEventLog) count(pred func(events.Event) bool) int {
	n := 0
	for _, ev := range l.all() {
		if pred(ev) {
			n++
		}
	}
	return n
}

// itestHarness is a production-shaped supervisor plus the caller-side views
// tests need.
type itestHarness struct {
	t            *testing.T
	sup          *Supervisor
	socket       string
	log          *itestEventLog
	shutdownOnce sync.Once
}

// newItestHarness builds a supervisor from cfg (defaulting the control socket
// and MaxRuntimes), installs factory as the provider source, and starts the
// control server.
func newItestHarness(t *testing.T, cfg Config, factory func(runtime.StartOptions, string) (runtime.Provider, error)) *itestHarness {
	t.Helper()
	if cfg.ControlSocket == "" {
		cfg.ControlSocket = newStableSocketPath(t, "itest")
	}
	if cfg.MaxRuntimes == 0 {
		cfg.MaxRuntimes = 10
	}
	sup := NewSupervisor(cfg)
	sup.newProviderFunc = factory
	if err := sup.control.Start(cfg.ControlSocket); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	h := &itestHarness{t: t, sup: sup, socket: cfg.ControlSocket}
	h.log = newItestEventLog(sup.control.SubscribeEvents(ctx))
	t.Cleanup(func() {
		h.shutdown("kill")
		sup.control.Stop()
		cancel()
	})
	return h
}

func (h *itestHarness) shutdown(mode string) {
	h.shutdownOnce.Do(func() {
		if err := h.sup.Shutdown(mode); err != nil {
			h.t.Errorf("Shutdown(%q): %v", mode, err)
		}
	})
}

// connectClient attaches a control client, which makes the control plane the
// resolver for permission requests that have no decider or other owner.
func (h *itestHarness) connectClient() *client.Client {
	h.t.Helper()
	c, err := client.Dial(h.socket)
	if err != nil {
		h.t.Fatalf("dial control socket: %v", err)
	}
	h.t.Cleanup(func() { c.Close() })
	// A round trip proves the server has registered the connection before any
	// request depends on a connected client.
	if _, err := c.Identity(); err != nil {
		h.t.Fatalf("control client identity: %v", err)
	}
	return c
}

// spawn starts a runtime through Spawn and returns its ID. params override the
// defaults (prompt, dir, backend).
func (h *itestHarness) spawn(params map[string]any) string {
	h.t.Helper()
	id, err := h.trySpawn(params)
	if err != nil {
		h.t.Fatalf("Spawn: %v", err)
	}
	return id
}

func (h *itestHarness) trySpawn(params map[string]any) (string, error) {
	merged := map[string]any{"prompt": "go", "dir": h.t.TempDir(), "backend": "pony"}
	for k, v := range params {
		merged[k] = v
	}
	raw, err := json.Marshal(merged)
	if err != nil {
		h.t.Fatalf("marshal spawn params: %v", err)
	}
	out, err := h.sup.Spawn(raw)
	if err != nil {
		return "", err
	}
	res, ok := out.(SpawnResult)
	if !ok || res.RuntimeID == "" {
		h.t.Fatalf("Spawn result = %#v, want SpawnResult with a runtime id", out)
	}
	return res.RuntimeID, nil
}

// itestView is a runtime as a consumer sees it: the raw RuntimeStatus
// normalised through runstate.Translate, with the completion sentinel applied
// to ended runtimes.
type itestView struct {
	Status            string
	Phase             string
	TurnComplete      bool
	StopReason        string
	PendingPermission bool
	DeciderExhausted  bool
	FinalOutput       string
}

func (h *itestHarness) view(rtID string) itestView {
	h.t.Helper()
	out, err := h.sup.RuntimeStatus(rtID)
	if err != nil {
		h.t.Fatalf("RuntimeStatus(%s): %v", rtID, err)
	}
	raw, _ := out.(map[string]any)
	rawStatus, _ := raw["status"].(string)
	rawPhase, _ := raw["phase"].(string)
	tr := runstate.Translate(rawStatus, rawPhase)
	v := itestView{Status: tr.Status, Phase: tr.Phase, TurnComplete: tr.TurnComplete}
	v.PendingPermission, _ = raw["pending_permission"].(bool)
	v.DeciderExhausted, _ = raw["permission_decider_exhausted"].(bool)
	v.FinalOutput, _ = raw["final_output"].(string)
	if rawStatus == "ended" {
		if path, _ := raw["sentinel_file"].(string); path != "" {
			if data, err := os.ReadFile(path); err == nil {
				v.Status, v.StopReason = itestSentinelStatus(string(data))
				v.TurnComplete = runstate.IsTerminalStatus(v.Status)
			}
		}
	}
	return v
}

// itestSentinelStatus maps a completion sentinel to the consumer-facing
// status and its stop reason.
func itestSentinelStatus(sentinel string) (status, stopReason string) {
	lines := strings.Split(strings.TrimSpace(sentinel), "\n")
	switch lines[0] {
	case "DONE":
		status = "done"
	case "FAILED", "BLOCKED":
		status = "failed"
	case "TIMEOUT":
		status = "timeout"
	case "KILLED":
		status = "killed"
	default:
		status = strings.ToLower(lines[0])
	}
	for _, line := range lines[1:] {
		if v, ok := strings.CutPrefix(line, "STOP_REASON="); ok {
			stopReason = v
		}
	}
	return status, stopReason
}

// awaitStatus polls RuntimeStatus, as consumer await loops do, until the
// translated status equals want. Status transitions that follow a turn
// settling have no wake-up channel of their own.
func (h *itestHarness) awaitStatus(rtID, want string) itestView {
	h.t.Helper()
	deadline := time.Now().Add(itestTimeout)
	for {
		v := h.view(rtID)
		if v.Status == want {
			return v
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("runtime %s status = %q (phase %q), want %q", rtID, v.Status, v.Phase, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (h *itestHarness) awaitCondition(what string, cond func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(itestTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			h.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// consistently fails the test if cond becomes false within the quiet window.
func (h *itestHarness) consistently(what string, cond func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(itestQuietWindow)
	for time.Now().Before(deadline) {
		if !cond() {
			h.t.Fatalf("%s did not hold", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (h *itestHarness) waitTurn(rtID string) map[string]any {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), itestTimeout)
	defer cancel()
	out, err := h.sup.RuntimeWaitTurn(ctx, rtID, 0)
	if err != nil {
		h.t.Fatalf("RuntimeWaitTurn(%s): %v", rtID, err)
	}
	res, _ := out.(map[string]any)
	return res
}

func (h *itestHarness) result(rtID string) string {
	h.t.Helper()
	out, err := h.sup.RuntimeResult(rtID)
	if err != nil {
		h.t.Fatalf("RuntimeResult(%s): %v", rtID, err)
	}
	res, _ := out.(map[string]any)
	output, _ := res["final_output"].(string)
	return output
}

// listed returns the List entry for each runtime whose label has prefix.
func (h *itestHarness) listed(labelPrefix string) []map[string]any {
	rows, _ := h.sup.List().([]map[string]any)
	var out []map[string]any
	for _, row := range rows {
		if label, _ := row["label"].(string); strings.HasPrefix(label, labelPrefix) {
			out = append(out, row)
		}
	}
	return out
}

// itestProvider wraps a stableScriptedProvider with call counters and hooks.
type itestProvider struct {
	*stableScriptedProvider
	mu       sync.Mutex
	starts   int
	resumes  int
	cancels  int
	answers  []runtime.PermissionResponse
	onPrompt func(ctx context.Context)
	onAnswer func(requestID string, resp runtime.PermissionResponse)
}

func newItestProvider(attempts ...stableScriptedAttempt) *itestProvider {
	return &itestProvider{stableScriptedProvider: &stableScriptedProvider{attempt: -1, scripts: attempts}}
}

func (p *itestProvider) Start(ctx context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	p.mu.Lock()
	p.starts++
	p.mu.Unlock()
	return p.stableScriptedProvider.Start(ctx, opts)
}

func (p *itestProvider) Resume(ctx context.Context, sessionID string) (runtime.Session, error) {
	p.mu.Lock()
	p.resumes++
	p.mu.Unlock()
	return p.stableScriptedProvider.Resume(ctx, sessionID)
}

func (p *itestProvider) Prompt(ctx context.Context, sessionID, text string) error {
	if p.onPrompt != nil {
		p.onPrompt(ctx)
	}
	return p.stableScriptedProvider.Prompt(ctx, sessionID, text)
}

func (p *itestProvider) Cancel(ctx context.Context, sessionID string) error {
	p.mu.Lock()
	p.cancels++
	p.mu.Unlock()
	return p.stableScriptedProvider.Cancel(ctx, sessionID)
}

func (p *itestProvider) AnswerPermission(_ context.Context, _ string, requestID string, resp runtime.PermissionResponse) error {
	p.mu.Lock()
	p.answers = append(p.answers, resp)
	p.mu.Unlock()
	if p.onAnswer != nil {
		p.onAnswer(requestID, resp)
	}
	return nil
}

func (p *itestProvider) counts() (starts, resumes int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.starts, p.resumes
}

func (p *itestProvider) recordedAnswers() []runtime.PermissionResponse {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]runtime.PermissionResponse(nil), p.answers...)
}

func itestAttempt(sessionID string, steps ...stableScriptedEvent) stableScriptedAttempt {
	return stableScriptedAttempt{sessionID: sessionID, events: steps}
}

func itestSessionEnd(sessionID, stopReason, output string) stableScriptedEvent {
	fields := map[string]any{"stop_reason": stopReason}
	if output != "" {
		fields["final_output"] = output
	}
	return stableScriptedEvent{event: events.Event{Event: "session.end", SessionID: sessionID, Fields: fields}}
}

func itestPermissionRequest(sessionID, requestID string) stableScriptedEvent {
	return stableScriptedEvent{event: events.Event{
		Event:     "permission.request",
		SessionID: sessionID,
		Fields: map[string]any{
			"request_id": requestID,
			"tool":       "bash",
			"command":    "rm -rf build",
			"options": []any{
				map[string]any{"optionId": "allow_once", "kind": "allow"},
				map[string]any{"optionId": "reject_once", "kind": "reject"},
			},
		},
	}}
}

// itestGate is a one-shot channel a script step can wait on.
type itestGate struct {
	ch   chan struct{}
	once sync.Once
}

func newItestGate() *itestGate { return &itestGate{ch: make(chan struct{})} }

func (g *itestGate) open() { g.once.Do(func() { close(g.ch) }) }

func (g *itestGate) step() stableScriptedEvent {
	return stableScriptedEvent{release: g.ch}
}

func itestSingleProvider(p runtime.Provider) func(runtime.StartOptions, string) (runtime.Provider, error) {
	return func(runtime.StartOptions, string) (runtime.Provider, error) { return p, nil }
}

// Lifecycle scenarios.

func TestSupervisorPublicAPISuccess(t *testing.T) {
	t.Parallel()
	provider := newItestProvider(itestAttempt("ses_ok", itestSessionEnd("ses_ok", "end_turn", "all done")))
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))

	rt := h.spawn(map[string]any{"label": "success"})
	h.log.waitFor(t, "session.end", itestStopReasonIs(rt, "end_turn"))
	v := h.awaitStatus(rt, "done")
	if !v.TurnComplete {
		t.Fatalf("done view TurnComplete = false, want true")
	}
	if v.FinalOutput != "all done" {
		t.Fatalf("status final_output = %q, want %q", v.FinalOutput, "all done")
	}
	if got := h.result(rt); got != "all done" {
		t.Fatalf("RuntimeResult final_output = %q, want %q", got, "all done")
	}
	if starts, resumes := provider.counts(); starts != 1 || resumes != 0 {
		t.Fatalf("provider starts/resumes = %d/%d, want 1/0", starts, resumes)
	}
	rows := h.listed("success")
	if len(rows) != 1 || rows[0]["runtime_id"] != rt {
		t.Fatalf("List = %v, want the one runtime %s", rows, rt)
	}
}

func TestSupervisorPublicAPIRetryWithBackoffThenSuccess(t *testing.T) {
	t.Parallel()
	provider := newItestProvider(
		itestAttempt("ses_try1", itestSessionEnd("ses_try1", "error", "")),
		itestAttempt("ses_try2", itestSessionEnd("ses_try2", "end_turn", "recovered")),
	)
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))

	rt := h.spawn(map[string]any{"max_retries": 1})
	h.log.waitFor(t, "failed attempt session.end", itestStopReasonIs(rt, "error"))
	// The failed attempt is not a terminal outcome: the runtime is backing
	// off to retry.
	v := h.view(rt)
	if v.TurnComplete || v.Status != "running" {
		t.Fatalf("view after failed attempt = %+v, want running and not complete", v)
	}
	h.log.waitFor(t, "retry session.end", itestStopReasonIs(rt, "end_turn"))
	h.awaitStatus(rt, "done")
	if got := h.result(rt); got != "recovered" {
		t.Fatalf("RuntimeResult final_output = %q, want %q", got, "recovered")
	}
	if starts, resumes := provider.counts(); starts != 1 || resumes != 1 {
		t.Fatalf("provider starts/resumes = %d/%d, want 1/1 (retry resumes the session)", starts, resumes)
	}
}

func TestSupervisorPublicAPIRetryExhaustion(t *testing.T) {
	t.Parallel()
	provider := newItestProvider(
		itestAttempt("ses_ex1", itestSessionEnd("ses_ex1", "error", "")),
		itestAttempt("ses_ex2", itestSessionEnd("ses_ex2", "error", "")),
	)
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))

	rt := h.spawn(map[string]any{"max_retries": 1})
	// The runtime ends once the retry budget is spent, so WaitTurn returns the
	// terminal turn without waiting for another one.
	turn := h.waitTurn(rt)
	if got, _ := turn["stop_reason"].(string); got != "error" {
		t.Fatalf("WaitTurn stop_reason = %q, want error", got)
	}
	v := h.awaitStatus(rt, "failed")
	if v.StopReason != "error" {
		t.Fatalf("failed view stop reason = %q, want error", v.StopReason)
	}
	if n := h.log.count(itestStopReasonIs(rt, "error")); n != 2 {
		t.Fatalf("failed session.end events = %d, want 2 (initial attempt plus one retry)", n)
	}
	if starts, resumes := provider.counts(); starts != 1 || resumes != 1 {
		t.Fatalf("provider starts/resumes = %d/%d, want 1/1", starts, resumes)
	}
}

func TestSupervisorPublicAPICancelWhileRunning(t *testing.T) {
	t.Parallel()
	gate := newItestGate()
	defer gate.open()
	provider := newItestProvider(itestAttempt("ses_cr",
		stableScriptedEvent{event: events.Event{Event: "agent.status", SessionID: "ses_cr", Fields: map[string]any{"phase": "working"}}},
		gate.step(),
		itestSessionEnd("ses_cr", "end_turn", "never reached"),
	))
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))

	rt := h.spawn(nil)
	h.log.waitFor(t, "working status", itestEventIs("agent.status", rt))
	if v := h.view(rt); v.Status != "running" {
		t.Fatalf("view before cancel = %+v, want running", v)
	}
	if err := h.sup.RuntimeCancel(rt); err != nil {
		t.Fatalf("RuntimeCancel: %v", err)
	}
	turn := h.waitTurn(rt)
	if got, _ := turn["stop_reason"].(string); got != "cancelled" {
		t.Fatalf("WaitTurn stop_reason = %q, want cancelled", got)
	}
	v := h.awaitStatus(rt, "killed")
	if v.StopReason != "cancelled" {
		t.Fatalf("killed view stop reason = %q, want cancelled", v.StopReason)
	}
	if got := h.result(rt); got == "never reached" {
		t.Fatal("cancelled runtime produced the output of a turn it never finished")
	}
}

func TestSupervisorPublicAPICancelDuringBackoff(t *testing.T) {
	t.Skip("#319: cancelling a runtime during retry backoff ends it with exit code 0, no sentinel and no cancelled session.end, so consumers see done instead of killed")
	t.Parallel()
	provider := newItestProvider(
		itestAttempt("ses_cb1", itestSessionEnd("ses_cb1", "error", "")),
		itestAttempt("ses_cb2", itestSessionEnd("ses_cb2", "end_turn", "retry ran anyway")),
	)
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))

	rt := h.spawn(map[string]any{"max_retries": 1})
	h.log.waitFor(t, "failed attempt session.end", itestStopReasonIs(rt, "error"))
	if err := h.sup.RuntimeCancel(rt); err != nil {
		t.Fatalf("RuntimeCancel: %v", err)
	}
	h.waitTurn(rt)
	v := h.awaitStatus(rt, "killed")
	if v.StopReason != "cancelled" {
		t.Fatalf("killed view stop reason = %q, want cancelled", v.StopReason)
	}
	// The cancelled backoff must not wake into another attempt.
	h.consistently("no retry attempt after cancel", func() bool {
		_, resumes := provider.counts()
		return resumes == 0 && h.log.count(itestStopReasonIs(rt, "end_turn")) == 0
	})
}

func TestSupervisorPublicAPIParkAndResume(t *testing.T) {
	t.Parallel()
	gate := newItestGate()
	defer gate.open()
	provider := newItestProvider(
		itestAttempt("ses_park1", itestSessionEnd("ses_park1", "end_turn", "first")),
		itestAttempt("ses_park2", gate.step(), itestSessionEnd("ses_park2", "end_turn", "second")),
	)
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))

	rt := h.spawn(nil)
	h.awaitStatus(rt, "done")
	if got := h.result(rt); got != "first" {
		t.Fatalf("RuntimeResult after first turn = %q, want first", got)
	}
	// A finished turn parks the runtime; a prompt resumes the session for
	// another turn.
	if err := h.sup.RuntimePrompt(rt, "again", ""); err != nil {
		t.Fatalf("RuntimePrompt: %v", err)
	}
	h.awaitStatus(rt, "running")
	if _, err := h.sup.RuntimeWaitTurn(context.Background(), rt, 100*time.Millisecond); !errors.Is(err, control.ErrWaitTurnTimeout) {
		t.Fatalf("RuntimeWaitTurn while the resumed turn is in flight = %v, want ErrWaitTurnTimeout", err)
	}
	gate.open()
	h.log.waitFor(t, "second turn session.end", func(ev events.Event) bool {
		out, _ := ev.Fields["final_output"].(string)
		return itestEventIs("session.end", rt)(ev) && out == "second"
	})
	h.awaitStatus(rt, "done")
	if got := h.result(rt); got != "second" {
		t.Fatalf("RuntimeResult after resumed turn = %q, want second", got)
	}
	if starts, resumes := provider.counts(); starts != 1 || resumes != 1 {
		t.Fatalf("provider starts/resumes = %d/%d, want 1/1", starts, resumes)
	}
}

func TestSupervisorPublicAPIShutdownWithActiveRuntimes(t *testing.T) {
	t.Parallel()
	gate := newItestGate()
	defer gate.open()
	var n atomic.Int64
	factory := func(runtime.StartOptions, string) (runtime.Provider, error) {
		id := fmt.Sprintf("ses_sd%d", n.Add(1))
		working := stableScriptedEvent{event: events.Event{Event: "agent.status", SessionID: id, Fields: map[string]any{"phase": "working"}}}
		return newItestProvider(itestAttempt(id, working, gate.step(), itestSessionEnd(id, "end_turn", "never"))), nil
	}
	h := newItestHarness(t, Config{}, factory)

	rt1 := h.spawn(map[string]any{"label": "sd-one"})
	rt2 := h.spawn(map[string]any{"label": "sd-two"})
	for _, rt := range []string{rt1, rt2} {
		h.log.waitFor(t, "working status", itestEventIs("agent.status", rt))
		if v := h.view(rt); v.Status != "running" {
			t.Fatalf("runtime %s before shutdown = %+v, want running", rt, v)
		}
	}
	if err := h.sup.Shutdown("sideways"); err == nil {
		t.Fatal("Shutdown with an unknown mode succeeded, want an error")
	}
	h.shutdown("graceful")
	for _, rt := range []string{rt1, rt2} {
		v := h.awaitStatus(rt, "killed")
		if v.StopReason != "cancelled" {
			t.Fatalf("runtime %s stop reason = %q, want cancelled", rt, v.StopReason)
		}
	}
	if _, err := h.trySpawn(map[string]any{"label": "after-shutdown"}); err == nil {
		t.Fatal("Spawn after Shutdown succeeded, want rejection")
	}
}

// Permission scenarios.

// itestWorker builds the provider for a worker that raises one permission
// request and finishes only after the request is answered.
func itestWorker(sessionID, requestID, output string) *itestProvider {
	gate := newItestGate()
	p := newItestProvider(itestAttempt(sessionID,
		itestPermissionRequest(sessionID, requestID),
		gate.step(),
		itestSessionEnd(sessionID, "end_turn", output),
	))
	p.onAnswer = func(string, runtime.PermissionResponse) { gate.open() }
	return p
}

func TestSupervisorPublicAPIPermissionRequestAnswerContinue(t *testing.T) {
	t.Parallel()
	worker := itestWorker("ses_pw", "req_human", "after answer")
	h := newItestHarness(t, Config{}, itestSingleProvider(worker))
	h.connectClient()

	rt := h.spawn(nil)
	h.log.waitFor(t, "permission.request", itestEventIs("permission.request", rt))
	v := h.view(rt)
	if !v.PendingPermission || v.Status != "running" {
		t.Fatalf("view with a pending request = %+v, want running with pending_permission", v)
	}
	if got := h.log.count(itestStopReasonIs(rt, "end_turn")); got != 0 {
		t.Fatalf("runtime finished before the request was answered (%d session.end events)", got)
	}
	if err := h.sup.RuntimeAnswerPermission(rt, "req_human", "allow_once", ""); err != nil {
		t.Fatalf("RuntimeAnswerPermission: %v", err)
	}
	h.log.waitFor(t, "permission.response", itestEventIs("permission.response", rt))
	h.awaitStatus(rt, "done")
	if v := h.view(rt); v.PendingPermission {
		t.Fatal("pending_permission still set after the answer")
	}
	if got := h.result(rt); got != "after answer" {
		t.Fatalf("RuntimeResult = %q, want %q", got, "after answer")
	}
	answers := worker.recordedAnswers()
	if len(answers) != 1 || !answers[0].Allow || answers[0].OptionID != "allow_once" {
		t.Fatalf("backend permission answers = %+v, want one allow_once", answers)
	}
}

func itestDecider() map[string]any {
	return map[string]any{"entry": "guard", "backend": "pony", "agent": "guard", "model": "guard-model"}
}

// itestDeciderLabel splits a decider run label into the worker runtime and
// request it was spawned for.
func itestDeciderLabel(label string) (runtimeID, requestID string) {
	parts := strings.SplitN(strings.TrimPrefix(label, "permdec:"), ":", 2)
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
}

// itestDeciderFactory routes provider requests: runs labelled permdec: go to
// decider, everything else to worker(label).
func itestDeciderFactory(worker func(label string) runtime.Provider, decider func(opts runtime.StartOptions) (runtime.Provider, error)) func(runtime.StartOptions, string) (runtime.Provider, error) {
	return func(opts runtime.StartOptions, _ string) (runtime.Provider, error) {
		if strings.HasPrefix(opts.Label, "permdec:") {
			return decider(opts)
		}
		return worker(opts.Label), nil
	}
}

// itestAnsweringDecider is a decider run that answers the request it was
// spawned for through RuntimeAnswerPermission, as an agent would through the
// avenor_answer_permission tool, then finishes its turn.
func itestAnsweringDecider(harness func() *itestHarness, optionID string, answered chan<- error) func(runtime.StartOptions) (runtime.Provider, error) {
	var n atomic.Int64
	return func(opts runtime.StartOptions) (runtime.Provider, error) {
		rtID, reqID := itestDeciderLabel(opts.Label)
		sessionID := fmt.Sprintf("ses_decider_%d", n.Add(1))
		p := newItestProvider(itestAttempt(sessionID, itestSessionEnd(sessionID, "end_turn", "")))
		p.onPrompt = func(context.Context) {
			err := harness().sup.RuntimeAnswerPermission(rtID, reqID, optionID, "")
			if answered != nil {
				answered <- err
			}
		}
		return p, nil
	}
}

func TestSupervisorPublicAPIDeciderAllow(t *testing.T) {
	t.Parallel()
	worker := itestWorker("ses_dw_allow", "req_allow", "allowed work")
	answered := make(chan error, 1)
	var h *itestHarness
	decider := itestAnsweringDecider(func() *itestHarness { return h }, "allow_once", answered)
	h = newItestHarness(t, Config{}, itestDeciderFactory(func(string) runtime.Provider { return worker }, decider))

	rt := h.spawn(map[string]any{"label": "worker", "permission_decider": itestDecider()})
	h.log.waitFor(t, "permission.request", itestEventIs("permission.request", rt))
	if err := <-answered; err != nil {
		t.Fatalf("decider answer: %v", err)
	}
	h.awaitStatus(rt, "done")
	if got := h.result(rt); got != "allowed work" {
		t.Fatalf("RuntimeResult = %q, want %q", got, "allowed work")
	}
	answers := worker.recordedAnswers()
	if len(answers) != 1 || !answers[0].Allow || answers[0].OptionID != "allow_once" {
		t.Fatalf("backend permission answers = %+v, want one allow_once", answers)
	}
	if rows := h.listed("permdec:"); len(rows) != 1 {
		t.Fatalf("decider runs listed = %d, want 1", len(rows))
	}
}

func TestSupervisorPublicAPIDeciderDeny(t *testing.T) {
	t.Parallel()
	worker := itestWorker("ses_dw_deny", "req_deny", "continued after deny")
	answered := make(chan error, 1)
	var h *itestHarness
	decider := itestAnsweringDecider(func() *itestHarness { return h }, "reject_once", answered)
	h = newItestHarness(t, Config{}, itestDeciderFactory(func(string) runtime.Provider { return worker }, decider))

	rt := h.spawn(map[string]any{"label": "worker", "permission_decider": itestDecider()})
	h.log.waitFor(t, "permission.request", itestEventIs("permission.request", rt))
	if err := <-answered; err != nil {
		t.Fatalf("decider answer: %v", err)
	}
	h.log.waitFor(t, "permission.response", itestEventIs("permission.response", rt))
	answers := worker.recordedAnswers()
	if len(answers) != 1 || answers[0].Allow || answers[0].OptionID != "reject_once" {
		t.Fatalf("backend permission answers = %+v, want one reject_once", answers)
	}
	resp, _ := h.log.find(itestEventIs("permission.response", rt))
	if kind, _ := resp.Fields["kind"].(string); kind != "reject" {
		t.Fatalf("permission.response kind = %v, want reject", resp.Fields["kind"])
	}
	h.awaitStatus(rt, "done")
}

func TestSupervisorPublicAPIDeciderSpawnFailureRetries(t *testing.T) {
	t.Parallel()
	worker := itestWorker("ses_dw_retry", "req_retry", "after retry")
	var deciderCalls atomic.Int64
	var h *itestHarness
	answering := itestAnsweringDecider(func() *itestHarness { return h }, "allow_once", nil)
	h = newItestHarness(t, Config{}, itestDeciderFactory(
		func(string) runtime.Provider { return worker },
		func(opts runtime.StartOptions) (runtime.Provider, error) {
			if deciderCalls.Add(1) == 1 {
				return nil, errors.New("decider backend unavailable")
			}
			return answering(opts)
		},
	))

	rt := h.spawn(map[string]any{"label": "worker", "permission_decider": itestDecider()})
	// The backend sends permission.request once and blocks; the failed decider
	// spawn must be retried without a further event.
	h.awaitStatus(rt, "done")
	if got := deciderCalls.Load(); got != 2 {
		t.Fatalf("decider spawns = %d, want 2 (one failure, one retry)", got)
	}
	if got := h.log.count(itestEventIs("permission.request", rt)); got != 1 {
		t.Fatalf("permission.request events = %d, want 1", got)
	}
	answers := worker.recordedAnswers()
	if len(answers) != 1 || !answers[0].Allow {
		t.Fatalf("backend permission answers = %+v, want one allow", answers)
	}
	if v := h.view(rt); v.DeciderExhausted {
		t.Fatal("permission_decider_exhausted set although the retry succeeded")
	}
}

func TestSupervisorPublicAPIDeciderSpawnFailuresExhaustToHuman(t *testing.T) {
	t.Parallel()
	worker := itestWorker("ses_dw_exhaust", "req_exhaust", "human approved")
	var deciderCalls atomic.Int64
	h := newItestHarness(t, Config{}, itestDeciderFactory(
		func(string) runtime.Provider { return worker },
		func(runtime.StartOptions) (runtime.Provider, error) {
			deciderCalls.Add(1)
			return nil, errors.New("decider backend unavailable")
		},
	))

	rt := h.spawn(map[string]any{"label": "worker", "permission_decider": itestDecider()})
	h.awaitCondition("decider retries to be exhausted", func() bool { return h.view(rt).DeciderExhausted })
	if got := deciderCalls.Load(); got != itestDeciderSpawnBound {
		t.Fatalf("decider spawns = %d, want the bound of %d", got, itestDeciderSpawnBound)
	}
	h.consistently("exhausted request stays pending without further spawns", func() bool {
		v := h.view(rt)
		return deciderCalls.Load() == itestDeciderSpawnBound && v.PendingPermission && v.Status == "running"
	})
	// The request stays open for a human, who can still answer it.
	if err := h.sup.RuntimeAnswerPermission(rt, "req_exhaust", "allow_once", ""); err != nil {
		t.Fatalf("human RuntimeAnswerPermission: %v", err)
	}
	h.awaitStatus(rt, "done")
	if got := h.result(rt); got != "human approved" {
		t.Fatalf("RuntimeResult = %q, want %q", got, "human approved")
	}
}

func TestSupervisorPublicAPIDeciderConcurrencyCapReleasesBlockedRequest(t *testing.T) {
	t.Parallel()
	const workers = itestDeciderCap + 1
	providers := map[string]*itestProvider{}
	for i := 1; i <= workers; i++ {
		label := fmt.Sprintf("w%d", i)
		providers[label] = itestWorker("ses_cap_"+label, "req_"+label, label+" finished")
	}
	var spawns atomic.Int64
	holds := map[string]*itestGate{}
	var holdsMu sync.Mutex
	hold := func(label string) *itestGate {
		holdsMu.Lock()
		defer holdsMu.Unlock()
		if holds[label] == nil {
			holds[label] = newItestGate()
		}
		return holds[label]
	}
	started := make(chan string, workers)
	var h *itestHarness
	h = newItestHarness(t, Config{}, itestDeciderFactory(
		func(label string) runtime.Provider { return providers[label] },
		func(opts runtime.StartOptions) (runtime.Provider, error) {
			spawns.Add(1)
			rtID, reqID := itestDeciderLabel(opts.Label)
			sessionID := fmt.Sprintf("ses_cap_decider_%d", spawns.Load())
			p := newItestProvider(itestAttempt(sessionID, itestSessionEnd(sessionID, "end_turn", "")))
			g := hold(reqID)
			p.onPrompt = func(ctx context.Context) {
				started <- reqID
				// The first deciders hold their slot until released; the
				// request that was blocked by the cap gets a decider that
				// answers at once.
				if reqID != fmt.Sprintf("req_w%d", workers) {
					select {
					case <-g.ch:
					case <-ctx.Done():
						return
					}
				}
				_ = h.sup.RuntimeAnswerPermission(rtID, reqID, "allow_once", "")
			}
			return p, nil
		},
	))
	t.Cleanup(func() {
		holdsMu.Lock()
		defer holdsMu.Unlock()
		for _, g := range holds {
			g.open()
		}
	})

	ids := map[string]string{}
	for i := 1; i <= itestDeciderCap; i++ {
		label := fmt.Sprintf("w%d", i)
		ids[label] = h.spawn(map[string]any{"label": label, "permission_decider": itestDecider()})
		<-started
	}
	// The cap is full: the next request is routed but no decider runs for it.
	blockedLabel := fmt.Sprintf("w%d", workers)
	ids[blockedLabel] = h.spawn(map[string]any{"label": blockedLabel, "permission_decider": itestDecider()})
	h.log.waitFor(t, "blocked permission.request", itestEventIs("permission.request", ids[blockedLabel]))
	h.consistently("decider cap holds", func() bool {
		v := h.view(ids[blockedLabel])
		return spawns.Load() == itestDeciderCap && v.PendingPermission && v.Status == "running"
	})
	if rows := h.listed("permdec:"); len(rows) != itestDeciderCap {
		t.Fatalf("decider runs listed = %d, want %d", len(rows), itestDeciderCap)
	}
	// Cancelling a decider run frees its slot; the blocked request is routed
	// to a new decider without another permission.request event.
	rows := h.listed("permdec:" + ids["w1"] + ":")
	if len(rows) != 1 {
		t.Fatalf("decider runs for w1 = %d, want 1", len(rows))
	}
	if err := h.sup.RuntimeCancel(rows[0]["runtime_id"].(string)); err != nil {
		t.Fatalf("cancel decider: %v", err)
	}
	h.awaitStatus(ids[blockedLabel], "done")
	if got := h.result(ids[blockedLabel]); got != blockedLabel+" finished" {
		t.Fatalf("blocked worker result = %q", got)
	}
	if got := spawns.Load(); got != itestDeciderCap+1 {
		t.Fatalf("decider spawns = %d, want %d", got, itestDeciderCap+1)
	}
	if got := h.log.count(itestEventIs("permission.request", ids[blockedLabel])); got != 1 {
		t.Fatalf("permission.request events for the blocked worker = %d, want 1", got)
	}
}

func TestSupervisorPublicAPIDeciderSlotFreedWhenDeciderFinishes(t *testing.T) {
	t.Skip("#320: a decider run that finishes its judgment parks instead of exiting and keeps its concurrency slot, so a request blocked by the cap stays pending")
	t.Parallel()
	const workers = itestDeciderCap + 1
	var h *itestHarness
	h = newItestHarness(t, Config{}, itestDeciderFactory(
		func(label string) runtime.Provider {
			return itestWorker("ses_slot_"+label, "req_"+label, label+" finished")
		},
		itestAnsweringDecider(func() *itestHarness { return h }, "allow_once", nil),
	))
	for i := 1; i <= workers; i++ {
		label := fmt.Sprintf("w%d", i)
		rt := h.spawn(map[string]any{"label": label, "permission_decider": itestDecider()})
		h.awaitStatus(rt, "done")
	}
}

func TestSupervisorPublicAPIPermissionClaimTimeoutFallsBackToFileHandler(t *testing.T) {
	t.Parallel()
	worker := itestWorker("ses_claim_file", "req_claim", "answered by file")
	h := newItestHarness(t, Config{PermissionClaimTimeout: 100 * time.Millisecond}, itestSingleProvider(worker))
	h.connectClient()
	base := filepath.Join(t.TempDir(), "perm")

	rt := h.spawn(map[string]any{"permission_handler": "file:" + base})
	h.log.waitFor(t, "permission.request", itestEventIs("permission.request", rt))
	// Nobody answers over the control plane: once the claim times out the
	// request moves to the file handler, which publishes the request file.
	h.awaitCondition("permission request file", func() bool {
		_, err := os.Stat(base + ".req")
		return err == nil
	})
	if v := h.view(rt); !v.PendingPermission || v.Status != "running" {
		t.Fatalf("view after claim timeout = %+v, want running with pending_permission", v)
	}
	if err := os.WriteFile(base+".req.response", []byte(`{"outcome":"selected","option_id":"allow_once"}`), 0o600); err != nil {
		t.Fatalf("write permission response: %v", err)
	}
	h.awaitStatus(rt, "done")
	if got := h.result(rt); got != "answered by file" {
		t.Fatalf("RuntimeResult = %q, want %q", got, "answered by file")
	}
	answers := worker.recordedAnswers()
	if len(answers) != 1 || !answers[0].Allow || answers[0].OptionID != "allow_once" {
		t.Fatalf("backend permission answers = %+v, want one allow_once", answers)
	}
}

func TestSupervisorPublicAPIDeciderDeferLeavesRequestForHuman(t *testing.T) {
	t.Parallel()
	worker := itestWorker("ses_defer", "req_defer", "human decided")
	var deciderRan = make(chan struct{}, 1)
	h := newItestHarness(t, Config{PermissionClaimTimeout: 100 * time.Millisecond}, itestDeciderFactory(
		func(string) runtime.Provider { return worker },
		func(runtime.StartOptions) (runtime.Provider, error) {
			// A deferring decider exits without answering.
			p := newItestProvider(itestAttempt("ses_defer_decider", itestSessionEnd("ses_defer_decider", "end_turn", "")))
			p.onPrompt = func(context.Context) { deciderRan <- struct{}{} }
			return p, nil
		},
	))

	rt := h.spawn(map[string]any{"label": "worker", "permission_decider": itestDecider()})
	<-deciderRan
	// The claim times out with no control or file resolver; the request is
	// still pending and the backend is still waiting.
	h.consistently("deferred request stays pending", func() bool {
		v := h.view(rt)
		return v.PendingPermission && v.Status == "running"
	})
	if err := h.sup.RuntimeAnswerPermission(rt, "req_defer", "allow_once", ""); err != nil {
		t.Fatalf("human RuntimeAnswerPermission: %v", err)
	}
	h.awaitStatus(rt, "done")
	if got := h.result(rt); got != "human decided" {
		t.Fatalf("RuntimeResult = %q, want %q", got, "human decided")
	}
}
