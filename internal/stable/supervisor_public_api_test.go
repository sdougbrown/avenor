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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/admission"
	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/permission"
	"github.com/sdougbrown/avenor/internal/runstate"
	"github.com/sdougbrown/avenor/internal/runtime"
)

const (
	itestTimeout = 10 * time.Second
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

// itestRecv bounds a channel receive by the test timeout so a silent sender
// cannot hang the suite.
func itestRecv[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(itestTimeout):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
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

// itestEventTS returns the supervisor's publish timestamp (the ts field, in
// Unix milliseconds) of ev.
func itestEventTS(t *testing.T, ev events.Event) time.Time {
	t.Helper()
	ts, ok := ev.Fields["ts"].(int64)
	if !ok {
		t.Fatalf("%s event has no numeric ts field: %#v", ev.Event, ev.Fields["ts"])
	}
	return time.UnixMilli(ts)
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
	sup := newTestSupervisor(t, cfg)
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
	ExitCode          int
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
	v.ExitCode, _ = raw["exit_code"].(int)
	v.PendingPermission, _ = raw["pending_permission"].(bool)
	v.DeciderExhausted, _ = raw["permission_decider_exhausted"].(bool)
	v.FinalOutput, _ = raw["final_output"].(string)
	if rawStatus == "ended" {
		if path, _ := raw["sentinel_file"].(string); path != "" {
			if data, err := os.ReadFile(path); err == nil {
				var sentinelExit int
				v.Status, v.StopReason, sentinelExit = itestSentinelStatus(string(data))
				v.TurnComplete = runstate.IsTerminalStatus(v.Status)
				if sentinelExit >= 0 {
					v.ExitCode = sentinelExit
				}
			}
		}
	}
	return v
}

// itestSentinelStatus reads a completion sentinel and returns the
// consumer-facing status, its stop reason, and the sentinel's declared exit
// code (-1 when the sentinel does not declare one).
func itestSentinelStatus(sentinel string) (status, stopReason string, exitCode int) {
	exitCode = -1
	lines := strings.Split(strings.TrimSpace(sentinel), "\n")
	status = runstate.SentinelStatus(lines[0])
	for _, line := range lines[1:] {
		if v, ok := strings.CutPrefix(line, "STOP_REASON="); ok {
			stopReason = v
		}
		if v, ok := strings.CutPrefix(line, "EXIT_CODE="); ok {
			if n, err := strconv.Atoi(v); err == nil {
				exitCode = n
			}
		}
	}
	return status, stopReason, exitCode
}

// rawExitCode reads exit_code straight from RuntimeStatus, before view applies
// the completion sentinel, so tests can pin the exit code the runtime itself
// recorded.
func (h *itestHarness) rawExitCode(rtID string) int {
	h.t.Helper()
	out, err := h.sup.RuntimeStatus(rtID)
	if err != nil {
		h.t.Fatalf("RuntimeStatus(%s): %v", rtID, err)
	}
	raw, _ := out.(map[string]any)
	code, _ := raw["exit_code"].(int)
	return code
}

// requireSessionEnd waits for the runtime's session.end carrying stopReason
// and fails unless its exit_code field matches want.
func (h *itestHarness) requireSessionEnd(rtID, stopReason string, want int) {
	h.t.Helper()
	end := h.log.waitFor(h.t, stopReason+" session.end", itestStopReasonIs(rtID, stopReason))
	if code, _ := end.Fields["exit_code"].(int); code != want {
		h.t.Fatalf("%s session.end exit_code = %d, want %d", stopReason, code, want)
	}
}

// requireIdleEnd proves a runtime's idle end end-to-end: the translated
// status carries stopReason, the raw exit code matches exitCode, the
// session.end event carries stopReason with exitCode, and WaitTurn reports
// stopReason. It returns the view from awaitStatus so callers can make
// extra checks.
func (h *itestHarness) requireIdleEnd(rtID, status, stopReason string, exitCode int) itestView {
	h.t.Helper()
	v := h.awaitStatus(rtID, status)
	if v.StopReason != stopReason {
		h.t.Fatalf("%s view stop reason = %q, want %s", status, v.StopReason, stopReason)
	}
	if code := h.rawExitCode(rtID); code != exitCode {
		h.t.Fatalf("raw RuntimeStatus exit_code = %d, want %d", code, exitCode)
	}
	h.requireSessionEnd(rtID, stopReason, exitCode)
	turn := h.waitTurn(rtID)
	if got, _ := turn["stop_reason"].(string); got != stopReason {
		h.t.Fatalf("WaitTurn stop_reason = %q, want %s", got, stopReason)
	}
	return v
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

// consistentlyFor fails the test if cond becomes false within the given
// window. Callers that must outlast a retry backoff pass a window longer
// than the backoff delay.
func (h *itestHarness) consistentlyFor(window time.Duration, what string, cond func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(window)
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
// resumeCalls records every Resume argument and promptTexts records every
// Prompt text argument, so tests can pin resume-ID fidelity and prompt
// fidelity against the production paths.
type itestProvider struct {
	*stableScriptedProvider
	mu          sync.Mutex
	starts      int
	resumes     int
	cancels     int
	answers     []runtime.PermissionResponse
	resumeCalls []string
	promptTexts []string
	onPrompt    func(ctx context.Context)
	onAnswer    func(requestID string, resp runtime.PermissionResponse)
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
	p.resumeCalls = append(p.resumeCalls, sessionID)
	p.mu.Unlock()
	return p.stableScriptedProvider.Resume(ctx, sessionID)
}

func (p *itestProvider) Prompt(ctx context.Context, sessionID, text string) error {
	p.mu.Lock()
	p.promptTexts = append(p.promptTexts, text)
	p.mu.Unlock()
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

// resumeIDs returns every session ID passed to Resume, in call order.
func (p *itestProvider) resumeIDs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.resumeCalls...)
}

// recordedPrompts returns every prompt text passed to Prompt, in call order.
func (p *itestProvider) recordedPrompts() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.promptTexts...)
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
	// The finished turn parks the runtime awaiting a follow-up prompt, so the
	// caller sees an idle row carrying the turn's zero exit code.
	if got := rows[0]["status"]; got != "idle" {
		t.Fatalf("List status = %v, want idle", got)
	}
	if got := rows[0]["exit_code"]; got != 0 {
		t.Fatalf("List exit_code = %v, want 0", got)
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
	failedEnd := h.log.waitFor(t, "failed attempt session.end", itestStopReasonIs(rt, "error"))
	// The failed attempt is not a terminal outcome: the runtime is backing
	// off to retry. The backoff is observable: the retry's session.end must
	// trail the failed attempt by at least one backoff delay. Both gaps use
	// the supervisor's publish timestamps, so observer lag cannot skew them.
	v := h.view(rt)
	if v.TurnComplete || v.Status != "running" {
		t.Fatalf("view after failed attempt = %+v, want running and not complete", v)
	}
	retryEnd := h.log.waitFor(t, "retry session.end", itestStopReasonIs(rt, "end_turn"))
	// Millisecond timestamps truncate, so allow one millisecond of rounding.
	minGap := backoffDelay(1) - time.Millisecond
	if gap := itestEventTS(t, retryEnd).Sub(itestEventTS(t, failedEnd)); gap < minGap {
		t.Fatalf("gap from failed attempt to retry = %s, want at least %s (backoff delay %s)", gap, minGap, backoffDelay(1))
	}
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
	// The retry budget is spent and the runtime has ended, so WaitTurn
	// returns the terminal snapshot through its completed branch without
	// waiting for a further turn.
	h.awaitStatus(rt, "failed")
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

// TestSupervisorPublicAPIRunDeadlineTimesOut proves a caller-set run deadline
// ends a turn that never settles: consumers see the timeout status, the
// TIMEOUT sentinel and exit code 124.
func TestSupervisorPublicAPIRunDeadlineTimesOut(t *testing.T) {
	t.Parallel()
	gate := newItestGate()
	defer gate.open()
	// The scripted attempt never reaches a session.end: only the run
	// deadline can stop the turn.
	provider := newItestProvider(itestAttempt("ses_deadline", gate.step()))
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))

	rt := h.spawn(map[string]any{"label": "deadline", "timeout": 1})
	h.log.waitFor(t, "timeout session.end", itestStopReasonIs(rt, "timeout"))
	v := h.awaitStatus(rt, "timeout")
	if v.StopReason != "timeout" {
		t.Fatalf("timeout view stop reason = %q, want timeout", v.StopReason)
	}
	if v.ExitCode != 124 {
		t.Fatalf("timeout view exit code = %d, want 124", v.ExitCode)
	}
	turn := h.waitTurn(rt)
	if got, _ := turn["stop_reason"].(string); got != "timeout" {
		t.Fatalf("WaitTurn stop_reason = %q, want timeout", got)
	}
	if _, err := h.sup.RuntimeResult(rt); err != nil {
		t.Fatalf("RuntimeResult after timeout: %v", err)
	}
}

// TestSupervisorPublicAPIRunDeadlineWhileParkedTimesOut proves a run deadline
// that passes while the runtime is parked after a finished turn ends it as a
// timeout: consumers see the timeout status, raw exit code 124, and a timeout
// session.end.
func TestSupervisorPublicAPIRunDeadlineWhileParkedTimesOut(t *testing.T) {
	t.Parallel()
	provider := newItestProvider(itestAttempt("ses_dpark", itestSessionEnd("ses_dpark", "end_turn", "parked work")))
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))

	// A three-second deadline leaves the first turn room to finish before it.
	rt := h.spawn(map[string]any{"label": "deadline-parked", "timeout": 3})
	h.awaitStatus(rt, "done")
	if got := h.result(rt); got != "parked work" {
		t.Fatalf("RuntimeResult after the turn = %q, want %q", got, "parked work")
	}
	// The turn is over and the runtime is parked waiting for a follow-up
	// prompt. The run deadline ends the parked runtime as a timeout, matching
	// a deadline during an attempt.
	h.requireIdleEnd(rt, "timeout", "timeout", 124)
}

// TestSupervisorPublicAPIRunDeadlineDuringBackoffTimesOut proves a run
// deadline that passes while the runtime is inside a retry backoff ends it as
// a timeout instead of letting the backoff wake into another attempt:
// consumers see the timeout status, raw exit code 124, and a timeout
// session.end, and the provider is never resumed for the retry.
func TestSupervisorPublicAPIRunDeadlineDuringBackoffTimesOut(t *testing.T) {
	t.Parallel()
	provider := newItestProvider(itestAttempt("ses_dback", itestSessionEnd("ses_dback", "error", "")))
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))

	// backoffDelay(1) is 2s, so the 1s run deadline lands inside the backoff
	// that follows the retryable failed attempt.
	rt := h.spawn(map[string]any{"label": "deadline-backoff", "max_retries": 1, "timeout": 1})
	h.log.waitFor(t, "failed attempt session.end", itestStopReasonIs(rt, "error"))
	// WaitTurn reports the idle end's stop reason, not the failed attempt's;
	// reading it after the runtime has ended avoids racing the failed
	// attempt's own settle.
	h.requireIdleEnd(rt, "timeout", "timeout", 124)
	// The deadline must end the run inside the backoff: the retry attempt is
	// never started.
	if starts, resumes := provider.counts(); starts != 1 || resumes != 0 {
		t.Fatalf("provider starts/resumes = %d/%d, want 1/0", starts, resumes)
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
	if v := h.view(rt); v.Status != "running" || v.Phase != "working" {
		t.Fatalf("view before cancel = %+v, want running in the working phase", v)
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
	t.Parallel()
	provider := newItestProvider(
		itestAttempt("ses_cb1", itestSessionEnd("ses_cb1", "error", "")),
		itestAttempt("ses_cb2", itestSessionEnd("ses_cb2", "end_turn", "retry ran anyway")),
	)
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))

	rt := h.spawn(map[string]any{"max_retries": 1})
	h.log.waitFor(t, "failed attempt session.end", itestStopReasonIs(rt, "error"))
	// The failed attempt has just ended and the runtime is heading into the
	// retry backoff. RuntimeCancel is level-triggered on the run context, so
	// issuing it now is guaranteed to be in force by the time the backoff
	// select runs — the backoff cannot fire into another attempt. (Waiting
	// for a turn settle here would be wrong: WaitTurn waits for the next
	// settle, which is the retry itself.)
	if err := h.sup.RuntimeCancel(rt); err != nil {
		t.Fatalf("RuntimeCancel: %v", err)
	}
	// WaitTurn reports the idle end's stop reason, not the failed attempt's;
	// reading it after the end avoids racing the failed attempt's own settle.
	v := h.requireIdleEnd(rt, "killed", "cancelled", 130)
	// The idle end records a terminal phase, so consumers see the turn as
	// complete even though the runtime was killed mid-backoff.
	if v.Phase != "done" {
		t.Fatalf("killed view phase = %q, want done", v.Phase)
	}
	// The cancelled backoff must not wake into another attempt. The window
	// outlasts the backoff delay so a late Resume cannot slip through.
	h.consistentlyFor(backoffDelay(1)+500*time.Millisecond, "no retry attempt after cancel", func() bool {
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

// TestSupervisorPublicAPIPromptQueuedDuringActiveTurn proves a prompt queued
// while a turn is still running is not lost when that turn ends: the runtime
// runs the queued prompt as its next turn with no further RuntimePrompt to
// wake it.
func TestSupervisorPublicAPIPromptQueuedDuringActiveTurn(t *testing.T) {
	t.Parallel()
	gate := newItestGate()
	defer gate.open()
	provider := newItestProvider(
		itestAttempt("ses_q1",
			stableScriptedEvent{event: events.Event{Event: "agent.status", SessionID: "ses_q1", Fields: map[string]any{"phase": "working"}}},
			gate.step(),
			itestSessionEnd("ses_q1", "end_turn", "first turn"),
		),
		itestAttempt("ses_q2", itestSessionEnd("ses_q2", "end_turn", "queued turn")),
	)
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))

	rt := h.spawn(map[string]any{"label": "queued"})
	h.log.waitFor(t, "working status", itestEventIs("agent.status", rt))
	// The turn is in flight: the queued prompt must run as the next turn the
	// moment this one ends, without another RuntimePrompt after the settle.
	if err := h.sup.RuntimePrompt(rt, "follow-up", ""); err != nil {
		t.Fatalf("RuntimePrompt during an active turn: %v", err)
	}
	gate.open()
	h.log.waitFor(t, "queued turn session.end", func(ev events.Event) bool {
		out, _ := ev.Fields["final_output"].(string)
		return itestEventIs("session.end", rt)(ev) && out == "queued turn"
	})
	v := h.awaitStatus(rt, "done")
	if v.FinalOutput != "queued turn" {
		t.Fatalf("status final_output = %q, want %q", v.FinalOutput, "queued turn")
	}
	if got := h.result(rt); got != "queued turn" {
		t.Fatalf("RuntimeResult final_output = %q, want %q", got, "queued turn")
	}
	if starts, resumes := provider.counts(); starts != 1 || resumes != 1 {
		t.Fatalf("provider starts/resumes = %d/%d, want 1/1 (queued prompt resumes the session)", starts, resumes)
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
		if v := h.view(rt); v.Status != "running" || v.Phase != "working" {
			t.Fatalf("runtime %s before shutdown = %+v, want running in the working phase", rt, v)
		}
	}
	// Both rows are live mid-turn while the runtimes run.
	if rows := h.listed("sd-"); len(rows) != 2 {
		t.Fatalf("List = %d rows, want 2", len(rows))
	} else {
		for _, row := range rows {
			if got := row["status"]; got != "running" {
				t.Fatalf("List status before shutdown = %v, want running", got)
			}
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
	// Shutdown cancels the mid-turn runtimes, so their rows carry the ended
	// status and the 130 exit code a cancelled turn records.
	rows := h.listed("sd-")
	if len(rows) != 2 {
		t.Fatalf("List after shutdown = %d rows, want 2", len(rows))
	}
	for _, row := range rows {
		if got := row["status"]; got != "ended" {
			t.Fatalf("List status after shutdown = %v, want ended", got)
		}
		if got := row["exit_code"]; got != 130 {
			t.Fatalf("List exit_code after shutdown = %v, want 130", got)
		}
	}
	if _, err := h.trySpawn(map[string]any{"label": "after-shutdown"}); err == nil {
		t.Fatal("Spawn after Shutdown succeeded, want rejection")
	}
}

// TestSupervisorPublicAPIShutdownKillEndsActiveRuntime proves kill-mode
// Shutdown tears down a mid-turn runtime and leaves the caller-visible killed
// end state.
func TestSupervisorPublicAPIShutdownKillEndsActiveRuntime(t *testing.T) {
	t.Parallel()
	gate := newItestGate()
	defer gate.open()
	provider := newItestProvider(itestAttempt("ses_sk",
		stableScriptedEvent{event: events.Event{Event: "agent.status", SessionID: "ses_sk", Fields: map[string]any{"phase": "working"}}},
		gate.step(),
		itestSessionEnd("ses_sk", "end_turn", "never reached"),
	))
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))

	rt := h.spawn(map[string]any{"label": "kill-shutdown"})
	h.log.waitFor(t, "working status", itestEventIs("agent.status", rt))
	if v := h.view(rt); v.Status != "running" || v.Phase != "working" {
		t.Fatalf("view before kill shutdown = %+v, want running in the working phase", v)
	}
	h.shutdown("kill")
	v := h.awaitStatus(rt, "killed")
	if v.StopReason != "cancelled" {
		t.Fatalf("killed view stop reason = %q, want cancelled", v.StopReason)
	}
	if v.ExitCode != 130 {
		t.Fatalf("killed view exit code = %d, want 130", v.ExitCode)
	}
	if got := h.result(rt); got == "never reached" {
		t.Fatal("shutdown runtime produced the output of a turn it never finished")
	}
}

// TestSupervisorPublicAPIShutdownGracefulEndsParkedRuntime proves graceful
// Shutdown ends a runtime parked after a finished turn, replacing the parked
// done view with the killed end state callers observe.
func TestSupervisorPublicAPIShutdownGracefulEndsParkedRuntime(t *testing.T) {
	t.Parallel()
	provider := newItestProvider(itestAttempt("ses_gp", itestSessionEnd("ses_gp", "end_turn", "parked work")))
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))

	rt := h.spawn(map[string]any{"label": "parked-shutdown"})
	h.awaitStatus(rt, "done")
	if got := h.result(rt); got != "parked work" {
		t.Fatalf("RuntimeResult after the turn = %q, want %q", got, "parked work")
	}
	// The runtime is parked waiting for a follow-up prompt. Graceful
	// shutdown ends it instead of leaving it idle.
	h.shutdown("graceful")
	v := h.requireIdleEnd(rt, "killed", "cancelled", 130)
	if v.ExitCode != 130 {
		t.Fatalf("killed view exit code = %d, want 130", v.ExitCode)
	}
	// The turn ended successfully before shutdown, so Translate passes the
	// done phase through to the killed view.
	if v.Phase != "done" {
		t.Fatalf("killed view phase = %q, want done", v.Phase)
	}
}

// TestSupervisorPublicAPIRunDeadlineWhileWaitingForTreeBudget proves a run
// deadline that passes while a resumed runtime is blocked waiting for a
// tree-budget slot ends it as a timeout instead of returning silently, which
// would leave the parked turn's DONE sentinel standing.
func TestSupervisorPublicAPIRunDeadlineWhileWaitingForTreeBudget(t *testing.T) {
	t.Parallel()
	gate := newItestGate()
	defer gate.open()
	var n atomic.Int64
	factory := func(runtime.StartOptions, string) (runtime.Provider, error) {
		id := fmt.Sprintf("ses_tb%d", n.Add(1))
		if n.Load() == 1 {
			return newItestProvider(itestAttempt(id, itestSessionEnd(id, "end_turn", "parked work"))), nil
		}
		working := stableScriptedEvent{event: events.Event{Event: "agent.status", SessionID: id, Fields: map[string]any{"phase": "working"}}}
		return newItestProvider(itestAttempt(id, working, gate.step(), itestSessionEnd(id, "end_turn", "holder done"))), nil
	}
	// A one-slot tree budget: the holder's in-flight turn occupies the only
	// slot, so a resumed peer blocks in the budget wait until its run
	// deadline passes.
	h := newItestHarness(t, Config{MaxTreeBudget: 1}, factory)

	// A five-second run deadline leaves a wide margin over setup, which is
	// measured from the spawn: slow setup must not let the deadline fire
	// before the runtime is parked in the tree-budget wait.
	const runDeadline = 5 * time.Second
	spawnedAt := time.Now()
	rt := h.spawn(map[string]any{"label": "budget-parked", "timeout": int(runDeadline / time.Second)})
	h.awaitStatus(rt, "done")
	// The parked runtime releases its slot after the turn; the holder spawn
	// can race that release, so retry the admission until it succeeds.
	var holder string
	h.awaitCondition("a free tree slot for the holder spawn", func() bool {
		id, err := h.trySpawn(map[string]any{"label": "budget-holder"})
		var ce *admission.CapacityError
		if err != nil && !errors.As(err, &ce) {
			h.t.Fatalf("holder spawn: %v", err)
		}
		if err != nil {
			return false
		}
		holder = id
		return true
	})
	h.log.waitFor(t, "holder working status", itestEventIs("agent.status", holder))
	// The follow-up prompt resumes the parked runtime, which blocks in the
	// tree-budget wait for the rest of its run deadline.
	if err := h.sup.RuntimePrompt(rt, "again", ""); err != nil {
		t.Fatalf("RuntimePrompt: %v", err)
	}
	// Wait out most of the run deadline, then prove the runtime is still
	// blocked in the tree-budget wait: it has not ended yet, and the holder
	// still holds the only slot. A deadline that fired in the parked wait or
	// during setup would already have ended the runtime and fails here.
	if remaining := runDeadline - 500*time.Millisecond - time.Since(spawnedAt); remaining > 0 {
		time.Sleep(remaining)
	}
	if v := h.view(rt); v.Status == "timeout" {
		t.Fatalf("runtime ended before its deadline: view %+v", v)
	}
	if v := h.view(holder); v.Status != "running" {
		t.Fatalf("holder status just before the deadline = %q, want running", v.Status)
	}
	h.requireIdleEnd(rt, "timeout", "timeout", 124)
	// The holder is unaffected: released from the gate, its turn finishes
	// normally.
	gate.open()
	h.awaitStatus(holder, "done")
}

// TestSupervisorPublicAPIRunDeadlineWhileWaitingForLocalSlot proves a run
// deadline that passes while a resumed runtime is blocked waiting for a local
// MaxRuntimes slot ends it as a timeout instead of returning silently, which
// would leave the parked turn's DONE sentinel standing.
func TestSupervisorPublicAPIRunDeadlineWhileWaitingForLocalSlot(t *testing.T) {
	t.Parallel()
	gate := newItestGate()
	defer gate.open()
	var n atomic.Int64
	factory := func(runtime.StartOptions, string) (runtime.Provider, error) {
		id := fmt.Sprintf("ses_ls%d", n.Add(1))
		if n.Load() == 1 {
			return newItestProvider(itestAttempt(id, itestSessionEnd(id, "end_turn", "parked work"))), nil
		}
		working := stableScriptedEvent{event: events.Event{Event: "agent.status", SessionID: id, Fields: map[string]any{"phase": "working"}}}
		return newItestProvider(itestAttempt(id, working, gate.step(), itestSessionEnd(id, "end_turn", "holder done"))), nil
	}
	// A one-runtime cap: the holder's in-flight turn occupies the only slot,
	// so a resumed peer blocks in the local slot wait until its run deadline
	// passes.
	h := newItestHarness(t, Config{MaxRuntimes: 1}, factory)

	// A five-second run deadline leaves a wide margin over setup, which is
	// measured from the spawn: slow setup must not let the deadline fire
	// before the runtime is parked in the local slot wait.
	const runDeadline = 5 * time.Second
	spawnedAt := time.Now()
	rt := h.spawn(map[string]any{"label": "slot-parked", "timeout": int(runDeadline / time.Second)})
	h.awaitStatus(rt, "done")
	// The parked runtime releases its slot after the turn; the holder spawn
	// can race that release, so retry the admission until it succeeds.
	var holder string
	h.awaitCondition("a free runtime slot for the holder spawn", func() bool {
		id, err := h.trySpawn(map[string]any{"label": "slot-holder"})
		var ce *admission.CapacityError
		if err != nil && !errors.As(err, &ce) {
			h.t.Fatalf("holder spawn: %v", err)
		}
		if err != nil {
			return false
		}
		holder = id
		return true
	})
	h.log.waitFor(t, "holder working status", itestEventIs("agent.status", holder))
	// The follow-up prompt resumes the parked runtime, which blocks in the
	// local slot wait for the rest of its run deadline.
	if err := h.sup.RuntimePrompt(rt, "again", ""); err != nil {
		t.Fatalf("RuntimePrompt: %v", err)
	}
	// Wait out most of the run deadline, then prove the runtime is still
	// blocked in the local slot wait: it has not ended yet, and the holder
	// still holds the only slot. A deadline that fired in the parked wait or
	// during setup would already have ended the runtime and fails here.
	if remaining := runDeadline - 500*time.Millisecond - time.Since(spawnedAt); remaining > 0 {
		time.Sleep(remaining)
	}
	if v := h.view(rt); v.Status == "timeout" {
		t.Fatalf("runtime ended before its deadline: view %+v", v)
	}
	if v := h.view(holder); v.Status != "running" {
		t.Fatalf("holder status just before the deadline = %q, want running", v.Status)
	}
	h.requireIdleEnd(rt, "timeout", "timeout", 124)
	// The holder is unaffected: released from the gate, its turn finishes
	// normally.
	gate.open()
	h.awaitStatus(holder, "done")
}

// TestSupervisorPublicAPICancelWhileWaitingForLocalSlot proves that a resumed
// runtime blocked in the local MaxRuntimes slot wait ends as a cancelled idle
// end when the runtime is cancelled, rather than resuming into a turn it can
// no longer run or dying silently with the parked sentinel standing.
func TestSupervisorPublicAPICancelWhileWaitingForLocalSlot(t *testing.T) {
	t.Parallel()
	gate := newItestGate()
	defer gate.open()
	var n atomic.Int64
	factory := func(runtime.StartOptions, string) (runtime.Provider, error) {
		id := fmt.Sprintf("ses_sc%d", n.Add(1))
		if n.Load() == 1 {
			return newItestProvider(itestAttempt(id, itestSessionEnd(id, "end_turn", "parked work"))), nil
		}
		working := stableScriptedEvent{event: events.Event{Event: "agent.status", SessionID: id, Fields: map[string]any{"phase": "working"}}}
		return newItestProvider(itestAttempt(id, working, gate.step(), itestSessionEnd(id, "end_turn", "holder done"))), nil
	}
	// A one-runtime cap: the holder's in-flight turn occupies the only slot,
	// so a resumed peer blocks in the local slot wait instead of starting.
	h := newItestHarness(t, Config{MaxRuntimes: 1}, factory)

	rt := h.spawn(map[string]any{"label": "slot-cancel"})
	h.awaitStatus(rt, "done")
	// The parked runtime releases its slot after the turn; the holder spawn
	// can race that release, so retry the admission until it succeeds.
	var holder string
	h.awaitCondition("a free runtime slot for the holder spawn", func() bool {
		id, err := h.trySpawn(map[string]any{"label": "slot-holder"})
		var ce *admission.CapacityError
		if err != nil && !errors.As(err, &ce) {
			h.t.Fatalf("holder spawn: %v", err)
		}
		if err != nil {
			return false
		}
		holder = id
		return true
	})
	h.log.waitFor(t, "holder working status", itestEventIs("agent.status", holder))
	// The follow-up prompt resumes the parked runtime, which dequeues the
	// prompt and blocks in the local slot wait while the holder runs.
	if err := h.sup.RuntimePrompt(rt, "again", ""); err != nil {
		t.Fatalf("RuntimePrompt: %v", err)
	}
	// Same-package sync check: the follow-up prompt is only dequeued once the
	// run loop left waitForNextPrompt, and with the holder holding the only
	// slot an empty queue means the runtime is blocked in the local slot wait
	// (still marked parked, not yet ended). Without this wait the cancel below
	// could fire while the runtime is still parked in waitForNextPrompt, which
	// would not exercise the slot-wait end at all.
	slotChild := fetchChild(t, h.sup, rt)
	h.awaitCondition("follow-up prompt dequeued into the local slot wait", func() bool {
		slotChild.mu.Lock()
		defer slotChild.mu.Unlock()
		return len(slotChild.promptQueue) == 0 && slotChild.parked && !slotChild.completed
	})
	// While blocked, the runtime must not start its follow-up turn: its first
	// turn already ended, so any second session.end would mean it resumed.
	h.consistentlyFor(300*time.Millisecond, "resumed runtime stays blocked on the slot", func() bool {
		return h.log.count(itestEventIs("session.end", rt)) == 1
	})
	// Cancelling the blocked runtime ends it as a cancelled idle end instead
	// of leaving it queued for a slot it will never take.
	if err := h.sup.RuntimeCancel(rt); err != nil {
		t.Fatalf("RuntimeCancel: %v", err)
	}
	v := h.requireIdleEnd(rt, "killed", "cancelled", 130)
	// The turn before the park ended successfully, so the killed view keeps
	// the done phase.
	if v.Phase != "done" {
		t.Fatalf("killed view phase = %q, want done", v.Phase)
	}
	// The holder is unaffected: released from the gate, its turn finishes
	// normally.
	gate.open()
	h.awaitStatus(holder, "done")
}

// TestSupervisorPublicAPIParkWakesLocalSlotWait proves that a runtime's park
// wakes a peer blocked in the local MaxRuntimes slot wait through the park
// capacity signal: the blocked follow-up must resume well inside
// WaitForCapacity's 2s poll fallback, which would otherwise gate every
// park-unblock on a fixed timer.
func TestSupervisorPublicAPIParkWakesLocalSlotWait(t *testing.T) {
	t.Parallel()
	gate := newItestGate()
	defer gate.open()
	var n atomic.Int64
	factory := func(runtime.StartOptions, string) (runtime.Provider, error) {
		id := fmt.Sprintf("ses_wake%d", n.Add(1))
		if n.Load() == 1 {
			// The parked runtime's follow-up is a resumed second attempt, so its
			// provider carries both scripts.
			return newItestProvider(
				itestAttempt(id, itestSessionEnd(id, "end_turn", "parked work")),
				itestAttempt(id, itestSessionEnd(id, "end_turn", "follow-up done")),
			), nil
		}
		working := stableScriptedEvent{event: events.Event{Event: "agent.status", SessionID: id, Fields: map[string]any{"phase": "working"}}}
		return newItestProvider(itestAttempt(id, working, gate.step(), itestSessionEnd(id, "end_turn", "holder done"))), nil
	}
	// A one-runtime cap: the holder's in-flight turn occupies the only slot,
	// so the parked runtime's follow-up blocks in the local slot wait.
	h := newItestHarness(t, Config{MaxRuntimes: 1}, factory)

	rt := h.spawn(map[string]any{"label": "slot-wake"})
	h.awaitStatus(rt, "done")
	// The parked runtime released its slot after the turn; the holder spawn
	// can race that release, so retry the admission until it succeeds.
	var holder string
	h.awaitCondition("a free runtime slot for the holder spawn", func() bool {
		id, err := h.trySpawn(map[string]any{"label": "slot-holder"})
		var ce *admission.CapacityError
		if err != nil && !errors.As(err, &ce) {
			h.t.Fatalf("holder spawn: %v", err)
		}
		if err != nil {
			return false
		}
		holder = id
		return true
	})
	h.log.waitFor(t, "holder working status", itestEventIs("agent.status", holder))
	// The follow-up prompt blocks the parked runtime in the local slot wait:
	// same-package check that the prompt was dequeued out of the parked wait.
	if err := h.sup.RuntimePrompt(rt, "wake me", ""); err != nil {
		t.Fatalf("RuntimePrompt: %v", err)
	}
	slotChild := fetchChild(t, h.sup, rt)
	h.awaitCondition("follow-up prompt dequeued into the local slot wait", func() bool {
		slotChild.mu.Lock()
		defer slotChild.mu.Unlock()
		return len(slotChild.promptQueue) == 0 && slotChild.parked && !slotChild.completed
	})
	// Releasing the holder's turn makes it park. The park's capacity signal
	// must wake the blocked slot wait: the follow-up's own session.end has to
	// arrive far inside the 2s poll fallback (bounded here at 1s).
	gate.open()
	h.awaitCondition("blocked follow-up resumed and ended", func() bool {
		return h.log.count(itestEventIs("session.end", rt)) == 2
	})
	// The wake latency is the gap between the holder's session.end (the park
	// that frees the slot) and the follow-up's session.end (the resumed turn
	// ending). Both use the supervisor's publish timestamps, so observer lag
	// cannot skew the gap.
	holderEnd, ok := h.log.find(itestEventIs("session.end", holder))
	if !ok {
		t.Fatal("holder session.end not found")
	}
	var followUpEnd events.Event
	for _, ev := range h.log.all() {
		if itestEventIs("session.end", rt)(ev) {
			followUpEnd = ev
		}
	}
	if gap := itestEventTS(t, followUpEnd).Sub(itestEventTS(t, holderEnd)); gap >= 1500*time.Millisecond {
		t.Fatalf("park capacity wake took %v, want well under the 2s poll fallback", gap)
	}
	// The holder finished its turn and parked normally, and the woken runtime
	// completed its follow-up turn and parked again.
	h.awaitStatus(holder, "done")
	h.awaitStatus(rt, "done")
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
	if err := itestRecv(t, answered, "decider answer"); err != nil {
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
	// The decider ends after its single turn, and its row can settle after
	// the worker, so poll until it reads ended with the turn's zero exit
	// code.
	h.awaitCondition("finished decider row to read ended with exit code 0", func() bool {
		rows := h.listed("permdec:")
		return len(rows) == 1 && rows[0]["status"] == "ended" && rows[0]["exit_code"] == 0
	})
}

func TestSupervisorPublicAPIDeciderDeny(t *testing.T) {
	t.Parallel()
	worker := itestWorker("ses_dw_deny", "req_deny", "continued after deny")
	answered := make(chan error, 1)
	var h *itestHarness
	decider := itestAnsweringDecider(func() *itestHarness { return h }, "reject_once", answered)
	h = newItestHarness(t, Config{}, itestDeciderFactory(func(string) runtime.Provider { return worker }, decider))

	rt := h.spawn(map[string]any{"label": "worker", "permission_decider": itestDecider()})
	if err := itestRecv(t, answered, "decider answer"); err != nil {
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
	h.consistentlyFor(backoffDelay(1)+500*time.Millisecond, "exhausted request stays pending without further spawns", func() bool {
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
		itestRecv(t, started, "decider start for "+label)
	}
	// The cap is full: the next request is routed but no decider runs for it.
	blockedLabel := fmt.Sprintf("w%d", workers)
	ids[blockedLabel] = h.spawn(map[string]any{"label": blockedLabel, "permission_decider": itestDecider()})
	h.log.waitFor(t, "blocked permission.request", itestEventIs("permission.request", ids[blockedLabel]))
	h.consistentlyFor(permission.DefaultPollInterval+500*time.Millisecond, "decider cap holds", func() bool {
		v := h.view(ids[blockedLabel])
		return spawns.Load() == itestDeciderCap && v.PendingPermission && v.Status == "running"
	})
	// Each listed decider run is mid-turn holding its concurrency slot.
	for _, row := range h.listed("permdec:") {
		if got := row["status"]; got != "running" {
			t.Fatalf("decider run status = %v, want running", got)
		}
	}
	if rows := h.listed("permdec:"); len(rows) != itestDeciderCap {
		t.Fatalf("decider runs listed = %d, want %d", len(rows), itestDeciderCap)
	}
	// Cancelling a decider run frees its slot; the blocked request is routed
	// to a new decider without another permission.request event.
	rows := h.listed("permdec:" + ids["w1"] + ":")
	if len(rows) != 1 {
		t.Fatalf("decider runs for w1 = %d, want 1", len(rows))
	}
	if got := rows[0]["status"]; got != "running" {
		t.Fatalf("decider run for w1 status = %v, want running", got)
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

// TestSupervisorPublicAPIDeciderQueuedPromptDoesNotResumeFailedTurn proves a
// decider's single turn is terminal on the failure path too: a prompt queued
// on the decider's own runtime mid-turn never resumes it after the turn
// fails, so the decider ends with the failure's exit code and accepts no
// further prompt.
func TestSupervisorPublicAPIDeciderQueuedPromptDoesNotResumeFailedTurn(t *testing.T) {
	t.Parallel()
	worker := itestWorker("ses_dq", "req_dq", "answered work")
	queued := make(chan error, 1)
	deciderReady := make(chan *itestProvider, 1)
	var h *itestHarness
	decider := func(opts runtime.StartOptions) (runtime.Provider, error) {
		workerRtID, reqID := itestDeciderLabel(opts.Label)
		// The decider answers, then its turn fails the way a backend crash
		// mid-judgment would.
		p := newItestProvider(itestAttempt("ses_dq_decider", itestSessionEnd("ses_dq_decider", "error", "")))
		p.onPrompt = func(context.Context) {
			// Registration precedes the turn, so the decider's own runtime is
			// discoverable by its label. The prompt queued here is resume bait:
			// it must never run.
			rows, _ := h.sup.List().([]map[string]any)
			var self string
			for _, row := range rows {
				if label, _ := row["label"].(string); label == opts.Label {
					self, _ = row["runtime_id"].(string)
				}
			}
			if self == "" {
				queued <- fmt.Errorf("decider runtime with label %q is not listed", opts.Label)
				return
			}
			queued <- h.sup.RuntimePrompt(self, "follow-up", "")
			_ = h.sup.RuntimeAnswerPermission(workerRtID, reqID, "allow_once", "")
		}
		deciderReady <- p
		return p, nil
	}
	h = newItestHarness(t, Config{}, itestDeciderFactory(func(string) runtime.Provider { return worker }, decider))

	rt := h.spawn(map[string]any{"label": "worker", "permission_decider": itestDecider()})
	if err := itestRecv(t, queued, "queued prompt on the decider runtime"); err != nil {
		t.Fatalf("queueing a prompt on the decider's own runtime: %v", err)
	}
	// The decider answered before its turn failed, so the worker finishes
	// normally.
	h.awaitStatus(rt, "done")
	if got := h.result(rt); got != "answered work" {
		t.Fatalf("RuntimeResult = %q, want %q", got, "answered work")
	}
	answers := worker.recordedAnswers()
	if len(answers) != 1 || !answers[0].Allow || answers[0].OptionID != "allow_once" {
		t.Fatalf("backend permission answers = %+v, want one allow_once", answers)
	}
	// The failed turn ends the decider; the queued prompt never resumes it.
	p := itestRecv(t, deciderReady, "decider provider")
	h.awaitCondition("failed decider row to read ended", func() bool {
		rows := h.listed("permdec:")
		return len(rows) == 1 && rows[0]["status"] == "ended"
	})
	rows := h.listed("permdec:")
	if starts, resumes := p.counts(); starts != 1 || resumes != 0 {
		t.Fatalf("decider provider starts/resumes = %d/%d, want 1/0 (a queued prompt must not resume a failed decider turn)", starts, resumes)
	}
	if got := rows[0]["exit_code"]; got != 1 {
		t.Fatalf("decider row exit_code = %v, want 1 (the failed turn's exit code)", got)
	}
	// The ended decider accepts no further prompt.
	deciderRT, _ := rows[0]["runtime_id"].(string)
	if err := h.sup.RuntimePrompt(deciderRT, "late follow-up", ""); err == nil {
		t.Fatal("RuntimePrompt on the ended decider succeeded, want rejection")
	}
}

// TestSupervisorPublicAPIUserRuntimeWithDeciderLabelParks proves a label is
// not what makes a runtime a permission decider: a caller-spawned runtime
// wearing a decider-shaped label parks after its first finished turn like any
// ordinary runtime, and a RuntimePrompt resumes it for another turn.
func TestSupervisorPublicAPIUserRuntimeWithDeciderLabelParks(t *testing.T) {
	t.Parallel()
	gate := newItestGate()
	defer gate.open()
	provider := newItestProvider(
		itestAttempt("ses_label1", itestSessionEnd("ses_label1", "end_turn", "first")),
		itestAttempt("ses_label2", gate.step(), itestSessionEnd("ses_label2", "end_turn", "second")),
	)
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))

	rt := h.spawn(map[string]any{"label": "permdec:spoof:req"})
	h.awaitStatus(rt, "done")
	// The finished turn parks the runtime rather than ending it: a prompt is
	// accepted and resumes the session for another turn.
	if err := h.sup.RuntimePrompt(rt, "again", ""); err != nil {
		t.Fatalf("RuntimePrompt on a runtime with a decider-shaped label: %v", err)
	}
	h.awaitStatus(rt, "running")
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

// TestSupervisorPublicAPIDeciderSlotSurvivesLabelCollision proves the decider
// slot release follows the run's own reservation, not its label: an ordinary
// runtime spawned with a live decider's exact label finishes and is ended
// without freeing the real decider's slot, so with the cap full a further
// permission request stays blocked and the live decider can still answer.
func TestSupervisorPublicAPIDeciderSlotSurvivesLabelCollision(t *testing.T) {
	t.Parallel()
	const workers = itestDeciderCap + 1
	providers := map[string]*itestProvider{}
	for i := 1; i <= workers; i++ {
		label := fmt.Sprintf("w%d", i)
		providers[label] = itestWorker("ses_coll_"+label, "req_"+label, label+" finished")
	}
	var spawns atomic.Int64
	holds := map[string]*itestGate{}
	var holdsMu sync.Mutex
	hold := func(reqID string) *itestGate {
		holdsMu.Lock()
		defer holdsMu.Unlock()
		if holds[reqID] == nil {
			holds[reqID] = newItestGate()
		}
		return holds[reqID]
	}
	started := make(chan string, workers)
	// collide holds the live decider's exact label once discovered, so the
	// factory can route the ordinary runtime wearing it to an ordinary turn.
	var collide atomic.Value
	var h *itestHarness
	h = newItestHarness(t, Config{}, func(opts runtime.StartOptions, _ string) (runtime.Provider, error) {
		if c, _ := collide.Load().(string); c != "" && opts.Label == c {
			return newItestProvider(itestAttempt("ses_collide", itestSessionEnd("ses_collide", "end_turn", "collision"))), nil
		}
		if strings.HasPrefix(opts.Label, "permdec:") {
			spawns.Add(1)
			rtID, reqID := itestDeciderLabel(opts.Label)
			sessionID := fmt.Sprintf("ses_coll_decider_%s", reqID)
			p := newItestProvider(itestAttempt(sessionID, itestSessionEnd(sessionID, "end_turn", "")))
			g := hold(reqID)
			p.onPrompt = func(ctx context.Context) {
				started <- reqID
				select {
				case <-g.ch:
				case <-ctx.Done():
					return
				}
				_ = h.sup.RuntimeAnswerPermission(rtID, reqID, "allow_once", "")
			}
			return p, nil
		}
		return providers[opts.Label], nil
	})
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
		itestRecv(t, started, "decider start for "+label)
	}
	// The cap is full: the next worker's request stays pending with no
	// decider for it.
	blockedLabel := fmt.Sprintf("w%d", workers)
	ids[blockedLabel] = h.spawn(map[string]any{"label": blockedLabel, "permission_decider": itestDecider()})
	h.log.waitFor(t, "blocked permission.request", itestEventIs("permission.request", ids[blockedLabel]))
	h.consistentlyFor(permission.DefaultPollInterval+500*time.Millisecond, "decider cap holds", func() bool {
		v := h.view(ids[blockedLabel])
		return spawns.Load() == itestDeciderCap && v.PendingPermission && v.Status == "running"
	})
	// Discover the live decider's exact label, then run an ordinary runtime
	// under it, let it finish, and end it: its teardown is where a label-keyed
	// release would free the real decider's slot.
	rows := h.listed("permdec:" + ids["w1"] + ":")
	if len(rows) != 1 {
		t.Fatalf("decider runs for w1 = %d, want 1", len(rows))
	}
	deciderLabel, _ := rows[0]["label"].(string)
	collide.Store(deciderLabel)
	collideRT := h.spawn(map[string]any{"label": deciderLabel})
	h.awaitStatus(collideRT, "done")
	if err := h.sup.RuntimeCancel(collideRT); err != nil {
		t.Fatalf("cancel the label-colliding runtime: %v", err)
	}
	h.awaitStatus(collideRT, "killed")
	// The collision must not have freed the real decider's slot: the blocked
	// request still has no decider and stays pending.
	h.consistentlyFor(permission.DefaultPollInterval+500*time.Millisecond, "decider slot held through the label collision", func() bool {
		v := h.view(ids[blockedLabel])
		return spawns.Load() == itestDeciderCap && v.PendingPermission && v.Status == "running"
	})
	// The live decider can still answer its request.
	hold("req_w1").open()
	h.awaitStatus(ids["w1"], "done")
	if got := h.result(ids["w1"]); got != "w1 finished" {
		t.Fatalf("w1 RuntimeResult = %q, want %q", got, "w1 finished")
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
	// The connected client reserves the request first: a claim that times out
	// falls back to the file handler, so the request must start reserved
	// rather than go straight to the file handler.
	req, ok := h.log.find(itestEventIs("permission.request", rt))
	if !ok {
		t.Fatal("permission.request event not found")
	}
	if got, _ := req.Fields["resolver"].(string); got != "reserved" {
		t.Fatalf("permission.request resolver = %q, want %q (control claim before the file fallback)", got, "reserved")
	}
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
	var deciderSpawns atomic.Int64
	deciderRan := make(chan struct{}, 1)
	h := newItestHarness(t, Config{}, itestDeciderFactory(
		func(string) runtime.Provider { return worker },
		func(runtime.StartOptions) (runtime.Provider, error) {
			// A deferring decider exits without answering.
			deciderSpawns.Add(1)
			p := newItestProvider(itestAttempt("ses_defer_decider", itestSessionEnd("ses_defer_decider", "end_turn", "")))
			p.onPrompt = func(context.Context) {
				// Non-blocking send: a second decider run must surface in the
				// spawn count instead of stalling silently on the channel.
				select {
				case deciderRan <- struct{}{}:
				default:
				}
			}
			return p, nil
		},
	))

	rt := h.spawn(map[string]any{"label": "worker", "permission_decider": itestDecider()})
	itestRecv(t, deciderRan, "decider turn")
	// The decider deferred. No control client is connected, so nothing claims
	// the request; it stays pending on the human gate, with no re-route or
	// respawn of the decider.
	h.consistentlyFor(permission.DefaultPollInterval+500*time.Millisecond, "deferred request stays pending with exactly one decider spawn", func() bool {
		v := h.view(rt)
		return v.PendingPermission && v.Status == "running" && deciderSpawns.Load() == 1
	})
	if err := h.sup.RuntimeAnswerPermission(rt, "req_defer", "allow_once", ""); err != nil {
		t.Fatalf("human RuntimeAnswerPermission: %v", err)
	}
	h.awaitStatus(rt, "done")
	if got := h.result(rt); got != "human decided" {
		t.Fatalf("RuntimeResult = %q, want %q", got, "human decided")
	}
	if got := deciderSpawns.Load(); got != 1 {
		t.Fatalf("decider spawns = %d, want 1 (a deferred request is not re-routed)", got)
	}
}
