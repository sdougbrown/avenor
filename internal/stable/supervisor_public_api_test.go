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
	itestDeciderCap = 3
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
	attachOwnerForTest(h.t, c, h.socket)
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
	tr := runstate.Translate(runstate.RuntimeStatus(rawStatus), runstate.Phase(rawPhase))
	v := itestView{Status: string(tr.Status), Phase: string(tr.Phase), TurnComplete: tr.TurnComplete}
	v.ExitCode, _ = raw["exit_code"].(int)
	v.PendingPermission, _ = raw["pending_permission"].(bool)
	v.DeciderExhausted, _ = raw["permission_decider_exhausted"].(bool)
	v.FinalOutput, _ = raw["final_output"].(string)
	if rawStatus == "ended" {
		if path, _ := raw["sentinel_file"].(string); path != "" {
			if data, err := os.ReadFile(path); err == nil {
				var sentinelExit int
				v.Status, v.StopReason, sentinelExit = itestSentinelStatus(string(data))
				v.TurnComplete = runstate.IsTerminalStatus(runstate.RuntimeStatus(v.Status))
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
	status = string(runstate.SentinelStatus(lines[0]))
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
	onStart     func(ctx context.Context)
	onAnswer    func(requestID string, resp runtime.PermissionResponse)
}

func newItestProvider(attempts ...stableScriptedAttempt) *itestProvider {
	return &itestProvider{stableScriptedProvider: &stableScriptedProvider{attempt: -1, scripts: attempts}}
}

func (p *itestProvider) Start(ctx context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	p.mu.Lock()
	p.starts++
	onStart := p.onStart
	p.mu.Unlock()
	if onStart != nil {
		onStart(ctx)
	}
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
		return len(slotChild.promptQueue) == 0 && slotChild.lc.isParked() && !slotChild.lc.isCompleted()
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
		return len(slotChild.promptQueue) == 0 && slotChild.lc.isParked() && !slotChild.lc.isCompleted()
	})
	// Releasing the holder's turn makes it park. The park's capacity signal
	// must wake the blocked slot wait: the follow-up's own session.end has to
	// arrive inside the 2s poll fallback (bounded here at 1.75s).
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
	if gap := itestEventTS(t, followUpEnd).Sub(itestEventTS(t, holderEnd)); gap >= 1750*time.Millisecond {
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

// itestAgentStatus scripts an agent.status event carrying a phase, as a
// backend publishes working state mid-turn. A non-nil release holds the
// scripted turn before the event is emitted.
func itestAgentStatus(sessionID, phase string, release <-chan struct{}) stableScriptedEvent {
	return stableScriptedEvent{
		release: release,
		event: events.Event{
			Event:     "agent.status",
			SessionID: sessionID,
			Fields:    map[string]any{"phase": phase},
		},
	}
}

// itestDeliveryWorker is an opt-in worker whose continuation output encodes
// the answer its provider actually received — the request ID, the option ID,
// and the normalized allow/reject value — as
// "answered:<requestID>:<optionID>:<allow|reject>". A canonical response
// event can look right while the provider payload is wrong; this output makes
// such a mismatch fail the test. Scenarios without a delivery proof keep the
// plain itestWorker output.
func itestDeliveryWorker(sessionID, requestID string) *itestProvider {
	gate := newItestGate()
	p := newItestProvider(itestAttempt(sessionID,
		itestPermissionRequest(sessionID, requestID),
		gate.step(),
		itestSessionEnd(sessionID, "end_turn", ""),
	))
	p.onAnswer = func(received string, resp runtime.PermissionResponse) {
		value := "reject"
		if resp.Allow {
			value = "allow"
		}
		// The worker is still parked on the gate when the answer is delivered,
		// so the continuation step has not run yet. Recording the encoded
		// output before opening the gate publishes it to the scripted turn.
		p.mu.Lock()
		p.scripts[0].events[len(p.scripts[0].events)-1] = itestSessionEnd(sessionID, "end_turn",
			fmt.Sprintf("answered:%s:%s:%s", received, resp.OptionID, value))
		p.mu.Unlock()
		gate.open()
	}
	return p
}

// rawRuntimeStatus returns the runtime's raw RuntimeStatus document before
// any consumer-side translation, the strict oracle for permission scenarios.
func (h *itestHarness) rawRuntimeStatus(rtID string) map[string]any {
	h.t.Helper()
	out, err := h.sup.RuntimeStatus(rtID)
	if err != nil {
		h.t.Fatalf("RuntimeStatus(%s): %v", rtID, err)
	}
	raw, _ := out.(map[string]any)
	return raw
}

// awaitRawStatus polls raw RuntimeStatus until the raw status equals want.
// Transitions that follow a settled turn have no wake-up channel of their own.
func (h *itestHarness) awaitRawStatus(rtID, want string) map[string]any {
	h.t.Helper()
	deadline := time.Now().Add(itestTimeout)
	for {
		raw := h.rawRuntimeStatus(rtID)
		if status, _ := raw["status"].(string); status == want {
			return raw
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("runtime %s raw status = %v, want %q", rtID, raw["status"], want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// awaitSettledStatus polls raw RuntimeStatus until the runtime's state is
// terminal: an ended runtime, or an end-turn runtime parked at idle with the
// done phase. It returns the final raw document.
func (h *itestHarness) awaitSettledStatus(rtID string) map[string]any {
	h.t.Helper()
	deadline := time.Now().Add(itestTimeout)
	for {
		raw := h.rawRuntimeStatus(rtID)
		status, _ := raw["status"].(string)
		phase, _ := raw["phase"].(string)
		if runstate.Translate(runstate.RuntimeStatus(status), runstate.Phase(phase)).TurnComplete {
			return raw
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("runtime %s raw status = %q phase = %q, want a settled terminal state", rtID, status, phase)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// awaitPermissionCleared polls raw RuntimeStatus until the runtime no longer
// reports a pending permission, and returns the final raw document.
func (h *itestHarness) awaitPermissionCleared(rtID string) map[string]any {
	h.t.Helper()
	deadline := time.Now().Add(itestTimeout)
	for {
		raw := h.rawRuntimeStatus(rtID)
		if pending, _ := raw["pending_permission"].(bool); !pending {
			return raw
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("runtime %s still reports pending_permission after %s", rtID, itestTimeout)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// itestEventRunLabel extracts the run label the supervisor stamps on events.
func itestEventRunLabel(ev events.Event) string {
	label, _ := ev.Fields["run_label"].(string)
	return label
}

// itestDeciderEventIs matches any event published by the decider run spawned
// for workerRT's request; the run's label is "permdec:<runtime>:<request>".
func itestDeciderEventIs(workerRT, reqID string) func(events.Event) bool {
	want := "permdec:" + workerRT + ":" + reqID
	return func(ev events.Event) bool { return itestEventRunLabel(ev) == want }
}

// deciderRuntimeID returns the runtime ID the supervisor published for the
// decider run spawned for workerRT's request, discovered from the decider's
// own events.
func (h *itestHarness) deciderRuntimeID(workerRT, reqID string) string {
	h.t.Helper()
	ev := h.log.waitFor(h.t, "decider event for "+reqID, itestDeciderEventIs(workerRT, reqID))
	id, _ := ev.Fields["runtime_id"].(string)
	return id
}

// deciderIdentityCount counts the distinct decider runtime IDs that published
// events, so scenarios can pin how many decider runs ever existed.
func (h *itestHarness) deciderIdentityCount() int {
	seen := map[string]bool{}
	for _, ev := range h.log.all() {
		if !strings.HasPrefix(itestEventRunLabel(ev), "permdec:") {
			continue
		}
		if id, _ := ev.Fields["runtime_id"].(string); id != "" {
			seen[id] = true
		}
	}
	return len(seen)
}

// requirePermissionResponse asserts the canonical response contract on the
// event log: exactly one permission.request for the runtime and request ID,
// and exactly one permission.response carrying the selected option and its
// normalized kind.
func (h *itestHarness) requirePermissionResponse(rtID, requestID, optionID, kind string) {
	h.t.Helper()
	// Event publication reaches the subscription stream asynchronously, so
	// wait for the response before counting anything.
	h.log.waitFor(h.t, "permission.response for "+requestID, func(ev events.Event) bool {
		id, _ := ev.Fields["runtime_id"].(string)
		req, _ := ev.Fields["request_id"].(string)
		return ev.Event == "permission.response" && id == rtID && req == requestID
	})
	var requests, responses int
	for _, ev := range h.log.all() {
		id, _ := ev.Fields["runtime_id"].(string)
		req, _ := ev.Fields["request_id"].(string)
		if id != rtID || req != requestID {
			continue
		}
		switch ev.Event {
		case "permission.request":
			requests++
		case "permission.response":
			responses++
			if got, _ := ev.Fields["option_id"].(string); got != optionID {
				h.t.Fatalf("permission.response option_id = %q, want %q", got, optionID)
			}
			if got, _ := ev.Fields["kind"].(string); got != kind {
				h.t.Fatalf("permission.response kind = %q, want %q", got, kind)
			}
		}
	}
	if requests != 1 {
		h.t.Fatalf("permission.request events for %s/%s = %d, want 1", rtID, requestID, requests)
	}
	if responses != 1 {
		h.t.Fatalf("permission.response events for %s/%s = %d, want 1", rtID, requestID, responses)
	}
}

// requireEncodedContinuation asserts the worker's continuation through events
// and raw RuntimeStatus: the session.end output encodes the answer the
// provider received, the canonical response was published, and the raw
// status no longer reports a pending permission.
func (h *itestHarness) requireEncodedContinuation(rtID, requestID, optionID, kind string) {
	h.t.Helper()
	want := fmt.Sprintf("answered:%s:%s:%s", requestID, optionID, kind)
	h.log.waitFor(h.t, "continuation session.end for "+rtID, func(ev events.Event) bool {
		out, _ := ev.Fields["final_output"].(string)
		return itestEventIs("session.end", rtID)(ev) && out == want
	})
	// The canonical permission.response and the worker's continuation are
	// not ordered: the direct-answer path publishes the response only after
	// the provider call returns, and the worker may already have finished
	// its resumed turn by then. Wait for the response with a bounded wait
	// rather than assuming it precedes the continuation in the log.
	h.log.waitFor(h.t, "permission.response for "+requestID, func(ev events.Event) bool {
		id, _ := ev.Fields["runtime_id"].(string)
		req, _ := ev.Fields["request_id"].(string)
		return ev.Event == "permission.response" && id == rtID && req == requestID
	})
	all := h.log.all()
	respIdx, endIdx := -1, -1
	for i, ev := range all {
		id, _ := ev.Fields["runtime_id"].(string)
		out, _ := ev.Fields["final_output"].(string)
		switch {
		case ev.Event == "permission.response" && id == rtID && respIdx == -1:
			respIdx = i
		case ev.Event == "session.end" && id == rtID && out == want && endIdx == -1:
			endIdx = i
			if code, _ := ev.Fields["exit_code"].(int); code != 0 {
				h.t.Fatalf("continuation session.end exit_code = %d, want 0", code)
			}
		}
	}
	if respIdx == -1 || endIdx == -1 {
		h.t.Fatalf("missing canonical response (%d) or encoded continuation (%d)", respIdx, endIdx)
	}
	raw := h.awaitPermissionCleared(rtID)
	if got, _ := raw["final_output"].(string); got != want {
		h.t.Fatalf("raw RuntimeStatus final_output = %q, want %q", got, want)
	}
	if code, _ := raw["exit_code"].(int); code != 0 {
		h.t.Fatalf("raw RuntimeStatus exit_code = %d, want 0", code)
	}
	// Clearing the pending permission precedes the resumed turn settling, so
	// poll the worker's raw state to a settled terminal value before callers
	// pin anything that must hold once the worker is done. An end-turn worker
	// parks at idle with the done phase instead of ending.
	h.awaitSettledStatus(rtID)
}

func TestSupervisorPublicAPIPermissionRequestAnswerContinue(t *testing.T) {
	t.Parallel()
	worker := itestDeliveryWorker("ses_pw", "req_human")
	h := newItestHarness(t, Config{}, itestSingleProvider(worker))
	h.connectClient()

	rt := h.spawn(nil)
	h.log.waitFor(t, "permission.request", itestEventIs("permission.request", rt))
	raw := h.rawRuntimeStatus(rt)
	if pending, _ := raw["pending_permission"].(bool); !pending {
		t.Fatalf("raw RuntimeStatus with a pending request = %v, want pending_permission", raw["pending_permission"])
	}
	if status, _ := raw["status"].(string); status != "running" {
		t.Fatalf("raw RuntimeStatus with a pending request = %q, want running", status)
	}
	if got := h.log.count(itestStopReasonIs(rt, "end_turn")); got != 0 {
		t.Fatalf("runtime finished before the request was answered (%d session.end events)", got)
	}
	if err := h.sup.RuntimeAnswerPermission(rt, "req_human", "allow_once", ""); err != nil {
		t.Fatalf("RuntimeAnswerPermission: %v", err)
	}
	h.requirePermissionResponse(rt, "req_human", "allow_once", "allow")
	h.requireEncodedContinuation(rt, "req_human", "allow_once", "allow")
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
	worker := itestDeliveryWorker("ses_dw_allow", "req_allow")
	answered := make(chan error, 1)
	var h *itestHarness
	decider := itestAnsweringDecider(func() *itestHarness { return h }, "allow_once", answered)
	h = newItestHarness(t, Config{}, itestDeciderFactory(func(string) runtime.Provider { return worker }, decider))

	rt := h.spawn(map[string]any{"label": "worker", "permission_decider": itestDecider()})
	if err := itestRecv(t, answered, "decider answer"); err != nil {
		t.Fatalf("decider answer: %v", err)
	}
	h.requirePermissionResponse(rt, "req_allow", "allow_once", "allow")
	h.requireEncodedContinuation(rt, "req_allow", "allow_once", "allow")
	// The decider run that answered ends after its single turn: its terminal
	// state is read from its own raw RuntimeStatus, keyed by the runtime ID
	// its events carried.
	deciderRT := h.deciderRuntimeID(rt, "req_allow")
	raw := h.awaitRawStatus(deciderRT, "ended")
	if code, _ := raw["exit_code"].(int); code != 0 {
		t.Fatalf("decider raw RuntimeStatus exit_code = %v, want 0", raw["exit_code"])
	}
	h.consistentlyFor(permission.DefaultPollInterval+500*time.Millisecond, "exactly one decider identity", func() bool {
		return h.deciderIdentityCount() == 1
	})
}

func TestSupervisorPublicAPIDeciderDeny(t *testing.T) {
	t.Parallel()
	worker := itestDeliveryWorker("ses_dw_deny", "req_deny")
	answered := make(chan error, 1)
	var h *itestHarness
	decider := itestAnsweringDecider(func() *itestHarness { return h }, "reject_once", answered)
	h = newItestHarness(t, Config{}, itestDeciderFactory(func(string) runtime.Provider { return worker }, decider))

	rt := h.spawn(map[string]any{"label": "worker", "permission_decider": itestDecider()})
	if err := itestRecv(t, answered, "decider answer"); err != nil {
		t.Fatalf("decider answer: %v", err)
	}
	// A reject_once decision must reach the provider as a rejection, not an
	// implicit allow: the continuation output encodes what was delivered.
	h.requirePermissionResponse(rt, "req_deny", "reject_once", "reject")
	h.requireEncodedContinuation(rt, "req_deny", "reject_once", "reject")
	deciderRT := h.deciderRuntimeID(rt, "req_deny")
	h.awaitRawStatus(deciderRT, "ended")
	h.consistentlyFor(permission.DefaultPollInterval+500*time.Millisecond, "exactly one decider identity", func() bool {
		return h.deciderIdentityCount() == 1
	})
}

func TestSupervisorPublicAPIDeciderSpawnFailureRetries(t *testing.T) {
	t.Parallel()
	worker := itestDeliveryWorker("ses_dw_retry", "req_retry")
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
	// spawn must be retried without a further event, and the retry's allow
	// must let the worker continue.
	h.requirePermissionResponse(rt, "req_retry", "allow_once", "allow")
	h.requireEncodedContinuation(rt, "req_retry", "allow_once", "allow")
	if got := h.log.count(itestEventIs("permission.request", rt)); got != 1 {
		t.Fatalf("permission.request events = %d, want 1", got)
	}
	raw := h.rawRuntimeStatus(rt)
	if exhausted, _ := raw["permission_decider_exhausted"].(bool); exhausted {
		t.Fatal("permission_decider_exhausted set although the retry succeeded")
	}
	deciderRT := h.deciderRuntimeID(rt, "req_retry")
	h.awaitRawStatus(deciderRT, "ended")
	h.consistentlyFor(permission.DefaultPollInterval+500*time.Millisecond, "exactly one decider identity (the failed spawn never became a run)", func() bool {
		return h.deciderIdentityCount() == 1
	})
}

func TestSupervisorPublicAPIDeciderSpawnFailuresExhaustToHuman(t *testing.T) {
	t.Parallel()
	worker := itestDeliveryWorker("ses_dw_exhaust", "req_exhaust")
	h := newItestHarness(t, Config{}, itestDeciderFactory(
		func(string) runtime.Provider { return worker },
		func(runtime.StartOptions) (runtime.Provider, error) {
			return nil, errors.New("decider backend unavailable")
		},
	))

	rt := h.spawn(map[string]any{"label": "worker", "permission_decider": itestDecider()})
	h.awaitCondition("decider retries to be exhausted", func() bool {
		raw := h.rawRuntimeStatus(rt)
		exhausted, _ := raw["permission_decider_exhausted"].(bool)
		return exhausted
	})
	h.consistentlyFor(backoffDelay(1)+500*time.Millisecond, "exhausted request stays pending without a decider run", func() bool {
		raw := h.rawRuntimeStatus(rt)
		pending, _ := raw["pending_permission"].(bool)
		status, _ := raw["status"].(string)
		return pending && status == "running" && h.deciderIdentityCount() == 0
	})
	// The request stays open for a human, who can still answer it.
	if err := h.sup.RuntimeAnswerPermission(rt, "req_exhaust", "allow_once", ""); err != nil {
		t.Fatalf("human RuntimeAnswerPermission: %v", err)
	}
	h.requirePermissionResponse(rt, "req_exhaust", "allow_once", "allow")
	h.requireEncodedContinuation(rt, "req_exhaust", "allow_once", "allow")
}

// itestDeciderCapScenario is a cap-fill setup: the decider cap is filled with
// held deciders and one further request is routed while the cap stays full.
type itestDeciderCapScenario struct {
	h          *itestHarness
	ids        map[string]string
	deciders   map[string]string
	holds      map[string]*itestGate
	answered   map[string]*itestGate
	blockedReq string
	blockedRT  string
}

// newItestDeciderCapScenario builds the harness, fills the cap with held
// deciders, spawns the request that the cap blocks, and returns once that
// request is routed but unresolved. Every worker and decider session ID is
// prefixed with sessionPrefix. A held decider publishes its identity, then
// holds its slot on a gate keyed by its request; with holdUntilAnswer its turn
// also waits on a second gate after its answer, so the slot stays taken until
// the test opens that gate. capHoldsMsg names the window asserting the cap is
// full. Every gate opens at cleanup.
func newItestDeciderCapScenario(t *testing.T, sessionPrefix, capHoldsMsg string, holdUntilAnswer bool) *itestDeciderCapScenario {
	t.Helper()
	const workers = itestDeciderCap + 1
	blockedReq := fmt.Sprintf("req_w%d", workers)
	providers := map[string]*itestProvider{}
	for i := 1; i <= workers; i++ {
		label := fmt.Sprintf("w%d", i)
		providers[label] = itestDeliveryWorker(sessionPrefix+label, "req_"+label)
	}
	holds := map[string]*itestGate{}
	answered := map[string]*itestGate{}
	var gatesMu sync.Mutex
	gate := func(m map[string]*itestGate, reqID string) *itestGate {
		gatesMu.Lock()
		defer gatesMu.Unlock()
		if m[reqID] == nil {
			m[reqID] = newItestGate()
		}
		return m[reqID]
	}
	var h *itestHarness
	h = newItestHarness(t, Config{}, itestDeciderFactory(
		func(label string) runtime.Provider { return providers[label] },
		func(opts runtime.StartOptions) (runtime.Provider, error) {
			workerRT, reqID := itestDeciderLabel(opts.Label)
			sessionID := sessionPrefix + "decider_" + reqID
			if reqID == blockedReq {
				// The request the cap blocked gets a decider that answers at
				// once once it finally runs.
				p := newItestProvider(itestAttempt(sessionID,
					itestAgentStatus(sessionID, "working", nil),
					itestSessionEnd(sessionID, "end_turn", ""),
				))
				p.onPrompt = func(context.Context) {
					_ = h.sup.RuntimeAnswerPermission(workerRT, reqID, "allow_once", "")
				}
				return p, nil
			}
			steps := []stableScriptedEvent{
				itestAgentStatus(sessionID, "working", nil),
				itestAgentStatus(sessionID, "working", gate(holds, reqID).ch),
			}
			end := itestSessionEnd(sessionID, "end_turn", "")
			if holdUntilAnswer {
				end.release = gate(answered, reqID).ch
			}
			return newItestProvider(itestAttempt(sessionID, append(steps, end)...)), nil
		},
	))
	t.Cleanup(func() {
		gatesMu.Lock()
		defer gatesMu.Unlock()
		for _, g := range holds {
			g.open()
		}
		for _, g := range answered {
			g.open()
		}
	})

	// Fill the cap with three held deciders, discovering each decider's
	// runtime ID from the start event its scripted turn publishes before the
	// hold, and confirming each holds a live running run.
	ids := map[string]string{}
	deciders := map[string]string{}
	for i := 1; i <= itestDeciderCap; i++ {
		label := fmt.Sprintf("w%d", i)
		ids[label] = h.spawn(map[string]any{"label": label, "permission_decider": itestDecider()})
		reqID := "req_" + label
		deciders[reqID] = h.deciderRuntimeID(ids[label], reqID)
		h.awaitRawStatus(deciders[reqID], "running")
	}
	// The cap is full: the next request is routed but no decider runs for it
	// and no response resolves it.
	blockedLabel := fmt.Sprintf("w%d", workers)
	ids[blockedLabel] = h.spawn(map[string]any{"label": blockedLabel, "permission_decider": itestDecider()})
	h.log.waitFor(t, "blocked permission.request", itestEventIs("permission.request", ids[blockedLabel]))
	h.consistentlyFor(permission.DefaultPollInterval+500*time.Millisecond, capHoldsMsg, func() bool {
		raw := h.rawRuntimeStatus(ids[blockedLabel])
		pending, _ := raw["pending_permission"].(bool)
		status, _ := raw["status"].(string)
		return pending && status == "running" &&
			h.log.count(itestEventIs("permission.response", ids[blockedLabel])) == 0 &&
			h.deciderIdentityCount() == itestDeciderCap
	})
	return &itestDeciderCapScenario{
		h:          h,
		ids:        ids,
		deciders:   deciders,
		holds:      holds,
		answered:   answered,
		blockedReq: blockedReq,
		blockedRT:  ids[blockedLabel],
	}
}

func TestSupervisorPublicAPIDeciderConcurrencyCapReleasesBlockedRequest(t *testing.T) {
	t.Parallel()
	s := newItestDeciderCapScenario(t, "ses_cap_", "decider cap holds", true)
	// Release the first held decider: it answers its worker's request, which
	// continues the worker, and its turn ends and frees the slot.
	s.holds["req_w1"].open()
	if err := s.h.sup.RuntimeAnswerPermission(s.ids["w1"], "req_w1", "allow_once", ""); err != nil {
		t.Fatalf("released decider answer: %v", err)
	}
	s.h.requirePermissionResponse(s.ids["w1"], "req_w1", "allow_once", "allow")
	s.h.requireEncodedContinuation(s.ids["w1"], "req_w1", "allow_once", "allow")
	// The released decider's turn is still open and still holds the slot: the
	// blocked request must keep waiting with no response and no new decider.
	s.h.consistentlyFor(permission.DefaultPollInterval+500*time.Millisecond, "blocked request waits while the released decider's turn is open", func() bool {
		return s.h.deciderIdentityCount() == itestDeciderCap &&
			s.h.log.count(itestEventIs("permission.response", s.blockedRT)) == 0
	})
	s.answered["req_w1"].open()
	s.h.awaitRawStatus(s.deciders["req_w1"], "ended")
	// The freed slot routes the blocked request to a new decider identity,
	// which answers it once; the worker never sends a second request.
	fourthDecider := s.h.deciderRuntimeID(s.blockedRT, s.blockedReq)
	s.h.requirePermissionResponse(s.blockedRT, s.blockedReq, "allow_once", "allow")
	s.h.requireEncodedContinuation(s.blockedRT, s.blockedReq, "allow_once", "allow")
	s.h.awaitRawStatus(fourthDecider, "ended")
	if got := s.h.deciderIdentityCount(); got != itestDeciderCap+1 {
		t.Fatalf("decider identities = %d, want %d (two held, one canceled, one replacement)", got, itestDeciderCap+1)
	}
	if got := s.h.log.count(itestEventIs("permission.request", s.blockedRT)); got != 1 {
		t.Fatalf("permission.request events for the blocked worker = %d, want 1", got)
	}
}

func TestSupervisorPublicAPIDeciderSlotFreedWhenDeciderCanceled(t *testing.T) {
	t.Parallel()
	s := newItestDeciderCapScenario(t, "ses_cc_", "decider cap holds before the cancel", false)
	// Cancelling one running decider through the public API frees its slot:
	// the blocked request routes to a new decider identity that answers it,
	// and the worker never sends a second request.
	if err := s.h.sup.RuntimeCancel(s.deciders["req_w1"]); err != nil {
		t.Fatalf("cancel decider: %v", err)
	}
	s.h.awaitRawStatus(s.deciders["req_w1"], "ended")
	fourthDecider := s.h.deciderRuntimeID(s.blockedRT, s.blockedReq)
	s.h.requirePermissionResponse(s.blockedRT, s.blockedReq, "allow_once", "allow")
	s.h.requireEncodedContinuation(s.blockedRT, s.blockedReq, "allow_once", "allow")
	s.h.awaitRawStatus(fourthDecider, "ended")
	if got := s.h.deciderIdentityCount(); got != itestDeciderCap+1 {
		t.Fatalf("decider identities = %d, want %d (two held, one canceled, one replacement)", got, itestDeciderCap+1)
	}
	if got := s.h.log.count(itestEventIs("permission.request", s.blockedRT)); got != 1 {
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
	worker := itestDeliveryWorker("ses_claim_file", "req_claim")
	h := newItestHarness(t, Config{PermissionClaimTimeout: 100 * time.Millisecond}, itestSingleProvider(worker))
	h.connectClient()
	base := filepath.Join(t.TempDir(), "perm")

	rt := h.spawn(map[string]any{"permission_handler": "file:" + base})
	req := h.log.waitFor(t, "permission.request", itestEventIs("permission.request", rt))
	// The connected client reserves the request first: a claim that times out
	// falls back to the file handler, so the request must start reserved
	// rather than go straight to the file handler.
	if got, _ := req.Fields["resolver"].(string); got != "reserved" {
		t.Fatalf("permission.request resolver = %q, want %q (control claim before the file fallback)", got, "reserved")
	}
	// Nobody answers over the control plane: once the claim times out the
	// request moves to the file handler, which publishes the request file.
	h.awaitCondition("permission request file", func() bool {
		_, err := os.Stat(base + ".req")
		return err == nil
	})
	raw := h.rawRuntimeStatus(rt)
	if pending, _ := raw["pending_permission"].(bool); !pending {
		t.Fatal("raw RuntimeStatus after claim timeout does not report pending_permission")
	}
	if err := os.WriteFile(base+".req.response", []byte(`{"outcome":"selected","option_id":"allow_once"}`), 0o600); err != nil {
		t.Fatalf("write permission response: %v", err)
	}
	h.requirePermissionResponse(rt, "req_claim", "allow_once", "allow")
	h.requireEncodedContinuation(rt, "req_claim", "allow_once", "allow")
}

func TestSupervisorPublicAPIDeciderDeferLeavesRequestForHuman(t *testing.T) {
	t.Parallel()
	worker := itestDeliveryWorker("ses_defer", "req_defer")
	h := newItestHarness(t, Config{}, itestDeciderFactory(
		func(string) runtime.Provider { return worker },
		func(runtime.StartOptions) (runtime.Provider, error) {
			// A deferring decider publishes its identity, then exits without
			// answering.
			return newItestProvider(itestAttempt("ses_defer_decider",
				itestAgentStatus("ses_defer_decider", "working", nil),
				itestSessionEnd("ses_defer_decider", "end_turn", ""),
			)), nil
		},
	))

	rt := h.spawn(map[string]any{"label": "worker", "permission_decider": itestDecider()})
	// Exactly one decider identity ever ran, and its turn ended deferred.
	deciderRT := h.deciderRuntimeID(rt, "req_defer")
	raw := h.awaitRawStatus(deciderRT, "ended")
	if code, _ := raw["exit_code"].(int); code != 0 {
		t.Fatalf("deferring decider raw RuntimeStatus exit_code = %v, want 0", raw["exit_code"])
	}
	// The decider deferred. No control client is connected, so nothing claims
	// the request; it stays pending on the human gate, with no re-route or
	// respawn of the decider.
	h.consistentlyFor(permission.DefaultPollInterval+500*time.Millisecond, "deferred request stays pending with exactly one decider identity", func() bool {
		raw := h.rawRuntimeStatus(rt)
		pending, _ := raw["pending_permission"].(bool)
		status, _ := raw["status"].(string)
		return pending && status == "running" && h.deciderIdentityCount() == 1
	})
	if err := h.sup.RuntimeAnswerPermission(rt, "req_defer", "allow_once", ""); err != nil {
		t.Fatalf("human RuntimeAnswerPermission: %v", err)
	}
	h.requirePermissionResponse(rt, "req_defer", "allow_once", "allow")
	h.requireEncodedContinuation(rt, "req_defer", "allow_once", "allow")
	h.consistentlyFor(permission.DefaultPollInterval+500*time.Millisecond, "exactly one decider identity (the human answer must not re-route the request)", func() bool {
		return h.deciderIdentityCount() == 1
	})
}
