package stable

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/looprunner"
	"github.com/sdougbrown/avenor/internal/phaseconfig"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
	"github.com/sdougbrown/avenor/internal/teamrunner"
)

func TestNewSupervisor(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket:   "/tmp/test-stable.sock",
		MaxRuntimes:     4,
		ShutdownTimeout: 0,
	})
	if sup == nil {
		t.Fatal("NewSupervisor returned nil")
	}
	if sup.config.MaxRuntimes != 4 {
		t.Errorf("MaxRuntimes = %d, want 4", sup.config.MaxRuntimes)
	}
}

func TestStableHandlerNoRuntimes(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket: "/tmp/test-stable-none.sock",
		MaxRuntimes:   4,
	})

	// List with no runtimes
	list := sup.List()
	runtimes, ok := list.([]map[string]any)
	if !ok {
		t.Fatalf("List() returned %T, want []map[string]any", list)
	}
	if len(runtimes) != 0 {
		t.Errorf("List() = %d runtimes, want 0", len(runtimes))
	}

	// Status for nonexistent runtime
	_, err := sup.RuntimeStatus("rt_nonexistent")
	if err == nil {
		t.Fatal("RuntimeStatus for nonexistent runtime should error")
	}

	// Cancel for nonexistent runtime
	err = sup.RuntimeCancel("rt_nonexistent")
	if err == nil {
		t.Fatal("RuntimeCancel for nonexistent runtime should error")
	}

	// Prompt for nonexistent runtime
	err = sup.RuntimePrompt("rt_nonexistent", "hello", "")
	if err == nil {
		t.Fatal("RuntimePrompt for nonexistent runtime should error")
	}

	// AnswerPermission for nonexistent runtime
	err = sup.RuntimeAnswerPermission("rt_nonexistent", "req_1", "allow")
	if err == nil {
		t.Fatal("RuntimeAnswerPermission for nonexistent runtime should error")
	}
}

func TestShutdownModeValidation(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket: "/tmp/test-shutdown.sock",
		MaxRuntimes:   4,
	})

	if err := sup.Shutdown("graceful"); err != nil {
		t.Errorf("Shutdown graceful: %v", err)
	}
	if err := sup.Shutdown("kill"); err != nil {
		t.Errorf("Shutdown kill: %v", err)
	}
	if err := sup.Shutdown("invalid"); err == nil {
		t.Fatal("Shutdown with invalid mode should error")
	}
}

func TestManagedHTTPServerStartupFailure(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket: "/tmp/test-http-server-fail.sock",
		MaxRuntimes:   2,
	})

	withFakeExec(t, func(name string, arg ...string) *exec.Cmd { return exec.Command("false") })

	_, err := sup.getOrCreateHTTPServer("/tmp")
	if err == nil {
		t.Fatal("getOrCreateHTTPServer should fail when the subprocess command fails")
	}
	// The error should mention opencode serve (the wrapper) rather than
	// "server-url is required".
	if !strings.Contains(err.Error(), "opencode serve") {
		t.Fatalf("error = %q, expected opencode serve startup error", err.Error())
	}
	if strings.Contains(err.Error(), "server-url is required") {
		t.Fatalf("error = %q, expected opencode serve startup error", err.Error())
	}
	if _, ok := sup.httpServers["/tmp"]; ok {
		t.Fatal("server entry leaked into map after startup failure")
	}
}

func TestStartHTTPServerSetsCmdDir(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket: "/tmp/test-http-server-dir.sock",
		MaxRuntimes:   2,
	})

	var capturedCmd *exec.Cmd
	withFakeExec(t, func(name string, arg ...string) *exec.Cmd {
		cmd := exec.Command("false")
		capturedCmd = cmd
		return cmd
	})

	dir := t.TempDir()
	absDir, err := filepath.Abs(dir)
	if err != nil {
		t.Fatalf("filepath.Abs: %v", err)
	}

	_, startErr := sup.getOrCreateHTTPServer(dir)
	if startErr == nil {
		t.Fatal("expected startup error from fake exec, got nil")
	}

	if capturedCmd == nil {
		t.Fatal("httpExecCommand was not called")
	}
	if capturedCmd.Dir != absDir {
		t.Fatalf("cmd.Dir = %q, want %q", capturedCmd.Dir, absDir)
	}
}

func TestManagedHTTPServerCleanupOnMap(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket: "/tmp/test-http-server-cleanup.sock",
		MaxRuntimes:   2,
	})

	withFakeExec(t, func(name string, arg ...string) *exec.Cmd { return exec.Command("true") })

	// Insert a managed server with a real subprocess (sleep 30) so
	// shutdownManagedHTTPServers can exercise SIGTERM + reap.
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	pid := cmd.Process.Pid
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()

	sup.httpServers["/tmp"] = &managedHTTPServer{
		dir:     "/tmp",
		url:     "http://127.0.0.1:12345",
		cmd:     cmd,
		exited:  exited,
		healthy: true,
	}

	sup.shutdownManagedHTTPServers()

	if len(sup.httpServers) != 0 {
		t.Fatalf("httpServers = %d, want 0 after shutdown", len(sup.httpServers))
	}

	// shutdown() already consumed the exited channel, so cmd.Wait() has
	// completed and the process is fully reaped. Verify it's gone.
	if proc, err := os.FindProcess(pid); err == nil {
		if err := proc.Signal(syscall.Signal(0)); err == nil {
			t.Fatalf("process %d still running after shutdown", pid)
		}
	}
}

func TestSpawnParamsValidation(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket: "/tmp/test-spawn-validate.sock",
		MaxRuntimes:   2,
	})

	// Replace the real exec command immediately to avoid starting real
	// opencode processes when the default backend is opencode-acp.
	withFakeExec(t, func(name string, arg ...string) *exec.Cmd { return exec.Command("false") })

	// Missing prompt, prompt_file, loop_file, and team_file
	_, err := sup.spawn(SpawnParams{Dir: "/tmp"})
	if err == nil {
		t.Fatal("spawn with no prompt should error")
	}

	// Missing dir with opencode-http backend — auto-start fails via fake exec.
	t.Setenv("AVENOR_OPENCODE_URL", "")
	_, err = sup.spawn(SpawnParams{Prompt: "hello", Backend: "opencode-http"})
	if err == nil {
		t.Fatal("spawn with backend opencode-http and no server_url should error when subprocess fails")
	}
	if strings.Contains(err.Error(), "server-url is required for backend opencode-http") {
		t.Errorf("error = %q, expected subprocess start error, not server-url required", err.Error())
	}

	// opencode-http without server_url now tries to auto-start a subprocess
	// via the fake execCommand("false") which always fails — assert the error
	// indicates subprocess startup failure rather than "server-url is required".
	_, err = sup.spawn(SpawnParams{
		Prompt:  "hello",
		Dir:     "/tmp",
		Backend: "opencode-http",
	})
	if err == nil {
		t.Fatal("spawn with backend opencode-http and no server_url should error when subprocess fails")
	}
	if strings.Contains(err.Error(), "server-url is required for backend opencode-http") {
		t.Errorf("error = %q, expected subprocess start error, not server-url required", err.Error())
	}

	_, err = sup.spawn(SpawnParams{Dir: "/tmp", LoopFile: "/tmp/loop.json", TeamFile: "/tmp/team.json"})
	if err == nil || !strings.Contains(err.Error(), "loop_file and team_file are mutually exclusive") {
		t.Fatalf("err = %v, want loop/team mutual exclusion", err)
	}
}

func TestSpawnLoopFileFailureCleansReservedRuntime(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket: "/tmp/test-loop-spawn-cleanup.sock",
		MaxRuntimes:   1,
	})

	_, err := sup.spawn(SpawnParams{LoopFile: "/path/does/not/exist.json"})
	if err == nil {
		t.Fatal("spawn with missing loop file should error")
	}
	if got := sup.activeRuntimeCount(); got != 0 {
		t.Fatalf("activeRuntimeCount() = %d, want 0", got)
	}
	if len(sup.runtimes) != 0 {
		t.Fatalf("runtimes = %d, want 0 after failed loop spawn", len(sup.runtimes))
	}
}

func TestSpawnTeamFileFailureCleansReservedRuntime(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket: "/tmp/test-team-spawn-cleanup.sock",
		MaxRuntimes:   1,
	})

	_, err := sup.spawn(SpawnParams{TeamFile: "/path/does/not/exist.json"})
	if err == nil {
		t.Fatal("spawn with missing team file should error")
	}
	if got := sup.activeRuntimeCount(); got != 0 {
		t.Fatalf("activeRuntimeCount() = %d, want 0", got)
	}
	if len(sup.runtimes) != 0 {
		t.Fatalf("runtimes = %d, want 0 after failed team spawn", len(sup.runtimes))
	}
}

func TestActiveRuntimeCountIgnoresCompletedHistory(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket: "/tmp/test-active-count.sock",
		MaxRuntimes:   1,
	})
	sup.runtimes["rt_done"] = &childRuntime{
		id:        "rt_done",
		done:      make(chan struct{}),
		promptCh:  make(chan struct{}, 1),
		completed: true,
	}

	if got := sup.activeRuntimeCount(); got != 0 {
		t.Fatalf("activeRuntimeCount() = %d, want 0 for completed history", got)
	}
}

func TestRuntimePromptRejectsCompletedRuntime(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket: "/tmp/test-prompt-completed.sock",
		MaxRuntimes:   1,
	})
	sup.runtimes["rt_done"] = &childRuntime{
		id:        "rt_done",
		done:      make(chan struct{}),
		promptCh:  make(chan struct{}, 1),
		completed: true,
	}

	if err := sup.RuntimePrompt("rt_done", "hello", ""); err == nil {
		t.Fatal("RuntimePrompt on completed runtime should error")
	}
}

func TestRuntimeInterruptAndPromptCancelsTurnNotRuntime(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket: "/tmp/test-interrupt-turn.sock",
		MaxRuntimes:   1,
	})
	runtimeCancelled := false
	turnInterrupted := false
	sup.runtimes["rt_1"] = &childRuntime{
		id:          "rt_1",
		done:        make(chan struct{}),
		promptCh:    make(chan struct{}, 1),
		cancelFn:    func() { runtimeCancelled = true },
		interruptFn: func() { turnInterrupted = true },
	}

	if err := sup.RuntimeInterruptAndPrompt("rt_1", "replacement", false); err != nil {
		t.Fatalf("RuntimeInterruptAndPrompt: %v", err)
	}
	if runtimeCancelled {
		t.Fatal("interrupt_and_prompt called runtime cancelFn; it should only cancel the active turn")
	}
	if !turnInterrupted {
		t.Fatal("interrupt_and_prompt did not call interruptFn")
	}
	rt := sup.runtimes["rt_1"]
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if len(rt.promptQueue) != 1 || rt.promptQueue[0] != "replacement" {
		t.Fatalf("promptQueue = %#v, want replacement queued first", rt.promptQueue)
	}
}

func TestRunChildAttemptUsesInitialSpawnSession(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket: "/tmp/test-initial-session.sock",
		MaxRuntimes:   1,
	})
	provider := &stableFakeProvider{
		events: make(chan events.Event, 1),
	}
	child := &childRuntime{
		id:          "rt_1",
		label:       "test",
		provider:    provider,
		session:     runtime.Session{SessionID: "ses_initial"},
		eventWriter: stableTestSink{},
		done:        make(chan struct{}),
		promptCh:    make(chan struct{}, 1),
	}

	result := sup.runChildAttempt(context.Background(), child, "", "hello", nil)
	if result.exitCode != 0 {
		t.Fatalf("exitCode = %d, want 0", result.exitCode)
	}
	if result.sessionID != "ses_initial" {
		t.Fatalf("sessionID = %q, want initial session", result.sessionID)
	}
	if provider.startCalls != 0 {
		t.Fatalf("Start called %d times, want 0 for initial spawn session", provider.startCalls)
	}
}

func TestShutdownTimeoutDoesNotHangWithMultipleStuckRuntimes(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket:   "/tmp/test-shutdown-timeout.sock",
		MaxRuntimes:     2,
		ShutdownTimeout: 10 * time.Millisecond,
	})
	sup.runtimes["rt_1"] = &childRuntime{
		id:       "rt_1",
		done:     make(chan struct{}),
		cancelFn: func() {},
	}
	sup.runtimes["rt_2"] = &childRuntime{
		id:       "rt_2",
		done:     make(chan struct{}),
		cancelFn: func() {},
	}

	done := make(chan struct{})
	go func() {
		sup.shutdown("graceful")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("shutdown did not return after timeout with multiple stuck runtimes")
	}
}

func TestRunLoopChildCleansUpOnLooprunnerError(t *testing.T) {
	b := broker.New("loop-test-token")
	if err := b.Start(); err != nil {
		t.Fatalf("broker.Start: %v", err)
	}
	defer b.Stop()

	sup := NewSupervisor(Config{
		ControlSocket: "/tmp/test-loop-cleanup.sock",
		MaxRuntimes:   1,
	})
	sup.broker = b
	sink := &closeRecordingSink{}
	cancelled := false
	child := &childRuntime{
		id:          "rt_loop",
		done:        make(chan struct{}),
		promptCh:    make(chan struct{}, 1),
		eventWriter: sink,
		cancelFn:    func() { cancelled = true },
	}
	sup.runtimes[child.id] = child

	cfg := &looprunner.LoopConfig{
		MaxIterations: 1,
		Pre:           []phaseconfig.Phase{{Name: "broken", Prompt: "{{"}},
	}
	sup.runLoopChild(context.Background(), child, cfg, 0, "", "", "", "")

	select {
	case <-child.done:
	default:
		t.Fatal("loop child done channel was not closed")
	}
	if !sink.closed {
		t.Fatal("loop child event writer was not closed")
	}
	if len(sink.events) == 0 {
		t.Fatal("loop child did not write lifecycle events")
	}
	for _, ev := range sink.events {
		if ev.Fields["runtime_id"] != "rt_loop" {
			t.Fatalf("event %s runtime_id = %v, want rt_loop", ev.Event, ev.Fields["runtime_id"])
		}
	}
	if !cancelled {
		t.Fatal("loop child cancel function was not called")
	}
	child.mu.Lock()
	completed := child.completed
	exitCode := child.exitCode
	child.mu.Unlock()
	if !completed {
		t.Fatal("loop child was not marked completed")
	}
	if exitCode != 1 {
		t.Fatalf("loop child exitCode = %d, want 1", exitCode)
	}
	if _, ok := sup.runtimes[child.id]; ok {
		t.Fatal("loop child was not removed from supervisor runtimes")
	}
	if got := b.RunCount(); got != 0 {
		t.Fatalf("broker run count = %d, want 0 after loop cleanup", got)
	}
}

func TestRunTeamChildCleansUpBrokerRuns(t *testing.T) {
	b := broker.New("team-test-token")
	if err := b.Start(); err != nil {
		t.Fatalf("broker.Start: %v", err)
	}
	defer b.Stop()

	sup := NewSupervisor(Config{
		ControlSocket: "/tmp/test-team-cleanup.sock",
		MaxRuntimes:   1,
	})
	sup.broker = b
	sink := &closeRecordingSink{}
	cancelled := false
	child := &childRuntime{
		id:          "rt_team",
		done:        make(chan struct{}),
		promptCh:    make(chan struct{}, 1),
		eventWriter: sink,
		cancelFn:    func() { cancelled = true },
	}
	sup.runtimes[child.id] = child

	cfg := &teamrunner.TeamConfig{
		Team: []phaseconfig.Phase{{Name: "review", Prompt: "review"}},
	}
	sup.runTeamChild(context.Background(), child, cfg, 0, "", "", "", "unknown-backend")

	select {
	case <-child.done:
	default:
		t.Fatal("team child done channel was not closed")
	}
	if !cancelled {
		t.Fatal("team child cancel function was not called")
	}
	child.mu.Lock()
	completed := child.completed
	child.mu.Unlock()
	if !completed {
		t.Fatal("team child was not marked completed")
	}
	if _, ok := sup.runtimes[child.id]; ok {
		t.Fatal("team child was not removed from supervisor runtimes")
	}

	if got := b.RunCount(); got != 0 {
		t.Fatalf("broker run count = %d, want 0 after team cleanup", got)
	}
}

func TestRuntimeFanoutWriterFeedsRecorder(t *testing.T) {
	b := broker.New("writer-test-token")
	if err := b.Start(); err != nil {
		t.Fatalf("broker.Start: %v", err)
	}
	defer b.Stop()
	if _, err := b.CreateRun("writer-run"); err != nil {
		t.Fatalf("broker.CreateRun: %v", err)
	}

	writer := &runtimeFanoutWriter{
		base:      stableTestSink{},
		runtimeID: "rt_writer",
		recorder:  broker.NewRecorder(b, "writer-run"),
	}

	if err := writer.Write(events.Event{
		Event:     "session.end",
		SessionID: "ses_writer",
		Fields: map[string]any{
			"stop_reason": "done",
			"exit_code":   0,
		},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if got := b.FinishCount("writer-run"); got != 1 {
		t.Fatalf("finish count = %d, want 1", got)
	}
	if got := b.LastFinishStatus("writer-run"); got != "done" {
		t.Fatalf("finish status = %q, want done", got)
	}

	t.Run("nil recorder", func(t *testing.T) {
		writer := &runtimeFanoutWriter{
			base:      stableTestSink{},
			runtimeID: "rt_writer_nil",
		}
		if err := writer.Write(events.Event{Event: "agent.status", Fields: map[string]any{"phase": "idle"}}); err != nil {
			t.Fatalf("Write with nil recorder: %v", err)
		}
	})
}

func TestRuntimeAnswerPermissionRejectsRuntimeWithoutActiveSession(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket: "/tmp/test-loop-answer-no-session.sock",
		MaxRuntimes:   1,
	})
	sup.runtimes["rt_loop"] = &childRuntime{
		id:       "rt_loop",
		done:     make(chan struct{}),
		promptCh: make(chan struct{}, 1),
	}

	if err := sup.RuntimeAnswerPermission("rt_loop", "req_1", "allow"); err == nil {
		t.Fatal("RuntimeAnswerPermission should reject runtime without active session")
	}
}

type stableTestSink struct{}

func (stableTestSink) Write(events.Event) error { return nil }
func (stableTestSink) Close() error             { return nil }

func cachePermissionOptionsThroughFanout(t *testing.T, sup *Supervisor, runtimeID string, ev events.Event) {
	t.Helper()
	writer := &runtimeFanoutWriter{
		base:            stableTestSink{},
		runtimeID:       runtimeID,
		onPermissionReq: sup.cachePermissionOptions,
	}
	if err := writer.Write(ev); err != nil {
		t.Fatalf("fanout write: %v", err)
	}
	if ev.Event == "permission.request" {
		requestID, _ := ev.Fields["request_id"].(string)
		sup.controlMu.Lock()
		_, ok := sup.permOptions[runtimeID+":"+requestID]
		sup.controlMu.Unlock()
		if !ok {
			t.Fatalf("fanout writer did not cache permission options for %s:%s", runtimeID, requestID)
		}
	}
}

type closeRecordingSink struct {
	closed bool
	events []events.Event
}

func (s *closeRecordingSink) Write(ev events.Event) error {
	s.events = append(s.events, ev)
	return nil
}
func (s *closeRecordingSink) Close() error {
	s.closed = true
	return nil
}

type stableFakeProvider struct {
	events     chan events.Event
	startCalls int
}

func (p *stableFakeProvider) Start(context.Context, runtime.StartOptions) (runtime.Session, error) {
	p.startCalls++
	return runtime.Session{SessionID: "ses_started"}, nil
}

func (p *stableFakeProvider) Resume(context.Context, string) (runtime.Session, error) {
	return runtime.Session{SessionID: "ses_resumed"}, nil
}

func (p *stableFakeProvider) Prompt(context.Context, string, string) error {
	p.events <- events.Event{
		Event:     "session.end",
		SessionID: "ses_initial",
		Fields:    map[string]any{"stop_reason": "end_turn"},
	}
	close(p.events)
	return nil
}

func (p *stableFakeProvider) Cancel(context.Context, string) error { return nil }

func (p *stableFakeProvider) Events(context.Context, string) (<-chan events.Event, error) {
	return p.events, nil
}

func (p *stableFakeProvider) AnswerPermission(context.Context, string, string, runtime.PermissionResponse) error {
	return nil
}

func (p *stableFakeProvider) Capabilities(context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{}, nil
}

// permRecordingProvider records PermissionResponse from AnswerPermission calls.
type permRecordingProvider struct {
	lastAllow    bool
	lastOptionID string
	called       bool
}

func (p *permRecordingProvider) Start(context.Context, runtime.StartOptions) (runtime.Session, error) {
	return runtime.Session{SessionID: "ses_rec"}, nil
}
func (p *permRecordingProvider) Resume(context.Context, string) (runtime.Session, error) {
	return runtime.Session{}, nil
}
func (p *permRecordingProvider) Prompt(context.Context, string, string) error { return nil }
func (p *permRecordingProvider) Cancel(context.Context, string) error         { return nil }
func (p *permRecordingProvider) Events(context.Context, string) (<-chan events.Event, error) {
	return nil, nil
}
func (p *permRecordingProvider) AnswerPermission(_ context.Context, _ string, _ string, resp runtime.PermissionResponse) error {
	p.called = true
	p.lastAllow = resp.Allow
	p.lastOptionID = resp.OptionID
	return nil
}
func (p *permRecordingProvider) Capabilities(context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{}, nil
}

func TestAnswerPermissionRejectsUnknownOptionIDWithoutConsumingCache(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket:          "/tmp/test-answer-unknown.sock",
		MaxRuntimes:            1,
		PermissionClaimTimeout: 5 * time.Second,
	})
	provider := &permRecordingProvider{}
	sup.runtimes["rt_unknown"] = &childRuntime{
		id:       "rt_unknown",
		provider: provider,
		session:  runtime.Session{SessionID: "ses_unknown"},
		done:     make(chan struct{}),
		promptCh: make(chan struct{}, 1),
	}
	cachePermissionOptionsThroughFanout(t, sup, "rt_unknown", events.Event{
		Event: "permission.request",
		Fields: map[string]any{
			"request_id": "req_unknown",
			"options": []any{
				map[string]any{"optionId": "allow_it", "kind": "allow"},
				map[string]any{"optionId": "deny_it", "kind": "reject"},
			},
		},
	})

	if err := sup.answerPermission("rt_unknown", "req_unknown", "missing"); err == nil {
		t.Fatal("answerPermission with unknown option_id should error")
	}
	if provider.called {
		t.Fatal("AnswerPermission was called for unknown option_id")
	}
	if _, ok := sup.permOptions["rt_unknown:req_unknown"]; !ok {
		t.Fatal("cache entry was consumed on invalid option_id")
	}
}

func TestAnswerPermissionRejectsUnsupportedKindWithoutConsumingCache(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket:          "/tmp/test-answer-kind-bad.sock",
		MaxRuntimes:            1,
		PermissionClaimTimeout: 5 * time.Second,
	})
	provider := &permRecordingProvider{}
	sup.runtimes["rt_kind_bad"] = &childRuntime{
		id:       "rt_kind_bad",
		provider: provider,
		session:  runtime.Session{SessionID: "ses_kind_bad"},
		done:     make(chan struct{}),
		promptCh: make(chan struct{}, 1),
	}
	cachePermissionOptionsThroughFanout(t, sup, "rt_kind_bad", events.Event{
		Event: "permission.request",
		Fields: map[string]any{
			"request_id": "req_kind_bad",
			"options": []any{
				map[string]any{"optionId": "weird", "kind": "maybe"},
			},
		},
	})

	if err := sup.answerPermission("rt_kind_bad", "req_kind_bad", "weird"); err == nil {
		t.Fatal("answerPermission with unsupported kind should error")
	}
	if provider.called {
		t.Fatal("AnswerPermission was called for unsupported kind")
	}
	if _, ok := sup.permOptions["rt_kind_bad:req_kind_bad"]; !ok {
		t.Fatal("cache entry was consumed on unsupported kind")
	}
}

func TestAnswerPermissionMapsAllowByKind(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket:          "/tmp/test-answer-kind.sock",
		MaxRuntimes:            1,
		PermissionClaimTimeout: 5 * time.Second,
	})
	provider := &permRecordingProvider{}
	sup.runtimes["rt_kind"] = &childRuntime{
		id:       "rt_kind",
		provider: provider,
		session:  runtime.Session{SessionID: "ses_kind"},
		done:     make(chan struct{}),
		promptCh: make(chan struct{}, 1),
	}
	cachePermissionOptionsThroughFanout(t, sup, "rt_kind", events.Event{
		Event: "permission.request",
		Fields: map[string]any{
			"request_id": "req_kind",
			"options": []any{
				map[string]any{"optionId": "nope_please", "kind": "reject"},
				map[string]any{"optionId": "yes_please", "kind": "allow"},
			},
		},
	})

	if err := sup.answerPermission("rt_kind", "req_kind", "yes_please"); err != nil {
		t.Fatalf("answerPermission allow: %v", err)
	}
	if !provider.called {
		t.Fatal("AnswerPermission was not called")
	}
	if !provider.lastAllow {
		t.Fatal("Allow = false, want true for kind=allow option")
	}
	if provider.lastOptionID != "yes_please" {
		t.Fatalf("OptionID = %q, want yes_please", provider.lastOptionID)
	}

	provider.called = false
	provider.lastAllow = false
	provider.lastOptionID = ""

	cachePermissionOptionsThroughFanout(t, sup, "rt_kind", events.Event{
		Event: "permission.request",
		Fields: map[string]any{
			"request_id": "req_kind",
			"options": []any{
				map[string]any{"optionId": "nope_please", "kind": "reject"},
				map[string]any{"optionId": "yes_please", "kind": "allow"},
			},
		},
	})

	if err := sup.answerPermission("rt_kind", "req_kind", "nope_please"); err != nil {
		t.Fatalf("answerPermission reject: %v", err)
	}
	if !provider.called {
		t.Fatal("AnswerPermission was not called")
	}
	if provider.lastAllow {
		t.Fatal("Allow = true, want false for kind=reject option")
	}
	if provider.lastOptionID != "nope_please" {
		t.Fatalf("OptionID = %q, want nope_please", provider.lastOptionID)
	}
}

func TestAnswerPermissionClearsCacheEntry(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket:          "/tmp/test-answer-clear.sock",
		MaxRuntimes:            1,
		PermissionClaimTimeout: 5 * time.Second,
	})
	provider := &permRecordingProvider{}
	sup.runtimes["rt_clear"] = &childRuntime{
		id:       "rt_clear",
		provider: provider,
		session:  runtime.Session{SessionID: "ses_clear"},
		done:     make(chan struct{}),
		promptCh: make(chan struct{}, 1),
	}
	cachePermissionOptionsThroughFanout(t, sup, "rt_clear", events.Event{
		Event: "permission.request",
		Fields: map[string]any{
			"request_id": "req_clear",
			"options": []any{
				map[string]any{"optionId": "ok", "kind": "allow"},
			},
		},
	})

	if err := sup.answerPermission("rt_clear", "req_clear", "ok"); err != nil {
		t.Fatalf("answerPermission: %v", err)
	}
	if _, ok := sup.permOptions["rt_clear:req_clear"]; ok {
		t.Fatal("cache entry was not cleared after use")
	}
}

func TestTombstoneOnStartFailed(t *testing.T) {
	tmpDir := newStableSocketTestDir(t, "startfail")
	parentFile := filepath.Join(tmpDir, "not-a-dir")
	if err := os.WriteFile(parentFile, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(parentFile, "test.sock")
	tombstonePath := filepath.Join(tmpDir, "startfail.dead")

	sup := NewSupervisor(Config{
		ControlSocket: socketPath,
		TombstoneFile: tombstonePath,
		MaxRuntimes:   1,
	})

	code := sup.Run()
	if code != 1 {
		t.Fatalf("Run() = %d, want 1", code)
	}

	data, err := os.ReadFile(tombstonePath)
	if err != nil {
		t.Fatalf("tombstone not written: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "reason=start_failed") {
		t.Fatalf("tombstone = %q, want reason=start_failed", content)
	}
	if !strings.Contains(content, "STOPPED ") {
		t.Fatalf("tombstone = %q, missing STOPPED prefix", content)
	}
}

func TestTombstoneOnGracefulShutdown(t *testing.T) {
	socketPath := newStableSocketPath(t, "shutdown")
	tombstonePath := socketPath + ".dead"

	sup := NewSupervisor(Config{
		ControlSocket:   socketPath,
		TombstoneFile:   tombstonePath,
		MaxRuntimes:     1,
		ShutdownTimeout: time.Second,
	})

	done := make(chan int, 1)
	go func() {
		done <- sup.Run()
	}()

	// Wait for socket to exist.
	ready := false
	for i := 0; i < 100; i++ {
		if _, err := os.Stat(socketPath); err == nil {
			ready = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ready {
		t.Fatal("socket never appeared")
	}

	// Trigger graceful shutdown.
	sup.Shutdown("graceful")

	var code int
	select {
	case code = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after shutdown")
	}
	if code != 0 {
		t.Fatalf("Run() = %d, want 0", code)
	}

	data, err := os.ReadFile(tombstonePath)
	if err != nil {
		t.Fatalf("tombstone not written: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "reason=shutdown") {
		t.Fatalf("tombstone = %q, want reason=shutdown", content)
	}
}

func TestTombstoneOnIdleTimeout(t *testing.T) {
	socketPath := newStableSocketPath(t, "idle")
	tombstonePath := socketPath + ".dead"

	sup := NewSupervisor(Config{
		ControlSocket: socketPath,
		TombstoneFile: tombstonePath,
		MaxRuntimes:   1,
		IdleTimeout:   200 * time.Millisecond,
	})

	done := make(chan int, 1)
	go func() {
		done <- sup.Run()
	}()

	// Wait for startup readiness before relying on idle timeout.
	ready := false
	for i := 0; i < 100; i++ {
		if _, err := os.Stat(socketPath); err == nil {
			ready = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ready {
		t.Fatal("socket never appeared")
	}

	var code int
	select {
	case code = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after idle timeout")
	}
	if code != 0 {
		t.Fatalf("Run() = %d, want 0", code)
	}

	data, err := os.ReadFile(tombstonePath)
	if err != nil {
		t.Fatalf("tombstone not written: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "reason=idle") {
		t.Fatalf("tombstone = %q, want reason=idle", content)
	}
}

func TestTombstoneOnSignal(t *testing.T) {
	if os.Getenv("AVENOR_TEST_SIGNAL_SUBPROCESS") == "1" {
		testTombstoneOnSignalSubprocess(t)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestTombstoneOnSignal$")
	cmd.Env = append(os.Environ(), "AVENOR_TEST_SIGNAL_SUBPROCESS=1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("signal subprocess failed: %v\n%s", err, output)
	}
}

func testTombstoneOnSignalSubprocess(t *testing.T) {
	if os.PathSeparator == '\\' {
		t.Skip("signal test skipped on Windows")
	}
	socketPath := newStableSocketPath(t, "signal")
	tombstonePath := socketPath + ".dead"

	sup := NewSupervisor(Config{
		ControlSocket: socketPath,
		TombstoneFile: tombstonePath,
		MaxRuntimes:   1,
	})

	done := make(chan int, 1)
	go func() {
		done <- sup.Run()
	}()

	// Wait for socket to exist. signal.NotifyContext is registered before
	// s.control.Start(), so the socket appearing means the handler is ready.
	ready := false
	for i := 0; i < 100; i++ {
		if _, err := os.Stat(socketPath); err == nil {
			ready = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ready {
		t.Fatal("socket never appeared")
	}
	p, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("FindProcess: %v", err)
	}
	if err := p.Signal(os.Interrupt); err != nil {
		t.Fatalf("Signal: %v", err)
	}

	var code int
	select {
	case code = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after signal")
	}
	if code != 0 {
		t.Fatalf("Run() = %d, want 0", code)
	}

	data, err := os.ReadFile(tombstonePath)
	if err != nil {
		t.Fatalf("tombstone not written: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "reason=signal") {
		t.Fatalf("tombstone = %q, want reason=signal", content)
	}
}

func TestGetOrCreateHTTPServerConcurrentSameDir(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket: "/tmp/test-http-server-concurrent.sock",
		MaxRuntimes:   2,
	})

	// Write a tiny script that blocks for a while, proving the sentinel
	// dedup works: only the first goroutine reaches the script while others
	// wait on the condition variable.
	scriptDir := t.TempDir()
	scriptPath := filepath.Join(scriptDir, "block.sh")
	if err := os.WriteFile(scriptPath, []byte("#!/bin/sh\nsleep 2\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	const numGoroutines = 5

	// maxInflight tracks the peak number of goroutines simultaneously in
	// the exec call. If the sentinel dedup works correctly, this should be
	// 1 even though numGoroutines goroutines call getOrCreateHTTPServer concurrently.
	var maxInflight atomic.Int32
	var curInflight int
	var mu sync.Mutex
	gate := make(chan struct{})
	arrived := make(chan struct{}, numGoroutines) // buffered so goroutines don't block

	fakeExec := func(name string, arg ...string) *exec.Cmd {
		mu.Lock()
		curInflight++
		if curInflight > int(maxInflight.Load()) {
			maxInflight.Store(int32(curInflight))
		}
		mu.Unlock()

		// Block until the gate opens (signaled after all goroutines have
		// arrived), then run the real script.
		<-gate

		mu.Lock()
		curInflight--
		mu.Unlock()
		return exec.Command(scriptPath)
	}
	oldExecCommand := httpExecCommand
	httpExecCommand = fakeExec
	defer func() { httpExecCommand = oldExecCommand }()

	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			arrived <- struct{}{}
			_, _ = sup.getOrCreateHTTPServer("/tmp/concurrent-test-dir")
		}()
	}

	// Wait for all goroutines to arrive before opening the gate.
	for i := 0; i < numGoroutines; i++ {
		<-arrived
	}
	close(gate)
	wg.Wait()

	if got := maxInflight.Load(); got != 1 {
		t.Fatalf("max concurrent exec calls = %d, want 1 (concurrent callers should be deduped by the sentinel)", got)
	}
}

func newStableSocketPath(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join(newStableSocketTestDir(t, name), name+".sock")
}

func newStableSocketTestDir(t *testing.T, name string) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "ast-"+name+"-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// withFakeExec replaces httpExecCommand for the duration of the test,
// restoring it on cleanup.
func withFakeExec(t *testing.T, fake func(name string, arg ...string) *exec.Cmd) {
	t.Helper()
	old := httpExecCommand
	httpExecCommand = fake
	t.Cleanup(func() { httpExecCommand = old })
}

// TestSpawnAndSendToParent exercises the full parent-child round-trip through
// the control socket: spawn a parent, verify tracking, spawn children with
// parent context, send a message from a child to the parent, and verify the
// child.question event is emitted.
func TestSpawnAndSendToParent(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "ctrl.sock")

	sup := NewSupervisor(Config{
		ControlSocket:   socketPath,
		MaxRuntimes:     10,
		ShutdownTimeout: 0,
	})
	// Start the control server so the client can connect.
	if err := sup.control.Start(socketPath); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	defer sup.control.Stop()

	// Connect a client to the control socket.
	c, err := client.Dial(socketPath)
	if err != nil {
		t.Fatalf("dial control socket: %v", err)
	}
	defer c.Close()

	// Subscribe to events so the server pushes them to this client.
	if err := c.Call("subscribe", nil, nil); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	eventCh := c.Events()

	// 1. Spawn a parent runtime (the jockey) using pony backend so no
	// external server is needed.
	parentParams := map[string]any{
		"prompt":  "You are a jockey.",
		"dir":     ".",
		"backend": "pony",
	}
	parentResult, err := c.Spawn(parentParams)
	if err != nil {
		t.Fatalf("spawn parent: %v", err)
	}
	parentID, ok := parentResult["runtime_id"].(string)
	if !ok || parentID == "" {
		t.Fatalf("parent spawn result missing runtime_id: %v", parentResult)
	}
	parentSesID, _ := parentResult["session_id"].(string)

	t.Logf("parent runtime_id=%s session_id=%s", parentID, parentSesID)

	// Verify parent has no parent and no children yet.
	list := sup.List()
	rts := list.([]map[string]any)
	parentEntry := findRuntimeByID(rts, parentID)
	if parentEntry == nil {
		t.Fatal("parent not found in list")
	}
	if pid, _ := parentEntry["parent_id"].(string); pid != "" {
		t.Errorf("parent.parent_id = %q, want empty", pid)
	}
	if kids := childrenList(parentEntry); len(kids) != 0 {
		t.Errorf("parent.children = %v, want empty", kids)
	}

	// 2. Spawn children using the parent's runtime_id for auto-population.
	childCount := 2
	var childIDs []string
	for i := 0; i < childCount; i++ {
		childParams := map[string]any{
			"prompt":     "Do work.",
			"dir":        ".",
			"backend":    "pony",
			"runtime_id": parentID, // tells supervisor "I am the parent"
			"label":      fmt.Sprintf("child_%d", i+1),
		}
		childResult, err := c.Spawn(childParams)
		if err != nil {
			t.Fatalf("spawn child %d: %v", i, err)
		}
		cid, ok := childResult["runtime_id"].(string)
		if !ok || cid == "" {
			t.Fatalf("child %d spawn result missing runtime_id: %v", i, childResult)
		}
		childIDs = append(childIDs, cid)
	}

	// 3. Verify parent-child tracking.
	list = sup.List()
	rts = list.([]map[string]any)
	parentEntry = findRuntimeByID(rts, parentID)
	kidIDs := childrenList(parentEntry)
	if len(kidIDs) != childCount {
		t.Fatalf("parent.children count = %d, want %d", len(kidIDs), childCount)
	}
	for i, cid := range childIDs {
		if kidIDs[i] != cid {
			t.Errorf("parent.children[%d] = %q, want %q", i, kidIDs[i], cid)
		}
	}

	// Verify each child has the correct parent_id.
	for _, cid := range childIDs {
		entry := findRuntimeByID(rts, cid)
		if entry == nil {
			t.Fatalf("child %s not found in list", cid)
		}
		pid, _ := entry["parent_id"].(string)
		if pid != parentID {
			t.Errorf("child %s parent_id = %q, want %q", cid, pid, parentID)
		}
	}

	// 4. Send a message from child_1 to its parent via send_to_parent RPC.
	message := "Which package should I use?"
	err = c.SendToParent(childIDs[0], message)
	if err != nil {
		t.Fatalf("send_to_parent: %v", err)
	}

	// 5. Verify the child.question event was emitted.
	// Drain events until we find it or time out.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	found := false
	for {
		select {
		case evt, ok := <-eventCh:
			if !ok {
				t.Fatal("event channel closed unexpectedly")
			}
			if evt.Event == runtime.EventChildQuestion {
				// Verify event fields.
				if evt.RuntimeID != parentID {
					t.Errorf("child.question runtime_id = %q, want %q", evt.RuntimeID, parentID)
				}
				if evt.SessionID != parentSesID {
					t.Errorf("child.question session_id = %q, want %q", evt.SessionID, parentSesID)
				}
				gotMsg, _ := evt.Raw["message"].(string)
				if gotMsg != message {
					t.Errorf("child.question message = %q, want %q", gotMsg, message)
				}
				gotChildID, _ := evt.Raw["child_id"].(string)
				if gotChildID != childIDs[0] {
					t.Errorf("child.question child_id = %q, want %q", gotChildID, childIDs[0])
				}
				gotRequestID, _ := evt.Raw["request_id"].(string)
				if gotRequestID == "" || !strings.HasPrefix(gotRequestID, "cq_") {
					t.Errorf("child.question request_id = %q, want prefix cq_", gotRequestID)
				}
				found = true
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for child.question event")
		}
		if found {
			break
		}
	}
}

func TestSendToParentTimeoutInjectsSyntheticPrompt(t *testing.T) {
	sup := NewSupervisor(Config{ChildQuestionTimeout: 25 * time.Millisecond})

	parent := &childRuntime{id: "rt_parent", promptCh: make(chan struct{}, 1)}
	parent.session.SessionID = "ses_parent"
	child := &childRuntime{id: "rt_child", parentID: "rt_parent", promptCh: make(chan struct{}, 1)}
	child.session.SessionID = "ses_child"

	sup.controlMu.Lock()
	sup.runtimes[parent.id] = parent
	sup.runtimes[child.id] = child
	sup.controlMu.Unlock()

	if err := sup.RuntimeSendToParent(child.id, "which package?"); err != nil {
		t.Fatalf("RuntimeSendToParent: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	child.mu.Lock()
	defer child.mu.Unlock()
	if len(child.promptQueue) != 1 {
		t.Fatalf("promptQueue len = %d, want 1", len(child.promptQueue))
	}
	if !strings.Contains(child.promptQueue[0], "No parent response received within") {
		t.Fatalf("timeout prompt = %q, want timeout guidance", child.promptQueue[0])
	}
}

func TestSendToParentTimeoutCancelledByPrompt(t *testing.T) {
	sup := NewSupervisor(Config{ChildQuestionTimeout: 50 * time.Millisecond})

	parent := &childRuntime{id: "rt_parent", promptCh: make(chan struct{}, 1)}
	parent.session.SessionID = "ses_parent"
	child := &childRuntime{id: "rt_child", parentID: "rt_parent", promptCh: make(chan struct{}, 1)}
	child.session.SessionID = "ses_child"

	sup.controlMu.Lock()
	sup.runtimes[parent.id] = parent
	sup.runtimes[child.id] = child
	sup.controlMu.Unlock()

	if err := sup.RuntimeSendToParent(child.id, "need clarification"); err != nil {
		t.Fatalf("RuntimeSendToParent: %v", err)
	}
	if err := sup.RuntimePrompt(child.id, "Use encoding/json", ""); err != nil {
		t.Fatalf("RuntimePrompt: %v", err)
	}

	time.Sleep(120 * time.Millisecond)

	child.mu.Lock()
	defer child.mu.Unlock()
	if len(child.promptQueue) != 1 {
		t.Fatalf("promptQueue len = %d, want 1", len(child.promptQueue))
	}
	if child.promptQueue[0] != "Use encoding/json" {
		t.Fatalf("promptQueue[0] = %q, want parent prompt", child.promptQueue[0])
	}
}

func TestSendToParentDuplicateRequestIDIgnored(t *testing.T) {
	sup := NewSupervisor(Config{ChildQuestionTimeout: time.Second})

	child := &childRuntime{id: "rt_child", promptCh: make(chan struct{}, 1)}
	sup.controlMu.Lock()
	sup.runtimes[child.id] = child
	sup.pendingQuestions[child.id] = pendingChildQuestion{requestID: "cq_42"}
	sup.controlMu.Unlock()

	if err := sup.RuntimePrompt(child.id, "First answer", "cq_42"); err != nil {
		t.Fatalf("RuntimePrompt first call: %v", err)
	}
	if err := sup.RuntimePrompt(child.id, "Duplicate answer", "cq_42"); err != nil {
		t.Fatalf("RuntimePrompt duplicate call: %v", err)
	}

	child.mu.Lock()
	defer child.mu.Unlock()
	if len(child.promptQueue) != 1 {
		t.Fatalf("promptQueue len = %d, want 1", len(child.promptQueue))
	}
	if child.promptQueue[0] != "First answer" {
		t.Fatalf("promptQueue[0] = %q, want First answer", child.promptQueue[0])
	}
}

// findRuntimeByID is a test helper to find a runtime entry by ID in a list.
func findRuntimeByID(rts []map[string]any, id string) map[string]any {
	for _, rt := range rts {
		if rt["runtime_id"] == id {
			return rt
		}
	}
	return nil
}

// childrenList extracts the children field from a runtime entry as a []string,
// handling both []string and []any representations that can arise from direct
// struct access vs JSON round-trips.
func childrenList(entry map[string]any) []string {
	v := entry["children"]
	switch xs := v.(type) {
	case []string:
		return xs
	case []any:
		out := make([]string, len(xs))
		for i, x := range xs {
			out[i], _ = x.(string)
		}
		return out
	default:
		return nil
	}
}
