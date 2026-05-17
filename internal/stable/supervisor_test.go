package stable

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/looprunner"
	"github.com/sdougbrown/avenor/internal/runtime"
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
	err = sup.RuntimePrompt("rt_nonexistent", "hello")
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

func TestSpawnParamsValidation(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket: "/tmp/test-spawn-validate.sock",
		MaxRuntimes:   2,
	})

	// Missing prompt and prompt_file
	_, err := sup.spawn(SpawnParams{Dir: "/tmp"})
	if err == nil {
		t.Fatal("spawn with no prompt should error")
	}

	// Missing dir
	_, err = sup.spawn(SpawnParams{Prompt: "hello"})
	// Dir defaults to ".", so this shouldn't error on validation alone
	// It might fail on starting the acp session though

	// opencode-http without server_url — unset env to avoid accidental
	// resolution via AVENOR_OPENCODE_URL.
	t.Setenv("AVENOR_OPENCODE_URL", "")
	_, err = sup.spawn(SpawnParams{
		Prompt:  "hello",
		Dir:     "/tmp",
		Backend: "opencode-http",
	})
	if err == nil {
		t.Fatal("spawn with backend opencode-http and no server_url should error")
	}
	if !strings.Contains(err.Error(), "server-url is required for backend opencode-http") {
		t.Errorf("error = %q, want server-url required message", err.Error())
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

	if err := sup.RuntimePrompt("rt_done", "hello"); err == nil {
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
	sup := NewSupervisor(Config{
		ControlSocket: "/tmp/test-loop-cleanup.sock",
		MaxRuntimes:   1,
	})
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
		Pre:           []looprunner.Phase{{Name: "broken", Prompt: "{{"}},
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
