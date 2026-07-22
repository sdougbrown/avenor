package stable

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
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
	"github.com/sdougbrown/avenor/internal/cli"
	"github.com/sdougbrown/avenor/internal/control"
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

// TestRuntimeStatusSurfacesPhaseAndPermission verifies that RuntimeStatus
// reflects phase, phase_label, and pending_permission updated via the event
// fanout writer. Without this, a stable-mode sub-agent asking a question or
// requesting a permission is invisible to status polling — the orchestrator
// spins forever because it never observes the blocked state.
func TestRuntimeStatusSurfacesPhaseAndPermission(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket: "/tmp/test-runtime-status-surface.sock",
		MaxRuntimes:   2,
	})
	child := &childRuntime{
		id:       "rt_status",
		done:     make(chan struct{}),
		promptCh: make(chan struct{}, 1),
		active:   true,
	}
	sup.runtimes[child.id] = child

	writer := &runtimeFanoutWriter{
		base:      stableTestSink{},
		runtimeID: child.id,
		child:     child,
	}

	// 1) agent.status with phase=waiting and a label must surface as phase
	//    "waiting" and phase_label.
	if err := writer.Write(events.Event{
		Event: "agent.status",
		Fields: map[string]any{
			"phase": "waiting",
			"label": "Allow exec?",
		},
	}); err != nil {
		t.Fatalf("write agent.status: %v", err)
	}

	statusAny, err := sup.RuntimeStatus(child.id)
	if err != nil {
		t.Fatalf("RuntimeStatus: %v", err)
	}
	status, ok := statusAny.(map[string]any)
	if !ok {
		t.Fatalf("RuntimeStatus returned %T, want map[string]any", statusAny)
	}
	if got, _ := status["phase"].(string); got != "waiting" {
		t.Errorf("phase = %q, want waiting", got)
	}
	if got, _ := status["phase_label"].(string); got != "Allow exec?" {
		t.Errorf("phase_label = %q, want \"Allow exec?\"", got)
	}
	if got, _ := status["pending_permission"].(bool); got {
		t.Errorf("pending_permission = %v, want false before permission.request", got)
	}

	// 2) permission.request must set pending_permission=true and copy the
	//    request fields into the permission map.
	if err := writer.Write(events.Event{
		Event: "permission.request",
		Fields: map[string]any{
			"request_id": "req_42",
			"question":   "Run bash?",
			"options":    []any{map[string]any{"optionId": "allow", "kind": "allow"}},
		},
	}); err != nil {
		t.Fatalf("write permission.request: %v", err)
	}

	statusAny, err = sup.RuntimeStatus(child.id)
	if err != nil {
		t.Fatalf("RuntimeStatus after permission.request: %v", err)
	}
	status, ok = statusAny.(map[string]any)
	if !ok {
		t.Fatalf("RuntimeStatus returned %T, want map[string]any", statusAny)
	}
	if got, _ := status["pending_permission"].(bool); !got {
		t.Errorf("pending_permission = false, want true after permission.request")
	}
	perm, ok := status["permission"].(map[string]any)
	if !ok {
		t.Fatalf("permission field missing or wrong type: %T", status["permission"])
	}
	if got, _ := perm["request_id"].(string); got != "req_42" {
		t.Errorf("permission.request_id = %q, want req_42", got)
	}

	// 3) permission.response must clear pending_permission. The phase stays
	//    "waiting" until a subsequent agent.status (e.g. PermissionAnswered
	//    emitting phase=working) updates it — the response itself does not
	//    flip the phase, so assert it's unchanged.
	if err := writer.Write(events.Event{
		Event: "permission.response",
		Fields: map[string]any{
			"request_id": "req_42",
			"outcome":    "selected",
		},
	}); err != nil {
		t.Fatalf("write permission.response: %v", err)
	}

	statusAny, _ = sup.RuntimeStatus(child.id)
	status, _ = statusAny.(map[string]any)
	if got, _ := status["pending_permission"].(bool); got {
		t.Errorf("pending_permission = true, want false after permission.response")
	}
	if status["permission"] != nil {
		t.Errorf("permission = %v, want nil after permission.response", status["permission"])
	}
	if got, _ := status["phase"].(string); got != "waiting" {
		t.Errorf("phase = %q, want waiting (response clears permission but not phase)", got)
	}

	// 4) session.end sets phase to "done".
	if err := writer.Write(events.Event{
		Event: "session.end",
		Fields: map[string]any{
			"stop_reason": "end_turn",
			"exit_code":   0,
		},
	}); err != nil {
		t.Fatalf("write session.end: %v", err)
	}

	statusAny, _ = sup.RuntimeStatus(child.id)
	status, _ = statusAny.(map[string]any)
	if got, _ := status["phase"].(string); got != "done" {
		t.Errorf("phase = %q, want done after session.end", got)
	}

	// 5) A stale agent.status arriving after session.end must not overwrite
	//    the terminal "done" phase. Event streams normally stop after
	//    session.end, but a defensive guard prevents a late event from
	//    resurrecting a completed runtime's phase.
	if err := writer.Write(events.Event{
		Event: "agent.status",
		Fields: map[string]any{
			"phase": "working",
			"label": "should be ignored",
		},
	}); err != nil {
		t.Fatalf("write stale agent.status: %v", err)
	}

	statusAny, _ = sup.RuntimeStatus(child.id)
	status, _ = statusAny.(map[string]any)
	if got, _ := status["phase"].(string); got != "done" {
		t.Errorf("phase = %q, want done (stale agent.status must not overwrite terminal phase)", got)
	}
}

// TestListRuntimesSurfacesPhaseAndPermission verifies the list path also
// surfaces phase and pending_permission for each runtime.
func TestListRuntimesSurfacesPhaseAndPermission(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket: "/tmp/test-list-surface.sock",
		MaxRuntimes:   2,
	})
	child := &childRuntime{
		id:                "rt_list",
		done:              make(chan struct{}),
		promptCh:          make(chan struct{}, 1),
		active:            true,
		phase:             "waiting",
		phaseLabel:        "Need approval",
		pendingPermission: true,
		permission:        map[string]any{"request_id": "req_99"},
	}
	sup.runtimes[child.id] = child

	listAny := sup.List()
	list, ok := listAny.([]map[string]any)
	if !ok {
		t.Fatalf("List() returned %T, want []map[string]any", listAny)
	}
	if len(list) != 1 {
		t.Fatalf("List() = %d entries, want 1", len(list))
	}
	entry := list[0]
	if got, _ := entry["phase"].(string); got != "waiting" {
		t.Errorf("list phase = %q, want waiting", got)
	}
	if got, _ := entry["phase_label"].(string); got != "Need approval" {
		t.Errorf("list phase_label = %q, want \"Need approval\"", got)
	}
	if got, _ := entry["pending_permission"].(bool); !got {
		t.Errorf("list pending_permission = false, want true")
	}
	perm, ok := entry["permission"].(map[string]any)
	if !ok {
		t.Fatalf("list permission missing or wrong type: %T", entry["permission"])
	}
	if got, _ := perm["request_id"].(string); got != "req_99" {
		t.Errorf("list permission.request_id = %q, want req_99", got)
	}
}

func TestRuntimeStatusMetadataIncludesUsageFinalOutputAndStamp(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/test-runtime-meta.sock", MaxRuntimes: 1})
	child := &childRuntime{
		id:           "rt_meta",
		label:        "worker",
		agent:        "horse",
		model:        "model-x",
		backend:      "pi",
		runID:        sup.runID,
		dir:          "/tmp/work",
		onEvent:      "/tmp/work/events.ndjson",
		sentinelFile: "/tmp/work/sentinel.env",
		done:         make(chan struct{}),
		promptCh:     make(chan struct{}, 1),
	}
	sup.runtimes[child.id] = child

	sink := &metadataCaptureSink{}
	writer := &runtimeFanoutWriter{base: sink, runtimeID: child.id, child: child, control: sup.control}
	if err := writer.Write(events.Event{Event: "session.end", SessionID: "ses_meta", Fields: map[string]any{"stop_reason": "end_turn", "usage": map[string]any{"total_tokens": 7}, "final_output": "final text"}}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if len(sink.events) != 1 {
		t.Fatalf("captured %d events, want 1", len(sink.events))
	}
	if got, _ := sink.events[0].Fields["run_id"].(string); got != sup.runID {
		t.Fatalf("run_id = %q, want %q", got, sup.runID)
	}
	if _, ok := sink.events[0].Fields["seq"]; !ok {
		t.Fatal("expected seq to be stamped")
	}
	if _, ok := sink.events[0].Fields["ts"]; !ok {
		t.Fatal("expected ts to be stamped")
	}

	statusAny, err := sup.RuntimeStatus(child.id)
	if err != nil {
		t.Fatalf("RuntimeStatus: %v", err)
	}
	status := statusAny.(map[string]any)
	for key, want := range map[string]any{"agent": "horse", "model": "model-x", "backend": "pi", "event_path": "/tmp/work/events.ndjson", "final_output": "final text"} {
		if got := status[key]; got != want {
			t.Fatalf("status[%q] = %v, want %v", key, got, want)
		}
	}
	usage, _ := status["usage"].(map[string]any)
	if got, _ := usage["total_tokens"].(int); got != 7 {
		if gotf, _ := usage["total_tokens"].(float64); int(gotf) != 7 {
			t.Fatalf("usage.total_tokens = %v, want 7", usage["total_tokens"])
		}
	}
	if got, _ := status["latest_seq"].(int64); got == 0 {
		if gotf, _ := status["latest_seq"].(float64); gotf == 0 {
			t.Fatal("expected latest_seq to be populated")
		}
	}

	list := sup.List().([]map[string]any)
	if len(list) != 1 {
		t.Fatalf("List() = %d entries, want 1", len(list))
	}
	if list[0]["final_output"] != "final text" {
		t.Fatalf("list final_output = %v, want final text", list[0]["final_output"])
	}
}

func TestRuntimeFanoutWriterPreservesBackendFinalOutputAndBoundsStatus(t *testing.T) {
	child := &childRuntime{id: "rt_bound"}
	sink := &metadataCaptureSink{}
	writer := &runtimeFanoutWriter{base: sink, runtimeID: child.id, child: child}
	finalOutput := strings.Repeat("é", events.MaxFinalOutputRunes+10)

	if err := writer.Write(events.Event{
		Event:  "session.end",
		Fields: map[string]any{"final_output": finalOutput},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(sink.events) != 1 {
		t.Fatalf("captured %d events, want 1", len(sink.events))
	}
	got, _ := sink.events[0].Fields["final_output"].(string)
	if got != finalOutput {
		t.Fatal("durable event did not preserve the complete final_output")
	}
	child.mu.Lock()
	childOutput := child.finalOutput
	fullChildOutput := child.fullFinalOutput
	child.mu.Unlock()
	if count := len([]rune(childOutput)); count != events.MaxFinalOutputRunes {
		t.Fatalf("status final_output rune count = %d, want %d", count, events.MaxFinalOutputRunes)
	}
	if fullChildOutput != finalOutput {
		t.Fatal("child result storage did not preserve the complete final_output")
	}
	if !child.finalOutputTruncated {
		t.Fatal("bounded stable status preview did not expose truncation metadata")
	}
	sup := &Supervisor{runtimes: map[string]*childRuntime{child.id: child}}
	result, err := sup.RuntimeResult(child.id)
	if err != nil {
		t.Fatalf("RuntimeResult: %v", err)
	}
	if got := result.(map[string]any)["final_output"]; got != finalOutput {
		t.Fatal("RuntimeResult did not return the complete terminal output")
	}
}

func TestStableResultOverControlSocketUsesRuntimeIDAndPreservesLargeOutput(t *testing.T) {
	socket := filepath.Join(newStableSocketTestDir(t, "stable-result"), "control.sock")
	sup := NewSupervisor(Config{ControlSocket: socket, MaxRuntimes: 1})
	complete := strings.Repeat("é", 40_000) // 80 KiB UTF-8, beyond Scanner's 64 KiB default.
	sup.runtimes["rt_large"] = &childRuntime{id: "rt_large", fullFinalOutput: complete}
	if err := sup.control.Start(socket); err != nil {
		t.Fatalf("start control: %v", err)
	}
	defer sup.control.Stop()

	c, err := client.Dial(socket)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()
	result, err := c.Result("rt_large")
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	if got, _ := result["final_output"].(string); got != complete {
		t.Fatalf("final_output byte length = %d, want %d", len(got), len(complete))
	}
}

func TestRuntimeFanoutWriterWritesSameStampedEventToFileAndControl(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/test-runtime-live.sock", MaxRuntimes: 1})
	path := filepath.Join(t.TempDir(), "events.ndjson")
	fileWriter, err := cli.NewEventWriter(path)
	if err != nil {
		t.Fatalf("NewEventWriter: %v", err)
	}

	child := &childRuntime{
		id:          "rt_live",
		runID:       sup.runID,
		label:       "review",
		eventWriter: fileWriter,
		done:        make(chan struct{}),
		promptCh:    make(chan struct{}, 1),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	liveCh := sup.control.SubscribeEvents(ctx)
	writer := &runtimeFanoutWriter{
		base:      fileWriter,
		runtimeID: child.id,
		child:     child,
		control:   sup.control,
		metadata:  cli.NewEventMetadata(sup.runID, child.label, child.id),
	}
	if err := writer.Write(events.Event{Event: "agent.status", SessionID: "ses_live", Fields: map[string]any{"phase": "working", "label": "dispatch"}}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var live events.Event
	select {
	case live = <-liveCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for live event")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	line := strings.TrimSpace(string(data))
	if line == "" {
		t.Fatal("expected stamped event in file")
	}
	var logged events.Event
	if err := json.Unmarshal([]byte(line), &logged); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if logged.Fields["run_id"] != sup.runID {
		t.Fatalf("run_id = %v, want %v", logged.Fields["run_id"], sup.runID)
	}
	if logged.Fields["run_label"] != "review" {
		t.Fatalf("run_label = %v, want review", logged.Fields["run_label"])
	}
	if logged.Fields["runtime_id"] != "rt_live" {
		t.Fatalf("runtime_id = %v, want rt_live", logged.Fields["runtime_id"])
	}
	if seq := logged.Fields["seq"]; seq != float64(1) {
		t.Fatalf("seq = %v, want 1", seq)
	}
	if ts, ok := logged.Fields["ts"].(float64); !ok || ts <= 0 {
		t.Fatalf("ts = %v, want positive", logged.Fields["ts"])
	}
	loggedJSON, err := json.Marshal(logged)
	if err != nil {
		t.Fatalf("Marshal logged: %v", err)
	}
	liveJSON, err := json.Marshal(live)
	if err != nil {
		t.Fatalf("Marshal live: %v", err)
	}
	if string(loggedJSON) != string(liveJSON) {
		t.Fatalf("file/live mismatch\nfile=%s\nlive=%s", loggedJSON, liveJSON)
	}
}

type metadataCaptureSink struct{ events []events.Event }

func (s *metadataCaptureSink) Write(ev events.Event) error {
	s.events = append(s.events, ev)
	return nil
}

func (s *metadataCaptureSink) Close() error { return nil }

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

type blockingPermissionProvider struct {
	permRecordingProvider
	started chan struct{}
	release chan struct{}
	mu      sync.Mutex
	calls   int
}

func (p *blockingPermissionProvider) AnswerPermission(_ context.Context, _ string, _ string, _ runtime.PermissionResponse) error {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	select {
	case p.started <- struct{}{}:
	default:
	}
	<-p.release
	return nil
}

func (p *blockingPermissionProvider) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

type retryPermissionProvider struct {
	permRecordingProvider
	mu    sync.Mutex
	calls int
}

func (p *retryPermissionProvider) AnswerPermission(_ context.Context, _ string, _ string, _ runtime.PermissionResponse) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.calls == 1 {
		return fmt.Errorf("temporary provider failure")
	}
	return nil
}

type stablePermissionProvider struct {
	answers chan string
}

func (p *stablePermissionProvider) Start(context.Context, runtime.StartOptions) (runtime.Session, error) {
	return runtime.Session{}, nil
}
func (p *stablePermissionProvider) Resume(context.Context, string) (runtime.Session, error) {
	return runtime.Session{}, nil
}
func (p *stablePermissionProvider) Prompt(context.Context, string, string) error { return nil }
func (p *stablePermissionProvider) Cancel(context.Context, string) error         { return nil }
func (p *stablePermissionProvider) Events(context.Context, string) (<-chan events.Event, error) {
	return nil, nil
}
func (p *stablePermissionProvider) AnswerPermission(_ context.Context, _ string, requestID string, _ runtime.PermissionResponse) error {
	p.answers <- requestID
	return nil
}
func (p *stablePermissionProvider) Capabilities(context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{}, nil
}

type synchronousStablePermissionSink struct {
	answer    func() error
	responses chan events.Event
}

func (s *synchronousStablePermissionSink) Write(event events.Event) error {
	if event.Event == "permission.request" {
		return s.answer()
	}
	if event.Event == "permission.response" {
		s.responses <- event
	}
	return nil
}
func (s *synchronousStablePermissionSink) Close() error { return nil }

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

// controlClaimProvider drives one permission.request through a real session so
// the control-claim wiring in runChildAttempt is exercised end to end.
type controlClaimProvider struct {
	events   chan events.Event
	answered chan string
}

func (p *controlClaimProvider) Start(context.Context, runtime.StartOptions) (runtime.Session, error) {
	return runtime.Session{SessionID: "ses_claim"}, nil
}
func (p *controlClaimProvider) Resume(context.Context, string) (runtime.Session, error) {
	return runtime.Session{}, nil
}
func (p *controlClaimProvider) Prompt(context.Context, string, string) error {
	p.events <- events.Event{
		Event:     "permission.request",
		SessionID: "ses_claim",
		Fields: map[string]any{
			"request_id": "0",
			"options":    []any{map[string]any{"optionId": "allow", "kind": "allow"}},
		},
	}
	return nil
}
func (p *controlClaimProvider) Cancel(context.Context, string) error { return nil }
func (p *controlClaimProvider) Events(context.Context, string) (<-chan events.Event, error) {
	return p.events, nil
}
func (p *controlClaimProvider) AnswerPermission(_ context.Context, _ string, requestID string, _ runtime.PermissionResponse) error {
	p.answered <- requestID
	p.events <- events.Event{
		Event:     "session.end",
		SessionID: "ses_claim",
		Fields:    map[string]any{"stop_reason": "end_turn"},
	}
	return nil
}
func (p *controlClaimProvider) Capabilities(context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{}, nil
}

// TestRunChildAttemptWiresControlServerForClaims guards the ownership-claim
// wiring: runChildAttempt must pass s.control into WaitForSession, otherwise no
// control permission claim is ever registered and avenor_answer_permission
// fails with "no registered resolver state" for every stable run.
func TestRunChildAttemptWiresControlServerForClaims(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket:          filepath.Join(newStableSocketTestDir(t, "stable-attempt-claim"), "control.sock"),
		MaxRuntimes:            1,
		PermissionClaimTimeout: 2 * time.Second,
	})
	if err := sup.control.Start(sup.config.ControlSocket); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	defer sup.control.Stop()

	conn, err := net.Dial("unix", sup.config.ControlSocket)
	if err != nil {
		t.Fatalf("dial control server: %v", err)
	}
	defer conn.Close()
	deadline := time.Now().Add(time.Second)
	for !sup.control.HasClients() {
		if time.Now().After(deadline) {
			t.Fatal("control client was not registered")
		}
		time.Sleep(time.Millisecond)
	}

	provider := &controlClaimProvider{
		events:   make(chan events.Event, 4),
		answered: make(chan string, 1),
	}
	answerErr := make(chan error, 1)
	child := &childRuntime{
		id:               "rt_claim",
		provider:         provider,
		permClaimTimeout: 2 * time.Second,
		done:             make(chan struct{}),
		promptCh:         make(chan struct{}, 1),
		eventWriter: &synchronousStablePermissionSink{
			responses: make(chan events.Event, 1),
			answer: func() error {
				err := sup.answerPermission("rt_claim", "0", "allow")
				answerErr <- err
				return err
			},
		},
	}
	sup.runtimes["rt_claim"] = child

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sup.runChildAttempt(ctx, child, "", "do it", nil)

	select {
	case err := <-answerErr:
		if err != nil {
			t.Fatalf("answerPermission through control claim failed: %v (ControlServer not wired into runChildAttempt?)", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("permission.request never reached the answer path")
	}

	select {
	case reqID := <-provider.answered:
		if reqID != "0" {
			t.Fatalf("provider answered request %q, want 0", reqID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("control claim did not deliver the answer to the provider")
	}
}

// TestRunLoopChildWiresControlServerForClaims exercises the full loop-run path
// through runLoopChild with a control permission claim. It overrides the
// provider-construction seam so that each phase attempt emits a
// permission.request, then answers via sup.answerPermission and verifies the
// provider received the answer.
func TestRunLoopChildWiresControlServerForClaims(t *testing.T) {
	socketPath := filepath.Join(newStableSocketTestDir(t, "loop-claim"), "control.sock")

	sup := NewSupervisor(Config{
		ControlSocket:          socketPath,
		MaxRuntimes:            1,
		PermissionClaimTimeout: 2 * time.Second,
	})
	if err := sup.control.Start(socketPath); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	defer sup.control.Stop()

	// Connect a client so the control claim can deliver.
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("dial control server: %v", err)
	}
	defer conn.Close()
	deadline := time.Now().Add(time.Second)
	for !sup.control.HasClients() {
		if time.Now().After(deadline) {
			t.Fatal("control client was not registered")
		}
		time.Sleep(time.Millisecond)
	}

	provider := &controlClaimProvider{
		events:   make(chan events.Event, 4),
		answered: make(chan string, 1),
	}
	answerErr := make(chan error, 1)
	child := &childRuntime{
		id:       "rt_loop_claim",
		done:     make(chan struct{}),
		promptCh: make(chan struct{}, 1),
		eventWriter: &synchronousStablePermissionSink{
			responses: make(chan events.Event, 1),
			answer: func() error {
				err := sup.answerPermission("rt_loop_claim", "0", "allow")
				answerErr <- err
				return err
			},
		},
		permClaimTimeout: 2 * time.Second,
		cancelFn:         func() {},
	}
	sup.runtimes[child.id] = child

	// Inject the controlClaimProvider into the provider construction seam.
	sup.newProviderFunc = func(startOpts runtime.StartOptions, backend string) (runtime.Provider, error) {
		return provider, nil
	}

	cfg := &looprunner.LoopConfig{
		MaxIterations: 1,
		Pre:           []phaseconfig.Phase{{Name: "test", Prompt: "do work"}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sup.runLoopChild(ctx, child, cfg, 0, "", "", "", "")

	// Wait for the child to complete (with timeout).
	select {
	case <-child.done:
	case <-time.After(5 * time.Second):
		t.Fatal("runLoopChild did not complete within timeout")
	}

	// Verify the permission.request was answered via sup.answerPermission.
	select {
	case err := <-answerErr:
		if err != nil {
			t.Fatalf("answerPermission through control claim failed: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("permission.request never reached the answer path")
	}

	// Verify the provider received the answer (end-to-end claim delivery).
	select {
	case reqID := <-provider.answered:
		if reqID != "0" {
			t.Fatalf("provider answered request %q, want 0", reqID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("control claim did not deliver the answer to the provider")
	}
}

// TestRunTeamChildWiresControlServerForClaims exercises the full team-run path
// through runTeamChild with a control permission claim. It overrides the
// provider-construction seam so that the phase attempt emits a
// permission.request, then answers via sup.answerPermission and verifies the
// provider received the answer.
func TestRunTeamChildWiresControlServerForClaims(t *testing.T) {
	soc := filepath.Join(newStableSocketTestDir(t, "team-claim"), "control.sock")

	sup := NewSupervisor(Config{
		ControlSocket:          soc,
		MaxRuntimes:            1,
		PermissionClaimTimeout: 2 * time.Second,
	})
	if err := sup.control.Start(soc); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	defer sup.control.Stop()

	// Connect a client so the control claim can deliver.
	conn, err := net.Dial("unix", soc)
	if err != nil {
		t.Fatalf("dial control server: %v", err)
	}
	defer conn.Close()
	deadline := time.Now().Add(time.Second)
	for !sup.control.HasClients() {
		if time.Now().After(deadline) {
			t.Fatal("control client was not registered")
		}
		time.Sleep(time.Millisecond)
	}

	provider := &controlClaimProvider{
		events:   make(chan events.Event, 4),
		answered: make(chan string, 1),
	}
	answerErr := make(chan error, 1)
	child := &childRuntime{
		id:       "rt_team_claim",
		done:     make(chan struct{}),
		promptCh: make(chan struct{}, 1),
		eventWriter: &synchronousStablePermissionSink{
			responses: make(chan events.Event, 1),
			answer: func() error {
				err := sup.answerPermission("rt_team_claim", "0", "allow")
				answerErr <- err
				return err
			},
		},
		permClaimTimeout: 2 * time.Second,
		cancelFn:         func() {},
	}
	sup.runtimes[child.id] = child

	// Inject the controlClaimProvider into the provider construction seam.
	sup.newProviderFunc = func(startOpts runtime.StartOptions, backend string) (runtime.Provider, error) {
		return provider, nil
	}

	cfg := &teamrunner.TeamConfig{
		Team: []phaseconfig.Phase{{Name: "review", Prompt: "do work"}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sup.runTeamChild(ctx, child, cfg, 0, "", "", "", "unknown-backend")

	// Wait for the child to complete (with timeout).
	select {
	case <-child.done:
	case <-time.After(5 * time.Second):
		t.Fatal("runTeamChild did not complete within timeout")
	}

	// Verify the permission.request was answered via sup.answerPermission.
	select {
	case err := <-answerErr:
		if err != nil {
			t.Fatalf("answerPermission through control claim failed: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("permission.request never reached the answer path")
	}

	// Verify the provider received the answer (end-to-end claim delivery).
	select {
	case reqID := <-provider.answered:
		if reqID != "0" {
			t.Fatalf("provider answered request %q, want 0", reqID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("control claim did not deliver the answer to the provider")
	}
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

func TestAnswerPermissionUsesActiveControlClaim(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket:          "/tmp/test-answer-claim.sock",
		MaxRuntimes:            1,
		PermissionClaimTimeout: 5 * time.Second,
	})
	provider := &permRecordingProvider{}
	sup.runtimes["rt_claim"] = &childRuntime{
		id:       "rt_claim",
		provider: provider,
		session:  runtime.Session{SessionID: "ses_claim"},
		done:     make(chan struct{}),
		promptCh: make(chan struct{}, 1),
	}
	cachePermissionOptionsThroughFanout(t, sup, "rt_claim", events.Event{
		Event: "permission.request",
		Fields: map[string]any{
			"request_id": "req_claim",
			"options": []any{
				map[string]any{"optionId": "allow_it", "kind": "allow"},
			},
		},
	})
	answerCh, ok := sup.control.BeginPermissionClaim("rt_claim", "req_claim")
	if !ok {
		t.Fatal("BeginPermissionClaim returned false")
	}
	defer sup.control.EndPermissionClaim("rt_claim", "req_claim")

	if err := sup.answerPermission("rt_claim", "req_claim", "allow_it"); err != nil {
		t.Fatalf("answerPermission: %v", err)
	}
	if provider.called {
		t.Fatal("provider was called directly instead of through the active resolver claim")
	}
	if _, ok := sup.permOptions["rt_claim:req_claim"]; ok {
		t.Fatal("cache entry was not consumed after delivering the claim answer")
	}
	select {
	case answer := <-answerCh:
		if answer.RequestID != "req_claim" || answer.OptionID != "allow_it" {
			t.Fatalf("claim answer = %+v", answer)
		}
	default:
		t.Fatal("active resolver claim did not receive the answer")
	}
}

func TestAnswerPermissionScopesSameRequestIDByRuntime(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket:          "/tmp/test-answer-scoped-claims.sock",
		MaxRuntimes:            2,
		PermissionClaimTimeout: 5 * time.Second,
	})
	for _, runtimeID := range []string{"rt_1", "rt_2"} {
		sup.runtimes[runtimeID] = &childRuntime{
			id:       runtimeID,
			provider: &permRecordingProvider{},
			session:  runtime.Session{SessionID: "ses_" + runtimeID},
			done:     make(chan struct{}),
			promptCh: make(chan struct{}, 1),
		}
		cachePermissionOptionsThroughFanout(t, sup, runtimeID, events.Event{
			Event: "permission.request",
			Fields: map[string]any{
				"request_id": "0",
				"options": []any{
					map[string]any{"optionId": "always_" + runtimeID, "kind": "allow_always"},
				},
			},
		})
	}

	first, ok := sup.control.BeginPermissionClaim("rt_1", "0")
	if !ok {
		t.Fatal("claim for rt_1 was rejected")
	}
	second, ok := sup.control.BeginPermissionClaim("rt_2", "0")
	if !ok {
		t.Fatal("claim for rt_2 was rejected")
	}
	defer sup.control.EndPermissionClaim("rt_1", "0")
	defer sup.control.EndPermissionClaim("rt_2", "0")

	if err := sup.answerPermission("rt_2", "0", "always_rt_2"); err != nil {
		t.Fatalf("answer rt_2: %v", err)
	}
	select {
	case answer := <-first:
		t.Fatalf("rt_2 answer was delivered to rt_1: %+v", answer)
	default:
	}
	select {
	case answer := <-second:
		if answer.OptionID != "always_rt_2" {
			t.Fatalf("rt_2 answer = %+v", answer)
		}
	default:
		t.Fatal("rt_2 claim did not receive its answer")
	}
	if _, ok := sup.permOptions["rt_1:0"]; !ok {
		t.Fatal("answering rt_2 consumed rt_1 options")
	}
}

func TestStableWaitersRouteSameRequestIDThroughScopedResolvers(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket:          filepath.Join(newStableSocketTestDir(t, "stable-claims"), "control.sock"),
		MaxRuntimes:            2,
		PermissionClaimTimeout: time.Second,
	})
	if err := sup.control.Start(sup.config.ControlSocket); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	defer sup.control.Stop()
	conn, err := net.Dial("unix", sup.config.ControlSocket)
	if err != nil {
		t.Fatalf("dial control server: %v", err)
	}
	defer conn.Close()
	deadline := time.Now().Add(time.Second)
	for !sup.control.HasClients() {
		if time.Now().After(deadline) {
			t.Fatal("control client was not registered")
		}
		time.Sleep(time.Millisecond)
	}

	type waiter struct {
		runtimeID string
		provider  *stablePermissionProvider
		child     *childRuntime
		responses chan events.Event
		result    chan int
	}
	waiters := []*waiter{
		{runtimeID: "rt_1", provider: &stablePermissionProvider{answers: make(chan string, 1)}, responses: make(chan events.Event, 1), result: make(chan int, 1)},
		{runtimeID: "rt_2", provider: &stablePermissionProvider{answers: make(chan string, 1)}, responses: make(chan events.Event, 1), result: make(chan int, 1)},
	}
	for _, w := range waiters {
		w.child = &childRuntime{
			id:       w.runtimeID,
			provider: w.provider,
			session:  runtime.Session{SessionID: "ses_" + w.runtimeID},
			done:     make(chan struct{}),
			promptCh: make(chan struct{}, 1),
		}
		sup.runtimes[w.runtimeID] = w.child
	}
	for _, w := range waiters {
		sink := &synchronousStablePermissionSink{
			responses: w.responses,
			answer: func() error {
				return sup.answerPermission(w.runtimeID, "0", "always_"+w.runtimeID)
			},
		}
		writer := &runtimeFanoutWriter{
			base:            sink,
			runtimeID:       w.runtimeID,
			child:           w.child,
			control:         sup.control,
			onPermissionReq: sup.cachePermissionOptions,
		}
		eventCh := make(chan events.Event, 2)
		eventCh <- events.Event{
			Event:     "permission.request",
			SessionID: "ses_" + w.runtimeID,
			Fields: map[string]any{
				"request_id": "0",
				"options": []any{
					map[string]any{"optionId": "always_" + w.runtimeID, "kind": "allow_always"},
				},
			},
		}
		eventCh <- events.Event{
			Event:     "session.end",
			SessionID: "ses_" + w.runtimeID,
			Fields:    map[string]any{"stop_reason": "end_turn"},
		}
		close(eventCh)
		promptDone := make(chan error, 1)
		promptDone <- nil
		go func() {
			result := cli.WaitForSession(context.Background(), w.provider, cli.SessionWaitConfig{
				EventCh:                eventCh,
				PromptDone:             promptDone,
				SessionID:              "ses_" + w.runtimeID,
				RunID:                  "run_1",
				PermissionClaimScope:   w.runtimeID,
				PermissionClaimTimeout: time.Second,
			}, cli.SessionWaitDeps{
				Writer:        writer,
				ControlServer: sup.control,
				Stderr:        io.Discard,
			})
			w.result <- result.ExitCode
		}()
	}

	for _, w := range waiters {
		select {
		case requestID := <-w.provider.answers:
			if requestID != "0" {
				t.Fatalf("%s provider request = %q", w.runtimeID, requestID)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s provider was not answered", w.runtimeID)
		}
		select {
		case response := <-w.responses:
			if got := response.Fields["runtime_id"]; got != w.runtimeID {
				t.Fatalf("%s permission.response runtime_id = %v", w.runtimeID, got)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s permission.response was not emitted", w.runtimeID)
		}
		select {
		case exitCode := <-w.result:
			if exitCode != 0 {
				t.Fatalf("%s WaitForSession exit code = %d", w.runtimeID, exitCode)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s WaitForSession did not complete", w.runtimeID)
		}
		w.child.mu.Lock()
		pending := w.child.pendingPermission
		w.child.mu.Unlock()
		if pending {
			t.Fatalf("%s pending permission was not cleared", w.runtimeID)
		}
	}
}

func TestAnswerPermissionRejectsLateAnswerOwnedByFileResolver(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/test-file-owned.sock", MaxRuntimes: 1})
	provider := &permRecordingProvider{}
	sup.runtimes["rt_file"] = &childRuntime{
		id:       "rt_file",
		provider: provider,
		session:  runtime.Session{SessionID: "ses_file"},
		done:     make(chan struct{}),
		promptCh: make(chan struct{}, 1),
	}
	cachePermissionOptionsThroughFanout(t, sup, "rt_file", events.Event{
		Event: "permission.request",
		Fields: map[string]any{
			"request_id": "0",
			"options": []any{
				map[string]any{"optionId": "always", "kind": "allow_always"},
			},
		},
	})
	sup.control.PreparePermissionClaim("rt_file", "0", control.PermissionResolverFile)

	if err := sup.answerPermission("rt_file", "0", "always"); err == nil {
		t.Fatal("late stable answer succeeded while file resolver owned the request")
	}
	if provider.called {
		t.Fatal("stable answer bypassed file resolver and called provider directly")
	}
	if _, ok := sup.permOptions["rt_file:0"]; !ok {
		t.Fatal("blocked late answer consumed cached permission options")
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
	sup.control.PreparePermissionClaim("rt_kind", "req_kind", control.PermissionResolverNoResolver)

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
	sup.control.PreparePermissionClaim("rt_kind", "req_kind", control.PermissionResolverNoResolver)

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
	sup.control.PreparePermissionClaim("rt_clear", "req_clear", control.PermissionResolverNoResolver)

	if err := sup.answerPermission("rt_clear", "req_clear", "ok"); err != nil {
		t.Fatalf("answerPermission: %v", err)
	}
	if _, ok := sup.permOptions["rt_clear:req_clear"]; ok {
		t.Fatal("cache entry was not cleared after use")
	}
}

func TestAnswerPermissionNoResolverAllowsOnlyOneConcurrentProviderCall(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket:          "/tmp/test-answer-direct-concurrent.sock",
		MaxRuntimes:            1,
		PermissionClaimTimeout: 5 * time.Second,
	})
	provider := &blockingPermissionProvider{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	var releaseOnce sync.Once
	releaseProvider := func() { releaseOnce.Do(func() { close(provider.release) }) }
	t.Cleanup(releaseProvider)
	sup.runtimes["rt_direct"] = &childRuntime{
		id:       "rt_direct",
		provider: provider,
		session:  runtime.Session{SessionID: "ses_direct"},
		done:     make(chan struct{}),
		promptCh: make(chan struct{}, 1),
	}
	cachePermissionOptionsThroughFanout(t, sup, "rt_direct", events.Event{
		Event: "permission.request",
		Fields: map[string]any{
			"request_id": "req_direct",
			"options": []any{
				map[string]any{"optionId": "allow_it", "kind": "allow"},
			},
		},
	})
	if !sup.control.PreparePermissionClaim("rt_direct", "req_direct", control.PermissionResolverNoResolver) {
		t.Fatal("PreparePermissionClaim returned false")
	}

	firstErr := make(chan error, 1)
	go func() {
		firstErr <- sup.answerPermission("rt_direct", "req_direct", "allow_it")
	}()
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("provider call did not start")
	}

	secondErrCh := make(chan error, 1)
	go func() {
		secondErrCh <- sup.answerPermission("rt_direct", "req_direct", "allow_it")
	}()
	var secondErr error
	select {
	case secondErr = <-secondErrCh:
	case <-time.After(time.Second):
		t.Fatal("duplicate answerPermission did not complete")
	}
	if secondErr == nil {
		t.Fatal("concurrent duplicate answer unexpectedly succeeded")
	}
	if got := provider.callCount(); got != 1 {
		t.Fatalf("provider calls before release = %d, want 1", got)
	}

	releaseProvider()
	select {
	case err := <-firstErr:
		if err != nil {
			t.Fatalf("first answerPermission: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first answerPermission did not complete")
	}
	if got := provider.callCount(); got != 1 {
		t.Fatalf("provider calls = %d, want 1", got)
	}
	if state := sup.control.PermissionResolverState("rt_direct", "req_direct"); state != control.PermissionResolverUnknown {
		t.Fatalf("resolver state after success = %v, want unknown", state)
	}
	if _, ok := sup.permOptions["rt_direct:req_direct"]; ok {
		t.Fatal("cache entry was not cleared after direct delivery")
	}
}

func TestAnswerPermissionNoResolverRetriesAfterProviderError(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket:          "/tmp/test-answer-direct-retry.sock",
		MaxRuntimes:            1,
		PermissionClaimTimeout: 5 * time.Second,
	})
	provider := &retryPermissionProvider{}
	sup.runtimes["rt_retry"] = &childRuntime{
		id:       "rt_retry",
		provider: provider,
		session:  runtime.Session{SessionID: "ses_retry"},
		done:     make(chan struct{}),
		promptCh: make(chan struct{}, 1),
	}
	cachePermissionOptionsThroughFanout(t, sup, "rt_retry", events.Event{
		Event: "permission.request",
		Fields: map[string]any{
			"request_id": "req_retry",
			"options": []any{
				map[string]any{"optionId": "allow_it", "kind": "allow"},
			},
		},
	})
	if !sup.control.PreparePermissionClaim("rt_retry", "req_retry", control.PermissionResolverNoResolver) {
		t.Fatal("PreparePermissionClaim returned false")
	}

	if err := sup.answerPermission("rt_retry", "req_retry", "allow_it"); err == nil {
		t.Fatal("first answerPermission unexpectedly succeeded")
	}
	if state := sup.control.PermissionResolverState("rt_retry", "req_retry"); state != control.PermissionResolverNoResolver {
		t.Fatalf("resolver state after failure = %v, want no-resolver", state)
	}
	if _, ok := sup.permOptions["rt_retry:req_retry"]; !ok {
		t.Fatal("cache entry was removed after failed provider delivery")
	}

	if err := sup.answerPermission("rt_retry", "req_retry", "allow_it"); err != nil {
		t.Fatalf("retry answerPermission: %v", err)
	}
	if state := sup.control.PermissionResolverState("rt_retry", "req_retry"); state != control.PermissionResolverUnknown {
		t.Fatalf("resolver state after retry = %v, want unknown", state)
	}
	if _, ok := sup.permOptions["rt_retry:req_retry"]; ok {
		t.Fatal("cache entry was not removed after successful retry")
	}
}

func TestDirectPermissionCleanupBlocksReuseUntilOldOptionsAreDeleted(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket: "/tmp/test-answer-cleanup-order.sock",
		MaxRuntimes:   1,
	})
	const key = "rt_reuse:req_reuse"
	sup.permOptions[key] = []any{
		map[string]any{"optionId": "old", "kind": "allow"},
	}
	if !sup.control.PreparePermissionClaim("rt_reuse", "req_reuse", control.PermissionResolverNoResolver) {
		t.Fatal("PreparePermissionClaim returned false")
	}
	if got := sup.control.DeliverPendingPermission("rt_reuse", "req_reuse", "old"); got != control.PermissionAnswerNoResolver {
		t.Fatalf("direct delivery = %v, want no-resolver", got)
	}

	cleanupReached := make(chan struct{})
	releaseCleanup := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseCleanup) }) }
	t.Cleanup(release)
	cleanupDone := make(chan struct{})
	go func() {
		sup.cleanupDirectPermission("rt_reuse", "req_reuse", func() {
			close(cleanupReached)
			<-releaseCleanup
		})
		close(cleanupDone)
	}()
	select {
	case <-cleanupReached:
	case <-time.After(time.Second):
		t.Fatal("production cleanup did not reach claim release")
	}

	replacementAttempt := make(chan bool, 1)
	go func() {
		prepared := sup.control.PreparePermissionClaim("rt_reuse", "req_reuse", control.PermissionResolverNoResolver)
		if prepared {
			sup.cachePermissionOptions("rt_reuse", "req_reuse", []any{
				map[string]any{"optionId": "new", "kind": "reject"},
			})
		}
		replacementAttempt <- prepared
	}()
	select {
	case prepared := <-replacementAttempt:
		if prepared {
			t.Fatal("replacement prepared while production cleanup retained the old claim")
		}
	case <-time.After(time.Second):
		t.Fatal("replacement attempt blocked during cleanup")
	}
	release()
	select {
	case <-cleanupDone:
	case <-time.After(time.Second):
		t.Fatal("production cleanup did not complete")
	}

	if !sup.control.PreparePermissionClaim("rt_reuse", "req_reuse", control.PermissionResolverNoResolver) {
		t.Fatal("replacement could not prepare after old cleanup")
	}
	replacement := []any{
		map[string]any{"optionId": "new", "kind": "reject"},
	}
	sup.cachePermissionOptions("rt_reuse", "req_reuse", replacement)
	if got := sup.permOptions[key]; len(got) != 1 {
		t.Fatalf("replacement options = %+v, want one intact entry", got)
	} else if option, _ := got[0].(map[string]any); option["optionId"] != "new" {
		t.Fatalf("replacement options were overwritten: %+v", got)
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
