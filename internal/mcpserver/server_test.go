package mcpserver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sdougbrown/avenor/client"
)

type fakeClient struct {
	listResult               []map[string]any
	statusResult             map[string]any
	statusFunc               func(runtimeID string) (map[string]any, error)
	spawnResult              map[string]any
	listErr                  error
	statusErr                error
	resultResult             map[string]any
	resultErr                error
	spawnErr                 error
	shutdownErr              error
	answerPermissionErr      error
	shutdownFunc             func(mode string) error
	spawnFunc                func(params map[string]any) (map[string]any, error)
	spawnCapturedParams      map[string]any
	answerPermissionCalls    []permissionCall
	closeCalls               int
	statusCapturedRuntimeIDs []string
	resultCapturedRuntimeIDs []string
}

type permissionCall struct {
	runtimeID string
	requestID string
	optionID  string
	message   string
}

func (f *fakeClient) Status(runtimeID string) (map[string]any, error) {
	f.statusCapturedRuntimeIDs = append(f.statusCapturedRuntimeIDs, runtimeID)
	if f.statusFunc != nil {
		return f.statusFunc(runtimeID)
	}
	return f.statusResult, f.statusErr
}

func (f *fakeClient) Result(runtimeID string) (map[string]any, error) {
	f.resultCapturedRuntimeIDs = append(f.resultCapturedRuntimeIDs, runtimeID)
	return f.resultResult, f.resultErr
}

func (f *fakeClient) List() ([]map[string]any, error) {
	return f.listResult, f.listErr
}

func (f *fakeClient) Spawn(params map[string]any) (map[string]any, error) {
	f.spawnCapturedParams = params
	if f.spawnFunc != nil {
		return f.spawnFunc(params)
	}
	return f.spawnResult, f.spawnErr
}

func (f *fakeClient) Shutdown(mode string) error {
	if f.shutdownFunc != nil {
		return f.shutdownFunc(mode)
	}
	return f.shutdownErr
}

func (f *fakeClient) AnswerPermission(runtimeID, requestID, optionID string) error {
	return f.AnswerPermissionWithMessage(runtimeID, requestID, optionID, "")
}

func (f *fakeClient) AnswerPermissionWithMessage(runtimeID, requestID, optionID, message string) error {
	f.answerPermissionCalls = append(f.answerPermissionCalls, permissionCall{runtimeID, requestID, optionID, message})
	return f.answerPermissionErr
}

func (f *fakeClient) Close() error {
	f.closeCalls++
	return nil
}

type spawnCountingClient struct {
	spawns atomic.Int32
}

func (c *spawnCountingClient) Status(string) (map[string]any, error) { return nil, nil }
func (c *spawnCountingClient) List() ([]map[string]any, error)       { return nil, nil }
func (c *spawnCountingClient) Spawn(map[string]any) (map[string]any, error) {
	c.spawns.Add(1)
	return map[string]any{"runtime_id": "rt-shared", "session_id": "ses-shared"}, nil
}
func (c *spawnCountingClient) Shutdown(string) error                         { return nil }
func (c *spawnCountingClient) Close() error                                  { return nil }
func (c *spawnCountingClient) AnswerPermission(string, string, string) error { return nil }

func TestParallelSpawnLazilyStartsOneSharedSupervisor(t *testing.T) {
	origStart := startSupervisorFunc
	origBeforeLock := beforeSupervisorLock
	defer func() {
		startSupervisorFunc = origStart
		beforeSupervisorLock = origBeforeLock
	}()

	const socketPath = "/tmp/avenor-parallel-start.sock"
	control := &spawnCountingClient{}
	var starts atomic.Int32
	startupEntered := make(chan struct{})
	releaseStartup := make(chan struct{})
	startSupervisorFunc = func(string, time.Duration) (*supervisorLifecycle, error) {
		starts.Add(1)
		startupEntered <- struct{}{}
		<-releaseStartup
		return &supervisorLifecycle{socketPath: socketPath, client: control}, nil
	}

	s, err := NewServer(Options{Transport: "stdio"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const calls = 16
	atLockBoundary := make(chan struct{}, calls)
	releaseLockBoundary := make(chan struct{})
	beforeSupervisorLock = func() {
		atLockBoundary <- struct{}{}
		<-releaseLockBoundary
	}
	spawn := func(wg *sync.WaitGroup) {
		defer wg.Done()
		_, _, err := s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
			Agent:   "test",
			RepoDir: ".",
			Prompt:  "hello",
		})
		if err != nil {
			t.Errorf("spawn: %v", err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(calls)
	for i := 0; i < calls; i++ {
		go spawn(&wg)
	}
	for i := 0; i < calls; i++ {
		<-atLockBoundary
	}

	// All calls are queued immediately before the same lock. Release them,
	// then keep the winning initialization in flight to prove no second call
	// can begin a startup.
	close(releaseLockBoundary)
	<-startupEntered
	if got := starts.Load(); got != 1 {
		t.Fatalf("supervisor starts while first startup is blocked = %d, want 1", got)
	}
	close(releaseStartup)
	wg.Wait()

	if got := starts.Load(); got != 1 {
		t.Fatalf("supervisor starts = %d, want 1", got)
	}
	if got := control.spawns.Load(); got != calls {
		t.Fatalf("spawn calls = %d, want %d", got, calls)
	}
	if s.controlClient != control || s.defaultSupervisorPath != socketPath {
		t.Fatal("parallel calls did not share the lazy supervisor client")
	}
}

func TestNewServerInvalidOptions(t *testing.T) {
	t.Run("empty transport", func(t *testing.T) {
		_, err := NewServer(Options{})
		if err == nil {
			t.Fatal("expected error for empty transport")
		}
		if !strings.Contains(err.Error(), "transport is required") {
			t.Fatalf("expected error to mention transport is required, got: %v", err)
		}
	})

	t.Run("no-autostart without supervisor socket", func(t *testing.T) {
		_, err := NewServer(Options{
			Transport:   "stdio",
			NoAutostart: true,
		})
		if err == nil {
			t.Fatal("expected error for no-autostart without supervisor socket")
		}
		if !strings.Contains(err.Error(), "no-autostart requires") {
			t.Fatalf("expected error to mention no-autostart requires, got: %v", err)
		}
	})

	t.Run("invalid transport", func(t *testing.T) {
		_, err := NewServer(Options{
			Transport: "invalid",
		})
		if err == nil {
			t.Fatal("expected error for invalid transport")
		}
		if !strings.Contains(err.Error(), "unsupported transport") {
			t.Fatalf("expected error to mention unsupported transport, got: %v", err)
		}
	})
}

// TestGetClientForSupervisorReusesOwnerConn guards the ownership bug: mutating
// calls (answer_permission, prompt, cancel, follow_up) resolve supervisor_id
// from the registry, so an explicit id equal to the default supervisor must
// reuse the persistent owner connection rather than dialing a fresh (non-owner)
// one — otherwise ensureOwner rejects the call with permission_denied.
func TestResultSupervisorIDUsesRegisteredRun(t *testing.T) {
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: &fakeClient{},
	})
	if err != nil {
		t.Fatal(err)
	}
	s.registry.Store(&RunInfo{
		RunID:        "run-on-secondary",
		SupervisorID: "/tmp/avenor-secondary.sock",
	})

	if got := s.resultSupervisorID("run-on-secondary", ""); got != "/tmp/avenor-secondary.sock" {
		t.Fatalf("result supervisor = %q, want registered secondary supervisor", got)
	}
	if got := s.resultSupervisorID("run-on-secondary", "/tmp/explicit.sock"); got != "/tmp/explicit.sock" {
		t.Fatalf("result supervisor = %q, want explicit supervisor", got)
	}
	if got := s.resultSupervisorID("unregistered-run", ""); got != "" {
		t.Fatalf("result supervisor = %q, want default supervisor fallback", got)
	}
}

func TestGetClientForSupervisorReusesOwnerConn(t *testing.T) {
	fake := &fakeClient{}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}
	s.defaultSupervisorPath = "/tmp/avenor-default-test.sock"

	// Omitted id → persistent client.
	cl, cleanup, err := s.getClientForSupervisor("")
	if err != nil {
		t.Fatal(err)
	}
	if cl != s.controlClient {
		t.Fatal("empty supervisor_id should reuse the persistent client")
	}
	cleanup()

	// Explicit id equal to the default supervisor → same persistent connection,
	// not a fresh dial.
	cl, cleanup, err = s.getClientForSupervisor(s.defaultSupervisorPath)
	if err != nil {
		t.Fatal(err)
	}
	if cl != s.controlClient {
		t.Fatal("default-path supervisor_id should reuse the persistent owner connection, got a fresh dial")
	}
	cleanup()

	// A different id must still dial (and here fail on the missing socket),
	// proving the two paths actually diverge.
	if _, _, err := s.getClientForSupervisor("/tmp/avenor-other-nonexistent.sock"); err == nil {
		t.Fatal("non-default supervisor_id should dial a fresh connection and fail on the missing socket")
	}
}

func TestAvenorStatusList(t *testing.T) {
	fake := &fakeClient{
		listResult: []map[string]any{
			{"runtime_id": "rt1", "status": "running", "session_id": "ses1"},
			{"runtime_id": "rt2", "status": "ended", "session_id": "ses2"},
		},
	}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{})
	if err != nil {
		t.Fatal(err)
	}
	list, ok := result.([]map[string]any)
	if !ok {
		t.Fatalf("expected []map[string]any, got %T", result)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 results, got %d", len(list))
	}
	if list[0]["status"] != "running" {
		t.Fatalf("expected status=running, got %v", list[0])
	}
	if list[1]["status"] != "done" {
		t.Fatalf("expected status=done, got %v", list[1])
	}
}

func TestAvenorStatusSingle(t *testing.T) {
	fake := &fakeClient{
		statusResult: map[string]any{"runtime_id": "rt1", "status": "running", "session_id": "ses1"},
	}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	s.registry.Store(&RunInfo{
		RunID:     "run-1",
		RuntimeID: "rt1",
	})

	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "run-1"})
	if err != nil {
		t.Fatal(err)
	}
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", result)
	}
	if m["status"] != "running" {
		t.Fatalf("expected status=running, got %v", m["status"])
	}
}

func TestAvenorStatusLifecycleViewOmitsFinalOutput(t *testing.T) {
	fake := &fakeClient{
		statusResult: map[string]any{
			"runtime_id":   "rt1",
			"status":       "done",
			"final_output": "final answer",
			"usage":        map[string]any{"total_tokens": 10},
		},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "rt1", View: "lifecycle"})
	if err != nil {
		t.Fatal(err)
	}
	status := result.(map[string]any)
	if status["status"] != "done" {
		t.Fatalf("status = %v, want done", status["status"])
	}
	if _, ok := status["final_output"]; ok {
		t.Fatal("lifecycle status unexpectedly included final_output")
	}
	if _, ok := status["usage"]; ok {
		t.Fatal("lifecycle status unexpectedly included usage")
	}
}

func TestAvenorResultReturnsTerminalOutput(t *testing.T) {
	fake := &fakeClient{
		statusResult: map[string]any{
			"runtime_id":   "rt1",
			"session_id":   "ses1",
			"status":       "done",
			"stop_reason":  "end_turn",
			"final_output": "final answer",
		},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	_, value, err := s.handleAvenorResult(context.Background(), nil, resultArgs{RunID: "rt1"})
	if err != nil {
		t.Fatal(err)
	}
	result := value.(map[string]any)
	if result["ready"] != true || result["output"] != "final answer" {
		t.Fatalf("unexpected result: %#v", result)
	}
	if _, ok := result["usage"]; ok {
		t.Fatal("result unexpectedly included diagnostic fields")
	}
}

func TestAvenorResultReturnsRunningWithoutWaiting(t *testing.T) {
	fake := &fakeClient{statusResult: map[string]any{"runtime_id": "rt1", "status": "running"}}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}
	wait := false

	_, value, err := s.handleAvenorResult(context.Background(), nil, resultArgs{RunID: "rt1", Wait: &wait})
	if err != nil {
		t.Fatal(err)
	}
	result := value.(map[string]any)
	if result["ready"] != false || result["status"] != "running" {
		t.Fatalf("unexpected result: %#v", result)
	}
	if len(fake.statusCapturedRuntimeIDs) != 1 {
		t.Fatalf("status calls = %d, want 1", len(fake.statusCapturedRuntimeIDs))
	}
}

func TestAvenorResultHonorsCancellation(t *testing.T) {
	fake := &fakeClient{statusResult: map[string]any{"runtime_id": "rt1", "status": "running"}}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err = s.handleAvenorResult(ctx, nil, resultArgs{RunID: "rt1"})
	if err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}

func TestAvenorResultDeadlineTimeout(t *testing.T) {
	// A running run with a short timeout should return ready:false and timed_out:true.
	fake := &fakeClient{}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	currentTime := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	fake.statusFunc = func(runtimeID string) (map[string]any, error) {
		currentTime = currentTime.Add(2 * time.Second)
		return map[string]any{"runtime_id": runtimeID, "status": "running"}, nil
	}
	s.clock = func() time.Time { return currentTime }

	_, value, err := s.handleAvenorResult(context.Background(), nil, resultArgs{
		RunID:   "run-1",
		Timeout: "1s",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result := value.(map[string]any)
	if result["ready"] != false {
		t.Errorf("expected ready=false, got %v", result["ready"])
	}
	if result["timed_out"] != true {
		t.Errorf("expected timed_out=true, got %v", result["timed_out"])
	}
	if result["status"] != "running" {
		t.Errorf("expected status=running, got %v", result["status"])
	}
}

func TestAvenorResultWaitingBranch(t *testing.T) {
	// A waiting run with wait=true should return immediately with ready:false,
	// preserve pending permission data, and perform exactly one status call.
	fake := &fakeClient{
		statusResult: map[string]any{
			"runtime_id":         "rt1",
			"status":             "waiting",
			"pending_permission": map[string]any{"request_id": "req-42", "description": "Allow?"},
		},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	_, value, err := s.handleAvenorResult(context.Background(), nil, resultArgs{RunID: "run-1"})
	if err != nil {
		t.Fatal(err)
	}
	result := value.(map[string]any)
	if result["ready"] != false {
		t.Errorf("expected ready=false, got %v", result["ready"])
	}
	if result["status"] != "waiting" {
		t.Errorf("expected status=waiting, got %v", result["status"])
	}
	pending, ok := result["pending_permission"].(map[string]any)
	if !ok || pending["request_id"] != "req-42" {
		t.Errorf("expected pending_permission request req-42, got %v", result["pending_permission"])
	}
	if len(fake.statusCapturedRuntimeIDs) != 1 {
		t.Fatalf("expected 1 status call, got %d", len(fake.statusCapturedRuntimeIDs))
	}
}

func TestAvenorResultStatusError(t *testing.T) {
	// A ControlClient.Status error should propagate with the existing "status:" context.
	fake := &fakeClient{
		statusErr: fmt.Errorf("runtime not found"),
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = s.handleAvenorResult(context.Background(), nil, resultArgs{RunID: "rt-broken"})
	if err == nil {
		t.Fatal("expected error from status")
	}
	if !strings.Contains(err.Error(), "status:") || !strings.Contains(err.Error(), "runtime not found") {
		t.Fatalf("expected error to contain 'status: runtime not found', got: %v", err)
	}
}

func TestAvenorResultInvalidTimeout(t *testing.T) {
	fake := &fakeClient{}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = s.handleAvenorResult(context.Background(), nil, resultArgs{
		RunID:   "rt1",
		Timeout: "not-a-number",
	})
	if err == nil || !strings.Contains(err.Error(), "invalid timeout") {
		t.Fatalf("expected invalid timeout error, got %v", err)
	}
}

func TestAvenorStatusInvalidView(t *testing.T) {
	fake := &fakeClient{}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "rt1", View: "invalid"})
	if err == nil || !strings.Contains(err.Error(), "view must be lifecycle or full") {
		t.Fatalf("expected view validation error, got %v", err)
	}
}

func TestAvenorResultReturnsCompleteControlOutput(t *testing.T) {
	preview := strings.Repeat("é", 4096)
	complete := preview + " complete explore report"
	fake := &fakeClient{
		statusResult: map[string]any{
			"runtime_id":   "rt_complete",
			"status":       "done",
			"final_output": preview,
		},
		resultResult: map[string]any{"final_output": complete},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	_, value, err := s.handleAvenorResult(context.Background(), nil, resultArgs{RunID: "rt_complete"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := value.(map[string]any)["output"]; got != complete {
		t.Fatalf("output = %q, want complete result", got)
	}
	if got := fake.resultCapturedRuntimeIDs; len(got) != 1 || got[0] != "rt_complete" {
		t.Fatalf("Result runtime IDs = %v, want [rt_complete]", got)
	}
}

func TestAvenorResultMarksOlderStatusPreviewAsPossiblyTruncated(t *testing.T) {
	fake := &fakeClient{
		statusResult: map[string]any{
			"runtime_id":   "rt_legacy",
			"status":       "done",
			"final_output": "legacy preview",
			"event_path":   "/tmp/events.ndjson",
		},
		resultErr: fmt.Errorf("rpc error [-32601]: method not found"),
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	_, value, err := s.handleAvenorResult(context.Background(), nil, resultArgs{RunID: "rt_legacy"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result := value.(map[string]any)
	if result["output"] != "legacy preview" || result["output_truncated"] != true {
		t.Fatalf("result = %#v, want explicitly uncertain preview", result)
	}
	if result["output_event_path"] != "/tmp/events.ndjson" {
		t.Fatalf("output_event_path = %v", result["output_event_path"])
	}
}

func TestAvenorResultReturnsLargeCompleteControlOutput(t *testing.T) {
	complete := strings.Repeat("é", 40_000)
	fake := &fakeClient{
		statusResult: map[string]any{"runtime_id": "rt_large", "status": "done", "final_output": "preview"},
		resultResult: map[string]any{"final_output": complete},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	_, value, err := s.handleAvenorResult(context.Background(), nil, resultArgs{RunID: "rt_large"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := value.(map[string]any)["output"]; got != complete {
		t.Fatalf("output byte length = %d, want %d", len(got.(string)), len(complete))
	}
}

func TestAvenorResultTerminalOutputFallbackFromEvents(t *testing.T) {
	// When a terminal run lacks final_output in live status, recover from
	// event history (registered run's EventLogPath). Only return the output,
	// not event details.
	dir := t.TempDir()
	eventLogPath := filepath.Join(dir, "fallback-test.log")
	eventsContent := `{"event":"start","type":"lifecycle"}
{"event":"session.end","type":"lifecycle","final_output":"recovered answer"}
`
	if err := os.WriteFile(eventLogPath, []byte(eventsContent), 0644); err != nil {
		t.Fatal(err)
	}

	fake := &fakeClient{
		statusResult: map[string]any{
			"runtime_id": "rt_fallback",
			"status":     "done",
		},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	s.registry.Store(&RunInfo{
		RunID:        "run-fallback-1",
		RuntimeID:    "rt_fallback",
		EventLogPath: eventLogPath,
	})

	_, value, err := s.handleAvenorResult(context.Background(), nil, resultArgs{RunID: "run-fallback-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result := value.(map[string]any)
	if result["ready"] != true {
		t.Errorf("expected ready=true, got %v", result["ready"])
	}
	if result["output"] != "recovered answer" {
		t.Errorf("expected output 'recovered answer', got %v", result["output"])
	}
	// Should not include event details
	if _, ok := result["events"]; ok {
		t.Error("result should not contain events")
	}
}

func TestAvenorResultTerminalOutputFallbackMissingEvents(t *testing.T) {
	// When event log doesn't exist or has no session.end, missing final_output
	// is non-fatal — return the terminal status without output.
	dir := t.TempDir()
	eventLogPath := filepath.Join(dir, "no-session-end.log")
	eventsContent := `{"event":"start","type":"lifecycle"}
`
	if err := os.WriteFile(eventLogPath, []byte(eventsContent), 0644); err != nil {
		t.Fatal(err)
	}

	fake := &fakeClient{
		statusResult: map[string]any{
			"runtime_id": "rt_no_end",
			"status":     "done",
		},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	s.registry.Store(&RunInfo{
		RunID:        "run-no-end",
		RuntimeID:    "rt_no_end",
		EventLogPath: eventLogPath,
	})

	_, value, err := s.handleAvenorResult(context.Background(), nil, resultArgs{RunID: "run-no-end"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result := value.(map[string]any)
	if result["ready"] != true {
		t.Errorf("expected ready=true, got %v", result["ready"])
	}
	if _, ok := result["output"]; ok {
		t.Error("expected no output key when no session.end found")
	}
}

func TestAvenorStatusForwardsSpecialCharsToControlClient(t *testing.T) {
	// Direct run IDs with special characters are forwarded to the control client
	// rather than rejected by regex validation.
	fake := &fakeClient{
		statusResult: map[string]any{"status": "running"},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "../../socket"})
	if err != nil {
		t.Fatalf("expected success forwarding special chars to control client, got: %v", err)
	}
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", result)
	}
	if m["run_id"] != "../../socket" {
		t.Errorf("expected run_id ../../socket, got %v", m["run_id"])
	}
	// Verify the exact run reference was forwarded to the control client.
	if len(fake.statusCapturedRuntimeIDs) != 1 || fake.statusCapturedRuntimeIDs[0] != "../../socket" {
		t.Errorf("expected statusCapturedRuntimeIDs [../../socket], got %v", fake.statusCapturedRuntimeIDs)
	}
}

func TestAvenorStatusError(t *testing.T) {
	t.Run("list error", func(t *testing.T) {
		fake := &fakeClient{
			listErr: fmt.Errorf("connection refused"),
		}
		s, err := NewServer(Options{
			Transport:     "stdio",
			NoAutostart:   true,
			ControlClient: fake,
		})
		if err != nil {
			t.Fatal(err)
		}

		_, _, err = s.handleAvenorStatus(context.Background(), nil, statusArgs{})
		if err == nil {
			t.Fatal("expected error from list")
		}
		if !strings.Contains(err.Error(), "list runs") || !strings.Contains(err.Error(), "connection refused") {
			t.Fatalf("expected error to contain 'list runs: connection refused', got: %v", err)
		}
	})

	t.Run("status error", func(t *testing.T) {
		fake := &fakeClient{
			statusErr: fmt.Errorf("runtime not found"),
		}
		s, err := NewServer(Options{
			Transport:     "stdio",
			NoAutostart:   true,
			ControlClient: fake,
		})
		if err != nil {
			t.Fatal(err)
		}

		s.registry.Store(&RunInfo{
			RunID:     "run-missing",
			RuntimeID: "rt_missing",
		})

		_, _, err = s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "run-missing"})
		if err == nil {
			t.Fatal("expected error from status")
		}
		if !strings.Contains(err.Error(), "status:") || !strings.Contains(err.Error(), "runtime not found") {
			t.Fatalf("expected error to contain 'status: runtime not found', got: %v", err)
		}
	})
}

func TestAvenorStatusNilClient(t *testing.T) {
	// With autostart, creating a server with no ControlClient and no
	// SupervisorSocket triggers startSupervisor, which fails in tests.
	// Verify the expected error path.
	_, err := NewServer(Options{
		Transport:   "stdio",
		NoAutostart: true,
	})
	if err == nil {
		t.Fatal("expected error for no-autostart without supervisor socket")
	}
	if !strings.Contains(err.Error(), "no-autostart requires") {
		t.Fatalf("expected error to contain 'no-autostart requires', got: %v", err)
	}
}

func startFakeSupervisor(t *testing.T) (string, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "fs")
	if err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	path := filepath.Join(dir, "s")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				scanner := bufio.NewScanner(c)
				for scanner.Scan() {
					var req client.Request
					if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
						continue
					}
					var resp client.Response
					switch req.Method {
					case "status":
						resp = client.Response{JSONRPC: "2.0", ID: req.ID}
						snap := map[string]any{"session_id": "ses_test", "phase": "working"}
						resp.Result, _ = json.Marshal(snap)
					case "list":
						resp = client.Response{JSONRPC: "2.0", ID: req.ID}
						list := []map[string]any{{"runtime_id": "rt_1", "status": "running"}}
						resp.Result, _ = json.Marshal(list)
					default:
						resp = client.Response{JSONRPC: "2.0", ID: req.ID, Error: &client.RespError{Code: -32601, Message: "method not found"}}
					}
					data, _ := json.Marshal(resp)
					data = append(data, '\n')
					c.Write(data)
				}
			}(conn)
		}
	}()

	return path, func() { ln.Close() }
}

func TestServerWithRealSocketStatus(t *testing.T) {
	path, cleanup := startFakeSupervisor(t)
	defer cleanup()

	s, err := NewServer(Options{
		Transport:        "stdio",
		SupervisorSocket: path,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer s.Close()

	s.registry.Store(&RunInfo{
		RunID:     "run1",
		RuntimeID: "rt_run1",
	})

	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "run1"})
	if err != nil {
		t.Fatalf("handleAvenorStatus: %v", err)
	}
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", result)
	}
	if m["session_id"] != "ses_test" {
		t.Errorf("session_id = %v, want ses_test", m["session_id"])
	}
	if m["phase"] != "working" {
		t.Errorf("phase = %v, want working", m["phase"])
	}
}

func TestServerWithRealSocketList(t *testing.T) {
	path, cleanup := startFakeSupervisor(t)
	defer cleanup()

	s, err := NewServer(Options{
		Transport:        "stdio",
		SupervisorSocket: path,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer s.Close()

	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{})
	if err != nil {
		t.Fatalf("handleAvenorStatus: %v", err)
	}
	list, ok := result.([]map[string]any)
	if !ok {
		t.Fatalf("expected []map[string]any, got %T", result)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 result, got %d", len(list))
	}
	if list[0]["status"] != "running" {
		t.Errorf("result = %v, want [{status: running}]", list)
	}
}

func TestServerClose(t *testing.T) {
	path, cleanup := startFakeSupervisor(t)
	defer cleanup()

	s, err := NewServer(Options{
		Transport:        "stdio",
		SupervisorSocket: path,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	// Use list (no run_id) to avoid registry-miss error
	_, _, err = s.handleAvenorStatus(context.Background(), nil, statusArgs{})
	if err != nil {
		t.Fatalf("handleAvenorStatus before close: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("second Close should be idempotent: %v", err)
	}

	_, _, err = s.handleAvenorStatus(context.Background(), nil, statusArgs{})
	if err == nil {
		t.Error("expected error after close")
	}
	if !strings.Contains(err.Error(), "control client not available") {
		t.Errorf("expected 'control client not available' error, got: %v", err)
	}
}

func TestServerCloseWithLifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "close-lifecycle-test.sock")

	if err := os.WriteFile(socketPath, []byte("fake"), 0600); err != nil {
		t.Fatal(err)
	}

	fc := &fakeClient{}

	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fc,
	})
	if err != nil {
		t.Fatal(err)
	}

	lc := &supervisorLifecycle{
		socketPath: socketPath,
		client:     fc,
	}
	s.lifecycle = lc

	// Verify pre-close state
	if s.controlClient == nil {
		t.Fatal("controlClient should be non-nil before close")
	}
	if s.lifecycle == nil {
		t.Fatal("lifecycle should be non-nil before close")
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Verify post-close state
	if s.controlClient != nil {
		t.Error("controlClient should be nil after close")
	}
	if s.lifecycle != nil {
		t.Error("lifecycle should be nil after close")
	}

	if _, err := os.Stat(socketPath); err != nil {
		t.Errorf("lifecycle parent unexpectedly removed socket: %v", err)
	}
	if fc.closeCalls != 1 {
		t.Fatalf("fake client Close calls = %d, want 1", fc.closeCalls)
	}
}

func TestServerWithNoAutostartAndSocket(t *testing.T) {
	// no-autostart with a non-existent socket should fail on dial
	_, err := NewServer(Options{
		Transport:        "stdio",
		SupervisorSocket: "/nonexistent/socket/path",
		NoAutostart:      true,
	})
	if err == nil {
		t.Fatal("expected error dialing non-existent socket")
	}
	if !strings.Contains(err.Error(), "dial supervisor socket") {
		t.Fatalf("expected error to mention dial supervisor socket, got: %v", err)
	}
}

func TestAvenorSpawn(t *testing.T) {
	fake := &fakeClient{
		spawnResult: map[string]any{
			"runtime_id": "rt_spawn_1",
			"session_id": "ses_spawn_1",
		},
	}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, result, err := s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		Agent:   "claude",
		RepoDir: "/tmp/test-repo",
	})
	if err != nil {
		t.Fatal(err)
	}
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", result)
	}
	runID, _ := m["run_id"].(string)
	if runID == "" {
		t.Fatal("expected non-empty run_id")
	}
	if m["label"] != runID {
		t.Errorf("expected label to default to run_id, got %v", m["label"])
	}

	ri := s.registry.Lookup(runID)
	if ri == nil {
		t.Fatal("expected registry entry for spawn")
	}
	if ri.RuntimeID != "rt_spawn_1" {
		t.Errorf("expected runtime_id rt_spawn_1, got %s", ri.RuntimeID)
	}
	if ri.Agent != "claude" {
		t.Errorf("expected agent claude, got %s", ri.Agent)
	}
	if ri.Dir != "/tmp/test-repo" {
		t.Errorf("expected dir /tmp/test-repo, got %s", ri.Dir)
	}

	p := fake.spawnCapturedParams
	if p == nil {
		t.Fatal("expected spawn params to be captured")
	}
	if _, ok := p["auto_approve"]; ok {
		t.Error("expected auto_approve key to be absent when not set")
	}
}

func TestAvenorSpawnError(t *testing.T) {
	cause := errors.New("control plane unavailable")
	fake := &fakeClient{spawnErr: cause}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = s.handleAvenorSpawn(context.Background(), nil, spawnArgs{Agent: "claude", RepoDir: "/tmp/test-repo"})
	if !errors.Is(err, cause) {
		t.Fatalf("spawn error does not wrap cause: %v", err)
	}
	if !strings.Contains(err.Error(), "spawn:") {
		t.Fatalf("spawn error lacks operation context: %v", err)
	}
}

func TestAvenorSpawnWithLabel(t *testing.T) {
	fake := &fakeClient{
		spawnResult: map[string]any{
			"runtime_id": "rt_label_1",
			"session_id": "ses_label_1",
		},
	}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, result, err := s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		Agent:   "codex",
		RepoDir: "/tmp/test-repo",
		Label:   "my-label",
	})
	if err != nil {
		t.Fatal(err)
	}
	m, _ := result.(map[string]any)
	label, _ := m["label"].(string)
	if label != "my-label" {
		t.Errorf("expected label my-label, got %s", label)
	}

	ri := s.registry.Lookup("my-label")
	if ri == nil {
		t.Fatal("expected registry entry lookup by label")
	}
}

func TestAvenorSpawnWithOptionalParams(t *testing.T) {
	fake := &fakeClient{
		spawnFunc: func(params map[string]any) (map[string]any, error) {
			return map[string]any{
				"runtime_id":    "rt_opt_1",
				"session_id":    "ses_opt_1",
				"supervisor_id": "/tmp/supervisor.sock",
			}, nil
		},
	}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, result, err := s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		Agent:      "codex",
		RepoDir:    "/tmp/test-repo",
		Prompt:     "initial prompt text",
		PromptFile: "/tmp/prompt.md",
		Model:      "gpt-5",
		Backend:    "pi",
		Timeout:    "300",
	})
	if err != nil {
		t.Fatal(err)
	}

	p := fake.spawnCapturedParams
	if p == nil {
		t.Fatal("expected spawn params to be captured")
	}
	if p["prompt"] != "initial prompt text" {
		t.Errorf("expected prompt 'initial prompt text', got %v", p["prompt"])
	}
	if p["prompt_file"] != "/tmp/prompt.md" {
		t.Errorf("expected prompt_file '/tmp/prompt.md', got %v", p["prompt_file"])
	}
	if p["model"] != "gpt-5" {
		t.Errorf("expected model 'gpt-5', got %v", p["model"])
	}
	if p["backend"] != "pi" {
		t.Errorf("expected backend 'pi', got %v", p["backend"])
	}
	if p["timeout"] != 300 {
		t.Errorf("expected timeout 300 (int), got %v (%T)", p["timeout"], p["timeout"])
	}
	if p["agent"] != "codex" {
		t.Errorf("expected agent 'codex', got %v", p["agent"])
	}
	if p["dir"] != "/tmp/test-repo" {
		t.Errorf("expected dir '/tmp/test-repo', got %v", p["dir"])
	}

	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", result)
	}
	runID, _ := m["run_id"].(string)
	if runID == "" {
		t.Fatal("expected non-empty run_id")
	}
	if m["label"] == "" {
		t.Fatal("expected non-empty label")
	}
	if _, ok := m["supervisor_id"]; !ok {
		t.Fatal("expected supervisor_id in result")
	}

	ri := s.registry.Lookup(runID)
	if ri == nil {
		t.Fatal("expected registry entry for spawn")
	}
	if ri.Agent != "codex" {
		t.Errorf("expected agent codex, got %s", ri.Agent)
	}
	if ri.Backend != "pi" {
		t.Errorf("expected backend pi, got %s", ri.Backend)
	}
	if ri.Dir != "/tmp/test-repo" {
		t.Errorf("expected dir /tmp/test-repo, got %s", ri.Dir)
	}
	if ri.SentinelPath == "" {
		t.Error("expected non-empty sentinel path")
	}
	if ri.EventLogPath == "" {
		t.Error("expected non-empty event log path")
	}
	if ri.RuntimeID != "rt_opt_1" {
		t.Errorf("expected runtime_id rt_opt_1, got %s", ri.RuntimeID)
	}
	if ri.SessionID != "ses_opt_1" {
		t.Errorf("expected session_id ses_opt_1, got %s", ri.SessionID)
	}
}

func TestAvenorSpawnAutoApproveTrue(t *testing.T) {
	fake := &fakeClient{
		spawnFunc: func(params map[string]any) (map[string]any, error) {
			return map[string]any{
				"runtime_id": "rt_auto_1",
				"session_id": "ses_auto_1",
			}, nil
		},
	}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, result, err := s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		Agent:       "codex",
		RepoDir:     "/tmp/test-repo",
		AutoApprove: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	runID, _ := result.(map[string]any)["run_id"].(string)
	if runID == "" {
		t.Fatal("expected spawned run ID")
	}
	if ri := s.registry.Lookup(runID); ri == nil || !ri.AutoApprove {
		t.Fatalf("initial registry auto-approve = %#v, want true", ri)
	}

	p := fake.spawnCapturedParams
	if p == nil {
		t.Fatal("expected spawn params to be captured")
	}
	if got, ok := p["auto_approve"].(bool); !ok || !got {
		t.Errorf("expected auto_approve bool true, got %T %v", p["auto_approve"], p["auto_approve"])
	}
}

func TestAvenorSpawnTimeoutDurationString(t *testing.T) {
	fake := &fakeClient{}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		Agent:   "claude",
		RepoDir: "/tmp/test-repo",
		Timeout: "5m",
	})
	if err != nil {
		t.Fatalf("expected duration timeout to be accepted, got: %v", err)
	}
	if got := fake.spawnCapturedParams["timeout"]; got != 300 {
		t.Errorf("expected timeout 300, got %v (%T)", got, got)
	}
}

func TestAvenorSpawnTimeoutInvalid(t *testing.T) {
	fake := &fakeClient{}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		Agent:   "claude",
		RepoDir: "/tmp/test-repo",
		Timeout: "forever",
	})
	if err == nil {
		t.Fatal("expected error for invalid timeout")
	}
	if !strings.Contains(err.Error(), "invalid timeout") {
		t.Errorf("expected invalid timeout error, got: %v", err)
	}
}

func TestAvenorSpawnMissingAgent(t *testing.T) {
	fake := &fakeClient{}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		RepoDir: "/tmp/test-repo",
	})
	if err == nil {
		t.Fatal("expected error for missing agent")
	}
	if !strings.Contains(err.Error(), "agent is required") {
		t.Errorf("expected 'agent is required', got: %v", err)
	}
}

func TestAvenorSpawnMissingRepoDir(t *testing.T) {
	fake := &fakeClient{}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		Agent: "claude",
	})
	if err == nil {
		t.Fatal("expected error for missing repo_dir")
	}
	if !strings.Contains(err.Error(), "repo_dir is required") {
		t.Errorf("expected 'repo_dir is required', got: %v", err)
	}
}

func TestAvenorShutdown(t *testing.T) {
	var capturedMode string
	fake := &fakeClient{
		shutdownFunc: func(mode string) error {
			capturedMode = mode
			return nil
		},
	}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, result, err := s.handleAvenorShutdown(context.Background(), nil, shutdownArgs{})
	if err != nil {
		t.Fatal(err)
	}
	m, _ := result.(map[string]any)
	if m["ok"] != true {
		t.Errorf("expected ok=true, got %v", m["ok"])
	}
	if capturedMode != "graceful" {
		t.Errorf("expected graceful mode, got %s", capturedMode)
	}
}

func TestAvenorShutdownError(t *testing.T) {
	cause := errors.New("control plane unavailable")
	fake := &fakeClient{shutdownErr: cause}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = s.handleAvenorShutdown(context.Background(), nil, shutdownArgs{})
	if !errors.Is(err, cause) {
		t.Fatalf("shutdown error does not wrap cause: %v", err)
	}
	if !strings.Contains(err.Error(), "shutdown:") {
		t.Fatalf("shutdown error lacks operation context: %v", err)
	}
}

func TestOwnedLifecycleShutdownErrorReachesMCPCaller(t *testing.T) {
	cause := errors.New("shutdown RPC failed")
	fake := &fakeClient{shutdownErr: cause}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}
	// An owned lifecycle takes the lifecycle shutdown path rather than the
	// direct client path used by TestAvenorShutdownError above.
	s.lifecycle = &supervisorLifecycle{client: fake}

	_, _, err = s.handleAvenorShutdown(context.Background(), nil, shutdownArgs{})
	if !errors.Is(err, cause) {
		t.Fatalf("owned lifecycle shutdown error does not wrap cause: %v", err)
	}
	if !strings.Contains(err.Error(), "shutdown:") {
		t.Fatalf("shutdown error lacks MCP operation context: %v", err)
	}
	if s.lifecycle != nil || s.controlClient != nil || !s.closed {
		t.Fatalf("shutdown error retained closed lifecycle/client: lifecycle=%v client=%v closed=%v", s.lifecycle, s.controlClient, s.closed)
	}
	if fake.closeCalls != 1 {
		t.Fatalf("fake client Close calls = %d, want 1", fake.closeCalls)
	}
}

func TestAvenorShutdownForce(t *testing.T) {
	var capturedMode string
	fake := &fakeClient{
		shutdownFunc: func(mode string) error {
			capturedMode = mode
			return nil
		},
	}

	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = s.handleAvenorShutdown(context.Background(), nil, shutdownArgs{Force: true})
	if err != nil {
		t.Fatal(err)
	}
	if capturedMode != "kill" {
		t.Errorf("expected kill mode, got %s", capturedMode)
	}
}

func TestAvenorShutdownCleanup(t *testing.T) {
	dir := t.TempDir()
	sentinelPath := filepath.Join(dir, "cleanup-test.done")
	eventLogPath := filepath.Join(dir, "cleanup-test.log")

	if err := os.WriteFile(sentinelPath, []byte("DONE\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(eventLogPath, []byte("event data\n"), 0644); err != nil {
		t.Fatal(err)
	}

	fake := &fakeClient{}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	s.registry.Store(&RunInfo{
		RunID:        "run-clean-1",
		Label:        "clean-test",
		RuntimeID:    "rt_clean_1",
		SentinelPath: sentinelPath,
		EventLogPath: eventLogPath,
	})

	_, result, err := s.handleAvenorShutdown(context.Background(), nil, shutdownArgs{})
	if err != nil {
		t.Fatal(err)
	}
	m, _ := result.(map[string]any)

	if m["ok"] != true {
		t.Errorf("expected ok=true, got %v", m["ok"])
	}

	cleanedUp, ok := m["cleaned_up"].([]string)
	if !ok {
		t.Fatalf("expected cleaned_up []string, got %T", m["cleaned_up"])
	}

	foundSentinel := false
	foundEventLog := false
	for _, path := range cleanedUp {
		if path == sentinelPath {
			foundSentinel = true
		}
		if path == eventLogPath {
			foundEventLog = true
		}
	}
	if !foundSentinel {
		t.Errorf("expected sentinel path %s in cleaned_up, got %v", sentinelPath, cleanedUp)
	}
	if !foundEventLog {
		t.Errorf("expected event log path %s in cleaned_up, got %v", eventLogPath, cleanedUp)
	}

	if _, err := os.Stat(sentinelPath); !os.IsNotExist(err) {
		t.Error("sentinel file was not removed from disk")
	}
	if _, err := os.Stat(eventLogPath); !os.IsNotExist(err) {
		t.Error("event log file was not removed from disk")
	}

	if ri := s.registry.Lookup("run-clean-1"); ri != nil {
		t.Error("registry entry was not removed")
	}
}

func TestAvenorStatusWithRegistry(t *testing.T) {
	dir := t.TempDir()
	sentinelPath := filepath.Join(dir, "test-run.done")
	if err := os.WriteFile(sentinelPath, []byte("DONE\nSESSION=ses_from_sentinel\nSTOP_REASON=end_turn\n"), 0644); err != nil {
		t.Fatal(err)
	}

	fake := &fakeClient{
		statusResult: map[string]any{
			"status":     "ended",
			"session_id": "ses_live",
			"phase":      "done",
		},
	}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	s.registry.Store(&RunInfo{
		RunID:        "run-test-1",
		Label:        "test-run",
		RuntimeID:    "rt_test_1",
		SentinelPath: sentinelPath,
	})

	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "run-test-1"})
	if err != nil {
		t.Fatal(err)
	}
	m, _ := result.(map[string]any)
	if m["status"] != "done" {
		t.Errorf("expected done, got %v", m["status"])
	}
	if m["run_id"] != "run-test-1" {
		t.Errorf("expected run_id run-test-1, got %v", m["run_id"])
	}
	if m["label"] != "test-run" {
		t.Errorf("expected label test-run, got %v", m["label"])
	}
	if m["stop_reason"] != "end_turn" {
		t.Errorf("expected stop_reason end_turn, got %v", m["stop_reason"])
	}
	if m["session_id"] != "ses_from_sentinel" {
		t.Errorf("expected session_id ses_from_sentinel, got %v", m["session_id"])
	}
	if len(fake.statusCapturedRuntimeIDs) != 1 || fake.statusCapturedRuntimeIDs[0] != "rt_test_1" {
		t.Errorf("expected Status call with rt_test_1, got %v", fake.statusCapturedRuntimeIDs)
	}
}

func TestAvenorStatusListWithRegistry(t *testing.T) {
	dir := t.TempDir()
	sentinelPath := filepath.Join(dir, "list-run.done")
	if err := os.WriteFile(sentinelPath, []byte("DONE\nSESSION=ses_sentinel\nSTOP_REASON=end_turn\n"), 0644); err != nil {
		t.Fatal(err)
	}

	fake := &fakeClient{
		listResult: []map[string]any{
			{"runtime_id": "rt_1", "status": "running", "session_id": "ses_1"},
			{"runtime_id": "rt_2", "status": "ended", "session_id": "ses_2"},
		},
	}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	s.registry.Store(&RunInfo{
		RunID:        "run-list-1",
		Label:        "list-run-1",
		RuntimeID:    "rt_1",
		SentinelPath: sentinelPath,
	})
	s.registry.Store(&RunInfo{
		RunID:     "run-list-2",
		Label:     "list-run-2",
		RuntimeID: "rt_2",
	})

	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{})
	if err != nil {
		t.Fatal(err)
	}
	list, ok := result.([]map[string]any)
	if !ok {
		t.Fatalf("expected []map[string]any, got %T", result)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 results, got %d", len(list))
	}

	if list[0]["run_id"] != "run-list-1" {
		t.Errorf("expected run_id run-list-1, got %v", list[0]["run_id"])
	}
	if list[0]["status"] != "running" {
		t.Errorf("expected running status for rt_1, got %v", list[0]["status"])
	}

	if list[1]["run_id"] != "run-list-2" {
		t.Errorf("expected run_id run-list-2, got %v", list[1]["run_id"])
	}
	if list[1]["status"] != "done" {
		t.Errorf("expected done status for rt_2, got %v", list[1]["status"])
	}
}

func TestAvenorStatusNoRegistryHitWithoutSupervisorID(t *testing.T) {
	fake := &fakeClient{
		statusResult: map[string]any{
			"status":     "running",
			"session_id": "ses_direct",
		},
	}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "rt_direct"})
	if err != nil {
		t.Fatalf("expected success querying runtime ID directly, got: %v", err)
	}
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", result)
	}
	if m["run_id"] != "rt_direct" {
		t.Errorf("expected run_id rt_direct, got %v", m["run_id"])
	}
	if m["status"] != "running" {
		t.Errorf("expected status running, got %v", m["status"])
	}
}

func TestAvenorStatusWithExplicitSupervisorID(t *testing.T) {
	path, cleanup := startFakeSupervisor(t)
	defer cleanup()

	s, err := NewServer(Options{
		Transport:        "stdio",
		SupervisorSocket: path,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer s.Close()

	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{
		RunID:        "rt_direct",
		SupervisorID: path,
	})
	if err != nil {
		t.Fatalf("expected success with explicit supervisor_id, got: %v", err)
	}
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", result)
	}
	if m["session_id"] != "ses_test" {
		t.Errorf("expected ses_test, got %v", m["session_id"])
	}
}

func TestAvenorStatusControlClientNotAvailable(t *testing.T) {
	_, err := NewServer(Options{
		Transport:   "stdio",
		NoAutostart: true,
	})
	if err == nil {
		t.Fatal("expected error for no-autostart without supervisor socket")
	}

	s := &Server{
		registry: NewRunRegistry(),
		opts:     Options{NoAutostart: true},
	}
	_, _, err = s.handleAvenorStatus(context.Background(), nil, statusArgs{})
	if err == nil {
		t.Fatal("expected error for unavailable control client")
	}
	if !strings.Contains(err.Error(), "control client not available") {
		t.Errorf("expected 'control client not available', got: %v", err)
	}
}

func TestAvenorAnswerPermission(t *testing.T) {
	fake := &fakeClient{}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	s.registry.Store(&RunInfo{
		RunID:     "run-perm-1",
		Label:     "perm-test",
		RuntimeID: "rt_perm_1",
	})

	_, result, err := s.handleAvenorAnswerPermission(context.Background(), nil, permissionArgs{
		RunID:     "run-perm-1",
		OptionID:  "opt_allow",
		RequestID: "req_123",
	})
	if err != nil {
		t.Fatal(err)
	}
	m, _ := result.(map[string]any)
	if m["ok"] != true {
		t.Errorf("expected ok=true, got %v", m["ok"])
	}
	if len(fake.answerPermissionCalls) != 1 {
		t.Fatalf("expected 1 call to AnswerPermission, got %d", len(fake.answerPermissionCalls))
	}
	call := fake.answerPermissionCalls[0]
	if call.runtimeID != "rt_perm_1" {
		t.Errorf("expected runtimeID rt_perm_1, got %s", call.runtimeID)
	}
	if call.requestID != "req_123" {
		t.Errorf("expected requestID req_123, got %s", call.requestID)
	}
	if call.optionID != "opt_allow" {
		t.Errorf("expected optionID opt_allow, got %s", call.optionID)
	}
}

func TestAvenorAnswerPermissionAutoDetectRequestID(t *testing.T) {
	fake := &fakeClient{
		// Mirror the real supervisor RuntimeStatus shape: pending_permission is a
		// bool, and the request details live in a separate "permission" map.
		statusResult: map[string]any{
			"pending_permission": true,
			"permission": map[string]any{
				"request_id": "req_auto_456",
			},
		},
	}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	s.registry.Store(&RunInfo{
		RunID:     "run-perm-2",
		Label:     "perm-auto-test",
		RuntimeID: "rt_perm_2",
	})

	_, result, err := s.handleAvenorAnswerPermission(context.Background(), nil, permissionArgs{
		RunID:    "run-perm-2",
		OptionID: "opt_deny",
	})
	if err != nil {
		t.Fatal(err)
	}
	m, _ := result.(map[string]any)
	if m["ok"] != true {
		t.Errorf("expected ok=true, got %v", m["ok"])
	}
	if len(fake.answerPermissionCalls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(fake.answerPermissionCalls))
	}
	call := fake.answerPermissionCalls[0]
	if call.requestID != "req_auto_456" {
		t.Errorf("expected auto-detected requestID req_auto_456, got %s", call.requestID)
	}
	if call.optionID != "opt_deny" {
		t.Errorf("expected optionID opt_deny, got %s", call.optionID)
	}
	if len(fake.statusCapturedRuntimeIDs) != 1 || fake.statusCapturedRuntimeIDs[0] != "rt_perm_2" {
		t.Errorf("expected Status call with rt_perm_2, got %v", fake.statusCapturedRuntimeIDs)
	}
}

func TestAvenorAnswerPermissionNoPending(t *testing.T) {
	fake := &fakeClient{
		statusResult: map[string]any{
			"status": "running",
		},
	}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	s.registry.Store(&RunInfo{
		RunID:     "run-perm-3",
		Label:     "perm-no-pending",
		RuntimeID: "rt_perm_3",
	})

	_, _, err = s.handleAvenorAnswerPermission(context.Background(), nil, permissionArgs{
		RunID:    "run-perm-3",
		OptionID: "opt_allow",
	})
	if err == nil {
		t.Fatal("expected error for no pending permission")
	}
	if !strings.Contains(err.Error(), "no pending permission request") {
		t.Errorf("expected 'no pending permission request', got: %v", err)
	}
	if len(fake.answerPermissionCalls) != 0 {
		t.Errorf("expected 0 AnswerPermission calls, got %d", len(fake.answerPermissionCalls))
	}
}

func TestAvenorAnswerPermissionNotFound(t *testing.T) {
	fake := &fakeClient{}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = s.handleAvenorAnswerPermission(context.Background(), nil, permissionArgs{
		RunID:    "nonexistent",
		OptionID: "opt_allow",
	})
	if err == nil {
		t.Fatal("expected error for run not found")
	}
	if !strings.Contains(err.Error(), "run \"nonexistent\" not found") {
		t.Errorf("expected run not found, got: %v", err)
	}
}

func TestAvenorAnswerPermissionError(t *testing.T) {
	fake := &fakeClient{
		answerPermissionErr: fmt.Errorf("permission denied"),
	}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	s.registry.Store(&RunInfo{
		RunID:     "run-perm-6",
		Label:     "perm-error-test",
		RuntimeID: "rt_perm_6",
	})

	_, _, err = s.handleAvenorAnswerPermission(context.Background(), nil, permissionArgs{
		RunID:     "run-perm-6",
		OptionID:  "opt_allow",
		RequestID: "req_789",
	})
	if err == nil {
		t.Fatal("expected error from AnswerPermission")
	}
	if !strings.Contains(err.Error(), "answer_permission:") || !strings.Contains(err.Error(), "permission denied") {
		t.Errorf("expected 'answer_permission: permission denied', got: %v", err)
	}
}

func TestAvenorEvents(t *testing.T) {
	dir := t.TempDir()
	eventLogPath := filepath.Join(dir, "events.log")
	content := `{"event":"start","type":"lifecycle"}
{"event":"prompt","type":"turn","text":"hello"}
{"event":"done","type":"lifecycle"}
`
	if err := os.WriteFile(eventLogPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	fake := &fakeClient{}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	s.registry.Store(&RunInfo{
		RunID:        "run-events-1",
		Label:        "events-test",
		RuntimeID:    "rt_events_1",
		EventLogPath: eventLogPath,
	})

	_, result, err := s.handleAvenorEvents(context.Background(), nil, eventsArgs{
		RunID: "run-events-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	rm, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", result)
	}
	events, ok := rm["events"].([]map[string]any)
	if !ok {
		t.Fatalf("expected events []map[string]any, got %T", rm["events"])
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	if events[0]["event"] != "start" {
		t.Errorf("expected first event 'start', got %v", events[0]["event"])
	}
}

func TestAvenorEventsFilterByType(t *testing.T) {
	dir := t.TempDir()
	eventLogPath := filepath.Join(dir, "events-filter.log")
	content := `{"event":"start","type":"lifecycle"}
{"event":"prompt","type":"turn"}
{"event":"done","type":"lifecycle"}
`
	if err := os.WriteFile(eventLogPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	fake := &fakeClient{}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	s.registry.Store(&RunInfo{
		RunID:        "run-events-2",
		Label:        "events-filter-test",
		RuntimeID:    "rt_events_2",
		EventLogPath: eventLogPath,
	})

	_, result, err := s.handleAvenorEvents(context.Background(), nil, eventsArgs{
		RunID: "run-events-2",
		Types: []string{"turn"},
	})
	if err != nil {
		t.Fatal(err)
	}
	rm, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", result)
	}
	events, ok := rm["events"].([]map[string]any)
	if !ok {
		t.Fatalf("expected events []map[string]any, got %T", rm["events"])
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event after filtering by turn, got %d", len(events))
	}
	if events[0]["event"] != "prompt" {
		t.Errorf("expected prompt event, got %v", events[0]["event"])
	}
}

func TestAvenorEventsWithLimit(t *testing.T) {
	dir := t.TempDir()
	eventLogPath := filepath.Join(dir, "events-limit.log")
	var lines string
	for i := 0; i < 100; i++ {
		lines += fmt.Sprintf(`{"event":"tick","n":%d}`+"\n", i)
	}
	if err := os.WriteFile(eventLogPath, []byte(lines), 0644); err != nil {
		t.Fatal(err)
	}

	fake := &fakeClient{}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	s.registry.Store(&RunInfo{
		RunID:        "run-events-3",
		Label:        "events-limit-test",
		RuntimeID:    "rt_events_3",
		EventLogPath: eventLogPath,
	})

	_, result, err := s.handleAvenorEvents(context.Background(), nil, eventsArgs{
		RunID: "run-events-3",
		Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	rm, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", result)
	}
	events, ok := rm["events"].([]map[string]any)
	if !ok {
		t.Fatalf("expected events []map[string]any, got %T", rm["events"])
	}
	if len(events) != 10 {
		t.Fatalf("expected 10 events, got %d", len(events))
	}
	last, _ := events[9]["n"].(float64)
	if last != 99 {
		t.Errorf("expected last event n=99, got %v", last)
	}
}

func TestAvenorEventsNoFile(t *testing.T) {
	fake := &fakeClient{}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	s.registry.Store(&RunInfo{
		RunID:        "run-events-4",
		Label:        "events-nofile-test",
		RuntimeID:    "rt_events_4",
		EventLogPath: "/nonexistent/events.log",
	})

	_, result, err := s.handleAvenorEvents(context.Background(), nil, eventsArgs{
		RunID: "run-events-4",
	})
	if err != nil {
		t.Fatal(err)
	}
	rm, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", result)
	}
	events, ok := rm["events"].([]map[string]any)
	if !ok {
		t.Fatalf("expected events []map[string]any, got %T", rm["events"])
	}
	if len(events) != 0 {
		t.Fatalf("expected 0 events when file doesn't exist, got %d", len(events))
	}
}

func TestAvenorFollowUp(t *testing.T) {
	dir := t.TempDir()
	sentinelPath := filepath.Join(dir, "followup-test.done")
	if err := os.WriteFile(sentinelPath, []byte("DONE\nSESSION=ses_from_prior\n"), 0644); err != nil {
		t.Fatal(err)
	}

	fake := &fakeClient{
		spawnResult: map[string]any{
			"runtime_id": "rt_followup_1",
			"session_id": "ses_followup_1",
		},
	}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	s.registry.Store(&RunInfo{
		RunID:        "run-prior-1",
		Label:        "prior-test",
		RuntimeID:    "rt_prior_1",
		SentinelPath: sentinelPath,
		Agent:        "claude",
		Backend:      "pi",
		Dir:          "/tmp/prior-repo",
	})

	_, result, err := s.handleAvenorFollowUp(context.Background(), nil, followUpArgs{
		RunID:   "run-prior-1",
		Message: "continue working",
	})
	if err != nil {
		t.Fatal(err)
	}
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", result)
	}
	if m["label"] != "prior-test-followup" {
		t.Errorf("expected label prior-test-followup, got %v", m["label"])
	}
	runID, _ := m["run_id"].(string)
	if runID == "" {
		t.Fatal("expected non-empty run_id")
	}

	p := fake.spawnCapturedParams
	if p == nil {
		t.Fatal("expected spawn params to be captured")
	}
	if p["session_id"] != "ses_from_prior" {
		t.Errorf("expected session_id ses_from_prior, got %v", p["session_id"])
	}
	if p["prompt"] != "continue working" {
		t.Errorf("expected prompt 'continue working', got %v", p["prompt"])
	}
	if p["agent"] != "claude" {
		t.Errorf("expected agent claude, got %v", p["agent"])
	}
	if p["backend"] != "pi" {
		t.Errorf("expected backend pi, got %v", p["backend"])
	}
	if p["dir"] != "/tmp/prior-repo" {
		t.Errorf("expected dir /tmp/prior-repo, got %v", p["dir"])
	}
	if p["label"] != "prior-test-followup" {
		t.Errorf("expected label prior-test-followup, got %v", p["label"])
	}

	ri := s.registry.Lookup(runID)
	if ri == nil {
		t.Fatal("expected new registry entry for follow-up run")
	}
	if ri.Agent != "claude" {
		t.Errorf("expected agent claude, got %s", ri.Agent)
	}
	if ri.Backend != "pi" {
		t.Errorf("expected backend pi, got %s", ri.Backend)
	}
	if ri.Dir != "/tmp/prior-repo" {
		t.Errorf("expected dir /tmp/prior-repo, got %s", ri.Dir)
	}
	if ri.RuntimeID != "rt_followup_1" {
		t.Errorf("expected new runtime_id rt_followup_1, got %s", ri.RuntimeID)
	}
	if ri.SessionID != "ses_followup_1" {
		t.Errorf("expected new session_id ses_followup_1, got %s", ri.SessionID)
	}
}

func TestAvenorFollowUpInheritsAutoApproveTransitively(t *testing.T) {
	var spawnParams []map[string]any
	spawnCount := 0
	fake := &fakeClient{
		spawnFunc: func(params map[string]any) (map[string]any, error) {
			captured := make(map[string]any, len(params))
			for key, value := range params {
				captured[key] = value
			}
			spawnParams = append(spawnParams, captured)
			spawnCount++
			return map[string]any{
				"runtime_id": fmt.Sprintf("rt_followup_%d", spawnCount),
				"session_id": fmt.Sprintf("ses_followup_%d", spawnCount),
			}, nil
		},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	s.registry.Store(&RunInfo{
		RunID:        "run-auto-prior",
		Label:        "auto-prior",
		RuntimeID:    "rt_auto_prior",
		SessionID:    "ses_auto_prior",
		SentinelPath: filepath.Join(t.TempDir(), "missing.done"),
		Agent:        "claude",
		Backend:      "pi",
		Dir:          "/tmp/auto-repo",
		AutoApprove:  true,
	})

	_, firstResult, err := s.handleAvenorFollowUp(context.Background(), nil, followUpArgs{
		RunID:   "run-auto-prior",
		Message: "first follow-up",
	})
	if err != nil {
		t.Fatal(err)
	}
	firstRunID, _ := firstResult.(map[string]any)["run_id"].(string)
	if firstRunID == "" {
		t.Fatal("expected first follow-up run ID")
	}
	if got, ok := spawnParams[0]["auto_approve"].(bool); !ok || !got {
		t.Fatalf("first follow-up auto_approve = %T %v, want bool true", spawnParams[0]["auto_approve"], spawnParams[0]["auto_approve"])
	}
	if ri := s.registry.Lookup(firstRunID); ri == nil || !ri.AutoApprove {
		t.Fatalf("first follow-up registry auto-approve = %#v, want true", ri)
	}

	_, secondResult, err := s.handleAvenorFollowUp(context.Background(), nil, followUpArgs{
		RunID:   firstRunID,
		Message: "second follow-up",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(spawnParams) != 2 {
		t.Fatalf("follow-up spawns = %d, want 2", len(spawnParams))
	}
	if got, ok := spawnParams[1]["auto_approve"].(bool); !ok || !got {
		t.Fatalf("second follow-up auto_approve = %T %v, want bool true", spawnParams[1]["auto_approve"], spawnParams[1]["auto_approve"])
	}
	secondRunID, _ := secondResult.(map[string]any)["run_id"].(string)
	if ri := s.registry.Lookup(secondRunID); ri == nil || !ri.AutoApprove {
		t.Fatalf("second follow-up registry auto-approve = %#v, want true", ri)
	}
}

func TestAvenorFollowUpOmitsAutoApproveWhenFalseOrUnset(t *testing.T) {
	tests := []struct {
		name string
		info RunInfo
	}{
		{
			name: "false",
			info: RunInfo{AutoApprove: false},
		},
		{
			name: "unset",
			info: RunInfo{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fake := &fakeClient{
				spawnResult: map[string]any{"runtime_id": "rt_followup", "session_id": "ses_followup"},
			}
			s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
			if err != nil {
				t.Fatal(err)
			}
			info := test.info
			info.RunID = "run-supervised"
			info.Label = "supervised"
			info.RuntimeID = "rt_supervised"
			info.SessionID = "ses_supervised"
			info.SentinelPath = filepath.Join(t.TempDir(), "missing.done")
			info.Agent = "claude"
			info.Dir = "/tmp/supervised-repo"
			if err := s.registry.Store(&info); err != nil {
				t.Fatal(err)
			}
			if _, _, err := s.handleAvenorFollowUp(context.Background(), nil, followUpArgs{
				RunID:   info.RunID,
				Message: "continue supervised",
			}); err != nil {
				t.Fatal(err)
			}
			if _, ok := fake.spawnCapturedParams["auto_approve"]; ok {
				t.Errorf("auto_approve unexpectedly forwarded: %#v", fake.spawnCapturedParams)
			}
		})
	}
}

func TestAvenorFollowUpUsesRegistrySessionWhenSentinelMissing(t *testing.T) {
	fake := &fakeClient{
		spawnResult: map[string]any{
			"runtime_id": "rt_followup_pi",
			"session_id": "ses_followup_pi",
		},
	}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	s.registry.Store(&RunInfo{
		RunID:        "run-pi-prior",
		Label:        "pi-prior",
		RuntimeID:    "rt_pi_prior",
		SessionID:    "ses_pi_prior",
		SentinelPath: filepath.Join(t.TempDir(), "missing.done"),
		Agent:        "explore",
		Backend:      "pi",
		Dir:          "/tmp/pi-repo",
	})

	_, _, err = s.handleAvenorFollowUp(context.Background(), nil, followUpArgs{
		RunID:   "run-pi-prior",
		Message: "continue",
	})
	if err != nil {
		t.Fatal(err)
	}

	p := fake.spawnCapturedParams
	if p["session_id"] != "ses_pi_prior" {
		t.Errorf("expected stored session ses_pi_prior, got %v", p["session_id"])
	}
	if p["backend"] != "pi" {
		t.Errorf("expected backend pi, got %v", p["backend"])
	}
	if p["dir"] != "/tmp/pi-repo" {
		t.Errorf("expected dir /tmp/pi-repo, got %v", p["dir"])
	}
	if v, ok := p["auto_approve"]; ok {
		t.Errorf("auto_approve unexpectedly present on supervised follow-up: %v", v)
	}
}

func TestAvenorFollowUpCustomLabel(t *testing.T) {
	dir := t.TempDir()
	sentinelPath := filepath.Join(dir, "followup-custom-label.done")
	if err := os.WriteFile(sentinelPath, []byte("DONE\nSESSION=ses_custom\n"), 0644); err != nil {
		t.Fatal(err)
	}

	fake := &fakeClient{
		spawnResult: map[string]any{
			"runtime_id": "rt_fup_custom",
			"session_id": "ses_fup_custom",
		},
	}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	s.registry.Store(&RunInfo{
		RunID:        "run-prior-2",
		Label:        "prior-test-2",
		RuntimeID:    "rt_prior_2",
		SentinelPath: sentinelPath,
		Agent:        "codex",
		Dir:          "/tmp/other-repo",
	})

	_, result, err := s.handleAvenorFollowUp(context.Background(), nil, followUpArgs{
		RunID:   "run-prior-2",
		Message: "keep going",
		Label:   "my-followup",
	})
	if err != nil {
		t.Fatal(err)
	}
	m, _ := result.(map[string]any)
	if m["label"] != "my-followup" {
		t.Errorf("expected custom label my-followup, got %v", m["label"])
	}
	if fake.spawnCapturedParams["label"] != "my-followup" {
		t.Errorf("expected spawn label my-followup, got %v", fake.spawnCapturedParams["label"])
	}
}

func TestAvenorFollowUpNoSession(t *testing.T) {
	dir := t.TempDir()
	sentinelPath := filepath.Join(dir, "followup-nosession.done")
	if err := os.WriteFile(sentinelPath, []byte("DONE\n"), 0644); err != nil {
		t.Fatal(err)
	}

	fake := &fakeClient{}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	s.registry.Store(&RunInfo{
		RunID:        "run-prior-3",
		Label:        "prior-no-session",
		RuntimeID:    "rt_prior_3",
		SentinelPath: sentinelPath,
		Agent:        "claude",
		Dir:          "/tmp/prior-repo",
	})

	_, _, err = s.handleAvenorFollowUp(context.Background(), nil, followUpArgs{
		RunID:   "run-prior-3",
		Message: "continue",
	})
	if err == nil {
		t.Fatal("expected error for no session in sentinel")
	}
	if !strings.Contains(err.Error(), "no session in sentinel") {
		t.Errorf("expected 'no session in sentinel', got: %v", err)
	}
}

func TestAvenorFollowUpNotResumable(t *testing.T) {
	dir := t.TempDir()
	sentinelPath := filepath.Join(dir, "followup-failed.done")
	if err := os.WriteFile(sentinelPath, []byte("FAILED\nSESSION=ses_failed\n"), 0644); err != nil {
		t.Fatal(err)
	}

	fake := &fakeClient{}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	s.registry.Store(&RunInfo{
		RunID:        "run-prior-5",
		Label:        "prior-failed",
		RuntimeID:    "rt_prior_5",
		SessionID:    "ses_failed",
		SentinelPath: sentinelPath,
		Agent:        "claude",
		Dir:          "/tmp/prior-repo",
	})

	_, _, err = s.handleAvenorFollowUp(context.Background(), nil, followUpArgs{
		RunID:   "run-prior-5",
		Message: "continue",
	})
	if err == nil {
		t.Fatal("expected error for non-resumable run")
	}
	if !strings.Contains(err.Error(), "not resumable") {
		t.Errorf("expected 'not resumable', got: %v", err)
	}
}

func TestAvenorFollowUpNotFound(t *testing.T) {
	fake := &fakeClient{}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = s.handleAvenorFollowUp(context.Background(), nil, followUpArgs{
		RunID:   "nonexistent",
		Message: "continue",
	})
	if err == nil {
		t.Fatal("expected error for run not found in registry")
	}
	if !strings.Contains(err.Error(), "run not found in registry") {
		t.Errorf("expected 'run not found in registry', got: %v", err)
	}
}

func TestNewServerHTTPTransport(t *testing.T) {
	s, err := NewServer(Options{
		Transport:     "http",
		Addr:          "127.0.0.1:0",
		AuthToken:     "test-token",
		NoAutostart:   true,
		ControlClient: &fakeClient{},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	} else {
		defer s.Close()
	}
}

func TestNewServerHTTPTransportRequiresAuth(t *testing.T) {
	_, err := NewServer(Options{
		Transport:     "http",
		Addr:          "127.0.0.1:0",
		NoAutostart:   true,
		ControlClient: &fakeClient{},
	})
	if err == nil {
		t.Fatal("expected auth error")
	}
	if !strings.Contains(err.Error(), "requires") {
		t.Fatalf("expected auth requirement error, got: %v", err)
	}
}

func TestHTTPHandlerRequiresBearerToken(t *testing.T) {
	s, err := NewServer(Options{
		Transport:     "http",
		Addr:          "127.0.0.1:0",
		AuthToken:     "test-token",
		NoAutostart:   true,
		ControlClient: &fakeClient{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ts := httptest.NewServer(s.HTTPHandler())
	defer ts.Close()

	req, err := http.NewRequest(http.MethodPost, ts.URL, strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}

	req, err = http.NewRequest(http.MethodPost, ts.URL, strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer test-token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		t.Fatal("authenticated request was rejected")
	}
}

func TestHTTPHandlerServesToolList(t *testing.T) {
	s, err := NewServer(Options{
		Transport:     "http",
		Addr:          "127.0.0.1:0",
		AuthToken:     "test-token",
		NoAutostart:   true,
		ControlClient: &fakeClient{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ts := httptest.NewServer(s.HTTPHandler())
	defer ts.Close()

	httpClient := &http.Client{Transport: bearerRoundTripper{
		token: "test-token",
		next:  http.DefaultTransport,
	}}
	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test-client", Version: "dev"}, nil)
	session, err := client.Connect(context.Background(), &mcpsdk.StreamableClientTransport{
		Endpoint:             ts.URL,
		HTTPClient:           httpClient,
		DisableStandaloneSSE: true,
	}, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer session.Close()

	result, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}
	got := make(map[string]bool, len(result.Tools))
	for _, tool := range result.Tools {
		got[tool.Name] = true
	}
	for _, name := range tsToolNames {
		if !got[name] {
			t.Errorf("HTTP tools/list missing %s", name)
		}
	}
}

type bearerRoundTripper struct {
	token string
	next  http.RoundTripper
}

func (b bearerRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.Header.Set("Authorization", "Bearer "+b.token)
	return b.next.RoundTrip(clone)
}

func TestAvenorAnswerPermissionPendingPermissionNull(t *testing.T) {
	fake := &fakeClient{
		statusResult: map[string]any{
			"status":             "running",
			"pending_permission": nil,
		},
	}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	s.registry.Store(&RunInfo{
		RunID:     "run-perm-4",
		Label:     "perm-null-pending",
		RuntimeID: "rt_perm_4",
	})

	_, _, err = s.handleAvenorAnswerPermission(context.Background(), nil, permissionArgs{
		RunID:    "run-perm-4",
		OptionID: "opt_allow",
	})
	if err == nil {
		t.Fatal("expected error for nil pending_permission")
	}
	if !strings.Contains(err.Error(), "no pending permission request") {
		t.Errorf("expected 'no pending permission request', got: %v", err)
	}
}

func TestAvenorAnswerPermissionRequestIDEmpty(t *testing.T) {
	fake := &fakeClient{
		statusResult: map[string]any{
			"pending_permission": true,
			"permission": map[string]any{
				"request_id": "",
			},
		},
	}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	s.registry.Store(&RunInfo{
		RunID:     "run-perm-5",
		Label:     "perm-empty-requestid",
		RuntimeID: "rt_perm_5",
	})

	_, _, err = s.handleAvenorAnswerPermission(context.Background(), nil, permissionArgs{
		RunID:    "run-perm-5",
		OptionID: "opt_allow",
	})
	if err == nil {
		t.Fatal("expected error for empty request_id")
	}
	if !strings.Contains(err.Error(), "missing request_id") {
		t.Errorf("expected 'pending_permission missing request_id', got: %v", err)
	}
}

func TestAvenorFollowUpSentinelFileNotFound(t *testing.T) {
	fake := &fakeClient{}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	s.registry.Store(&RunInfo{
		RunID:        "run-prior-4",
		Label:        "prior-sentinel-missing",
		RuntimeID:    "rt_prior_4",
		SentinelPath: "/nonexistent/sentinel.done",
		Agent:        "claude",
		Dir:          "/tmp/prior-repo",
	})

	_, _, err = s.handleAvenorFollowUp(context.Background(), nil, followUpArgs{
		RunID:   "run-prior-4",
		Message: "continue",
	})
	if err == nil {
		t.Fatal("expected error for missing sentinel file")
	}
	if !strings.Contains(err.Error(), "read sentinel session") {
		t.Errorf("expected 'read sentinel session', got: %v", err)
	}
}

func TestAvenorShutdownWithExplicitLifecyclePath(t *testing.T) {
	const sockPath = "/tmp/test-lifecycle-shutdown.sock"

	var lifecycleShutdownCalled bool
	var otherClientCalled bool

	lifecycleClient := &fakeClient{
		shutdownFunc: func(mode string) error {
			lifecycleShutdownCalled = true
			return nil
		},
	}
	otherClient := &fakeClient{
		shutdownFunc: func(mode string) error {
			otherClientCalled = true
			return nil
		},
	}

	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: otherClient,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Inject lifecycle as if the MCP server had autostarted a supervisor at sockPath.
	s.lifecycle = &supervisorLifecycle{socketPath: sockPath, client: lifecycleClient}
	s.defaultSupervisorPath = sockPath

	_, _, err = s.handleAvenorShutdown(context.Background(), nil, shutdownArgs{
		SupervisorID: sockPath,
	})
	if err != nil {
		t.Fatal(err)
	}

	if !lifecycleShutdownCalled {
		t.Error("lifecycle client Shutdown was not called — explicit supervisor_id matching defaultSupervisorPath should use lifecycle path")
	}
	if otherClientCalled {
		t.Error("non-lifecycle client Shutdown was called — should not have been reached")
	}
	if s.lifecycle != nil {
		t.Error("lifecycle was not cleared after shutdown")
	}
}
