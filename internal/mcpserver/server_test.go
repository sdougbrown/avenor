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

// statusOutputMap flattens a typed avenor_status tool output into the map
// form used by assertions.
func statusOutputMap(t *testing.T, v any) map[string]any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal status output: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal status output: %v", err)
	}
	return m
}

// statusOutputRuns returns the runs array from a list-form avenor_status
// tool output.
func statusOutputRuns(t *testing.T, v any) []map[string]any {
	t.Helper()
	m := statusOutputMap(t, v)
	raw, ok := m["runs"].([]any)
	if !ok {
		t.Fatalf("expected runs array in output, got %#v", m)
	}
	runs := make([]map[string]any, 0, len(raw))
	for _, entry := range raw {
		r, ok := entry.(map[string]any)
		if !ok {
			t.Fatalf("expected run object, got %#v", entry)
		}
		runs = append(runs, r)
	}
	return runs
}

type fakeClient struct {
	listResult               []map[string]any
	listFunc                 func() ([]map[string]any, error)
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
	closed                   bool
	statusCapturedRuntimeIDs []string
	resultCapturedRuntimeIDs []string

	workflowStatusResult   map[string]any
	workflowWaitResult     map[string]any
	workflowInspectResult  map[string]any
	workflowEventsResult   map[string]any
	workflowCompleteResult map[string]any
	workflowGateResult     map[string]any
	workflowStatusErr      error
	workflowWaitErr        error
	workflowInspectErr     error
	workflowEventsErr      error
	workflowCompleteErr    error
	workflowGateErr        error
	workflowStatusCalls    []string
	workflowWaitCalls      []workflowWaitCall
	workflowEventsCalls    []workflowEventsCall
	workflowCompleteCalls  []workflowCompleteCall
	workflowGateCalls      []workflowGateCall

	workflowControllerStatusResult map[string]any
	workflowControllerListResult   map[string]any
	workflowControllerStatusErr    error
	workflowControllerListErr      error
	workflowControllerStatusCalls  []string
	workflowControllerListCalls    int
}

type workflowWaitCall struct {
	workflowID string
	timeout    time.Duration
}

type workflowEventsCall struct {
	workflowID string
	afterSeq   int64
	limit      int
}

type workflowCompleteCall struct {
	workflowID string
	fields     map[string]any
}

type workflowGateCall struct {
	workflowID string
	fields     map[string]any
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
	if f.listFunc != nil {
		return f.listFunc()
	}
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

func (f *fakeClient) Closed() bool { return f.closed }

func (f *fakeClient) WorkflowStatus(workflowID string) (map[string]any, error) {
	f.workflowStatusCalls = append(f.workflowStatusCalls, workflowID)
	return f.workflowStatusResult, f.workflowStatusErr
}

func (f *fakeClient) WorkflowWait(workflowID string, timeout time.Duration) (map[string]any, error) {
	f.workflowWaitCalls = append(f.workflowWaitCalls, workflowWaitCall{workflowID, timeout})
	return f.workflowWaitResult, f.workflowWaitErr
}

func (f *fakeClient) WorkflowInspect(workflowID string) (map[string]any, error) {
	return f.workflowInspectResult, f.workflowInspectErr
}

func (f *fakeClient) WorkflowEvents(workflowID string, afterSeq int64, limit int) (map[string]any, error) {
	f.workflowEventsCalls = append(f.workflowEventsCalls, workflowEventsCall{workflowID, afterSeq, limit})
	return f.workflowEventsResult, f.workflowEventsErr
}

func (f *fakeClient) WorkflowComplete(workflowID string, fields map[string]any) (map[string]any, error) {
	f.workflowCompleteCalls = append(f.workflowCompleteCalls, workflowCompleteCall{workflowID, fields})
	return f.workflowCompleteResult, f.workflowCompleteErr
}

func (f *fakeClient) WorkflowGate(workflowID string, fields map[string]any) (map[string]any, error) {
	f.workflowGateCalls = append(f.workflowGateCalls, workflowGateCall{workflowID, fields})
	return f.workflowGateResult, f.workflowGateErr
}

func (f *fakeClient) WorkflowControllerStatus(controllerID string) (map[string]any, error) {
	f.workflowControllerStatusCalls = append(f.workflowControllerStatusCalls, controllerID)
	return f.workflowControllerStatusResult, f.workflowControllerStatusErr
}

func (f *fakeClient) WorkflowControllerList() (map[string]any, error) {
	f.workflowControllerListCalls++
	return f.workflowControllerListResult, f.workflowControllerListErr
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
func (c *spawnCountingClient) Closed() bool                                  { return false }
func (c *spawnCountingClient) AnswerPermission(string, string, string) error { return nil }
func (c *spawnCountingClient) WorkflowStatus(string) (map[string]any, error) { return nil, nil }
func (c *spawnCountingClient) WorkflowWait(string, time.Duration) (map[string]any, error) {
	return nil, nil
}
func (c *spawnCountingClient) WorkflowInspect(string) (map[string]any, error) { return nil, nil }
func (c *spawnCountingClient) WorkflowEvents(string, int64, int) (map[string]any, error) {
	return nil, nil
}
func (c *spawnCountingClient) WorkflowComplete(string, map[string]any) (map[string]any, error) {
	return nil, nil
}
func (c *spawnCountingClient) WorkflowGate(string, map[string]any) (map[string]any, error) {
	return nil, nil
}
func (c *spawnCountingClient) WorkflowControllerStatus(string) (map[string]any, error) {
	return nil, nil
}
func (c *spawnCountingClient) WorkflowControllerList() (map[string]any, error) {
	return nil, nil
}

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
	list := statusOutputRuns(t, result)
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
	m := statusOutputMap(t, result)
	if m["status"] != "running" {
		t.Fatalf("expected status=running, got %v", m["status"])
	}
}

// TestAvenorStatusListMCPShape drives the avenor_status list and single-run
// forms through a real MCP session: the list form must return a record as
// structuredContent (the MCP spec forbids arrays there), the tool must
// declare its output schema, and the SDK validates both forms against it.
func TestAvenorStatusListMCPShape(t *testing.T) {
	// The list form (no run_id) must return a record as structuredContent: the
	// MCP spec forbids arrays there. The tool must also declare its output
	// schema so clients can validate the result.
	fake := &fakeClient{listResult: []map[string]any{
		{"runtime_id": "rt_1", "status": "running", "label": "one"},
		{"runtime_id": "rt_2", "status": "done", "label": "two"},
	}, statusResult: map[string]any{"runtime_id": "rt_1", "status": "running", "label": "one", "permission": map[string]any{"request_id": "req-1", "description": "Read file"}}}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	serverTransport, clientTransport := mcpsdk.NewInMemoryTransports()
	serverSession, err := s.mcpServer.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatalf("connect server: %v", err)
	}
	defer serverSession.Close()
	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test-client", Version: "dev"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("connect client: %v", err)
	}
	defer clientSession.Close()

	tools, err := clientSession.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}
	var statusTool *mcpsdk.Tool
	for _, tool := range tools.Tools {
		if tool.Name == "avenor_status" {
			statusTool = tool
			break
		}
	}
	if statusTool == nil {
		t.Fatal("avenor_status not listed")
	}
	if statusTool.OutputSchema == nil {
		t.Fatal("avenor_status does not declare an output schema")
	}

	res, err := clientSession.CallTool(ctx, &mcpsdk.CallToolParams{
		Name:      "avenor_status",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("call tool: %v", err)
	}
	structured, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("expected record structuredContent, got %T", res.StructuredContent)
	}
	runs, ok := structured["runs"].([]any)
	if !ok {
		t.Fatalf("expected runs array, got %#v", structured)
	}
	if len(runs) != 2 {
		t.Fatalf("expected 2 runs, got %d", len(runs))
	}
	if structured["count"] != float64(2) {
		t.Fatalf("expected count 2, got %#v", structured["count"])
	}
	first, ok := runs[0].(map[string]any)
	if !ok || first["label"] != "one" || first["status"] != "running" {
		t.Fatalf("unexpected first run: %#v", runs[0])
	}

	// The single-run form remains a flat status record and must validate
	// against the same output schema.
	single, err := clientSession.CallTool(ctx, &mcpsdk.CallToolParams{
		Name:      "avenor_status",
		Arguments: map[string]any{"run_id": "rt_1"},
	})
	if err != nil {
		t.Fatalf("call tool with run_id: %v", err)
	}
	singleRecord, ok := single.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("expected record structuredContent, got %T", single.StructuredContent)
	}
	if singleRecord["run_id"] != "rt_1" || singleRecord["status"] != "running" {
		t.Fatalf("unexpected single-run record: %#v", singleRecord)
	}
	perm, ok := singleRecord["permission"].(map[string]any)
	if !ok || perm["request_id"] != "req-1" {
		t.Fatalf("permission = %#v, want request record", singleRecord["permission"])
	}
	if _, present := singleRecord["runs"]; present {
		t.Fatal("single-run form should not include runs")
	}
	if _, present := singleRecord["count"]; present {
		t.Fatal("single-run form should not include count")
	}
}

func TestAvenorStatusListEmptyMCPShape(t *testing.T) {
	// An empty supervisor must still emit "runs": [] — a plain slice with
	// omitempty would drop the key.
	fake := &fakeClient{listResult: []map[string]any{}}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	serverTransport, clientTransport := mcpsdk.NewInMemoryTransports()
	serverSession, err := s.mcpServer.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatalf("connect server: %v", err)
	}
	defer serverSession.Close()
	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test-client", Version: "dev"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("connect client: %v", err)
	}
	defer clientSession.Close()

	res, err := clientSession.CallTool(ctx, &mcpsdk.CallToolParams{
		Name:      "avenor_status",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("call tool: %v", err)
	}
	structured, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("expected record structuredContent, got %T", res.StructuredContent)
	}
	runs, ok := structured["runs"].([]any)
	if !ok {
		t.Fatalf("expected runs array in output, got %#v", structured)
	}
	if len(runs) != 0 {
		t.Fatalf("expected empty runs, got %#v", runs)
	}
	if structured["count"] != float64(0) {
		t.Fatalf("expected count 0, got %#v", structured["count"])
	}
}

func TestAvenorStatusUsesTerminalSentinelAfterWorkflowRuntimeRemoval(t *testing.T) {
	sentinelPath := filepath.Join(t.TempDir(), "workflow.done")
	if err := os.WriteFile(sentinelPath, []byte("DONE\nSESSION=ses_workflow\nSTOP_REASON=end_turn\n"), 0644); err != nil {
		t.Fatal(err)
	}
	fake := &fakeClient{statusErr: errors.New("runtime not found")}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}
	s.registry.Store(&RunInfo{
		RunID:            "run-workflow",
		Label:            "workflow",
		RuntimeID:        "rt-workflow",
		SentinelPath:     sentinelPath,
		AgentProfile:     "cloud",
		EffectiveBackend: "agy",
	})

	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "run-workflow"})
	if err != nil {
		t.Fatal(err)
	}
	status := statusOutputMap(t, result)
	if status["status"] != "done" || status["session_id"] != "ses_workflow" {
		t.Fatalf("terminal status = %#v", status)
	}
	if status["agent_profile"] != "cloud" || status["effective_backend"] != "agy" {
		t.Fatalf("terminal identity = %#v", status)
	}
}

func TestAvenorStatusListIncludesTerminalRegistryRunAfterWorkflowRemoval(t *testing.T) {
	sentinelPath := filepath.Join(t.TempDir(), "workflow.done")
	if err := os.WriteFile(sentinelPath, []byte("FAILED\nSESSION=ses_workflow\nSTOP_REASON=error\n"), 0644); err != nil {
		t.Fatal(err)
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: &fakeClient{}})
	if err != nil {
		t.Fatal(err)
	}
	s.registry.Store(&RunInfo{
		RunID:        "run-workflow-list",
		Label:        "workflow-list",
		SentinelPath: sentinelPath,
		AgentProfile: "cloud",
	})

	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{})
	if err != nil {
		t.Fatal(err)
	}
	list := statusOutputRuns(t, result)
	if len(list) != 1 || list[0]["run_id"] != "run-workflow-list" || list[0]["status"] != "failed" {
		t.Fatalf("terminal list = %#v", list)
	}
	if list[0]["agent_profile"] != "cloud" {
		t.Fatalf("terminal list profile = %#v", list[0])
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
	status := statusOutputMap(t, result)
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
	if sid, _ := result["session_id"].(string); sid != "ses1" {
		t.Fatalf("result session_id = %q, want ses1 (adopted id from status)", sid)
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
	m := statusOutputMap(t, result)
	if m["run_id"] != "../../socket" {
		t.Errorf("expected run_id ../../socket, got %v", m["run_id"])
	}
	// Verify the exact run reference was forwarded to the control client.
	if len(fake.statusCapturedRuntimeIDs) != 1 || fake.statusCapturedRuntimeIDs[0] != "../../socket" {
		t.Errorf("expected statusCapturedRuntimeIDs [../../socket], got %v", fake.statusCapturedRuntimeIDs)
	}
}

func TestAvenorStatusPendingPermissionShapes(t *testing.T) {
	permission := map[string]any{"request_id": "req-42", "description": "Read file", "options": []any{map[string]any{"option_id": "allow_once"}}}
	t.Run("legacy object", func(t *testing.T) {
		fake := &fakeClient{
			statusResult: map[string]any{"status": "running", "pending_permission": map[string]any{"request_id": "req-42"}, "permission": permission},
		}
		s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
		if err != nil {
			t.Fatal(err)
		}
		_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "rt-x"})
		if err != nil {
			t.Fatal(err)
		}
		m := statusOutputMap(t, result)
		pending, ok := m["pending_permission"].(map[string]any)
		if !ok {
			t.Fatalf("pending_permission = %T, want map[string]any", m["pending_permission"])
		}
		if pending["request_id"] != "req-42" {
			t.Fatalf("request_id = %v, want req-42", pending["request_id"])
		}
		perm, ok := m["permission"].(map[string]any)
		if !ok || perm["request_id"] != "req-42" || perm["description"] != "Read file" {
			t.Fatalf("permission = %#v, want request record", m["permission"])
		}
	})

	t.Run("boolean", func(t *testing.T) {
		fake := &fakeClient{
			statusResult: map[string]any{"status": "running", "pending_permission": true, "permission": permission},
		}
		s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
		if err != nil {
			t.Fatal(err)
		}
		_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "rt-x"})
		if err != nil {
			t.Fatal(err)
		}
		m := statusOutputMap(t, result)
		if m["pending_permission"] != true {
			t.Fatalf("pending_permission = %v, want true", m["pending_permission"])
		}
		perm, ok := m["permission"].(map[string]any)
		if !ok || perm["request_id"] != "req-42" {
			t.Fatalf("permission = %#v, want request record", m["permission"])
		}
	})

	t.Run("lifecycle view keeps the permission record", func(t *testing.T) {
		fake := &fakeClient{
			statusResult: map[string]any{"status": "running", "pending_permission": true, "permission": permission},
		}
		s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
		if err != nil {
			t.Fatal(err)
		}
		_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "rt-x", View: "lifecycle"})
		if err != nil {
			t.Fatal(err)
		}
		m := statusOutputMap(t, result)
		perm, ok := m["permission"].(map[string]any)
		if !ok || perm["request_id"] != "req-42" {
			t.Fatalf("lifecycle permission = %#v, want request record", m["permission"])
		}
		if _, ok := m["dir"]; ok {
			t.Fatal("lifecycle view unexpectedly included dir")
		}
	})
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
	m := statusOutputMap(t, result)
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
	list := statusOutputRuns(t, result)
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
	// no-autostart with a non-existent socket must not fail at construction:
	// the explicit socket is dialed lazily, and the first acquisition reports
	// the supervisor as unavailable instead of falling back to autostart.
	s, err := NewServer(Options{
		Transport:        "stdio",
		SupervisorSocket: "/nonexistent/socket/path",
		NoAutostart:      true,
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v, want nil", err)
	}
	defer s.Close()

	_, _, err = s.getClientForSupervisor("")
	if err == nil {
		t.Fatal("expected error acquiring client for non-existent socket")
	}
	if !strings.Contains(err.Error(), "supervisor unavailable at /nonexistent/socket/path") {
		t.Fatalf("expected supervisor-unavailable error, got: %v", err)
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

	ri := s.registry.LookupUnique(runID)
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

func TestAvenorSpawnRosterSelectorAndResolvedIdentity(t *testing.T) {
	rosterPath := filepath.Join(t.TempDir(), "roster.json")
	fake := &fakeClient{
		statusResult: map[string]any{
			"runtime_id":        "rt_roster_1",
			"session_id":        "ses_roster_1",
			"backend":           "agy",
			"agent_profile":     "cloud",
			"agent":             "planner-agent",
			"model":             "planner-model",
			"roster_file":       rosterPath,
			"roster_entry":      "planner",
			"effective_backend": "agy",
			"effective_agent":   "planner-agent",
			"effective_model":   "planner-model",
		},
		spawnResult: map[string]any{
			"runtime_id": "rt_roster_1",
			"session_id": "ses_roster_1",
		},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	_, result, err := s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		RepoDir:     "/tmp/roster-repo",
		RosterFile:  rosterPath,
		RosterEntry: "planner",
	})
	if err != nil {
		t.Fatalf("roster spawn: %v", err)
	}
	if result.(map[string]any)["run_id"] == "" {
		t.Fatal("expected run ID")
	}
	if fake.spawnCapturedParams["roster_file"] != rosterPath || fake.spawnCapturedParams["roster_entry"] != "planner" {
		t.Fatalf("roster selector not forwarded: %#v", fake.spawnCapturedParams)
	}

	ri := s.registry.LookupUnique(result.(map[string]any)["run_id"].(string))
	if ri == nil {
		t.Fatal("expected registry entry")
	}
	if ri.RosterFile != rosterPath || ri.RosterEntry != "planner" {
		t.Fatalf("roster metadata = %#v", ri)
	}
	if ri.EffectiveBackend != "agy" || ri.EffectiveAgent != "planner-agent" || ri.EffectiveModel != "planner-model" || ri.AgentProfile != "cloud" {
		t.Fatalf("effective identity = %#v", ri)
	}
}

func TestAvenorSpawnRejectsMixedRosterSelectors(t *testing.T) {
	cases := []spawnArgs{
		{RepoDir: "/tmp/repo", RosterFile: "/tmp/roster.json"},
		{RepoDir: "/tmp/repo", RosterEntry: "planner"},
		{RepoDir: "/tmp/repo", RosterFile: "/tmp/roster.json", RosterEntry: "planner", Agent: "inline"},
		{RepoDir: "/tmp/repo", RosterFile: "/tmp/roster.json", RosterEntry: "planner", Model: "inline"},
		{RepoDir: "/tmp/repo", RosterFile: "/tmp/roster.json", RosterEntry: "planner", Backend: "pi"},
	}
	for i, args := range cases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			fake := &fakeClient{}
			s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := s.handleAvenorSpawn(context.Background(), nil, args); err == nil {
				t.Fatal("expected invalid roster selector")
			}
			if fake.spawnCapturedParams != nil {
				t.Fatalf("invalid selector reached control client: %#v", fake.spawnCapturedParams)
			}
		})
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

	ri := s.registry.LookupUnique("my-label")
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
		Thinking:   "high",
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
	if p["thinking"] != "high" {
		t.Errorf("expected thinking 'high', got %v", p["thinking"])
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

	ri := s.registry.LookupUnique(runID)
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

func TestAvenorSpawnRejectsInvalidThinkingBeforeClient(t *testing.T) {
	fake := &fakeClient{}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = s.handleAvenorSpawn(context.Background(), nil, spawnArgs{RepoDir: "/tmp/test-repo", Thinking: "HIGH"})
	if err == nil || !strings.Contains(err.Error(), "invalid thinking value") {
		t.Fatalf("error = %v", err)
	}
	if fake.spawnCapturedParams != nil {
		t.Fatal("spawn client called for invalid thinking")
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
	if ri := s.registry.LookupUnique(runID); ri == nil || !ri.AutoApprove {
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

func TestAvenorSpawnWithoutAgent(t *testing.T) {
	tests := []struct {
		name      string
		args      spawnArgs
		wantModel string
	}{
		{
			name: "neither agent nor model",
			args: spawnArgs{RepoDir: "/tmp/test-repo"},
		},
		{
			name:      "model only",
			args:      spawnArgs{RepoDir: "/tmp/test-repo", Model: "gpt-5"},
			wantModel: "gpt-5",
		},
		{
			name:      "empty agent is omitted",
			args:      spawnArgs{RepoDir: "/tmp/test-repo", Agent: "", Model: "gpt-5"},
			wantModel: "gpt-5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeClient{
				spawnResult: map[string]any{
					"runtime_id": "rt_spawn_without_agent",
					"session_id": "ses_spawn_without_agent",
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

			if _, _, err := s.handleAvenorSpawn(context.Background(), nil, tt.args); err != nil {
				t.Fatalf("agent-less spawn failed: %v", err)
			}
			if fake.spawnCapturedParams == nil {
				t.Fatal("expected spawn params to be captured")
			}
			if _, ok := fake.spawnCapturedParams["agent"]; ok {
				t.Errorf("agent key should be absent, params=%#v", fake.spawnCapturedParams)
			}
			if tt.wantModel == "" {
				if _, ok := fake.spawnCapturedParams["model"]; ok {
					t.Errorf("model key should be absent, params=%#v", fake.spawnCapturedParams)
				}
			} else if fake.spawnCapturedParams["model"] != tt.wantModel {
				t.Errorf("model = %v, want %q", fake.spawnCapturedParams["model"], tt.wantModel)
			}
		})
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

func TestSpawnAfterShutdownAutostartsNewSupervisor(t *testing.T) {
	origStart := startSupervisorFunc
	defer func() { startSupervisorFunc = origStart }()

	var starts atomic.Int32
	startSupervisorFunc = func(socketPath string, idleTimeout time.Duration) (*supervisorLifecycle, error) {
		n := starts.Add(1)
		return &supervisorLifecycle{
			socketPath: fmt.Sprintf("/tmp/avenor-restart-%d.sock", n),
			client:     &spawnCountingClient{},
		}, nil
	}

	s, err := NewServer(Options{Transport: "stdio"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	spawn := func(what string) {
		t.Helper()
		if _, _, err := s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
			Agent:   "test",
			RepoDir: ".",
			Prompt:  "hello",
		}); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}

	// The first spawn autostarts the default supervisor.
	spawn("first spawn")

	for cycle := 1; cycle <= 2; cycle++ {
		if _, _, err := s.handleAvenorShutdown(context.Background(), nil, shutdownArgs{}); err != nil {
			t.Fatalf("shutdown %d: %v", cycle, err)
		}
		// Shutdown must leave the server live: closed means only that
		// Close() ran, so a later tool call may autostart a replacement.
		if s.closed {
			t.Fatalf("cycle %d: shutdown set closed, later tools would fail", cycle)
		}
		spawn(fmt.Sprintf("spawn after shutdown %d", cycle))

		want := fmt.Sprintf("/tmp/avenor-restart-%d.sock", cycle+1)
		if s.defaultSupervisorPath != want {
			t.Fatalf("defaultSupervisorPath after restart %d = %q, want %q", cycle, s.defaultSupervisorPath, want)
		}
	}

	if got := starts.Load(); got != 3 {
		t.Fatalf("supervisor starts after two shutdown+spawn cycles = %d, want 3", got)
	}

	// Server teardown still blocks later tool calls: shutdown must not set
	// closed, but Close() must.
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, _, err := s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		Agent:   "test",
		RepoDir: ".",
		Prompt:  "hello",
	}); err == nil || !strings.Contains(err.Error(), "control client not available") {
		t.Fatalf("spawn after Close error = %v, want 'control client not available'", err)
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
	if s.lifecycle != nil || s.controlClient != nil || s.closed || s.defaultSupervisorPath != "" {
		t.Fatalf("shutdown error retained closed lifecycle/client: lifecycle=%v client=%v closed=%v defaultPath=%q", s.lifecycle, s.controlClient, s.closed, s.defaultSupervisorPath)
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

	if ri := s.registry.LookupUnique("run-clean-1"); ri != nil {
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
	m := statusOutputMap(t, result)
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

func TestAvenorStatusRegisteredParkedTerminalViews(t *testing.T) {
	dir := t.TempDir()
	sentinelPath := writeSentinel(t, dir, "parked.done", "DONE\nSESSION=ses_stale\nSTOP_REASON=stale\n")
	fake := &fakeClient{statusResult: map[string]any{
		"runtime_id":   "rt_parked",
		"status":       "idle",
		"session_id":   "ses_live",
		"phase":        "done",
		"stop_reason":  "current_turn",
		"final_output": "parked answer",
		"usage":        map[string]any{"total_tokens": 7},
	}}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}
	s.registry.Store(&RunInfo{RunID: "run-parked", Label: "parked", RuntimeID: "rt_parked", SentinelPath: sentinelPath})

	for _, view := range []string{"", "lifecycle"} {
		_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "run-parked", View: view})
		if err != nil {
			t.Fatalf("view %q: %v", view, err)
		}
		status := statusOutputMap(t, result)
		if status["status"] != "done" || status["phase"] != "done" {
			t.Fatalf("view %q status = %#v, want done/done", view, status)
		}
		if view == "" && (status["session_id"] != "ses_live" || status["stop_reason"] != "current_turn") {
			t.Fatalf("view %q metadata = %#v, want current raw values", view, status)
		}
		if status["run_id"] != "run-parked" || status["label"] != "parked" {
			t.Fatalf("view %q identity = %#v", view, status)
		}
		if view == "lifecycle" {
			if _, ok := status["final_output"]; ok {
				t.Fatal("lifecycle view included final_output")
			}
			if _, ok := status["usage"]; ok {
				t.Fatal("lifecycle view included usage")
			}
		} else if status["final_output"] != "parked answer" {
			t.Fatalf("full view final_output = %v", status["final_output"])
		}
	}
}

func TestAvenorResultParkedTerminalAndRetrySnapshots(t *testing.T) {
	dir := t.TempDir()
	parkedSentinel := writeSentinel(t, dir, "parked.done", "DONE\nSESSION=ses_parked\nSTOP_REASON=end_turn\n")
	fake := &fakeClient{
		statusResult: map[string]any{
			"runtime_id":   "rt_parked",
			"status":       "idle",
			"session_id":   "ses_live",
			"phase":        "done",
			"stop_reason":  "current_turn",
			"final_output": "preview",
		},
		resultResult: map[string]any{"final_output": "complete parked answer"},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}
	s.registry.Store(&RunInfo{RunID: "run-parked", Label: "parked", RuntimeID: "rt_parked", SentinelPath: parkedSentinel})

	_, value, err := s.handleAvenorResult(context.Background(), nil, resultArgs{RunID: "run-parked"})
	if err != nil {
		t.Fatal(err)
	}
	parked := value.(map[string]any)
	if parked["ready"] != true || parked["status"] != "done" || parked["output"] != "complete parked answer" || parked["session_id"] != "ses_live" || parked["stop_reason"] != "current_turn" {
		t.Fatalf("parked result = %#v", parked)
	}

	staleSentinel := writeSentinel(t, dir, "retry.done", "DONE\nSESSION=ses_stale\nSTOP_REASON=stale\n")
	currentTime := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	fake.statusFunc = func(runtimeID string) (map[string]any, error) {
		currentTime = currentTime.Add(2 * time.Second)
		return map[string]any{"runtime_id": runtimeID, "status": "idle", "session_id": "ses_retry", "phase": ""}, nil
	}
	fake.statusResult = nil
	s.clock = func() time.Time { return currentTime }
	s.registry.Store(&RunInfo{RunID: "run-retry", Label: "retry", RuntimeID: "rt_retry", SentinelPath: staleSentinel})
	_, value, err = s.handleAvenorResult(context.Background(), nil, resultArgs{RunID: "run-retry", Timeout: "1s"})
	if err != nil {
		t.Fatal(err)
	}
	retry := value.(map[string]any)
	if retry["ready"] != false || retry["status"] != "running" || retry["timed_out"] != true {
		t.Fatalf("retry result = %#v, want bounded non-ready polling", retry)
	}
}

func TestAvenorStatusListWithParkedAndRetrySnapshots(t *testing.T) {
	dir := t.TempDir()
	parkedSentinel := writeSentinel(t, dir, "parked.done", "DONE\nSESSION=ses_stale\nSTOP_REASON=stale\n")
	staleSentinel := writeSentinel(t, dir, "retry.done", "DONE\nSESSION=ses_stale\nSTOP_REASON=stale\n")
	fake := &fakeClient{listResult: []map[string]any{
		{"runtime_id": "rt_parked", "status": "idle", "session_id": "ses_live", "phase": "done", "stop_reason": "current_turn"},
		{"runtime_id": "rt_retry", "status": "idle", "session_id": "ses_retry", "phase": ""},
	}}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}
	s.registry.Store(&RunInfo{RunID: "run-parked", Label: "parked", RuntimeID: "rt_parked", SentinelPath: parkedSentinel})
	s.registry.Store(&RunInfo{RunID: "run-retry", Label: "retry", RuntimeID: "rt_retry", SentinelPath: staleSentinel})

	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{})
	if err != nil {
		t.Fatal(err)
	}
	list := statusOutputRuns(t, result)
	if len(list) != 2 {
		t.Fatalf("list length = %d, want 2", len(list))
	}
	if list[0]["status"] != "done" || list[0]["phase"] != "done" || list[0]["session_id"] != "ses_live" || list[0]["stop_reason"] != "current_turn" {
		t.Fatalf("parked list entry = %#v", list[0])
	}
	if list[0]["run_id"] != "run-parked" || list[0]["label"] != "parked" {
		t.Fatalf("parked list identity = %#v", list[0])
	}
	if list[1]["status"] != "running" || list[1]["phase"] != "" || list[1]["session_id"] != "ses_retry" {
		t.Fatalf("retry list entry = %#v", list[1])
	}
	if list[1]["run_id"] != "run-retry" || list[1]["label"] != "retry" {
		t.Fatalf("retry list identity = %#v", list[1])
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
		RunID:            "run-list-1",
		Label:            "list-run-1",
		RuntimeID:        "rt_1",
		SentinelPath:     sentinelPath,
		RosterFile:       "/repo/roster.json",
		RosterEntry:      "planner",
		EffectiveBackend: "agy",
		EffectiveAgent:   "planner-agent",
		EffectiveModel:   "planner-model",
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
	list := statusOutputRuns(t, result)
	if len(list) != 2 {
		t.Fatalf("expected 2 results, got %d", len(list))
	}

	if list[0]["run_id"] != "run-list-1" {
		t.Errorf("expected run_id run-list-1, got %v", list[0]["run_id"])
	}
	if list[0]["status"] != "running" {
		t.Errorf("expected running status for rt_1, got %v", list[0]["status"])
	}
	if list[0]["roster_entry"] != "planner" || list[0]["effective_backend"] != "agy" || list[0]["effective_agent"] != "planner-agent" {
		t.Errorf("roster identity missing from list: %#v", list[0])
	}

	if list[1]["run_id"] != "run-list-2" {
		t.Errorf("expected run_id run-list-2, got %v", list[1]["run_id"])
	}
	if list[1]["status"] != "done" {
		t.Errorf("expected done status for rt_2, got %v", list[1]["status"])
	}
}

func TestAvenorStatusPreservesRosterIdentity(t *testing.T) {
	fake := &fakeClient{statusResult: map[string]any{"status": "running", "runtime_id": "rt_roster_status"}}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}
	s.registry.Store(&RunInfo{
		RunID:            "run-roster-status",
		Label:            "roster-status",
		RuntimeID:        "rt_roster_status",
		RosterFile:       "/repo/roster.json",
		RosterEntry:      "planner",
		EffectiveBackend: "agy",
		EffectiveAgent:   "planner-agent",
		EffectiveModel:   "planner-model",
	})

	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "run-roster-status"})
	if err != nil {
		t.Fatal(err)
	}
	status := statusOutputMap(t, result)
	for key, want := range map[string]any{
		"roster_file":       "/repo/roster.json",
		"roster_entry":      "planner",
		"effective_backend": "agy",
		"effective_agent":   "planner-agent",
		"effective_model":   "planner-model",
	} {
		if status[key] != want {
			t.Fatalf("status[%q] = %v, want %v", key, status[key], want)
		}
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
	m := statusOutputMap(t, result)
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
	m := statusOutputMap(t, result)
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
	if !strings.Contains(err.Error(), "no supervisor running") {
		t.Errorf("expected 'no supervisor running', got: %v", err)
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
	m := statusOutputMap(t, result)
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
	if !strings.Contains(err.Error(), "pass request_id to retry a resolved answer") {
		t.Errorf("expected retry hint, got: %v", err)
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

func TestAvenorEventsAfterSeqFlowsToLatestSeq(t *testing.T) {
	dir := t.TempDir()
	eventLogPath := filepath.Join(dir, "events-cursor.log")
	content := `{"event":"start","seq":1,"type":"lifecycle"}
{"event":"prompt","seq":2,"type":"turn","text":"hello"}
{"event":"done","seq":3,"type":"lifecycle"}
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
		RunID:        "run-events-5",
		Label:        "events-cursor-test",
		RuntimeID:    "rt_events_5",
		EventLogPath: eventLogPath,
	})

	// A mid-log cursor (seq 1) resumes after it: only seq 2 and 3 remain.
	cursor := int64(1)
	_, result, err := s.handleAvenorEvents(context.Background(), nil, eventsArgs{
		RunID:    "run-events-5",
		AfterSeq: &cursor,
	})
	if err != nil {
		t.Fatal(err)
	}
	// The response is a raw map, not a typed struct: a regression renaming
	// "latest_seq" must fail the key lookup, not a field access.
	rm, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", result)
	}
	events, ok := rm["events"].([]map[string]any)
	if !ok {
		t.Fatalf("expected events []map[string]any, got %T", rm["events"])
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events after cursor 1, got %d", len(events))
	}
	if events[0]["seq"] != float64(2) {
		t.Errorf("expected first event seq=2, got %v", events[0]["seq"])
	}
	if events[1]["seq"] != float64(3) {
		t.Errorf("expected second event seq=3, got %v", events[1]["seq"])
	}
	// after_seq flows to latest_seq: the safe resume point is the highest
	// seq among the returned events.
	if got, ok := rm["latest_seq"].(int64); !ok || got != 3 {
		t.Fatalf("latest_seq = %#v, want int64 3", rm["latest_seq"])
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
		AgentProfile: "cloud",
		Backend:      "pi",
		Thinking:     "high",
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
	if p["thinking"] != "high" {
		t.Errorf("expected thinking high, got %v", p["thinking"])
	}
	if p["agent_profile"] != "cloud" {
		t.Errorf("expected agent_profile cloud, got %v", p["agent_profile"])
	}
	if p["dir"] != "/tmp/prior-repo" {
		t.Errorf("expected dir /tmp/prior-repo, got %v", p["dir"])
	}
	if p["label"] != "prior-test-followup" {
		t.Errorf("expected label prior-test-followup, got %v", p["label"])
	}

	ri := s.registry.LookupUnique(runID)
	if ri == nil {
		t.Fatal("expected new registry entry for follow-up run")
	}
	if ri.Agent != "claude" {
		t.Errorf("expected agent claude, got %s", ri.Agent)
	}
	if ri.Backend != "pi" {
		t.Errorf("expected backend pi, got %s", ri.Backend)
	}
	if ri.Thinking != "high" {
		t.Errorf("expected thinking high, got %s", ri.Thinking)
	}
	if ri.AgentProfile != "cloud" {
		t.Errorf("expected agent_profile cloud, got %s", ri.AgentProfile)
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

func TestAvenorRosterFollowUpUsesResolvedIdentity(t *testing.T) {
	sentinelPath := filepath.Join(t.TempDir(), "roster-followup.done")
	if err := os.WriteFile(sentinelPath, []byte("DONE\nSESSION=ses_prior\n"), 0644); err != nil {
		t.Fatal(err)
	}
	var captured map[string]any
	fake := &fakeClient{
		spawnFunc: func(params map[string]any) (map[string]any, error) {
			captured = params
			return map[string]any{"runtime_id": "rt_roster_followup", "session_id": "ses_roster_followup"}, nil
		},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}
	s.registry.Store(&RunInfo{
		RunID:            "run-roster-prior",
		Label:            "roster-prior",
		RuntimeID:        "rt-roster-prior",
		SessionID:        "ses-prior",
		SentinelPath:     sentinelPath,
		RosterFile:       "/tmp/mutable-roster.json",
		RosterEntry:      "planner",
		EffectiveAgent:   "resolved-agent",
		EffectiveModel:   "resolved-model",
		EffectiveBackend: "agy",
		Agent:            "resolved-agent",
		Model:            "resolved-model",
		Backend:          "agy",
		Dir:              "/tmp/roster-repo",
	})

	_, result, err := s.handleAvenorFollowUp(context.Background(), nil, followUpArgs{
		RunID: "run-roster-prior", Message: "continue",
	})
	if err != nil {
		t.Fatal(err)
	}
	if captured["backend"] != "agy" || captured["agent"] != "resolved-agent" || captured["model"] != "resolved-model" {
		t.Fatalf("follow-up identity = %#v", captured)
	}
	if _, ok := captured["roster_file"]; ok {
		t.Fatalf("follow-up reread roster_file: %#v", captured)
	}
	if _, ok := captured["roster_entry"]; ok {
		t.Fatalf("follow-up reread roster_entry: %#v", captured)
	}
	followupID := result.(map[string]any)["run_id"].(string)
	followup := s.registry.LookupUnique(followupID)
	if followup == nil || followup.RosterFile != "/tmp/mutable-roster.json" || followup.EffectiveBackend != "agy" {
		t.Fatalf("follow-up metadata = %#v", followup)
	}
}

func TestAvenorWorkflowFollowUpClearsStaleModelFromAgentOnlyFinalPhase(t *testing.T) {
	sentinelPath := filepath.Join(t.TempDir(), "agent-only-followup.done")
	if err := os.WriteFile(sentinelPath, []byte("DONE\nSESSION=final-session\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var captured map[string]any
	fake := &fakeClient{
		statusResult: map[string]any{
			"session_id": "final-session", "effective_agent": "final-agent",
			"effective_model": "", "effective_backend": "agy", "agent_profile": "cloud",
		},
		spawnFunc: func(params map[string]any) (map[string]any, error) {
			captured = params
			return map[string]any{"runtime_id": "rt-agent-only-followup"}, nil
		},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.registry.Store(&RunInfo{
		RunID: "agent-only-workflow", Label: "agent-only-workflow", RuntimeID: "rt-agent-only",
		SentinelPath: sentinelPath, EffectiveAgent: "stale-agent", EffectiveModel: "stale-model",
		EffectiveBackend: "pi", AgentProfile: "cloud", Dir: "/tmp/repo",
	}); err != nil {
		t.Fatal(err)
	}

	if _, _, err := s.handleAvenorFollowUp(context.Background(), nil, followUpArgs{
		RunID: "agent-only-workflow", Message: "continue",
	}); err != nil {
		t.Fatal(err)
	}
	if captured["agent"] != "final-agent" || captured["backend"] != "agy" || captured["agent_profile"] != "cloud" {
		t.Fatalf("final identity not forwarded: %#v", captured)
	}
	if _, exists := captured["model"]; exists {
		t.Fatalf("stale run-level model was forwarded: %#v", captured)
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
	if ri := s.registry.LookupUnique(firstRunID); ri == nil || !ri.AutoApprove {
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
	if ri := s.registry.LookupUnique(secondRunID); ri == nil || !ri.AutoApprove {
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

func TestAvenorFollowUpUsesCurrentSupervisorSessionWhenSentinelMissing(t *testing.T) {
	fake := &fakeClient{
		statusResult: map[string]any{
			"runtime_id": "rt_live_adopted",
			"session_id": "conv-current-real",
			"status":     "running",
		},
		spawnResult: map[string]any{
			"runtime_id": "rt_followup_live",
			"session_id": "ses_followup_live",
		},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	// Registry carries the spawn-time provisional id; the terminal sentinel is
	// absent because the supervisor has not written one yet.
	s.registry.Store(&RunInfo{
		RunID:        "run-live-adopt",
		Label:        "live-adopt",
		RuntimeID:    "rt_live_adopted",
		SessionID:    "agy-pending-live",
		SentinelPath: filepath.Join(t.TempDir(), "missing-live.done"),
		Agent:        "agy",
		Backend:      "agy",
		Dir:          "/tmp/live-repo",
	})

	if _, _, err := s.handleAvenorFollowUp(context.Background(), nil, followUpArgs{
		RunID:   "run-live-adopt",
		Message: "continue",
	}); err != nil {
		t.Fatal(err)
	}

	if len(fake.statusCapturedRuntimeIDs) == 0 || fake.statusCapturedRuntimeIDs[0] != "rt_live_adopted" {
		t.Fatalf("status lookups = %#v", fake.statusCapturedRuntimeIDs)
	}
	p := fake.spawnCapturedParams
	if p["session_id"] != "conv-current-real" {
		t.Fatalf("expected adopted session conv-current-real, got %v", p["session_id"])
	}
	if p["backend"] != "agy" {
		t.Fatalf("expected backend agy, got %v", p["backend"])
	}
}

func TestAvenorFollowUpRetriesMissingSentinelAfterStatusRace(t *testing.T) {
	tests := []struct {
		name             string
		statusResult     map[string]any
		statusErr        error
		createSentinel   bool
		sentinelContents string
		wantSession      string
		wantError        string
	}{
		{
			name:             "status error then valid sentinel",
			statusErr:        errors.New("status unavailable"),
			createSentinel:   true,
			sentinelContents: "DONE\nSESSION=ses_after_status\n",
			wantSession:      "ses_after_status",
		},
		{
			name:             "status without session then valid sentinel",
			statusResult:     map[string]any{"runtime_id": "rt_race", "status": "running"},
			createSentinel:   true,
			sentinelContents: "DONE\nSESSION=ses_after_empty_status\n",
			wantSession:      "ses_after_empty_status",
		},
		{
			name:             "status error then failed sentinel",
			statusErr:        errors.New("status unavailable"),
			createSentinel:   true,
			sentinelContents: "FAILED\nSESSION=ses_failed\n",
			wantSession:      "ses_failed",
		},
		{
			name:             "status error then blocked sentinel",
			statusErr:        errors.New("status unavailable"),
			createSentinel:   true,
			sentinelContents: "BLOCKED\nSESSION=ses_blocked\nSTOP_REASON=blocked\n",
			wantSession:      "ses_blocked",
		},
		{
			name:             "status error then killed sentinel",
			statusErr:        errors.New("status unavailable"),
			createSentinel:   true,
			sentinelContents: "KILLED\nSESSION=ses_killed\nEXIT_CODE=130\n",
			wantError:        "not resumable",
		},
		{
			name:             "status error then timeout sentinel",
			statusErr:        errors.New("status unavailable"),
			createSentinel:   true,
			sentinelContents: "TIMEOUT\nSESSION=ses_timeout\n",
			wantError:        "not resumable",
		},
		{
			name:             "status without session then invalid sentinel",
			statusResult:     map[string]any{"runtime_id": "rt_race", "status": "running"},
			createSentinel:   true,
			sentinelContents: "DONE\n",
			wantError:        "no session in sentinel",
		},
		{
			name:        "status error then still missing",
			statusErr:   errors.New("status unavailable"),
			wantSession: "agy-pending-race",
		},
		{
			name:         "status without session then still missing",
			statusResult: map[string]any{"runtime_id": "rt_race", "status": "running"},
			wantSession:  "agy-pending-race",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			sentinelPath := filepath.Join(dir, "missing.done")
			fake := &fakeClient{
				statusResult: test.statusResult,
				statusErr:    test.statusErr,
				spawnResult: map[string]any{
					"runtime_id": "rt_followup_race",
					"session_id": "ses_followup_race",
				},
			}
			fake.statusFunc = func(string) (map[string]any, error) {
				if test.createSentinel {
					if err := os.WriteFile(sentinelPath, []byte(test.sentinelContents), 0644); err != nil {
						t.Fatal(err)
					}
				}
				return test.statusResult, test.statusErr
			}

			s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
			if err != nil {
				t.Fatal(err)
			}
			if err := s.registry.Store(&RunInfo{
				RunID:        "run-race",
				Label:        "race",
				RuntimeID:    "rt_race",
				SessionID:    "agy-pending-race",
				SentinelPath: sentinelPath,
				Agent:        "agy",
				Backend:      "agy",
				Dir:          "/tmp/race-repo",
			}); err != nil {
				t.Fatal(err)
			}

			_, _, err = s.handleAvenorFollowUp(context.Background(), nil, followUpArgs{
				RunID:   "run-race",
				Message: "continue",
			})
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("error = %v, want substring %q", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := fake.spawnCapturedParams["session_id"]; got != test.wantSession {
				t.Fatalf("spawn session_id = %v, want %q", got, test.wantSession)
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
	sentinelPath := filepath.Join(dir, "followup-killed.done")
	if err := os.WriteFile(sentinelPath, []byte("KILLED\nSESSION=ses_killed\nEXIT_CODE=130\n"), 0644); err != nil {
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
		Label:        "prior-killed",
		RuntimeID:    "rt_prior_5",
		SessionID:    "ses_killed",
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

func TestServerIsAllowedHTTPHost(t *testing.T) {
	empty := &Server{opts: Options{}}
	populated := &Server{opts: Options{AllowedHosts: []string{"box.example.ts.net", "BOX.example.TS.net"}}}

	tests := []struct {
		name     string
		s        *Server
		hostport string
		want     bool
	}{
		{"loopback localhost empty", empty, "localhost", true},
		{"loopback localhost populated", populated, "localhost", true},
		{"loopback 127.0.0.1 empty", empty, "127.0.0.1:3748", true},
		{"loopback ::1 populated", populated, "[::1]:3748", true},
		{"exact host with port", populated, "box.example.ts.net:8443", true},
		{"case variant entry", populated, "Box.Example.ts.net", true},
		{"lookalike prefix", populated, "evil-box.example.ts.net", false},
		{"lookalike suffix", populated, "box.example.ts.net.evil.com", false},
		{"lookalike suffix-only", populated, "ts.net", false},
		{"unlisted host empty allowlist", empty, "box.example.ts.net", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.isAllowedHTTPHost(tt.hostport); got != tt.want {
				t.Fatalf("isAllowedHTTPHost(%q) = %v, want %v", tt.hostport, got, tt.want)
			}
		})
	}
}

func TestServerIsAllowedHTTPOrigin(t *testing.T) {
	empty := &Server{opts: Options{}}
	populated := &Server{opts: Options{AllowedHosts: []string{"box.example.ts.net"}}}

	tests := []struct {
		name   string
		s      *Server
		origin string
		want   bool
	}{
		{"empty origin empty allowlist", empty, "", true},
		{"empty origin populated", populated, "", true},
		{"loopback http empty", empty, "http://localhost", true},
		{"loopback http with port populated", populated, "http://127.0.0.1:3748", true},
		{"https exact host", populated, "https://box.example.ts.net", true},
		{"http exact host", populated, "http://box.example.ts.net", true},
		{"https with port", populated, "https://box.example.ts.net:8443", true},
		{"http with port", populated, "http://box.example.ts.net:8443", true},
		{"lookalike prefix", populated, "https://evil-box.example.ts.net", false},
		{"lookalike suffix", populated, "https://box.example.ts.net.evil.com", false},
		{"lookalike suffix-only", populated, "https://ts.net", false},
		{"unlisted host empty allowlist", empty, "https://box.example.ts.net", false},
		{"loopback https not allowed", empty, "https://localhost", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.isAllowedHTTPOrigin(tt.origin); got != tt.want {
				t.Fatalf("isAllowedHTTPOrigin(%q) = %v, want %v", tt.origin, got, tt.want)
			}
		})
	}
}

func TestServerIsAllowedHTTPHostRejectsNonASCII(t *testing.T) {
	// The KELVIN SIGN (U+212A) simple-folds to 'k', so a non-ASCII host could
	// otherwise match an allowlist entry via EqualFold.
	s := &Server{opts: Options{AllowedHosts: []string{"kbox.example.ts.net"}}}
	const kelvin = "\u212A"
	if got := s.isAllowedHTTPHost(kelvin + "box.example.ts.net"); got {
		t.Fatalf("isAllowedHTTPHost(%q) = true, want false (non-ASCII host)", kelvin+"box.example.ts.net")
	}
}

func TestServerIsAllowedHTTPOriginRejectsNonASCII(t *testing.T) {
	s := &Server{opts: Options{AllowedHosts: []string{"kbox.example.ts.net"}}}
	const kelvin = "\u212A"
	if got := s.isAllowedHTTPOrigin("https://" + kelvin + "box.example.ts.net"); got {
		t.Fatalf("isAllowedHTTPOrigin(%q) = true, want false (non-ASCII origin)", "https://"+kelvin+"box.example.ts.net")
	}
}

func TestServerAuthenticatedHTTPHandlerHostChecks(t *testing.T) {
	s := &Server{opts: Options{AuthToken: "t", AllowedHosts: []string{"box.example.ts.net"}}}
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	ts := httptest.NewServer(s.authenticatedHTTPHandler(next))
	defer ts.Close()

	t.Run("unlisted host rejected", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, ts.URL, nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		req.Host = "unlisted.example.ts.net"
		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatalf("Do: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusForbidden)
		}
	})

	t.Run("allowed host with valid token passes through", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, ts.URL, nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		req.Host = "box.example.ts.net"
		req.Header.Set("Authorization", "Bearer t")
		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatalf("Do: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
		}
	})

	t.Run("unlisted origin rejected", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, ts.URL, nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		req.Host = "box.example.ts.net"
		req.Header.Set("Origin", "https://unlisted.example.ts.net")
		req.Header.Set("Authorization", "Bearer t")
		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatalf("Do: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusForbidden)
		}
	})

	t.Run("allowed origin with valid token passes through", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, ts.URL, nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		req.Host = "box.example.ts.net"
		req.Header.Set("Origin", "https://box.example.ts.net")
		req.Header.Set("Authorization", "Bearer t")
		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatalf("Do: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
		}
	})
}

// startStubSupervisorListener runs a listener that accepts supervisor
// connections and parks them without speaking the control protocol; tests
// use it as a dial target and close accepted conns to simulate a restart.
func startStubSupervisorListener(t *testing.T, socketPath string) (acceptedCount func() int, closeAccepted func(), cleanup func()) {
	t.Helper()
	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("listen %s: %v", socketPath, err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
		}
	}()
	acceptedCount = func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(conns)
	}
	closeAccepted = func() {
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			c.Close()
		}
		conns = nil
	}
	cleanup = func() {
		ln.Close()
		closeAccepted()
		os.Remove(socketPath)
	}
	return acceptedCount, closeAccepted, cleanup
}

// withCountedDials swaps dialSupervisorClient for a counting wrapper that
// delegates to the original dial, restoring the seam when the test ends.
// Tests that must block inside the dial keep their own wrapper instead.
func withCountedDials(t *testing.T) *atomic.Int32 {
	t.Helper()
	var dials atomic.Int32
	origDial := dialSupervisorClient
	dialSupervisorClient = func(p string) (*client.Client, error) {
		dials.Add(1)
		return origDial(p)
	}
	t.Cleanup(func() { dialSupervisorClient = origDial })
	return &dials
}

func TestRedialExplicitSupervisorSocketAfterDisconnect(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "sup.sock")

	dials := withCountedDials(t)

	s, err := NewServer(Options{Transport: "stdio", SupervisorSocket: socketPath, NoAutostart: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Before the supervisor listens, acquisition fails per call and the
	// explicit socket never falls back to autostart.
	if _, _, err := s.getClientForSupervisor(""); err == nil {
		t.Fatal("expected supervisor-unavailable error before listener exists")
	}

	acceptedCount, closeAccepted, stopListener := startStubSupervisorListener(t, socketPath)
	defer stopListener()

	// The next acquisition redials and succeeds once the supervisor listens.
	deadline := time.Now().Add(5 * time.Second)
	for {
		cl, cleanup, err := s.getClientForSupervisor("")
		if err == nil {
			cleanup()
			_ = cl
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("acquisition never succeeded after listener started: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for the server side to have accepted the client's connection, then
	// simulate a supervisor restart by dropping it.
	deadline = time.Now().Add(5 * time.Second)
	for acceptedCount() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("supervisor never accepted the client connection")
		}
		time.Sleep(10 * time.Millisecond)
	}
	closeAccepted()

	dead := s.persistentControlClientForTest()
	deadline = time.Now().Add(5 * time.Second)
	for !dead.Closed() {
		if time.Now().After(deadline) {
			t.Fatal("client never observed the closed connection")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The next acquisition must redial exactly once, and it must succeed.
	before := dials.Load()
	cl, cleanup, err := s.getClientForSupervisor("")
	if err != nil {
		t.Fatalf("redial acquisition: %v", err)
	}
	cleanup()
	_ = cl
	if got := dials.Load() - before; got != 1 {
		t.Fatalf("new dials after disconnect = %d, want exactly 1", got)
	}
}

func TestAutostartRedialsAfterDeadClient(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "sup.sock")
	_, _, stopListener := startStubSupervisorListener(t, socketPath)
	defer stopListener()

	origStart := startSupervisorFunc
	defer func() { startSupervisorFunc = origStart }()
	var starts atomic.Int32
	startSupervisorFunc = func(string, time.Duration) (*supervisorLifecycle, error) {
		starts.Add(1)
		return nil, fmt.Errorf("dial control socket: connection refused")
	}

	// Establish then kill a real control connection so the server's
	// persistent client is dead (Closed() == true).
	cl, err := client.Dial(socketPath)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := cl.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !cl.Closed() {
		t.Fatal("client is not closed after Close()")
	}

	s, err := NewServer(Options{Transport: "stdio", ControlClient: cl})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// A supervisor-backed tool call must restart the supervisor (redial)
	// instead of failing on the dead connection.
	_, _, err = s.handleAvenorStatus(context.Background(), nil, statusArgs{})
	if err == nil {
		t.Fatal("expected an error (the replacement supervisor is not listening)")
	}
	if got := starts.Load(); got != 1 {
		t.Fatalf("supervisor starts = %d, want 1 (the dead client must trigger a restart)", got)
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("error = %v, want the clean dial error", err)
	}
	if strings.Contains(err.Error(), "connection closed") {
		t.Fatalf("error = %v, must not be 'connection closed' (the dead client was used)", err)
	}
}

func TestConcurrentAcquisitionsShareOneDial(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "sup.sock")
	acceptedCount, _, stopListener := startStubSupervisorListener(t, socketPath)
	defer stopListener()

	const n = 8

	var dials atomic.Int32
	dialEntered := make(chan struct{})
	releaseDial := make(chan struct{})
	origDial := dialSupervisorClient
	dialSupervisorClient = func(p string) (*client.Client, error) {
		dials.Add(1)
		dialEntered <- struct{}{}
		<-releaseDial
		return origDial(p)
	}
	defer func() { dialSupervisorClient = origDial }()

	atLockBoundary := make(chan struct{}, n)
	releaseLockBoundary := make(chan struct{})
	origBeforeLock := beforeSupervisorLock
	beforeSupervisorLock = func() {
		atLockBoundary <- struct{}{}
		<-releaseLockBoundary
	}
	defer func() { beforeSupervisorLock = origBeforeLock }()

	s, err := NewServer(Options{Transport: "stdio", SupervisorSocket: socketPath, NoAutostart: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	errs := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl, cleanup, err := s.getClientForSupervisor("")
			if err == nil {
				cleanup()
				_ = cl
			} else {
				errs <- err
			}
		}()
	}
	// Hold every caller immediately before the lock so a serialized
	// (non-shared) dial schedule is impossible: no caller can complete a
	// dial before the others are parked at the boundary.
	for i := 0; i < n; i++ {
		select {
		case <-atLockBoundary:
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for callers at the lock boundary")
		}
	}
	// Release the callers: exactly one may enter the dial, the rest must
	// wait behind it for the lock.
	close(releaseLockBoundary)
	select {
	case <-dialEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: no dial began while callers were held")
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("dials while other callers are held at the lock = %d, want exactly 1", got)
	}
	// Unblock the single in-flight dial; every caller must then observe the
	// established client.
	close(releaseDial)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent acquisition failed: %v", err)
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("dials = %d, want exactly 1 shared dial", got)
	}
	// The kernel may complete the connect before the accept goroutine runs;
	// poll for the accepted connection.
	deadline := time.Now().Add(5 * time.Second)
	for acceptedCount() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("supervisor never accepted the shared dial")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestInjectedControlClientNeverReplaced(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "sup.sock")

	dials := withCountedDials(t)

	fake := &fakeClient{}
	s, err := NewServer(Options{
		Transport:        "stdio",
		SupervisorSocket: socketPath,
		NoAutostart:      true,
		ControlClient:    fake,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	cl, cleanup, err := s.getClientForSupervisor("")
	if err != nil {
		t.Fatal(err)
	}
	cleanup()
	if cl != ControlClient(fake) {
		t.Fatal("injected control client was replaced")
	}
	if got := dials.Load(); got != 0 {
		t.Fatalf("dials with injected client = %d, want 0", got)
	}
}

func TestNewServerWithSupervisorSocketDialsZeroTimes(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "sup.sock")

	dials := withCountedDials(t)

	for _, noAutostart := range []bool{true, false} {
		s, err := NewServer(Options{
			Transport:        "stdio",
			SupervisorSocket: socketPath,
			NoAutostart:      noAutostart,
		})
		if err != nil {
			t.Fatal(err)
		}
		s.Close()
	}
	if got := dials.Load(); got != 0 {
		t.Fatalf("dials during construction = %d, want 0", got)
	}
}
