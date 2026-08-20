package control

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// fakeWorkflowHandler is a recording stub implementing WorkflowHandler for
// dispatch routing tests.
type fakeWorkflowHandler struct {
	createCalled      bool
	instantiateCalled bool
	statusID          string
	statusResult      any
	statusErr         error
	waitID            string
	waitTimeout       time.Duration
	inspectID         string
	eventsID          string
	eventsAfterSeq    int64
	eventsLimit       int
	commandID         string
	commandPayload    json.RawMessage
}

var _ WorkflowHandler = (*fakeWorkflowHandler)(nil)

func (f *fakeWorkflowHandler) WorkflowCreate(params json.RawMessage) (any, error) {
	f.createCalled = true
	return map[string]any{"workflow_id": "wf_new"}, nil
}

func (f *fakeWorkflowHandler) WorkflowInstantiate(params json.RawMessage) (any, error) {
	f.instantiateCalled = true
	return map[string]any{"workflow_id": "wf_inst"}, nil
}

func (f *fakeWorkflowHandler) WorkflowStatus(id string) (any, error) {
	f.statusID = id
	return f.statusResult, f.statusErr
}

func (f *fakeWorkflowHandler) WorkflowWait(id string, timeout time.Duration) (any, error) {
	f.waitID = id
	f.waitTimeout = timeout
	return map[string]any{"status": "done"}, nil
}

func (f *fakeWorkflowHandler) WorkflowInspect(id string) (any, error) {
	f.inspectID = id
	return map[string]any{"workflow_id": id}, nil
}

func (f *fakeWorkflowHandler) WorkflowEvents(id string, afterSeq int64, limit int) (any, error) {
	f.eventsID = id
	f.eventsAfterSeq = afterSeq
	f.eventsLimit = limit
	return []map[string]any{{"seq": afterSeq + 1}}, nil
}

func (f *fakeWorkflowHandler) WorkflowCommand(id string, payload json.RawMessage) (any, error) {
	f.commandID = id
	f.commandPayload = payload
	return map[string]any{"accepted": true}, nil
}

func newWorkflowTestServer(t *testing.T) (*ControlServer, *fakeWorkflowHandler) {
	t.Helper()
	s := NewServer(NewState("run_1", "demo", 0))
	fake := &fakeWorkflowHandler{}
	s.SetWorkflowHandler(fake)
	return s, fake
}

func TestWorkflowStatusDispatch(t *testing.T) {
	s, fake := newWorkflowTestServer(t)
	fake.statusResult = map[string]any{"workflow_id": "wf_1", "status": "running"}

	resp := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 1, Method: "workflow.status", Params: json.RawMessage(`{"workflow_id":"wf_1"}`)})
	if resp.Error != nil {
		t.Fatalf("workflow.status returned error: %+v", resp.Error)
	}
	if fake.statusID != "wf_1" {
		t.Fatalf("handler called with workflow_id %q, want %q", fake.statusID, "wf_1")
	}
	result, ok := resp.Result.(map[string]any)
	if !ok || result["status"] != "running" {
		t.Fatalf("unexpected result: %#v", resp.Result)
	}
}

func TestWorkflowStatusMissingWorkflowID(t *testing.T) {
	s, fake := newWorkflowTestServer(t)

	resp := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 1, Method: "workflow.status", Params: json.RawMessage(`{}`)})
	if resp.Error == nil || resp.Error.Code != -32602 || resp.Error.Message != "invalid params" {
		t.Fatalf("expected -32602 invalid params, got: %+v", resp.Error)
	}
	if fake.statusID != "" {
		t.Fatalf("handler should not be called for malformed params, got %q", fake.statusID)
	}

	resp = s.dispatch(nil, Request{JSONRPC: "2.0", ID: 2, Method: "workflow.status", Params: json.RawMessage(`not-json`)})
	if resp.Error == nil || resp.Error.Code != -32602 {
		t.Fatalf("expected -32602 for malformed JSON, got: %+v", resp.Error)
	}
}

func TestWorkflowCreateAndInstantiateDispatch(t *testing.T) {
	s, fake := newWorkflowTestServer(t)

	resp := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 1, Method: "workflow.create", Params: json.RawMessage(`{"name":"review"}`)})
	if resp.Error != nil {
		t.Fatalf("workflow.create returned error: %+v", resp.Error)
	}
	if !fake.createCalled {
		t.Fatal("workflow.create did not reach the handler")
	}
	if result, ok := resp.Result.(map[string]any); !ok || result["workflow_id"] != "wf_new" {
		t.Fatalf("unexpected create result: %#v", resp.Result)
	}

	resp = s.dispatch(nil, Request{JSONRPC: "2.0", ID: 2, Method: "workflow.instantiate", Params: json.RawMessage(`{"template":"review"}`)})
	if resp.Error != nil {
		t.Fatalf("workflow.instantiate returned error: %+v", resp.Error)
	}
	if !fake.instantiateCalled {
		t.Fatal("workflow.instantiate did not reach the handler")
	}
	if result, ok := resp.Result.(map[string]any); !ok || result["workflow_id"] != "wf_inst" {
		t.Fatalf("unexpected instantiate result: %#v", resp.Result)
	}
}

func TestWorkflowEventsDispatchForwardsAfterSeqAndLimit(t *testing.T) {
	s, fake := newWorkflowTestServer(t)

	resp := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 1, Method: "workflow.events", Params: json.RawMessage(`{"workflow_id":"wf_1","after_seq":7,"limit":3}`)})
	if resp.Error != nil {
		t.Fatalf("workflow.events returned error: %+v", resp.Error)
	}
	if fake.eventsID != "wf_1" || fake.eventsAfterSeq != 7 || fake.eventsLimit != 3 {
		t.Fatalf("forwarded (id=%q afterSeq=%d limit=%d), want (wf_1 7 3)", fake.eventsID, fake.eventsAfterSeq, fake.eventsLimit)
	}
	if events, ok := resp.Result.([]map[string]any); !ok || len(events) != 1 {
		t.Fatalf("unexpected events result: %#v", resp.Result)
	}
}

func TestWorkflowWaitAndInspectDispatch(t *testing.T) {
	s, fake := newWorkflowTestServer(t)

	resp := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 1, Method: "workflow.wait", Params: json.RawMessage(`{"workflow_id":"wf_1","timeout_ms":123}`)})
	if resp.Error != nil {
		t.Fatalf("workflow.wait returned error: %+v", resp.Error)
	}
	if fake.waitID != "wf_1" || fake.waitTimeout != 123*time.Millisecond {
		t.Fatalf("wait forwarded (id=%q timeout=%s), want (wf_1 123ms)", fake.waitID, fake.waitTimeout)
	}

	// Absent or non-positive timeout_ms defaults to 5s.
	resp = s.dispatch(nil, Request{JSONRPC: "2.0", ID: 2, Method: "workflow.wait", Params: json.RawMessage(`{"workflow_id":"wf_1"}`)})
	if resp.Error != nil {
		t.Fatalf("workflow.wait default timeout returned error: %+v", resp.Error)
	}
	if fake.waitTimeout != 5*time.Second {
		t.Fatalf("default wait timeout = %s, want 5s", fake.waitTimeout)
	}

	resp = s.dispatch(nil, Request{JSONRPC: "2.0", ID: 3, Method: "workflow.inspect", Params: json.RawMessage(`{"workflow_id":"wf_9"}`)})
	if resp.Error != nil {
		t.Fatalf("workflow.inspect returned error: %+v", resp.Error)
	}
	if fake.inspectID != "wf_9" {
		t.Fatalf("inspect called with %q, want wf_9", fake.inspectID)
	}
}

func TestWorkflowCommandDispatch(t *testing.T) {
	s, fake := newWorkflowTestServer(t)

	resp := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 1, Method: "workflow.command", Params: json.RawMessage(`{"workflow_id":"wf_1","command":{"op":"pause"}}`)})
	if resp.Error != nil {
		t.Fatalf("workflow.command returned error: %+v", resp.Error)
	}
	if fake.commandID != "wf_1" {
		t.Fatalf("command called with workflow_id %q, want wf_1", fake.commandID)
	}
	if string(fake.commandPayload) != `{"op":"pause"}` {
		t.Fatalf("command payload = %s, want nested command object", fake.commandPayload)
	}
}

func TestWorkflowHandlerErrorSurfacesAsInternalError(t *testing.T) {
	s, fake := newWorkflowTestServer(t)
	fake.statusErr = errors.New("no such workflow")

	resp := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 1, Method: "workflow.status", Params: json.RawMessage(`{"workflow_id":"missing"}`)})
	if resp.Error == nil || resp.Error.Code != -32000 || resp.Error.Message != "no such workflow" {
		t.Fatalf("expected -32000 with handler error, got: %+v", resp.Error)
	}
}

func TestWorkflowDispatchWithoutHandlerReturnsMethodNotFound(t *testing.T) {
	s := NewServer(NewState("run_1", "demo", 0))

	resp := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 1, Method: "workflow.status", Params: json.RawMessage(`{"workflow_id":"wf_1"}`)})
	if resp.Error == nil || resp.Error.Code != -32601 || resp.Error.Message != "method not found" {
		t.Fatalf("expected -32601 method not found without handler, got: %+v", resp.Error)
	}
}

func TestWorkflowBranchDoesNotInterceptExistingMethods(t *testing.T) {
	s, fake := newWorkflowTestServer(t)
	stable := &mockStableHandler{listResult: []any{"rt_1"}}
	s.SetStableHandler(stable)

	// "list" must still route through the StableHandler path and return its
	// result, even with a workflow handler registered.
	resp := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 1, Method: "list"})
	if resp.Error != nil {
		t.Fatalf("list returned error: %+v", resp.Error)
	}
	if list, ok := resp.Result.([]any); !ok || len(list) != 1 || list[0] != "rt_1" {
		t.Fatalf("list result = %#v, want the stable handler's list result", resp.Result)
	}

	// "status" without runtime_id still returns the control snapshot, not a
	// workflow status.
	resp = s.dispatch(nil, Request{JSONRPC: "2.0", ID: 2, Method: "status"})
	if resp.Error != nil {
		t.Fatalf("status returned error: %+v", resp.Error)
	}
	if _, ok := resp.Result.(Snapshot); !ok {
		t.Fatalf("status result type = %T, want Snapshot", resp.Result)
	}

	// No workflow handler method should have been touched by either call.
	if fake.statusID != "" || fake.createCalled || fake.instantiateCalled {
		t.Fatal("existing methods leaked into the workflow handler")
	}
}
