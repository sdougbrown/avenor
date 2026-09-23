package control

import (
	"encoding/json"
	"errors"
	"testing"
)

// fakeWorkflowControllerHandler is a recording stub implementing
// WorkflowControllerHandler for dispatch routing tests.
type fakeWorkflowControllerHandler struct {
	createParams  json.RawMessage
	enableID      string
	enableErr     error
	disableID     string
	disableParams json.RawMessage
	statusID      string
	listCalled    bool
	readyID       string
	readyLimit    int
}

var _ WorkflowControllerHandler = (*fakeWorkflowControllerHandler)(nil)

func (f *fakeWorkflowControllerHandler) WorkflowControllerCreate(params json.RawMessage) (any, error) {
	f.createParams = params
	return map[string]any{"created": true}, nil
}

func (f *fakeWorkflowControllerHandler) WorkflowControllerEnable(id string) (any, error) {
	f.enableID = id
	if f.enableErr != nil {
		return nil, f.enableErr
	}
	return map[string]any{"enabled": id}, nil
}

func (f *fakeWorkflowControllerHandler) WorkflowControllerDisable(id string, params json.RawMessage) (any, error) {
	f.disableID = id
	f.disableParams = params
	return map[string]any{"disabled": id}, nil
}

func (f *fakeWorkflowControllerHandler) WorkflowControllerStatus(id string) (any, error) {
	f.statusID = id
	return map[string]any{"status": "enabled"}, nil
}

func (f *fakeWorkflowControllerHandler) WorkflowControllerList() (any, error) {
	f.listCalled = true
	return map[string]any{"controllers": []any{}}, nil
}

func (f *fakeWorkflowControllerHandler) WorkflowReady(id string, limit int) (any, error) {
	f.readyID = id
	f.readyLimit = limit
	return map[string]any{"ready": id, "limit": limit}, nil
}

func newWorkflowControllerTestServer(t *testing.T) (*ControlServer, *fakeWorkflowControllerHandler) {
	t.Helper()
	s := NewServer(NewState("run_1", "demo", 0))
	fake := &fakeWorkflowControllerHandler{}
	s.SetWorkflowControllerHandler(fake)
	return s, fake
}

// TestWorkflowControllerDispatchWithoutHandlerReturnsMethodNotFound pins that
// every controller method reports -32601 when no controller handler is set,
// including workflow.ready which must not fall through to the workflow
// handler.
func TestWorkflowControllerDispatchWithoutHandlerReturnsMethodNotFound(t *testing.T) {
	s := NewServer(NewState("run_1", "demo", 0))
	methods := []struct {
		method string
		params json.RawMessage
	}{
		{"workflow.controller.create", json.RawMessage(`{}`)},
		{"workflow.controller.enable", json.RawMessage(`{"controller_id":"c1"}`)},
		{"workflow.controller.disable", json.RawMessage(`{"controller_id":"c1","reason":"r"}`)},
		{"workflow.controller.status", json.RawMessage(`{"controller_id":"c1"}`)},
		{"workflow.controller.list", nil},
		{"workflow.ready", json.RawMessage(`{"controller_id":"c1","limit":3}`)},
	}
	for _, m := range methods {
		resp := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 1, Method: m.method, Params: m.params})
		if resp.Error == nil || resp.Error.Code != -32601 || resp.Error.Message != "method not found" {
			t.Fatalf("%s without handler: expected -32601 method not found, got: %+v", m.method, resp.Error)
		}
	}
}

func TestWorkflowControllerCreateDispatch(t *testing.T) {
	s, fake := newWorkflowControllerTestServer(t)

	resp := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 1, Method: "workflow.controller.create", Params: json.RawMessage(`{"name":"c1"}`)})
	if resp.Error != nil {
		t.Fatalf("create returned error: %+v", resp.Error)
	}
	if string(fake.createParams) != `{"name":"c1"}` {
		t.Fatalf("create raw params = %s, want the raw body", fake.createParams)
	}
	if result, ok := resp.Result.(map[string]any); !ok || result["created"] != true {
		t.Fatalf("unexpected create result: %#v", resp.Result)
	}
}

func TestWorkflowControllerEnableDispatch(t *testing.T) {
	s, fake := newWorkflowControllerTestServer(t)

	resp := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 1, Method: "workflow.controller.enable", Params: json.RawMessage(`{"controller_id":"c1"}`)})
	if resp.Error != nil {
		t.Fatalf("enable returned error: %+v", resp.Error)
	}
	if fake.enableID != "c1" {
		t.Fatalf("enable called with %q, want c1", fake.enableID)
	}
	if result, ok := resp.Result.(map[string]any); !ok || result["enabled"] != "c1" {
		t.Fatalf("unexpected enable result: %#v", resp.Result)
	}
}

func TestWorkflowControllerDisableDispatch(t *testing.T) {
	s, fake := newWorkflowControllerTestServer(t)

	resp := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 1, Method: "workflow.controller.disable", Params: json.RawMessage(`{"controller_id":"c1","reason":"r"}`)})
	if resp.Error != nil {
		t.Fatalf("disable returned error: %+v", resp.Error)
	}
	if fake.disableID != "c1" {
		t.Fatalf("disable called with %q, want c1", fake.disableID)
	}
	if string(fake.disableParams) != `{"controller_id":"c1","reason":"r"}` {
		t.Fatalf("disable raw params = %s, want full raw params passed through", fake.disableParams)
	}
	if result, ok := resp.Result.(map[string]any); !ok || result["disabled"] != "c1" {
		t.Fatalf("unexpected disable result: %#v", resp.Result)
	}
}

func TestWorkflowControllerStatusDispatch(t *testing.T) {
	s, fake := newWorkflowControllerTestServer(t)

	resp := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 1, Method: "workflow.controller.status", Params: json.RawMessage(`{"controller_id":"c1"}`)})
	if resp.Error != nil {
		t.Fatalf("status returned error: %+v", resp.Error)
	}
	if fake.statusID != "c1" {
		t.Fatalf("status called with %q, want c1", fake.statusID)
	}
	if result, ok := resp.Result.(map[string]any); !ok || result["status"] != "enabled" {
		t.Fatalf("unexpected status result: %#v", resp.Result)
	}
}

func TestWorkflowControllerListDispatch(t *testing.T) {
	s, fake := newWorkflowControllerTestServer(t)

	resp := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 1, Method: "workflow.controller.list"})
	if resp.Error != nil {
		t.Fatalf("list returned error: %+v", resp.Error)
	}
	if !fake.listCalled {
		t.Fatal("list did not reach the handler")
	}
	if _, ok := resp.Result.(map[string]any); !ok {
		t.Fatalf("unexpected list result: %#v", resp.Result)
	}
}

func TestWorkflowReadyDispatch(t *testing.T) {
	s, fake := newWorkflowControllerTestServer(t)

	resp := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 1, Method: "workflow.ready", Params: json.RawMessage(`{"controller_id":"c1","limit":3}`)})
	if resp.Error != nil {
		t.Fatalf("ready returned error: %+v", resp.Error)
	}
	if fake.readyID != "c1" || fake.readyLimit != 3 {
		t.Fatalf("ready forwarded (id=%q limit=%d), want (c1 3)", fake.readyID, fake.readyLimit)
	}
	if result, ok := resp.Result.(map[string]any); !ok || result["ready"] != "c1" {
		t.Fatalf("unexpected ready result: %#v", resp.Result)
	}
}

// TestWorkflowControllerMissingControllerID pins the -32602 invalid-params
// path for the methods that require a controller_id.
func TestWorkflowControllerMissingControllerID(t *testing.T) {
	s, fake := newWorkflowControllerTestServer(t)
	methods := []struct {
		method string
		params json.RawMessage
	}{
		{"workflow.controller.enable", json.RawMessage(`{}`)},
		{"workflow.controller.disable", json.RawMessage(`{}`)},
		{"workflow.controller.status", json.RawMessage(`{}`)},
		{"workflow.ready", json.RawMessage(`{}`)},
	}
	for _, m := range methods {
		resp := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 1, Method: m.method, Params: m.params})
		if resp.Error == nil || resp.Error.Code != -32602 || resp.Error.Message != "invalid params" {
			t.Fatalf("%s missing controller_id: expected -32602 invalid params, got: %+v", m.method, resp.Error)
		}
	}
	if fake.enableID != "" || fake.disableID != "" || fake.statusID != "" || fake.readyID != "" {
		t.Fatal("handler should not be called for malformed params")
	}
}

// TestWorkflowControllerHandlerErrorSurfacesAsInternalError pins that a
// handler error maps to -32000.
func TestWorkflowControllerHandlerErrorSurfacesAsInternalError(t *testing.T) {
	s, fake := newWorkflowControllerTestServer(t)
	fake.enableErr = errors.New("no such controller")

	resp := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 1, Method: "workflow.controller.enable", Params: json.RawMessage(`{"controller_id":"missing"}`)})
	if resp.Error == nil || resp.Error.Code != -32000 || resp.Error.Message != "no such controller" {
		t.Fatalf("expected -32000 with handler error, got: %+v", resp.Error)
	}
}
