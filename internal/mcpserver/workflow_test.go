package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/control"
)

// newWorkflowTestServer builds a server wired to a fake control client and no
// autostart, the same harness the sibling control-tool tests use.
func newWorkflowTestServer(t *testing.T) (*Server, *fakeClient) {
	t.Helper()
	fake := &fakeClient{}
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, fake
}

// callWorkflow drives the merged tool with one raw argument object.
func callWorkflow(t *testing.T, s *Server, args string) *mcpsdk.CallToolResult {
	t.Helper()
	result, err := s.handleAvenorWorkflow(context.Background(), &mcpsdk.CallToolRequest{
		Params: &mcpsdk.CallToolParamsRaw{Arguments: json.RawMessage(args)},
	})
	if err != nil {
		t.Fatalf("handleAvenorWorkflow: %v", err)
	}
	return result
}

// workflowError asserts the call answered a tool error and returns its code and
// rendered text.
func workflowError(t *testing.T, result *mcpsdk.CallToolResult) (code, text string) {
	t.Helper()
	if result == nil || !result.IsError {
		t.Fatalf("expected a tool error result, got %#v", result)
	}
	object, _ := result.StructuredContent.(map[string]any)
	code, _ = object["code"].(string)
	text, _ = object["text"].(string)
	return code, text
}

func TestWorkflowRequiredArgs(t *testing.T) {
	cases := []struct {
		name string
		args string
		want string
	}{
		{"status missing workflow_id", `{"action":"status"}`, `missing required property "workflow_id"`},
		{"wait missing workflow_id", `{"action":"wait"}`, `missing required property "workflow_id"`},
		{"inspect missing workflow_id", `{"action":"inspect"}`, `missing required property "workflow_id"`},
		{"events missing workflow_id", `{"action":"events"}`, `missing required property "workflow_id"`},
		{"ready missing controller_id", `{"action":"ready"}`, `missing required property "controller_id"`},
		{"complete missing node_id", `{"action":"complete","workflow_id":"wf"}`, `missing required property "node_id"`},
		{"complete missing owner_token", `{"action":"complete","workflow_id":"wf","node_id":"n","activation_id":"a","attempt_id":"at","lease_id":"l","outcome":"ok"}`, `missing required property "owner_token"`},
		{"gate missing node_id", `{"action":"gate","workflow_id":"wf","gate_id":"g","activation_id":"a","operation":"satisfy"}`, `missing required property "node_id"`},
		{"cancel missing reason", `{"action":"cancel","workflow_id":"wf","actor":"bob"}`, `missing required property "reason"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, fake := newWorkflowTestServer(t)
			code, text := workflowError(t, callWorkflow(t, s, tc.args))
			if code != "invalid_request" {
				t.Fatalf("code = %q, want invalid_request", code)
			}
			if !strings.Contains(text, tc.want) {
				t.Fatalf("error %q does not mention %q", text, tc.want)
			}
			if len(fake.workflowStatusCalls)+len(fake.workflowWaitCalls)+len(fake.workflowEventsCalls)+len(fake.workflowReadyCalls)+len(fake.workflowCompleteCalls)+len(fake.workflowGateCalls)+len(fake.workflowCancelCalls) != 0 {
				t.Fatal("a refused call reached the control client")
			}
		})
	}
}

func TestWorkflowUnknownAction(t *testing.T) {
	s, _ := newWorkflowTestServer(t)
	code, text := workflowError(t, callWorkflow(t, s, `{"action":"explode"}`))
	if code != "invalid_action" {
		t.Fatalf("code = %q, want invalid_action", code)
	}
	if !strings.Contains(text, "not a known action") {
		t.Fatalf("error %q does not name the action vocabulary", text)
	}
}

func TestWorkflowGateUnknownOperation(t *testing.T) {
	s, fake := newWorkflowTestServer(t)
	code, text := workflowError(t, callWorkflow(t, s, `{"action":"gate","workflow_id":"wf","node_id":"n","gate_id":"g","activation_id":"a","operation":"bogus"}`))
	if code != "invalid_request" {
		t.Fatalf("code = %q, want invalid_request", code)
	}
	if !strings.Contains(text, `property "operation" must be one of`) {
		t.Fatalf("error %q does not name the operation enum", text)
	}
	if len(fake.workflowGateCalls) != 0 {
		t.Fatal("unknown gate operation must not reach the control client")
	}
}

func TestWorkflowWaitInvalidTimeout(t *testing.T) {
	s, fake := newWorkflowTestServer(t)
	code, text := workflowError(t, callWorkflow(t, s, `{"action":"wait","workflow_id":"wf","timeout":"not-a-timeout"}`))
	if code != "invalid_request" {
		t.Fatalf("code = %q, want invalid_request", code)
	}
	if !strings.Contains(text, "invalid timeout") {
		t.Fatalf("error %q does not name the timeout", text)
	}
	if len(fake.workflowWaitCalls) != 0 {
		t.Fatal("invalid timeout must not reach the control client")
	}
}

func TestWorkflowGateInvalidObservedAt(t *testing.T) {
	s, fake := newWorkflowTestServer(t)
	code, text := workflowError(t, callWorkflow(t, s, `{"action":"gate","workflow_id":"wf","node_id":"n","gate_id":"g","activation_id":"a","operation":"external_result","observed_at":"not-a-timestamp"}`))
	if code != "invalid_request" {
		t.Fatalf("code = %q, want invalid_request", code)
	}
	if !strings.Contains(text, "RFC3339") {
		t.Fatalf("error %q does not name the timestamp rule", text)
	}
	if len(fake.workflowGateCalls) != 0 {
		t.Fatal("invalid observed_at must not reach the control client")
	}
}

// TestWorkflowIdentifierRoundTrip verifies that workflow IDs pass through the
// MCP tool surface to the typed control client unchanged — no identifier is
// dropped, remapped, or rewritten between argument plumbing and the handler.
func TestWorkflowIdentifierRoundTrip(t *testing.T) {
	s, fake := newWorkflowTestServer(t)
	fake.workflowStatusResult = map[string]any{"workflow_id": "wf-abc", "status": "running"}
	result := callWorkflow(t, s, `{"action":"status","workflow_id":"wf-abc"}`)
	if len(fake.workflowStatusCalls) != 1 || fake.workflowStatusCalls[0] != "wf-abc" {
		t.Fatalf("workflow_id not round-tripped: %v", fake.workflowStatusCalls)
	}
	res, _ := result.StructuredContent.(map[string]any)
	if res["workflow_id"] != "wf-abc" {
		t.Fatalf("workflow_id munged in result: %v", res["workflow_id"])
	}
}

func TestWorkflowEventsForwarding(t *testing.T) {
	s, fake := newWorkflowTestServer(t)
	fake.workflowEventsResult = map[string]any{"events": []any{}}
	callWorkflow(t, s, `{"action":"events","workflow_id":"wf-1","after_seq":7,"limit":25}`)
	if len(fake.workflowEventsCalls) != 1 {
		t.Fatalf("workflow events not called")
	}
	got := fake.workflowEventsCalls[0]
	if got.workflowID != "wf-1" || got.afterSeq != 7 || got.limit != 25 {
		t.Fatalf("workflow events args not forwarded: %+v", got)
	}
}

func TestWorkflowWaitDuration(t *testing.T) {
	s, fake := newWorkflowTestServer(t)
	callWorkflow(t, s, `{"action":"wait","workflow_id":"wf-1","timeout":"5m"}`)
	if len(fake.workflowWaitCalls) != 1 || fake.workflowWaitCalls[0].timeout != 5*time.Minute {
		t.Fatalf("workflow wait timeout not parsed: %+v", fake.workflowWaitCalls)
	}
	if fake.workflowWaitCalls[0].workflowID != "wf-1" {
		t.Fatalf("workflow_id not forwarded to wait: %v", fake.workflowWaitCalls[0].workflowID)
	}
}

func TestWorkflowReadyForwarding(t *testing.T) {
	s, fake := newWorkflowTestServer(t)
	fake.workflowReadyResult = map[string]any{"candidates": []any{}}
	callWorkflow(t, s, `{"action":"ready","controller_id":"ctl-1","limit":3}`)
	if len(fake.workflowReadyCalls) != 1 {
		t.Fatalf("workflow ready not called")
	}
	got := fake.workflowReadyCalls[0]
	if got.controllerID != "ctl-1" || got.limit != 3 {
		t.Fatalf("workflow ready args not forwarded: %+v", got)
	}
}

// TestWorkflowErrorPropagation verifies that a rejected workflow operation
// surfaces as the shared refused tool error rather than a raw transport error.
func TestWorkflowErrorPropagation(t *testing.T) {
	cases := []struct {
		name  string
		setup func(*fakeClient)
		args  string
	}{
		{"status rejects bad workflow_id", func(f *fakeClient) {
			f.workflowStatusErr = errors.New("workflow not found: wf-missing")
		}, `{"action":"status","workflow_id":"wf-missing"}`},
		{"inspect rejects bad workflow_id", func(f *fakeClient) {
			f.workflowInspectErr = errors.New("workflow not found: wf-missing")
		}, `{"action":"inspect","workflow_id":"wf-missing"}`},
		{"wait rejects missing workflow", func(f *fakeClient) {
			f.workflowWaitErr = errors.New("workflow not found: wf-missing")
		}, `{"action":"wait","workflow_id":"wf-missing"}`},
		{"events rejects missing workflow", func(f *fakeClient) {
			f.workflowEventsErr = errors.New("workflow not found: wf-missing")
		}, `{"action":"events","workflow_id":"wf-missing"}`},
		{"complete rejects undeclared outcome", func(f *fakeClient) {
			f.workflowCompleteErr = errors.New("outcome not declared: bogus")
		}, `{"action":"complete","workflow_id":"wf","node_id":"n","activation_id":"a","attempt_id":"at","lease_id":"l","owner_token":"tok","outcome":"bogus"}`},
		{"gate rejects bad node_id", func(f *fakeClient) {
			f.workflowGateErr = errors.New("activation not found for node")
		}, `{"action":"gate","workflow_id":"wf","node_id":"nope","gate_id":"g","activation_id":"a","operation":"satisfy"}`},
		{"cancel rejects rejected cancel", func(f *fakeClient) {
			f.workflowCancelErr = errors.New("cannot cancel workflow wf: instance is already completed")
		}, `{"action":"cancel","workflow_id":"wf","reason":"test","actor":"test"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, fake := newWorkflowTestServer(t)
			tc.setup(fake)
			code, text := workflowError(t, callWorkflow(t, s, tc.args))
			if code != "refused" {
				t.Fatalf("code = %q, want refused", code)
			}
			if text == "" {
				t.Fatal("refused error carries no text")
			}
		})
	}
}

// TestWorkflowControllerIdentifierRoundTrip verifies that controller IDs pass
// through the MCP tool surface to the typed control client unchanged.
func TestWorkflowControllerIdentifierRoundTrip(t *testing.T) {
	s, fake := newWorkflowTestServer(t)
	fake.workflowControllerStatusResult = map[string]any{"controller_id": "c-abc", "state": "enabled"}
	result := callWorkflow(t, s, `{"action":"controller_status","controller_id":"c-abc"}`)
	if len(fake.workflowControllerStatusCalls) != 1 || fake.workflowControllerStatusCalls[0] != "c-abc" {
		t.Fatalf("controller_id not round-tripped: %v", fake.workflowControllerStatusCalls)
	}
	res, _ := result.StructuredContent.(map[string]any)
	if res["controller_id"] != "c-abc" {
		t.Fatalf("controller_id munged in result: %v", res["controller_id"])
	}
}

// TestWorkflowControllerListForwarding verifies that omitting controller_id
// routes the action to the control client's list method and returns its
// snapshot unchanged.
func TestWorkflowControllerListForwarding(t *testing.T) {
	s, fake := newWorkflowTestServer(t)
	fake.workflowControllerListResult = map[string]any{"controllers": []any{}}
	result := callWorkflow(t, s, `{"action":"controller_status"}`)
	if fake.workflowControllerListCalls != 1 {
		t.Fatalf("expected 1 list call, got %d", fake.workflowControllerListCalls)
	}
	if len(fake.workflowControllerStatusCalls) != 0 {
		t.Fatalf("expected 0 status calls when controller_id is omitted, got %d", len(fake.workflowControllerStatusCalls))
	}
	res, _ := result.StructuredContent.(map[string]any)
	if _, ok := res["controllers"]; !ok {
		t.Fatalf("list result missing controllers: %v", res)
	}
}

// TestWorkflowControllerErrorPropagation verifies controller errors propagate
// from the control client without being swallowed.
func TestWorkflowControllerErrorPropagation(t *testing.T) {
	cases := []struct {
		name  string
		setup func(*fakeClient)
		args  string
	}{
		{"controller status error", func(f *fakeClient) {
			f.workflowControllerStatusErr = errors.New("controller not found: c-missing")
		}, `{"action":"controller_status","controller_id":"c-missing"}`},
		{"controller list error", func(f *fakeClient) {
			f.workflowControllerListErr = errors.New("list failed")
		}, `{"action":"controller_status"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, fake := newWorkflowTestServer(t)
			tc.setup(fake)
			code, text := workflowError(t, callWorkflow(t, s, tc.args))
			if code != "refused" {
				t.Fatalf("code = %q, want refused", code)
			}
			if text == "" {
				t.Fatal("refused error carries no text")
			}
		})
	}
}

func TestWorkflowCancelHandler(t *testing.T) {
	s, fake := newWorkflowTestServer(t)
	fake.workflowCancelResult = map[string]any{"status": "canceled", "revision": 7, "canceled_runtimes": 0}
	result := callWorkflow(t, s, `{"action":"cancel","workflow_id":"wf_1","reason":"pr head moved"}`)
	if len(fake.workflowCancelCalls) != 1 {
		t.Fatalf("expected one cancel call, got %d", len(fake.workflowCancelCalls))
	}
	call := fake.workflowCancelCalls[0]
	if call.workflowID != "wf_1" || call.reason != "pr head moved" {
		t.Fatalf("unexpected cancel call: %#v", call)
	}
	if call.actor != "mcp" {
		t.Fatalf("default actor got %q want mcp", call.actor)
	}
	res, _ := result.StructuredContent.(map[string]any)
	if res["status"] != "canceled" {
		t.Fatalf("result status got %v want canceled", res["status"])
	}
}

func TestWorkflowCancelHandlerEmptyActor(t *testing.T) {
	s, fake := newWorkflowTestServer(t)
	callWorkflow(t, s, `{"action":"cancel","workflow_id":"wf_1","reason":"why","actor":""}`)
	if got := fake.workflowCancelCalls[0].actor; got != "mcp" {
		t.Fatalf("empty actor got %q want mcp", got)
	}
}

func TestWorkflowCancelHandlerNonStringActor(t *testing.T) {
	s, fake := newWorkflowTestServer(t)
	code, text := workflowError(t, callWorkflow(t, s, `{"action":"cancel","workflow_id":"wf_1","reason":"why","actor":7}`))
	if code != "invalid_request" {
		t.Fatalf("code got %q want invalid_request (text %q)", code, text)
	}
	if len(fake.workflowCancelCalls) != 0 {
		t.Fatalf("a refused cancel reached the client: %#v", fake.workflowCancelCalls)
	}
}

func TestWorkflowCancelHandlerEmptyReason(t *testing.T) {
	s, fake := newWorkflowTestServer(t)
	code, text := workflowError(t, callWorkflow(t, s, `{"action":"cancel","workflow_id":"wf_1","reason":"","actor":"op"}`))
	if code != "invalid_request" {
		t.Fatalf("code got %q want invalid_request (text %q)", code, text)
	}
	if len(fake.workflowCancelCalls) != 0 {
		t.Fatalf("a refused cancel reached the client: %#v", fake.workflowCancelCalls)
	}
}

func TestWorkflowCancelHandlerExplicitActor(t *testing.T) {
	s, fake := newWorkflowTestServer(t)
	callWorkflow(t, s, `{"action":"cancel","workflow_id":"wf_1","reason":"why","actor":"review-pr-auto"}`)
	if got := fake.workflowCancelCalls[0].actor; got != "review-pr-auto" {
		t.Fatalf("actor got %q want review-pr-auto", got)
	}
}

// fakeControlWorkflowHandler answers the workflow methods a merged-tool call
// dispatches, over a real in-process control server.
type fakeControlWorkflowHandler struct{ mu sync.Mutex }

func (f *fakeControlWorkflowHandler) WorkflowCreate(json.RawMessage) (any, error) {
	return map[string]any{}, nil
}
func (f *fakeControlWorkflowHandler) WorkflowInstantiate(json.RawMessage) (any, error) {
	return map[string]any{}, nil
}
func (f *fakeControlWorkflowHandler) WorkflowStatus(string) (any, error) {
	return map[string]any{"status": "active"}, nil
}
func (f *fakeControlWorkflowHandler) WorkflowWait(string, time.Duration) (any, error) {
	return map[string]any{"terminal": true}, nil
}
func (f *fakeControlWorkflowHandler) WorkflowInspect(string) (any, error) {
	return map[string]any{"attempts": map[string]any{}}, nil
}
func (f *fakeControlWorkflowHandler) WorkflowEvents(string, int64, int) (any, error) {
	return map[string]any{"events": []any{}}, nil
}
func (f *fakeControlWorkflowHandler) WorkflowCancel(string, json.RawMessage) (any, error) {
	return map[string]any{"status": "canceled"}, nil
}
func (f *fakeControlWorkflowHandler) WorkflowCommand(string, json.RawMessage) (any, error) {
	return map[string]any{"accepted": true}, nil
}

// TestWorkflowActionAccessChoosesTheDialSeam proves the access level the shared
// contract carries decides the credential mode: a read action dials the
// credential-free seam, a mutating action attaches as the socket owner.
func TestWorkflowActionAccessChoosesTheDialSeam(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "wf-access.sock")
	owner, err := control.GenerateOwnerCapability()
	if err != nil {
		t.Fatal(err)
	}
	controlServer := control.NewServer(control.NewState("run_1", "demo", 0), owner)
	controlServer.SetWorkflowHandler(&fakeControlWorkflowHandler{})
	if err := controlServer.Start(sock); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	t.Cleanup(func() { _ = controlServer.Stop() })

	var readDials, ownerDials atomic.Int32
	origDial, origAttach := dialSupervisorClient, attachSupervisorClient
	dialSupervisorClient = func(p string) (*client.Client, error) {
		readDials.Add(1)
		return origDial(p)
	}
	attachSupervisorClient = func(p string) (*client.Client, error) {
		ownerDials.Add(1)
		return origAttach(p)
	}
	t.Cleanup(func() { dialSupervisorClient, attachSupervisorClient = origDial, origAttach })

	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, SupervisorSocket: sock})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })

	result := callWorkflow(t, s, `{"action":"status","workflow_id":"wf-1"}`)
	if res, ok := result.StructuredContent.(map[string]any); !ok || res["status"] != "active" {
		t.Fatalf("status result = %#v, want the fake handler's snapshot", result.StructuredContent)
	}
	if readDials.Load() != 1 || ownerDials.Load() != 0 {
		t.Fatalf("read action: read dials = %d, owner dials = %d, want 1/0", readDials.Load(), ownerDials.Load())
	}

	result = callWorkflow(t, s, `{"action":"cancel","workflow_id":"wf-1","reason":"why","actor":"verify"}`)
	if res, ok := result.StructuredContent.(map[string]any); !ok || res["status"] != "canceled" {
		t.Fatalf("cancel result = %#v, want the fake handler's snapshot", result.StructuredContent)
	}
	if ownerDials.Load() != 1 {
		t.Fatalf("mutating action: owner dials = %d, want 1", ownerDials.Load())
	}
}
