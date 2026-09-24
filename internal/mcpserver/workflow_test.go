package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
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

func TestWorkflowRequiredArgs(t *testing.T) {
	cases := []struct {
		name string
		want string
		call func(s *Server) error
	}{
		{"status missing workflow_id", "workflow_id is required", func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowStatus(context.Background(), nil, workflowStatusArgs{})
			return err
		}},
		{"wait missing workflow_id", "workflow_id is required", func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowWait(context.Background(), nil, workflowWaitArgs{})
			return err
		}},
		{"inspect missing workflow_id", "workflow_id is required", func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowInspect(context.Background(), nil, workflowInspectArgs{})
			return err
		}},
		{"events missing workflow_id", "workflow_id is required", func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowEvents(context.Background(), nil, workflowEventsArgs{})
			return err
		}},
		{"complete missing all", "node_id is required", func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowComplete(context.Background(), nil, workflowCompleteArgs{WorkflowID: "wf"})
			return err
		}},
		{"complete missing owner_token", "owner_token is required", func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowComplete(context.Background(), nil, workflowCompleteArgs{
				WorkflowID: "wf", NodeID: "n", ActivationID: "a", AttemptID: "at", LeaseID: "l",
			})
			return err
		}},
		{"complete missing lease_id", "lease_id is required", func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowComplete(context.Background(), nil, workflowCompleteArgs{
				WorkflowID: "wf", NodeID: "n", ActivationID: "a", AttemptID: "at", OwnerToken: "tok", Outcome: "success",
			})
			return err
		}},
		{"gate missing all", "node_id is required", func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowGate(context.Background(), nil, workflowGateArgs{WorkflowID: "wf"})
			return err
		}},
		{"gate missing node_id", "node_id is required", func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowGate(context.Background(), nil, workflowGateArgs{WorkflowID: "wf", GateID: "g", ActivationID: "a", Operation: "satisfy"})
			return err
		}},
		{"controller status missing controller_id", "controller_id is required", func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowControllerStatus(context.Background(), nil, workflowControllerStatusArgs{})
			return err
		}},
		{"controller create missing controller_id", "controller_id is required", func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowControllerCreate(context.Background(), nil, workflowControllerCreateArgs{})
			return err
		}},
		{"controller create missing max_inflight", "max_inflight must be a positive integer", func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowControllerCreate(context.Background(), nil, workflowControllerCreateArgs{ControllerID: "c"})
			return err
		}},
		{"controller create negative max_inflight", "max_inflight must be a positive integer", func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowControllerCreate(context.Background(), nil, workflowControllerCreateArgs{ControllerID: "c", MaxInflight: -1})
			return err
		}},
		{"controller enable missing controller_id", "controller_id is required", func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowControllerEnable(context.Background(), nil, workflowControllerEnableArgs{})
			return err
		}},
		{"controller disable missing controller_id", "controller_id is required", func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowControllerDisable(context.Background(), nil, workflowControllerDisableArgs{})
			return err
		}},
		{"controller disable missing reason", "reason is required", func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowControllerDisable(context.Background(), nil, workflowControllerDisableArgs{ControllerID: "c"})
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newWorkflowTestServer(t)
			err := tc.call(s)
			if err == nil {
				t.Fatalf("expected required-arg error, got nil")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err.Error(), tc.want)
			}
		})
	}
}

func TestWorkflowGateUnknownOperation(t *testing.T) {
	s, fake := newWorkflowTestServer(t)
	ctx := context.Background()
	_, _, err := s.handleAvenorWorkflowGate(ctx, nil, workflowGateArgs{
		WorkflowID: "wf", NodeID: "n", GateID: "g", ActivationID: "a", Operation: "bogus",
	})
	if err == nil {
		t.Fatal("expected unknown gate operation error")
	}
	if len(fake.workflowGateCalls) != 0 {
		t.Fatal("unknown gate operation must not reach the control client")
	}
}

func TestWorkflowWaitInvalidTimeout(t *testing.T) {
	s, _ := newWorkflowTestServer(t)
	ctx := context.Background()
	_, _, err := s.handleAvenorWorkflowWait(ctx, nil, workflowWaitArgs{WorkflowID: "wf", Timeout: "not-a-timeout"})
	if err == nil {
		t.Fatal("expected invalid timeout error")
	}
}

func TestWorkflowCompleteInvalidJSON(t *testing.T) {
	s, fake := newWorkflowTestServer(t)
	ctx := context.Background()
	_, _, err := s.handleAvenorWorkflowComplete(ctx, nil, workflowCompleteArgs{
		WorkflowID: "wf", NodeID: "n", ActivationID: "a", AttemptID: "at", LeaseID: "l",
		OwnerToken: "tok", Outcome: "success", Outputs: json.RawMessage(`not json`),
	})
	if err == nil {
		t.Fatal("expected invalid outputs JSON error")
	}
	if len(fake.workflowCompleteCalls) != 0 {
		t.Fatal("invalid outputs must not reach the control client")
	}
}

func TestWorkflowGateInvalidObservedAt(t *testing.T) {
	s, fake := newWorkflowTestServer(t)
	ctx := context.Background()
	_, _, err := s.handleAvenorWorkflowGate(ctx, nil, workflowGateArgs{
		WorkflowID: "wf", NodeID: "n", GateID: "g", ActivationID: "a", Operation: "external_result",
		ObservedAt: "not-a-timestamp",
	})
	if err == nil {
		t.Fatal("expected invalid observed_at error")
	}
	if len(fake.workflowGateCalls) != 0 {
		t.Fatal("invalid observed_at must not reach the control client")
	}
}

// TestWorkflowIdentifierRoundTrip verifies that workflow IDs and supervisor IDs
// pass through the MCP tool surface to the typed control client unchanged — no
// identifier is dropped, remapped, or rewritten between registration/argument
// plumbing and the handler.
func TestWorkflowIdentifierRoundTrip(t *testing.T) {
	s, fake := newWorkflowTestServer(t)
	ctx := context.Background()

	fake.workflowStatusResult = map[string]any{"workflow_id": "wf-abc", "status": "running"}
	_, result, err := s.handleAvenorWorkflowStatus(ctx, nil, workflowStatusArgs{WorkflowID: "wf-abc"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fake.workflowStatusCalls) != 1 || fake.workflowStatusCalls[0] != "wf-abc" {
		t.Fatalf("workflow_id not round-tripped: %v", fake.workflowStatusCalls)
	}

	// The returned snapshot must carry the same workflow_id the caller supplied.
	res, _ := result.(map[string]any)
	if res["workflow_id"] != "wf-abc" {
		t.Fatalf("workflow_id munged in result: %v", res["workflow_id"])
	}
}

func TestWorkflowEventsForwarding(t *testing.T) {
	s, fake := newWorkflowTestServer(t)
	ctx := context.Background()
	fake.workflowEventsResult = map[string]any{"events": []any{}}
	if _, _, err := s.handleAvenorWorkflowEvents(ctx, nil, workflowEventsArgs{WorkflowID: "wf-1", AfterSeq: 7, Limit: 25}); err != nil {
		t.Fatal(err)
	}
	if len(fake.workflowEventsCalls) != 1 {
		t.Fatalf("workflow events not called")
	}
	got := fake.workflowEventsCalls[0]
	if got.workflowID != "wf-1" || got.afterSeq != 7 || got.limit != 25 {
		t.Fatalf("workflow_events args not forwarded: %+v", got)
	}
}

func TestWorkflowWaitDuration(t *testing.T) {
	s, fake := newWorkflowTestServer(t)
	ctx := context.Background()
	if _, _, err := s.handleAvenorWorkflowWait(ctx, nil, workflowWaitArgs{WorkflowID: "wf-1", Timeout: "5m"}); err != nil {
		t.Fatal(err)
	}
	if len(fake.workflowWaitCalls) != 1 || fake.workflowWaitCalls[0].timeout != 5*time.Minute {
		t.Fatalf("workflow wait timeout not parsed: %+v", fake.workflowWaitCalls)
	}
	if fake.workflowWaitCalls[0].workflowID != "wf-1" {
		t.Fatalf("workflow_id not forwarded to wait: %v", fake.workflowWaitCalls[0].workflowID)
	}
}

// TestWorkflowErrorPropagation verifies that a rejected/invalid workflow
// operation surfaces as the same error class as the existing control tools:
// the handler returns an error (never a partial result) when the underlying
// control client rejects the operation.
func TestWorkflowErrorPropagation(t *testing.T) {
	// Each simulation mirrors a server-side rejection (bad workflow ID,
	// undeclared outcome, wrong lease, rejected gate) returning an error from
	// the control client; the MCP handler must propagate it as a handler error.
	// Each case gets a fresh fake/server so error fields never leak across cases.
	cases := []struct {
		name  string
		setup func(*fakeClient)
		call  func(*Server) error
	}{
		{"status rejects bad workflow_id", func(f *fakeClient) {
			f.workflowStatusErr = errors.New("workflow not found: wf-missing")
		}, func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowStatus(context.Background(), nil, workflowStatusArgs{WorkflowID: "wf-missing"})
			return err
		}},
		{"inspect rejects bad workflow_id", func(f *fakeClient) {
			f.workflowInspectErr = errors.New("workflow not found: wf-missing")
		}, func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowInspect(context.Background(), nil, workflowInspectArgs{WorkflowID: "wf-missing"})
			return err
		}},
		{"wait rejects missing workflow", func(f *fakeClient) {
			f.workflowWaitErr = errors.New("workflow not found: wf-missing")
		}, func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowWait(context.Background(), nil, workflowWaitArgs{WorkflowID: "wf-missing"})
			return err
		}},
		{"events rejects missing workflow", func(f *fakeClient) {
			f.workflowEventsErr = errors.New("workflow not found: wf-missing")
		}, func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowEvents(context.Background(), nil, workflowEventsArgs{WorkflowID: "wf-missing"})
			return err
		}},
		{"complete rejects undeclared outcome", func(f *fakeClient) {
			f.workflowCompleteErr = errors.New("outcome not declared: bogus")
		}, func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowComplete(context.Background(), nil, workflowCompleteArgs{
				WorkflowID: "wf", NodeID: "n", ActivationID: "a", AttemptID: "at", LeaseID: "l",
				OwnerToken: "tok", Outcome: "bogus",
			})
			return err
		}},
		{"gate rejects bad node_id", func(f *fakeClient) {
			f.workflowGateErr = errors.New("activation not found for node")
		}, func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowGate(context.Background(), nil, workflowGateArgs{
				WorkflowID: "wf", NodeID: "nope", GateID: "g", ActivationID: "a", Operation: "satisfy",
			})
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, fake := newWorkflowTestServer(t)
			tc.setup(fake)
			if err := tc.call(s); err == nil {
				t.Fatal("expected propagated error, got nil")
			}
		})
	}
}

// TestWorkflowControllerIdentifierRoundTrip verifies that controller IDs and
// supervisor IDs pass through the MCP tool surface to the typed control client
// unchanged — no identifier is dropped, remapped, or rewritten.
func TestWorkflowControllerIdentifierRoundTrip(t *testing.T) {
	s, fake := newWorkflowTestServer(t)
	ctx := context.Background()

	fake.workflowControllerStatusResult = map[string]any{"controller_id": "c-abc", "state": "enabled"}
	_, result, err := s.handleAvenorWorkflowControllerStatus(ctx, nil, workflowControllerStatusArgs{ControllerID: "c-abc"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fake.workflowControllerStatusCalls) != 1 || fake.workflowControllerStatusCalls[0] != "c-abc" {
		t.Fatalf("controller_id not round-tripped: %v", fake.workflowControllerStatusCalls)
	}
	res, _ := result.(map[string]any)
	if res["controller_id"] != "c-abc" {
		t.Fatalf("controller_id munged in result: %v", res["controller_id"])
	}

	fake.workflowControllerEnableResult = map[string]any{"controller_id": "c-abc", "state": "enabled"}
	_, _, err = s.handleAvenorWorkflowControllerEnable(ctx, nil, workflowControllerEnableArgs{ControllerID: "c-abc"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fake.workflowControllerEnableCalls) != 1 || fake.workflowControllerEnableCalls[0] != "c-abc" {
		t.Fatalf("enable controller_id not round-tripped: %v", fake.workflowControllerEnableCalls)
	}

	fake.workflowControllerDisableResult = map[string]any{"controller_id": "c-abc", "state": "disabled"}
	_, _, err = s.handleAvenorWorkflowControllerDisable(ctx, nil, workflowControllerDisableArgs{ControllerID: "c-abc", Reason: "why"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fake.workflowControllerDisableCalls) != 1 || fake.workflowControllerDisableCalls[0].controllerID != "c-abc" || fake.workflowControllerDisableCalls[0].reason != "why" {
		t.Fatalf("disable controller_id/reason not round-tripped: %v", fake.workflowControllerDisableCalls)
	}
}

// TestWorkflowControllerListForwarding verifies the list tool reaches the
// control client and returns its snapshot unchanged.
func TestWorkflowControllerListForwarding(t *testing.T) {
	s, fake := newWorkflowTestServer(t)
	ctx := context.Background()
	fake.workflowControllerListResult = map[string]any{"controllers": []any{}}
	_, result, err := s.handleAvenorWorkflowControllerList(ctx, nil, workflowControllerListArgs{})
	if err != nil {
		t.Fatal(err)
	}
	if fake.workflowControllerListCalls != 1 {
		t.Fatalf("expected 1 list call, got %d", fake.workflowControllerListCalls)
	}
	res, _ := result.(map[string]any)
	if _, ok := res["controllers"]; !ok {
		t.Fatalf("list result missing controllers: %v", res)
	}
}

// TestWorkflowControllerCreateForwarding verifies the create tool builds the
// {"controller_id","max_inflight"} request and forwards it to the control
// client, and that the controller starts disabled.
func TestWorkflowControllerCreateForwarding(t *testing.T) {
	s, fake := newWorkflowTestServer(t)
	ctx := context.Background()
	fake.workflowControllerCreateResult = map[string]any{"controller_id": "c-1", "state": "disabled"}
	_, result, err := s.handleAvenorWorkflowControllerCreate(ctx, nil, workflowControllerCreateArgs{ControllerID: "c-1", MaxInflight: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(fake.workflowControllerCreateCalls) != 1 {
		t.Fatalf("expected 1 create call, got %d", len(fake.workflowControllerCreateCalls))
	}
	var sent map[string]any
	if err := json.Unmarshal(fake.workflowControllerCreateCalls[0], &sent); err != nil {
		t.Fatalf("create request not valid JSON: %v", err)
	}
	if sent["controller_id"] != "c-1" || sent["max_inflight"] != float64(5) {
		t.Fatalf("create request not built correctly: %v", sent)
	}
	res, _ := result.(map[string]any)
	if res["state"] != "disabled" {
		t.Fatalf("create result should report disabled: %v", res)
	}
}

// TestWorkflowControllerErrorPropagation verifies controller handler errors
// propagate from the control client without being swallowed.
func TestWorkflowControllerErrorPropagation(t *testing.T) {
	cases := []struct {
		name  string
		setup func(*fakeClient)
		call  func(*Server) error
	}{
		{"controller status error", func(f *fakeClient) {
			f.workflowControllerStatusErr = errors.New("controller not found: c-missing")
		}, func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowControllerStatus(context.Background(), nil, workflowControllerStatusArgs{ControllerID: "c-missing"})
			return err
		}},
		{"controller list error", func(f *fakeClient) {
			f.workflowControllerListErr = errors.New("list failed")
		}, func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowControllerList(context.Background(), nil, workflowControllerListArgs{})
			return err
		}},
		{"controller create error", func(f *fakeClient) {
			f.workflowControllerCreateErr = errors.New("create failed")
		}, func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowControllerCreate(context.Background(), nil, workflowControllerCreateArgs{ControllerID: "c", MaxInflight: 5})
			return err
		}},
		{"controller enable error", func(f *fakeClient) {
			f.workflowControllerEnableErr = errors.New("enable failed")
		}, func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowControllerEnable(context.Background(), nil, workflowControllerEnableArgs{ControllerID: "c"})
			return err
		}},
		{"controller disable error", func(f *fakeClient) {
			f.workflowControllerDisableErr = errors.New("disable failed")
		}, func(s *Server) error {
			_, _, err := s.handleAvenorWorkflowControllerDisable(context.Background(), nil, workflowControllerDisableArgs{ControllerID: "c", Reason: "why"})
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, fake := newWorkflowTestServer(t)
			tc.setup(fake)
			if err := tc.call(s); err == nil {
				t.Fatal("expected propagated error, got nil")
			}
		})
	}
}
