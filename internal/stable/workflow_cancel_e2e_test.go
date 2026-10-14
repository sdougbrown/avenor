package stable

// workflow_cancel_e2e_test.go runs workflow.cancel through the supervisor's
// control RPC surface end to end: the cancel dispatches to the handler the
// control server has registered (the lazy workflow handler, not the bare
// manager), terminates the in-flight attempt runtime, and records the
// attempt as canceled on the durable instance.

import (
	"testing"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/workflow"
)

// TestWorkflowCancelTerminatesInFlightRuntimes cancels an auto-dispatched
// workflow whose worker runtime is still running and asserts the runtime is
// torn down and its attempt records canceled. The cancel must travel through
// the supervisor-wrapped workflow handler: the bare manager cancels the
// instance durably but never signals this supervisor's runtimes, so the
// runtime-termination half of cancel is observable only through the wrapper.
func TestWorkflowCancelTerminatesInFlightRuntimes(t *testing.T) {
	t.Chdir(t.TempDir())
	// One scripted attempt that never finishes: its only event stays gated on
	// a release that never fires, so the runtime is in flight when cancel
	// lands.
	blocked := make(chan struct{})
	provider := &stableScriptedProvider{attempt: -1, scripts: []stableScriptedAttempt{{
		sessionID: "ses_cancel_me",
		events: []stableScriptedEvent{{event: events.Event{
			Event: "agent.status", SessionID: "ses_cancel_me", Fields: map[string]any{"phase": "working"},
		}, release: blocked}},
	}}}
	f := newAutoHandoffFixture(t, "auto-cancel", provider)
	wf := f.addWorkflow(t, "tmpl-auto-cancel", mustJSON(t, map[string]any{
		"schema_version":   1,
		"template_id":      "tmpl-auto-cancel",
		"template_version": "1",
		"entry_nodes":      []string{"start"},
		"nodes": []any{map[string]any{
			"id":           "start",
			"action":       map[string]any{"type": "run", "prompt": "do the thing"},
			"dispatch":     map[string]any{"mode": "auto", "controller_id": "c1"},
			"retry_policy": map[string]any{"max_attempts": 1, "exhaustion": "block"},
		}},
		"terminal_outcomes": []string{"done"},
	}))
	f.enableController(t, 2)

	// The node dispatches and its runtime is in flight: the activation is
	// running and this supervisor owns one live runtime for the attempt.
	f.waitForInstance(t, wf, "the auto node's activation to be running", func(inst *workflow.WorkflowInstance) bool {
		act := activationFor(inst, "start")
		return act != nil && act.Status == workflow.ActivationRunning
	})
	waitFor(t, "the attempt runtime to be in flight", func() bool {
		return f.sup.activeRuntimeCount() == 1
	})

	// Cancel through the real control RPC, so the request lands on whatever
	// workflow handler the supervisor registered — exactly as production does.
	socket := f.sup.config.ControlSocket
	if err := f.sup.control.Start(socket); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	t.Cleanup(func() { f.sup.control.Stop() })
	cl, err := client.Dial(socket)
	if err != nil {
		t.Fatalf("dial control server: %v", err)
	}
	defer cl.Close()
	var cancelResult map[string]any
	if err := cl.Call("workflow.cancel", map[string]any{
		"workflow_id": wf, "actor": "test", "reason": "cancel e2e",
	}, &cancelResult); err != nil {
		t.Fatalf("workflow.cancel: %v", err)
	}
	if n, _ := cancelResult["canceled_runtimes"].(float64); n != 1 {
		t.Fatalf("workflow.cancel result = %#v, want canceled_runtimes=1 (the supervisor-wrapped handler must signal the in-flight runtime)", cancelResult)
	}

	// The attempt records canceled and the runtime tears down.
	f.waitForInstance(t, wf, "the attempt to record canceled", func(inst *workflow.WorkflowInstance) bool {
		if inst.Status != workflow.WorkflowCanceled {
			return false
		}
		attempts := attemptsForNode(inst, "start")
		return len(attempts) == 1 && attempts[0].Status == workflow.AttemptCanceled
	})
	waitFor(t, "the in-flight runtime to tear down after cancel", func() bool {
		return f.sup.activeRuntimeCount() == 0
	})
}
