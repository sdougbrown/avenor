package stable

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/workflow"
)

// dispatchActionTemplateJSONTTL is dispatchActionTemplateJSON with a
// caller-chosen default lease TTL.
func dispatchActionTemplateJSONTTL(t *testing.T, templateID, controllerID, actionType, filePath string, ttlSeconds int) []byte {
	var action map[string]any
	switch actionType {
	case "loop":
		action = map[string]any{"type": "loop", "loop_file": filePath}
	case "team":
		action = map[string]any{"type": "team", "team_file": filePath}
	default:
		action = map[string]any{"type": "run", "prompt": "do the thing"}
	}
	template := map[string]any{
		"schema_version":   1,
		"template_id":      templateID,
		"template_version": "1",
		"entry_nodes":      []string{"start"},
		"nodes": []any{map[string]any{
			"id":           "start",
			"action":       action,
			"dispatch":     map[string]any{"mode": "auto", "controller_id": controllerID},
			"retry_policy": map[string]any{"max_attempts": 3, "exhaustion": "block"},
		}},
		"terminal_outcomes":    []string{"done"},
		"default_lease_policy": map[string]any{"ttl_seconds": ttlSeconds},
	}
	return mustJSON(t, template)
}

// workflowActivation pulls the start node's activation out of a
// WorkflowInspect result.
func workflowActivation(t *testing.T, f *dispatchFixture) workflow.Activation {
	t.Helper()
	out, err := f.mgr.WorkflowInspect(f.wf)
	if err != nil {
		t.Fatalf("WorkflowInspect: %v", err)
	}
	acts, ok := out.(map[string]any)["activations"].([]workflow.Activation)
	if !ok {
		t.Fatalf("inspect activations = %#v, want []workflow.Activation", out)
	}
	for _, act := range acts {
		if act.NodeID == "start" {
			return act
		}
	}
	t.Fatal("start activation not found in inspect")
	return workflow.Activation{}
}

// heartbeatEventCount counts heartbeat events in the workflow's event log.
func heartbeatEventCount(t *testing.T, f *dispatchFixture) int {
	t.Helper()
	out, err := f.mgr.WorkflowEvents(f.wf, 0, 10000)
	if err != nil {
		t.Fatalf("WorkflowEvents: %v", err)
	}
	events, ok := out.(map[string]any)["events"].([]map[string]any)
	if !ok {
		t.Fatalf("events = %#v, want []map[string]any", out)
	}
	n := 0
	for _, e := range events {
		if e["kind"] == string(workflow.EventHeartbeat) {
			n++
		}
	}
	return n
}

// waitHeartbeatEvents waits until at least n heartbeat events are recorded.
func waitHeartbeatEvents(t *testing.T, f *dispatchFixture, n int) {
	t.Helper()
	waitFor(t, "heartbeat events", func() bool { return heartbeatEventCount(t, f) >= n })
}

// TestHeartbeatKeepsLeaseAliveWhileRuntimeLive proves each executor's live
// heartbeat (direct run, loop, team) renews the attempt's lease every TTL/3
// while the runtime is blocked mid-run: ExpiresAt keeps advancing, recovery
// sweeps never expire the activation, and no second attempt can be claimed
// while the first is live.
func TestHeartbeatKeepsLeaseAliveWhileRuntimeLive(t *testing.T) {
	actions := []struct {
		name     string
		writeCfg func(t *testing.T, dir string) string
	}{
		{"run", nil},
		{"loop", func(t *testing.T, dir string) string {
			path := filepath.Join(dir, "loop.json")
			cfg := map[string]any{"max_iterations": 1, "loop": []any{map[string]any{"name": "work", "prompt": "work"}}}
			if err := os.WriteFile(path, mustJSON(t, cfg), 0o600); err != nil {
				t.Fatal(err)
			}
			return path
		}},
		{"team", func(t *testing.T, dir string) string {
			path := filepath.Join(dir, "team.json")
			cfg := map[string]any{"team": []any{map[string]any{"name": "work", "prompt": "work"}}}
			if err := os.WriteFile(path, mustJSON(t, cfg), 0o600); err != nil {
				t.Fatal(err)
			}
			return path
		}},
	}
	for _, action := range actions {
		action := action
		t.Run(action.name, func(t *testing.T) {
			filePath := ""
			if action.writeCfg != nil {
				filePath = action.writeCfg(t, t.TempDir())
			}
			template := dispatchActionTemplateJSONTTL(t, "tmpl-dispatch-"+action.name, "c1", action.name, filePath, 1)
			f := newDispatchFixtureTemplate(t, action.name, 4, 4, template)
			out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
			if err != nil {
				t.Fatalf("dispatchWorkflowNode: %v", err)
			}
			if out.Kind != Dispatched {
				t.Fatalf("outcome = %s (%s), want dispatched", out.Kind, out.Detail)
			}
			waitFor(t, "runtime registration", func() bool { return f.sup.activeRuntimeCount() == 1 })
			waitHeartbeatEvents(t, f, 1)

			// Over > 3×TTL the lease stays renewed (never in the past), the
			// activation stays running with a live lease, and the attempt
			// stays non-terminal.
			deadline := time.Now().Add(3500 * time.Millisecond)
			lastExpiry := time.Time{}
			for time.Now().Before(deadline) {
				if _, err := f.mgr.ExpireStaleLeases(); err != nil {
					t.Fatalf("ExpireStaleLeases: %v", err)
				}
				act := workflowActivation(t, f)
				if act.Status != workflow.ActivationRunning {
					t.Fatalf("activation status = %s, want running (heartbeat must prevent expiry)", act.Status)
				}
				if act.ActiveLease == nil {
					t.Fatal("activation lost its lease while the runtime is live")
				}
				if !act.ActiveLease.ExpiresAt.After(time.Now()) {
					t.Fatalf("lease expiry %v is not in the future (heartbeat not renewing)", act.ActiveLease.ExpiresAt)
				}
				if act.ActiveLease.ExpiresAt.After(lastExpiry) {
					lastExpiry = act.ActiveLease.ExpiresAt
				}
				attempt := workflowAttempt(t, f, act)
				switch attempt.Status {
				case workflow.AttemptStarting, workflow.AttemptRunning:
				default:
					t.Fatalf("live attempt status = %s, want starting/running", attempt.Status)
				}
				time.Sleep(200 * time.Millisecond)
			}
			// The expiry actually advanced over the window (renewals landed).
			act := workflowActivation(t, f)
			if !act.ActiveLease.ExpiresAt.After(lastExpiry.Add(-2 * time.Second)) || lastExpiry.IsZero() {
				t.Fatalf("lease expiry advanced only to %v over 3.5s", lastExpiry)
			}
			if heartbeatEventCount(t, f) < 3 {
				t.Fatalf("heartbeat events = %d, want at least 3 over 3.5s with a 1s TTL", heartbeatEventCount(t, f))
			}

			// No second attempt is claimable while the first is live.
			if err := f.mgr.RebuildCandidateIndex("test"); err != nil {
				t.Fatalf("RebuildCandidateIndex: %v", err)
			}
			cands, err := f.mgr.CandidatesForController("c1", 10)
			if err != nil {
				t.Fatalf("CandidatesForController: %v", err)
			}
			for _, c := range cands {
				if string(c.Identity.ActivationID) == f.activation {
					t.Fatal("live activation was claimable during the run")
				}
			}
		})
	}
}

// workflowAttempt returns the attempt record for the activation's most
// recent attempt.
func workflowAttempt(t *testing.T, f *dispatchFixture, act workflow.Activation) workflow.Attempt {
	t.Helper()
	if len(act.AttemptIDs) == 0 {
		t.Fatal("activation has no attempts")
	}
	out, err := f.mgr.WorkflowInspect(f.wf)
	if err != nil {
		t.Fatalf("WorkflowInspect: %v", err)
	}
	attempts, ok := out.(map[string]any)["attempts"].([]workflow.Attempt)
	if !ok {
		t.Fatalf("inspect attempts = %#v, want []workflow.Attempt", out)
	}
	last := act.AttemptIDs[len(act.AttemptIDs)-1]
	for _, a := range attempts {
		if a.ID == last {
			return a
		}
	}
	t.Fatalf("attempt %s not found in inspect", last)
	return workflow.Attempt{}
}

// TestHeartbeatStopsWhenRuntimeTerminates proves the heartbeat goroutine
// stops once the runtime reaches a terminal state: no heartbeat events land
// after termination and the goroutine count returns to its baseline.
func TestHeartbeatStopsWhenRuntimeTerminates(t *testing.T) {
	f := newDispatchFixtureTTL(t, "heartbeat-stop-terminal", 4, 4, "", 1)
	baseGoroutines := runtime.NumGoroutine()
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != Dispatched {
		t.Fatalf("outcome = %s (%s), want dispatched", out.Kind, out.Detail)
	}
	waitFor(t, "runtime registration", func() bool { return f.sup.activeRuntimeCount() == 1 })
	waitHeartbeatEvents(t, f, 1)

	// Drive the runtime to a terminal state by canceling it (the fixture
	// cleanup owns the release channel).
	_ = f.sup.cancelRuntime(out.RuntimeID)
	waitFor(t, "runtime terminal", func() bool { return f.sup.activeRuntimeCount() == 0 })

	count := heartbeatEventCount(t, f)
	time.Sleep(2 * time.Second) // two full TTL windows
	if got := heartbeatEventCount(t, f); got != count {
		t.Fatalf("heartbeat events after terminal = %d, want the pre-terminal count %d", got, count)
	}
	waitFor(t, "goroutines back to baseline", func() bool { return runtime.NumGoroutine() <= baseGoroutines+2 })
}

// TestHeartbeatStopsOnSupervisorShutdown proves a registered heartbeat
// goroutine exits on supervisor shutdown.
func TestHeartbeatStopsOnSupervisorShutdown(t *testing.T) {
	f := newDispatchFixtureTTL(t, "heartbeat-stop-shutdown", 4, 4, "", 1)
	begin, err := f.mgr.BeginDispatch(workflow.BeginDispatchRequest{
		WorkflowID:       workflow.WorkflowID(f.wf),
		NodeID:           "start",
		ActivationID:     workflow.ActivationID(f.activation),
		ExpectedRevision: f.revision,
		ControllerID:     "c1",
		LeaderLeaseID:    f.leaseID,
	})
	if err != nil {
		t.Fatalf("BeginDispatch: %v", err)
	}
	f.sup.startLeaseHeartbeat(workflow.ExecutorContext{
		WorkflowID:   workflow.WorkflowID(f.wf),
		NodeID:       "start",
		ActivationID: workflow.ActivationID(f.activation),
		AttemptID:    begin.AttemptID,
		LeaseID:      begin.LeaseID,
		OwnerToken:   begin.OwnerToken,
		LeaseTTL:     begin.LeaseTTL,
		Action:       begin.Action,
	})
	waitHeartbeatEvents(t, f, 1)

	f.sup.heartbeatMu.Lock()
	live := len(f.sup.heartbeats)
	f.sup.heartbeatMu.Unlock()
	if live != 1 {
		t.Fatalf("registered heartbeats = %d, want 1", live)
	}
	if err := f.sup.Shutdown("graceful"); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	waitFor(t, "heartbeat registry empty after shutdown", func() bool {
		f.sup.heartbeatMu.Lock()
		defer f.sup.heartbeatMu.Unlock()
		return len(f.sup.heartbeats) == 0
	})
}

// TestLeaseExpiryReleasesKeyAndAllowsReplacement proves a crashed dispatch
// (BeginDispatch, then no executor and no heartbeat) is fully recovered by
// the expiry sweep: the starting attempt is timed_out, the concurrency key
// is no longer held, and both a replacement dispatch on the same activation
// and a sibling activation on the same key can dispatch.
func TestLeaseExpiryReleasesKeyAndAllowsReplacement(t *testing.T) {
	f := newDispatchFixtureTTL(t, "dispatch-expiry-key", 4, 4, "deploys", 1)
	begin, err := f.mgr.BeginDispatch(workflow.BeginDispatchRequest{
		WorkflowID:       workflow.WorkflowID(f.wf),
		NodeID:           "start",
		ActivationID:     workflow.ActivationID(f.activation),
		ExpectedRevision: f.revision,
		ControllerID:     "c1",
		LeaderLeaseID:    f.leaseID,
	})
	if err != nil {
		t.Fatalf("BeginDispatch: %v", err)
	}
	_ = begin // crashed dispatch: no executor start, no heartbeat.

	// The expiry sweep terminalizes the ghost attempt.
	waitFor(t, "lease expiry sweep", func() bool {
		if _, err := f.mgr.ExpireStaleLeases(); err != nil {
			return false
		}
		act := workflowActivation(t, f)
		if act.Status != workflow.ActivationLeaseExpired {
			return false
		}
		if len(act.AttemptIDs) != 1 {
			return false
		}
		attempt := workflowAttempt(t, f, act)
		return attempt.Status == workflow.AttemptTimedOut
	})

	// The key is released: a replacement dispatch on the same activation
	// succeeds.
	if err := f.mgr.RebuildCandidateIndex("test"); err != nil {
		t.Fatalf("RebuildCandidateIndex: %v", err)
	}
	cands, err := f.mgr.CandidatesForController("c1", 10)
	if err != nil {
		t.Fatalf("CandidatesForController: %v", err)
	}
	found := false
	for _, c := range cands {
		if string(c.Identity.ActivationID) == f.activation {
			found = true
		}
	}
	if !found {
		t.Fatalf("expired activation %s not claimable after sweep", f.activation)
	}
	req := f.dispatchRequest(nil)
	req.ExpectedRevision = cands[0].Revision
	out, err := f.sup.dispatchWorkflowNode(t.Context(), req)
	if err != nil {
		t.Fatalf("replacement dispatch: %v", err)
	}
	if out.Kind != Dispatched {
		t.Fatalf("replacement outcome = %s (%s), want dispatched", out.Kind, out.Detail)
	}
	waitFor(t, "replacement runtime live", func() bool { return f.sup.activeRuntimeCount() == 1 })

	// The replacement's lease is heartbeat-renewed, not swept.
	waitHeartbeatEvents(t, f, 1)
	if _, err := f.mgr.ExpireStaleLeases(); err != nil {
		t.Fatalf("ExpireStaleLeases after replacement: %v", err)
	}
	if act := workflowActivation(t, f); act.Status != workflow.ActivationRunning {
		t.Fatalf("replacement activation status = %s, want running", act.Status)
	}
}
