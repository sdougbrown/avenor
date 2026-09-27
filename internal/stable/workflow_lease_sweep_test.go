package stable

// workflow_lease_sweep_test.go exercises the supervisor's live lease-expiry
// sweep end to end over a real supervisor, controller leader loop, and
// workflow manager: the residual case where the success fact is recorded but
// the auto-completion command fails, the guarantee that a live heartbeated
// attempt is never expired, and the sweep's shutdown lifecycle. Only the
// inference provider is scripted (stableScriptedProvider); the CompleteAuto
// failure is forced through the Supervisor's testHooks.completeAutoPre seam.

import (
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/workflow"
)

// TestLeaseSweepReDispatchesAfterFailedAutoCompletion proves the residual
// stall case recovers without a restart: the scripted worker exits
// successfully, the supervisor records the success fact, and its
// CompleteAuto command fails once (injected through the completeAutoPre
// seam). The heartbeat has stopped, so the live lease sweep
// expires the dead lease within one sweep interval, the controller
// re-dispatches the node, and the second attempt's completion succeeds and
// dispatches the dependent consume node.
func TestLeaseSweepReDispatchesAfterFailedAutoCompletion(t *testing.T) {
	t.Chdir(t.TempDir())
	provider := &stableScriptedProvider{attempt: -1}
	declared := produceWorkerDeclaredResult(t, provider, "ses_produce1", "result.md", "the declared summary")
	_ = produceWorkerDeclaredResult(t, provider, "ses_produce2", "result.md", "the declared summary")
	_ = produceWorkerDeclaredResult(t, provider, "ses_consume", "", "")
	f := newAutoHandoffFixture(t, "lease-sweep-residual", provider, func(f *autoHandoffFixture) {
		f.sup.config.WorkflowLeaseSweepInterval = 200 * time.Millisecond
	})
	// 2s lease TTL: once the heartbeat stops, the dead lease is sweepable
	// quickly, while a live heartbeated attempt renews well inside the TTL.
	wf := f.addWorkflow(t, "tmpl-lease-sweep-residual", autoHandoffChainTemplate(t, "tmpl-lease-sweep-residual", 2))

	// Force the first auto-completion to fail after the success fact, as an
	// evidence staging I/O error would in production. Later completions run.
	var completeCalls atomic.Int32
	f.sup.testHooks.completeAutoPre = func() error {
		if completeCalls.Add(1) == 1 {
			return errors.New("injected evidence staging failure")
		}
		return nil
	}

	// The hook is installed before the controller is enabled, so the first
	// completion is produce's and the supervisor never races the write.
	f.enableController(t, 2)

	f.waitForInstance(t, wf, "produce's first attempt to succeed", func(inst *workflow.WorkflowInstance) bool {
		attempts := attemptsForNode(inst, "produce")
		return len(attempts) == 1 && attempts[0].Status == workflow.AttemptSucceeded
	})

	// The failed completion leaves the activation running with a dead lease;
	// the live sweep expires it and the controller re-dispatches.
	f.waitForInstance(t, wf, "the live sweep to expire the dead lease and re-dispatch produce", func(inst *workflow.WorkflowInstance) bool {
		return len(attemptsForNode(inst, "produce")) == 2
	})

	// The second attempt completes and the dependent node dispatches.
	f.waitForInstance(t, wf, "produce satisfied with outcome done and consume dispatched", func(inst *workflow.WorkflowInstance) bool {
		produce := activationFor(inst, "produce")
		consume := activationFor(inst, "consume")
		return produce != nil && produce.Status == workflow.ActivationSatisfied &&
			produce.SelectedOutcome == workflow.OutcomeName(declared.Outcome) &&
			consume != nil && len(consume.AttemptIDs) > 0
	})
	f.waitForInstance(t, wf, "the workflow to complete", func(inst *workflow.WorkflowInstance) bool {
		return inst.Status == workflow.WorkflowCompleted
	})

	inst := f.instance(t, wf)
	attempts := attemptsForNode(&inst, "produce")
	if len(attempts) != 2 {
		t.Fatalf("produce recorded %d attempts, want exactly 2; observed %s", len(attempts), describeInstance(&inst, f.providerCalls.Load()))
	}
	if attempts[0].Status != workflow.AttemptSucceeded {
		t.Fatalf("produce's first attempt status = %s, want succeeded (the success fact survives the lease expiry); observed %s",
			attempts[0].Status, describeInstance(&inst, f.providerCalls.Load()))
	}
	if calls := f.providerCalls.Load(); calls != 3 {
		t.Fatalf("provider invoked %d times, want exactly 3 (two produce attempts + consume); observed %s",
			calls, describeInstance(&inst, calls))
	}
	produce := activationFor(&inst, "produce")
	if produce == nil || produce.Status != workflow.ActivationSatisfied {
		t.Fatalf("produce not satisfied; observed %s", describeInstance(&inst, f.providerCalls.Load()))
	}
	if produce.SelectedOutcome != workflow.OutcomeName(declared.Outcome) {
		t.Fatalf("produce selected outcome = %q, want %q; observed %s",
			produce.SelectedOutcome, declared.Outcome, describeInstance(&inst, f.providerCalls.Load()))
	}
}

// TestLeaseSweepSparesLiveHeartbeatedAttempt proves the sweep never expires a
// live attempt: the scripted worker stays alive (its session end is gated on
// a release channel) across several sweep intervals and multiple TTL windows
// of a 1s lease, while the executor's heartbeat loop keeps renewing. The
// worker is only released after the sweep has run repeatedly, and the
// workflow then completes with exactly one attempt — a swept lease would have
// re-dispatched a second one.
func TestLeaseSweepSparesLiveHeartbeatedAttempt(t *testing.T) {
	t.Chdir(t.TempDir())
	release := make(chan struct{})
	provider := &stableScriptedProvider{attempt: -1}
	provider.mu.Lock()
	provider.scripts = append(provider.scripts, stableScriptedAttempt{
		sessionID: "ses_live",
		events: []stableScriptedEvent{
			{event: events.Event{Event: "agent.message_chunk", SessionID: "ses_live", Fields: map[string]any{"delta": "working"}},
				release: release},
			{event: events.Event{Event: "session.end", SessionID: "ses_live", Fields: map[string]any{"stop_reason": "end_turn"}}},
		},
	})
	provider.mu.Unlock()
	var sweeps atomic.Int32
	f := newAutoHandoffFixture(t, "lease-sweep-live", provider, func(f *autoHandoffFixture) {
		f.sup.config.WorkflowLeaseSweepInterval = 200 * time.Millisecond
		// Set before the barrier: the sweep loop reads the hook every tick.
		f.sup.testHooks.leaseSweepPost = func(workflow.LeaseExpirySummary) { sweeps.Add(1) }
	})
	template := map[string]any{
		"schema_version":   1,
		"template_id":      "tmpl-lease-sweep-live",
		"template_version": "1",
		"entry_nodes":      []string{"start"},
		"nodes": []any{map[string]any{
			"id":           "start",
			"action":       map[string]any{"type": "run", "prompt": "do the thing"},
			"dispatch":     map[string]any{"mode": "auto", "controller_id": "c1"},
			"retry_policy": map[string]any{"max_attempts": 3, "exhaustion": "block"},
		}},
		"terminal_outcomes":    []string{"done"},
		"default_lease_policy": map[string]any{"ttl_seconds": 3},
	}
	wf := f.addWorkflow(t, "tmpl-lease-sweep-live", mustJSON(t, template))
	f.enableController(t, 2)

	// Hold the worker alive until the sweep has run at least twenty times
	// (4s, past the 3s TTL, which the heartbeat renews every second).
	deadline := time.Now().Add(10 * time.Second)
	for sweeps.Load() < 20 {
		if time.Now().After(deadline) {
			inst := f.instance(t, wf)
			t.Fatalf("timed out waiting for twenty lease sweeps; observed %s", describeInstance(&inst, f.providerCalls.Load()))
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Still exactly one live, leased attempt after repeated sweeps.
	inst := f.instance(t, wf)
	act := activationFor(&inst, "start")
	if act == nil || act.Status != workflow.ActivationRunning || act.ActiveLease == nil {
		t.Fatalf("start activation not live after %d sweeps; observed %s", sweeps.Load(), describeInstance(&inst, f.providerCalls.Load()))
	}
	if len(inst.Attempts) != 1 {
		t.Fatalf("workflow recorded %d attempts while the worker was alive, want exactly 1; observed %s",
			len(inst.Attempts), describeInstance(&inst, f.providerCalls.Load()))
	}

	close(release)
	f.waitForInstance(t, wf, "the workflow to complete", func(inst *workflow.WorkflowInstance) bool {
		return inst.Status == workflow.WorkflowCompleted
	})
	inst = f.instance(t, wf)
	if calls := f.providerCalls.Load(); calls != 1 {
		t.Fatalf("provider invoked %d times, want exactly 1; observed %s", calls, describeInstance(&inst, calls))
	}
	if len(inst.Attempts) != 1 {
		t.Fatalf("workflow recorded %d attempts after completion, want exactly 1; observed %s",
			len(inst.Attempts), describeInstance(&inst, f.providerCalls.Load()))
	}
}

// TestLeaseSweepStopsAfterShutdown proves the sweep goroutine is stopped and
// joined by Shutdown: after the call returns, no further sweep tick ever runs.
func TestLeaseSweepStopsAfterShutdown(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket:              newStableSocketPath(t, "lease-sweep-stop"),
		MaxRuntimes:                2,
		MaxTreeBudget:              2,
		ShutdownTimeout:            0,
		WorkflowRoot:               filepath.Join(t.TempDir(), "wfroot"),
		WorkflowLeaseSweepInterval: 100 * time.Millisecond,
	})
	var sweeps atomic.Int32
	sup.testHooks.leaseSweepPost = func(workflow.LeaseExpirySummary) { sweeps.Add(1) }
	if _, _, err := sup.workflowBarrierResult(); err != nil {
		t.Fatalf("workflow barrier: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for sweeps.Load() < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for three lease sweeps; observed %d", sweeps.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := sup.Shutdown("graceful"); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	afterShutdown := sweeps.Load()
	// stopLeaseSweep joined the goroutine before Shutdown returned, so no
	// further tick can ever run; observe several intervals to prove it.
	deadline = time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if cur := sweeps.Load(); cur != afterShutdown {
			t.Fatalf("sweep ran after Shutdown: %d -> %d", afterShutdown, cur)
		}
		time.Sleep(50 * time.Millisecond)
	}
	_ = sup.broker.Stop()
}

// TestLeaseSweepNeverStartsAfterStop proves a lazy workflow barrier that
// completes during shutdown cannot launch a sweep loop the shutdown join has
// already passed: once stopLeaseSweep has run, startLeaseSweepLoop is a no-op.
func TestLeaseSweepNeverStartsAfterStop(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket:              newStableSocketPath(t, "lease-sweep-late-start"),
		MaxRuntimes:                2,
		MaxTreeBudget:              2,
		ShutdownTimeout:            0,
		WorkflowRoot:               filepath.Join(t.TempDir(), "wfroot"),
		WorkflowLeaseSweepInterval: 50 * time.Millisecond,
	})
	var sweeps atomic.Int32
	sup.testHooks.leaseSweepPost = func(workflow.LeaseExpirySummary) { sweeps.Add(1) }

	t.Cleanup(func() { _ = sup.broker.Stop() })
	if _, _, err := sup.workflowBarrierResult(); err != nil {
		t.Fatalf("workflow barrier: %v", err)
	}

	sup.stopLeaseSweep()
	sweeps.Store(0)
	sup.startLeaseSweepLoop()

	// A loop started despite the stop would tick every 50ms; six intervals
	// with no tick shows none was started.
	time.Sleep(300 * time.Millisecond)
	if n := sweeps.Load(); n != 0 {
		t.Fatalf("observed %d sweeps after stop, want 0", n)
	}
}
