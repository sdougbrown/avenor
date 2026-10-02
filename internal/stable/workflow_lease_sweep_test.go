package stable

// workflow_lease_sweep_test.go exercises the supervisor's live lease-expiry
// sweep end to end over a real supervisor, controller leader loop, and
// workflow manager: the residual case where the success fact is recorded but
// the auto-completion command fails, the guarantee that a live heartbeated
// attempt is never expired, and the sweep's shutdown lifecycle. Only the
// inference provider is scripted (stableScriptedProvider); the CompleteAuto
// failure is forced through the Supervisor's testHooks.completeAutoPre seam.
//
// Synchronization is deterministic: the sweep loop runs on an injected tick
// channel (testHooks.leaseSweepTick) the test steps explicitly, the workflow
// manager's lease-liveness clock is a manual clock (testHooks.workflowNow)
// the test advances explicitly, and every state transition is awaited through
// the manager's change notifications (waitForInstance), not by polling the
// wall clock.

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/workflow"
)

// sweepClock is a manual clock the test advances explicitly; it stands in for
// the workflow manager's lease-liveness clock so TTLs are virtual.
type sweepClock struct {
	mu  sync.Mutex
	now time.Time
}

func newSweepClock() *sweepClock {
	return &sweepClock{now: time.Date(2025, 3, 4, 5, 6, 7, 0, time.UTC)}
}

func (c *sweepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *sweepClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// leaseSweepDriver couples the injected sweep tick channel with the post-tick
// hook: step sends one tick and blocks until that sweep has run, recording
// its summary. Sweeps happen only when the test steps them.
type leaseSweepDriver struct {
	tick    chan time.Time
	ran     chan struct{}
	mu      sync.Mutex
	steps   int
	summary workflow.LeaseExpirySummary
}

func newLeaseSweepDriver() *leaseSweepDriver {
	return &leaseSweepDriver{tick: make(chan time.Time), ran: make(chan struct{}, 8)}
}

// hook is installed as testHooks.leaseSweepPost.
func (d *leaseSweepDriver) hook(summary workflow.LeaseExpirySummary) {
	d.mu.Lock()
	d.steps++
	d.summary = summary
	d.mu.Unlock()
	d.ran <- struct{}{}
}

// step fires one sweep tick and waits for the sweep to complete.
func (d *leaseSweepDriver) step() {
	d.tick <- time.Now()
	<-d.ran
}

// stepCount reports how many sweeps have completed.
func (d *leaseSweepDriver) stepCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.steps
}

// lastExpired reports the Expired count of the most recent completed sweep.
func (d *leaseSweepDriver) lastExpired() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.summary.Expired
}

// TestLeaseSweepReDispatchesAfterFailedAutoCompletion proves the residual
// stall case recovers without a restart: the scripted worker exits
// successfully, the supervisor records the success fact, and its
// CompleteAuto command fails once (injected through the completeAutoPre
// seam). The heartbeat has stopped, so the test advances the lease clock
// past the TTL and steps one sweep: the live sweep expires the dead lease,
// the controller re-dispatches the node, and the second attempt's completion
// succeeds and dispatches the dependent consume node.
func TestLeaseSweepReDispatchesAfterFailedAutoCompletion(t *testing.T) {
	t.Chdir(t.TempDir())
	provider := &stableScriptedProvider{attempt: -1}
	declared := produceWorkerDeclaredResult(t, provider, "ses_produce1", "result.md", "the declared summary")
	_ = produceWorkerDeclaredResult(t, provider, "ses_produce2", "result.md", "the declared summary")
	_ = produceWorkerDeclaredResult(t, provider, "ses_consume", "", "")
	clock := newSweepClock()
	sweeps := newLeaseSweepDriver()
	f := newAutoHandoffFixture(t, "lease-sweep-residual", provider, func(f *autoHandoffFixture) {
		// The interval only gates loop startup; the injected tick channel
		// drives every sweep.
		f.sup.config.WorkflowLeaseSweepInterval = 200 * time.Millisecond
		f.sup.testHooks.leaseSweepTick = sweeps.tick
		f.sup.testHooks.leaseSweepPost = sweeps.hook
		f.sup.testHooks.workflowNow = clock.Now
	})
	// 2 test-second lease TTL: once the heartbeat stops, one clock advance
	// past the TTL makes the dead lease sweepable, while a live heartbeated
	// attempt renews well inside the TTL.
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
	// The failed completion stopped the attempt's heartbeat (StopAndWait
	// removes it from the registry before returning), so no renewal can land
	// after this point and the clock advance is final.
	f.waitForNoHeartbeats(t, "produce's heartbeat to stop after the failed completion")

	// Advance the lease clock past the 2s TTL and sweep: exactly the dead
	// lease expires and the controller re-dispatches.
	clock.Advance(3 * time.Second)
	sweeps.step()
	if expired := sweeps.lastExpired(); expired != 1 {
		inst := f.instance(t, wf)
		t.Fatalf("sweep after the TTL advance expired %d leases, want exactly 1; observed %s", expired, describeInstance(&inst, f.providerCalls.Load()))
	}
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
// a release channel) while the test advances the lease clock across the 3s
// TTL in repeated steps, letting each step's heartbeat renewal land (observed
// through the manager's change notification) before stepping one sweep. A
// swept lease would have re-dispatched a second attempt; after the release
// the workflow completes with exactly one.
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
	clock := newSweepClock()
	sweeps := newLeaseSweepDriver()
	f := newAutoHandoffFixture(t, "lease-sweep-live", provider, func(f *autoHandoffFixture) {
		// The interval only gates loop startup; the injected tick channel
		// drives every sweep.
		f.sup.config.WorkflowLeaseSweepInterval = 200 * time.Millisecond
		f.sup.testHooks.leaseSweepTick = sweeps.tick
		f.sup.testHooks.leaseSweepPost = sweeps.hook
		f.sup.testHooks.workflowNow = clock.Now
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

	// Wait for the first heartbeat renewal to land (the heartbeat commit
	// notifies subscribers), so every clock advance below starts from a
	// freshly renewed lease.
	f.waitForInstance(t, wf, "the attempt's first lease heartbeat renewal", func(inst *workflow.WorkflowInstance) bool {
		act := activationFor(inst, "start")
		return act != nil && act.ActiveLease != nil && act.ActiveLease.LastHeartbeatAt != nil
	})

	// Cross the original 3s TTL in three 1s steps. Each step advances the
	// clock, waits for a renewal stamped at the advanced time (so the lease's
	// ExpiresAt is now+3s again), then sweeps once: the live lease is
	// retained every time despite the clock having moved past the original
	// expiry.
	for i := 1; i <= 3; i++ {
		clock.Advance(1 * time.Second)
		want := clock.Now()
		f.waitForInstance(t, wf, "a lease heartbeat renewal stamped after the clock advance", func(inst *workflow.WorkflowInstance) bool {
			act := activationFor(inst, "start")
			return act != nil && act.ActiveLease != nil &&
				act.ActiveLease.LastHeartbeatAt != nil &&
				act.ActiveLease.LastHeartbeatAt.Equal(want)
		})
		sweeps.step()
		if expired := sweeps.lastExpired(); expired != 0 {
			inst := f.instance(t, wf)
			t.Fatalf("sweep %d expired %d live leases, want 0; observed %s", i, expired, describeInstance(&inst, f.providerCalls.Load()))
		}
	}

	// Still exactly one live, leased attempt after the TTL was crossed three
	// times under renewal.
	inst := f.instance(t, wf)
	act := activationFor(&inst, "start")
	if act == nil || act.Status != workflow.ActivationRunning || act.ActiveLease == nil {
		t.Fatalf("start activation not live after the TTL-crossing sweeps; observed %s", describeInstance(&inst, f.providerCalls.Load()))
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
// joined by Shutdown: after the call returns, an offered tick can never run a
// sweep, because the joined goroutine is the only receiver of the tick
// channel.
func TestLeaseSweepStopsAfterShutdown(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket:              newStableSocketPath(t, "lease-sweep-stop"),
		MaxRuntimes:                2,
		MaxTreeBudget:              2,
		ShutdownTimeout:            0,
		WorkflowRoot:               filepath.Join(t.TempDir(), "wfroot"),
		WorkflowLeaseSweepInterval: 100 * time.Millisecond,
	})
	t.Cleanup(func() { _ = sup.broker.Stop() })
	sweeps := newLeaseSweepDriver()
	sup.testHooks.leaseSweepPost = sweeps.hook
	sup.testHooks.leaseSweepTick = sweeps.tick
	if _, _, err := sup.workflowBarrierResult(); err != nil {
		t.Fatalf("workflow barrier: %v", err)
	}
	sweeps.step()
	sweeps.step()
	sweeps.step()
	if err := sup.Shutdown("graceful"); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	// stopLeaseSweep joined the goroutine before Shutdown returned, so the
	// offered tick has no receiver: the non-blocking send must fail.
	select {
	case sweeps.tick <- time.Now():
		t.Fatal("a tick was received after Shutdown; the sweep loop was not joined")
	default:
	}
	if n := sweeps.stepCount(); n != 3 {
		t.Fatalf("observed %d sweeps after Shutdown, want exactly the 3 stepped before it", n)
	}
}

// TestLeaseSweepNeverStartsAfterStop proves a lazy workflow barrier that
// completes during shutdown cannot launch a sweep loop the shutdown join has
// already passed: once stopLeaseSweep has run, startLeaseSweepLoop is a no-op
// — structurally, no loop-start is ever reported.
func TestLeaseSweepNeverStartsAfterStop(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket:              newStableSocketPath(t, "lease-sweep-late-start"),
		MaxRuntimes:                2,
		MaxTreeBudget:              2,
		ShutdownTimeout:            0,
		WorkflowRoot:               filepath.Join(t.TempDir(), "wfroot"),
		WorkflowLeaseSweepInterval: 50 * time.Millisecond,
	})
	var loopStarts atomic.Int32
	sup.testHooks.leaseSweepLoopStart = func() { loopStarts.Add(1) }

	t.Cleanup(func() { _ = sup.broker.Stop() })
	if _, _, err := sup.workflowBarrierResult(); err != nil {
		t.Fatalf("workflow barrier: %v", err)
	}
	if n := loopStarts.Load(); n != 1 {
		t.Fatalf("loop-start reports after the barrier = %d, want exactly 1", n)
	}

	sup.stopLeaseSweep()
	loopStarts.Store(0)
	sup.startLeaseSweepLoop()
	if n := loopStarts.Load(); n != 0 {
		t.Fatalf("loop started after stop (%d starts), want 0", n)
	}
	if sup.leaseSweepRunning() {
		t.Fatal("sweep loop reports running after stop, want stopped")
	}
}

// TestLeaseSweepDoesNotStartWhenDisabledOrBarrierFails pins the start
// guard: a zero WorkflowLeaseSweepInterval disables the live sweep, and a
// failed workflow barrier never starts it, so leases then expire only
// through restart recovery. Both facts are checked structurally through the
// loop-start seam, with no reliance on observing the absence of ticks.
func TestLeaseSweepDoesNotStartWhenDisabledOrBarrierFails(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		root     func(t *testing.T) string
	}{
		{
			name:     "zero interval",
			interval: 0,
			root:     func(t *testing.T) string { return filepath.Join(t.TempDir(), "wfroot") },
		},
		{
			name:     "barrier failure",
			interval: 50 * time.Millisecond,
			root: func(t *testing.T) string {
				root := filepath.Join(t.TempDir(), "wfroot")
				_, childID := stageTerminalChildComposition(t, root)
				eventsPath := filepath.Join(root, "instances", childID, "events.ndjson")
				if err := os.WriteFile(eventsPath, []byte("not-json\n"+`{"kind":"created","seq":1}`+"\n"), 0o644); err != nil {
					t.Fatalf("corrupt event log: %v", err)
				}
				return root
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sup := NewSupervisor(Config{
				ControlSocket:              newStableSocketPath(t, "lease-sweep-off"),
				WorkflowRoot:               tc.root(t),
				WorkflowLeaseSweepInterval: tc.interval,
			})
			t.Cleanup(func() {
				sup.stopLeaseSweep()
				_ = sup.broker.Stop()
			})
			var loopStarts atomic.Int32
			sup.testHooks.leaseSweepLoopStart = func() { loopStarts.Add(1) }
			_, _, _ = sup.workflowBarrierResult()

			if n := loopStarts.Load(); n != 0 {
				t.Fatalf("sweep loop started (%d starts) despite %s, want none", n, tc.name)
			}
			if sup.leaseSweepRunning() {
				t.Fatalf("sweep loop reports running despite %s, want stopped", tc.name)
			}
		})
	}
}
