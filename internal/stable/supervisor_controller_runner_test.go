package stable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/workflow"
	"github.com/sdougbrown/avenor/internal/workflowcontroller"
)

// runnerExecutor is a workflow.Executor fake for controller-runner integration
// tests. It records every dispatched ExecutorContext, blocks the dispatch
// until the test releases it, and then either completes the workflow node with
// the granted lease (the machine handoff a real child runtime performs) or,
// when abandoned, returns without touching workflow state so the attempt stays
// durably live across a simulated supervisor death.
type runnerExecutor struct {
	mgr       func() *workflow.Manager
	started   chan workflow.ExecutorContext
	pending   chan chan struct{}
	failFirst atomic.Int32 // number of initial calls that fail (start failure)
	abandon   atomic.Bool  // released dispatches leave the attempt live
}

func newRunnerExecutor(mgr func() *workflow.Manager) *runnerExecutor {
	return &runnerExecutor{
		mgr:     mgr,
		started: make(chan workflow.ExecutorContext, 64),
		pending: make(chan chan struct{}, 64),
	}
}

func (e *runnerExecutor) Dispatch(_ context.Context, ec workflow.ExecutorContext) error {
	if n := e.failFirst.Add(-1); n >= 0 {
		return fmt.Errorf("injected executor start failure (%d remaining)", n)
	}
	select {
	case e.started <- ec:
	default:
	}
	done := make(chan struct{})
	e.pending <- done
	<-done
	if e.abandon.Load() {
		// Leave the attempt durably live: no completion, no termination.
		return nil
	}
	payload, err := json.Marshal(map[string]any{
		"op":            "complete",
		"node_id":       string(ec.NodeID),
		"activation_id": string(ec.ActivationID),
		"attempt_id":    string(ec.AttemptID),
		"lease_id":      ec.LeaseID,
		"owner_token":   ec.OwnerToken,
		"outcome":       "done",
	})
	if err != nil {
		return err
	}
	_, err = e.mgr().WorkflowCommand(string(ec.WorkflowID), payload)
	return err
}

// finish releases one blocked dispatch, completing (or abandoning) it.
func (e *runnerExecutor) finish() {
	done := <-e.pending
	close(done)
}

// drainFinished unblocks every dispatch blocked at cleanup time. Callers must
// have disabled the controller first so released workers are not replaced by
// fresh dispatches.
func (e *runnerExecutor) drainFinished() {
	deadline := time.Now().Add(2 * time.Second)
	quiescent := 0
	for quiescent < 2 && time.Now().Before(deadline) {
		select {
		case done := <-e.pending:
			close(done)
			quiescent = 0
		default:
			quiescent++
			time.Sleep(20 * time.Millisecond)
		}
	}
}

// runnerFixture wires a supervisor over a workflow root with either a blocking
// admission provider (real runtimes) or a runnerExecutor (controller-dispatch
// without real runtimes).
type runnerFixture struct {
	sup          *Supervisor
	mgr          *workflow.Manager
	cstore       *workflowcontroller.ControllerStore
	controllerID string        // controller this fixture owns and disables at cleanup
	release      chan struct{} // real-provider mode gate
	closeOnce    sync.Once
	exec         *runnerExecutor
}

// releaseProvider unblocks every runtime blocked in the admission provider
// (idempotent).
func (f *runnerFixture) releaseProvider() { f.closeOnce.Do(func() { close(f.release) }) }

// newRunnerFixture builds a supervisor on the given workflow root ("" for a
// fresh temp dir). realProvider selects the provider mode; the executor mode
// registers a runnerExecutor as the run action's executor.
func newRunnerFixture(t *testing.T, name, root string, maxRuntimes, maxTreeBudget int, realProvider bool) *runnerFixture {
	t.Helper()
	if root == "" {
		root = filepath.Join(t.TempDir(), "wfroot")
	}
	sup := NewSupervisor(Config{
		ControlSocket:   newStableSocketPath(t, name),
		MaxRuntimes:     maxRuntimes,
		MaxTreeBudget:   maxTreeBudget,
		ShutdownTimeout: 0,
		WorkflowRoot:    root,
	})
	// The startup barrier starts leader loops for recovered enabled
	// controllers, so the fast test cadence must be set before the barrier
	// runs.
	sup.controllerRenewInterval = 25 * time.Millisecond
	mgr, cstore, err := sup.workflowBarrierResult()
	if err != nil {
		t.Fatalf("workflow barrier: %v", err)
	}
	f := &runnerFixture{sup: sup, mgr: mgr, cstore: cstore, controllerID: "c1", release: make(chan struct{})}
	if realProvider {
		sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
			return &blockingAdmissionProvider{release: f.release}, nil
		}
	} else {
		f.exec = newRunnerExecutor(func() *workflow.Manager { return sup.workflowManager() })
		mgr.RegisterExecutor(workflow.ActionRun, f.exec)
	}
	t.Cleanup(func() { f.stop(t) })
	return f
}

// stop tears the fixture down in a deadlock-free order: disable controllers so
// no fresh dispatch starts, unblock in-flight dispatches, stop the leader
// loops, then cancel any real runtimes before the workflow root disappears.
func (f *runnerFixture) stop(t *testing.T) {
	t.Helper()
	// Disable first so no canceled attempt's retry re-arm triggers a fresh
	// dispatch during teardown.
	if _, err := f.cstore.SetDesiredState(f.controllerID, workflowcontroller.DesiredDisabled, "test cleanup"); err != nil {
		t.Logf("cleanup disable: %v", err)
	}
	if f.exec != nil {
		f.exec.drainFinished()
	}
	f.releaseProvider()
	f.sup.stopControllerLoops()
	for _, rt := range f.sup.listRuntimes() {
		if id, ok := rt["runtime_id"].(string); ok {
			_ = f.sup.cancelRuntime(id)
		}
	}
	waitFor(t, f.sup.supervisorIdentity()+" runtimes terminal at cleanup", func() bool {
		return f.sup.activeRuntimeCount() == 0
	})
	// The terminal attempt-termination write lags the runtime's completed
	// flag; give it the same settle window the dispatch tests use.
	time.Sleep(100 * time.Millisecond)
	_ = f.sup.broker.Stop()
	f.sup.stopReaper()
}

// addWorkflow registers the standard one-node auto-dispatch template under a
// unique id, instantiates it, and returns the workflow id.
func (f *runnerFixture) addWorkflow(t *testing.T, name, controllerID, concurrencyKey string) string {
	t.Helper()
	tmplID := "tmpl-runner-" + name
	if _, err := f.mgr.WorkflowCreate(dispatchTemplateJSONTTL(t, tmplID, controllerID, concurrencyKey, 900)); err != nil {
		t.Fatalf("WorkflowCreate %s: %v", tmplID, err)
	}
	out, err := f.mgr.WorkflowInstantiate(mustJSON(t, map[string]string{"template_id": tmplID, "template_version": "1"}))
	if err != nil {
		t.Fatalf("WorkflowInstantiate %s: %v", tmplID, err)
	}
	wf, ok := out.(map[string]any)["workflow_id"].(string)
	if !ok || wf == "" {
		t.Fatalf("instantiate result missing workflow_id: %#v", out)
	}
	return wf
}

// enableController creates (unless it already exists, as after a restart),
// enables, and starts the leader loop for the controller with a fast renew
// cadence.
func (f *runnerFixture) enableController(t *testing.T, id string, maxInflight int) {
	t.Helper()
	if _, err := f.sup.WorkflowControllerCreate(createControllerParams(t, id, maxInflight)); err != nil &&
		!errors.Is(err, workflowcontroller.ErrConflict) {
		t.Fatalf("controller create: %v", err)
	}
	if _, err := f.sup.WorkflowControllerEnable(id); err != nil {
		t.Fatalf("controller enable: %v", err)
	}
	waitForControllerLeader(t, f.cstore, id, func(rec workflowcontroller.ControllerRecord) bool {
		return rec.Leader.OwnerID == f.sup.supervisorIdentity()
	})
}

// workflowInstance reads the instance snapshot for a workflow.
func (f *runnerFixture) workflowInstance(t *testing.T, wf string) workflow.WorkflowInstance {
	t.Helper()
	insp, err := f.mgr.WorkflowInspect(wf)
	if err != nil {
		t.Fatalf("WorkflowInspect %s: %v", wf, err)
	}
	inst, ok := insp.(map[string]any)["instance"].(workflow.WorkflowInstance)
	if !ok {
		t.Fatalf("inspect %s missing instance: %#v", wf, insp)
	}
	return inst
}

// runningAttempts returns the workflow's attempts in starting/running status.
func (f *runnerFixture) runningAttempts(t *testing.T, wf string) []workflow.Attempt {
	t.Helper()
	var out []workflow.Attempt
	for _, a := range f.workflowInstance(t, wf).Attempts {
		if a.Status == workflow.AttemptStarting || a.Status == workflow.AttemptRunning {
			out = append(out, a)
		}
	}
	return out
}

// waitRunning polls until the workflow has exactly one live attempt.
func (f *runnerFixture) waitRunning(t *testing.T, wf string) {
	t.Helper()
	waitFor(t, "workflow "+wf+" running attempt", func() bool {
		return len(f.runningAttempts(t, wf)) == 1
	})
}

// TestControllerRunnerAutoProgressesMultipleWorkflows proves several
// independent controller-dispatched workflows run to terminal completion with
// no manual start: the runner dispatches every ready candidate, the executor
// handoff completes each node with its granted lease, and all instances reach
// completed.
func TestControllerRunnerAutoProgressesMultipleWorkflows(t *testing.T) {
	f := newRunnerFixture(t, "runner-auto", "", 4, 4, false)
	var wfs []string
	for _, name := range []string{"alpha", "beta", "gamma"} {
		wfs = append(wfs, f.addWorkflow(t, "auto-"+name, "c1", ""))
	}
	f.enableController(t, "c1", 5)

	// All three workflows dispatch automatically, with no manual start.
	waitFor(t, "three automatic dispatches", func() bool { return len(f.exec.started) >= 3 })
	for _, wf := range wfs {
		f.waitRunning(t, wf)
		if got := len(f.workflowInstance(t, wf).Attempts); got != 1 {
			t.Fatalf("workflow %s has %d attempts, want exactly one automatic dispatch", wf, got)
		}
	}

	// Release each attempt: the executor completes the node with the lease
	// the runner's dispatch granted.
	for range wfs {
		f.exec.finish()
	}
	for _, wf := range wfs {
		waitFor(t, "workflow "+wf+" terminal", func() bool {
			return f.workflowInstance(t, wf).Status == workflow.WorkflowCompleted
		})
		if outcome := f.workflowInstance(t, wf).TerminalOutcome; outcome != "done" {
			t.Fatalf("workflow %s terminal outcome = %q, want done", wf, outcome)
		}
	}
}

// TestControllerRunnerNeverExceedsMaxInflight proves concurrent controller
// dispatching never exceeds max_inflight or the supervisor's local admission:
// with max_inflight=2 and four ready workflows, only two runtimes are ever
// live at once, and the remaining two dispatch only as earlier attempts go
// terminal.
func TestControllerRunnerNeverExceedsMaxInflight(t *testing.T) {
	f := newRunnerFixture(t, "runner-bound", "", 4, 4, true)
	for _, name := range []string{"w1", "w2", "w3", "w4"} {
		f.addWorkflow(t, "bound-"+name, "c1", "")
	}
	f.enableController(t, "c1", 2)

	// Sample the live-runtime bound continuously while the runner works.
	var maxActive atomic.Int32
	sampling := make(chan struct{})
	go func() {
		defer close(sampling)
		for {
			n := int32(f.sup.activeRuntimeCount())
			for {
				cur := maxActive.Load()
				if n <= cur || maxActive.CompareAndSwap(cur, n) {
					break
				}
			}
			select {
			case <-f.release:
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	}()

	waitFor(t, "first two dispatches", func() bool { return f.sup.activeRuntimeCount() == 2 })
	// Several fast passes must not exceed the bound while two runtimes are
	// unreported.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if n := f.sup.activeRuntimeCount(); n > 2 {
			t.Fatalf("%d live runtimes with max_inflight=2", n)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Release the first two: they go terminal and the remaining two
	// dispatch automatically.
	f.releaseProvider()
	<-sampling
	if max := maxActive.Load(); max > 2 {
		t.Fatalf("peak live runtimes = %d, want <= max_inflight (2) and <= local admission", max)
	}
	waitFor(t, "all four attempts recorded", func() bool {
		live := f.mgr.LiveAttempts()
		total := 0
		for _, la := range live {
			if la.ControllerID == "c1" {
				total++
			}
		}
		return total == 2 && f.sup.activeRuntimeCount() == 2
	})
	// Let the final two runtimes park before cleanup re-closes release.
}

// TestControllerRunnerReplenishesAfterTerminalAttempt proves replenishment:
// with max_inflight=1 and two ready workflows, the second dispatches as soon
// as the first attempt terminates, driven by the manager's change
// notification on the terminal attempt — no external signal from the test.
func TestControllerRunnerReplenishesAfterTerminalAttempt(t *testing.T) {
	f := newRunnerFixture(t, "runner-replenish", "", 4, 4, false)
	wf1 := f.addWorkflow(t, "replenish-1", "c1", "")
	wf2 := f.addWorkflow(t, "replenish-2", "c1", "")
	f.enableController(t, "c1", 1)

	// Only the first workflow dispatches at max_inflight=1.
	waitFor(t, "first dispatch", func() bool { return len(f.exec.started) >= 1 })
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if n := len(f.exec.started); n > 1 {
			t.Fatalf("%d dispatches with max_inflight=1", n)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Terminate the first attempt: its completion commits a workflow
	// transition, the change notification wakes the runner, and the second
	// workflow dispatches without any test-driven signal.
	f.exec.finish()
	waitFor(t, "second dispatch after replenishment", func() bool {
		return len(f.exec.started) >= 2
	})
	f.waitRunning(t, wf2)
	waitFor(t, "first attempt no longer live", func() bool {
		for _, la := range f.mgr.LiveAttempts() {
			if string(la.Identity.WorkflowID) == wf1 {
				return false
			}
		}
		return true
	})
	if f.workflowInstance(t, wf1).Status != workflow.WorkflowCompleted {
		t.Fatalf("first workflow status = %q, want completed", f.workflowInstance(t, wf1).Status)
	}

	// The second completes the same way.
	f.exec.finish()
	waitFor(t, "second workflow terminal", func() bool {
		return f.workflowInstance(t, wf2).Status == workflow.WorkflowCompleted
	})
}

// waitBlockedSource polls the controller status until the runner reports a
// capacity block from the given source.
func waitBlockedSource(t *testing.T, f *runnerFixture, id, source string) {
	t.Helper()
	waitFor(t, "capacity blocked ("+source+") in runner status", func() bool {
		st, err := f.sup.WorkflowControllerStatus(id)
		if err != nil {
			return false
		}
		blocked, ok := st.(map[string]any)["capacity_blocked"].(map[string]any)
		return ok && blocked["source"] == source
	})
}

// TestControllerRunnerFullCapacityWaitThenProgress proves the runner waits
// when admission is exhausted and dispatches as soon as capacity frees: no
// dispatch while a manual runtime holds the only local slot, then an
// automatic dispatch after it is canceled.
func TestControllerRunnerFullCapacityWaitThenProgress(t *testing.T) {
	f := newRunnerFixture(t, "runner-capacity", "", 1, 4, true)
	wf := f.addWorkflow(t, "capacity-1", "c1", "")
	// Exhaust the single local slot with an ordinary runtime before the
	// controller leads.
	res, err := f.sup.spawn(SpawnParams{Prompt: "occupy", Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("occupy spawn: %v", err)
	}
	f.enableController(t, "c1", 5)

	// The runner observes the capacity block and does not dispatch.
	waitBlockedSource(t, f, "c1", "local")
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if n := len(f.runningAttempts(t, wf)); n > 0 {
			t.Fatalf("workflow dispatched while local admission was exhausted")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Free the slot: the runtime parks, capacity signals fire, and the
	// runner dispatches the workflow without any workflow-store change from
	// the test.
	_ = f.sup.cancelRuntime(res.RuntimeID)
	f.waitRunning(t, wf)
	if got := len(f.workflowInstance(t, wf).Attempts); got != 1 {
		t.Fatalf("workflow has %d attempts after capacity freed, want 1", got)
	}
}

// TestControllerRunnerRetriesFailedAttempt proves retry readiness
// re-dispatches: a failed executor start records the terminal failure, the
// retry policy re-arms the activation, and the runner re-dispatches it
// automatically.
func TestControllerRunnerRetriesFailedAttempt(t *testing.T) {
	f := newRunnerFixture(t, "runner-retry", "", 4, 4, false)
	f.exec.failFirst.Store(1)
	wf := f.addWorkflow(t, "retry-1", "c1", "")
	f.enableController(t, "c1", 2)

	// The first dispatch fails pre-start; the retry policy re-arms and the
	// runner dispatches a second attempt with no external signal. (The
	// failed call never reaches the executor body, so one started record
	// is the successful retry.)
	waitFor(t, "retry re-dispatch after failed start", func() bool {
		return len(f.exec.started) >= 1 && len(f.workflowInstance(t, wf).Attempts) >= 2
	})
	inst := f.workflowInstance(t, wf)
	if got := len(inst.Attempts); got != 2 {
		t.Fatalf("attempts = %d, want 2 (failed first + retry)", got)
	}
	if inst.Attempts[0].Status != workflow.AttemptFailed {
		t.Fatalf("first attempt status = %q, want failed", inst.Attempts[0].Status)
	}
	f.waitRunning(t, wf)

	f.exec.finish()
	waitFor(t, "workflow terminal after retry", func() bool {
		return f.workflowInstance(t, wf).Status == workflow.WorkflowCompleted
	})
}

// TestControllerRunnerGateParkedActivationNeverDispatched proves a parked
// activation is not a dispatch candidate: after the node's completion is
// parked on a required gate (the same non-claimable-status rule checkpoint
// parking rests on — see claimableActivationStatus), the runner never
// dispatches it again.
func TestControllerRunnerGateParkedActivationNeverDispatched(t *testing.T) {
	f := newRunnerFixture(t, "runner-parked", "", 4, 4, false)
	tmplID := "tmpl-runner-parked"
	gated := map[string]any{
		"schema_version":   1,
		"template_id":      tmplID,
		"template_version": "1",
		"entry_nodes":      []string{"start"},
		"nodes": []any{map[string]any{
			"id":           "start",
			"action":       map[string]any{"type": "run", "prompt": "do the thing"},
			"dispatch":     map[string]any{"mode": "auto", "controller_id": "c1"},
			"gates":        []any{map[string]any{"id": "review", "type": "human", "required": true}},
			"retry_policy": map[string]any{"max_attempts": 3, "exhaustion": "block"},
		}},
		"terminal_outcomes":    []string{"done"},
		"default_lease_policy": map[string]any{"ttl_seconds": 900},
	}
	if _, err := f.mgr.WorkflowCreate(mustJSON(t, gated)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	out, err := f.mgr.WorkflowInstantiate(mustJSON(t, map[string]string{"template_id": tmplID, "template_version": "1"}))
	if err != nil {
		t.Fatalf("WorkflowInstantiate: %v", err)
	}
	wf := out.(map[string]any)["workflow_id"].(string)
	f.enableController(t, "c1", 5)

	waitFor(t, "gated node dispatched", func() bool { return len(f.exec.started) >= 1 })
	f.waitRunning(t, wf)

	// Completing parks the activation awaiting the unresolved required gate.
	f.exec.finish()
	waitFor(t, "activation parked awaiting_gate", func() bool {
		insp, err := f.mgr.WorkflowInspect(wf)
		if err != nil {
			return false
		}
		acts := insp.(map[string]any)["activations"].([]workflow.Activation)
		for _, act := range acts {
			if string(act.NodeID) == "start" && act.Status == workflow.ActivationAwaitingGate {
				return true
			}
		}
		return false
	})

	// The parked activation is not a candidate and is never re-dispatched,
	// across several fast reconcile passes.
	cands, err := f.mgr.CandidatesForController("c1", 10)
	if err != nil {
		t.Fatalf("CandidatesForController: %v", err)
	}
	if len(cands) != 0 {
		t.Fatalf("parked activation returned %d candidates, want 0", len(cands))
	}
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if n := len(f.exec.started); n > 1 {
			t.Fatalf("parked activation re-dispatched: %d executor starts", n)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := len(f.workflowInstance(t, wf).Attempts); got != 1 {
		t.Fatalf("attempts = %d, want exactly the one parked attempt", got)
	}
}

// TestControllerRunnerRestartResumesWithoutDuplicateDispatch proves a
// supervisor restart on the same workflow root resumes leadership and
// continues progress without duplicating an already-running attempt: the
// surviving live attempt from the old supervisor is stale for selection and
// only the new workflow is dispatched.
var debugRestart = os.Getenv("DEBUG_RESTART") != ""

// waitForDeadline is waitFor with a caller-supplied deadline.
func waitForDeadline(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestControllerRunnerRestartResumesWithoutDuplicateDispatch(t *testing.T) {
	root := filepath.Join(t.TempDir(), "wfroot")
	store := workflowcontroller.NewStore(root)

	fa := newRunnerFixture(t, "runner-restart-a", root, 4, 4, false)
	wf1 := fa.addWorkflow(t, "restart-1", "c1", "")
	fa.enableController(t, "c1", 4)
	waitFor(t, "first supervisor dispatched wf1", func() bool { return len(fa.exec.started) >= 1 })
	fa.waitRunning(t, wf1)

	// The old supervisor "dies": its in-flight dispatch abandons (the
	// attempt stays durably live) and its leader loop stops.
	fa.exec.abandon.Store(true)
	fa.exec.finish()
	fa.sup.stopControllerLoops()
	if rec, _, err := store.Get("c1"); err != nil || rec.Leader != nil {
		t.Fatalf("lease not released on restart: rec=%+v err=%v", rec, err)
	}

	// A new supervisor on the same root recovers state, re-acquires
	// leadership, and continues progress.
	fb := newRunnerFixture(t, "runner-restart-b", root, 4, 4, false)
	wf2 := fb.addWorkflow(t, "restart-2", "c1", "")
	fb.enableController(t, "c1", 4)
	if rec := waitForControllerLeader(t, store, "c1", func(rec workflowcontroller.ControllerRecord) bool {
		return rec.Leader.OwnerID == fb.sup.supervisorIdentity()
	}); rec.Leader.OwnerEpoch != 2 {
		t.Fatalf("restarted leader epoch = %d, want 2", rec.Leader.OwnerEpoch)
	}

	// The new workflow dispatches under the new supervisor; the surviving
	// attempt is never duplicated.
	// The manager's change notification is a coalescing hint (it may be
	// dropped when commits land during a pass), so continued progress after
	// the restart is bounded by the runner's 5s anti-entropy refresh rather
	// than instant signal delivery.
	waitForDeadline(t, 12*time.Second, "restarted supervisor dispatched wf2", func() bool {
		return len(fb.exec.started) >= 1
	})
	fb.waitRunning(t, wf2)
	inst1 := fb.workflowInstance(t, wf1)
	if got := len(inst1.Attempts); got != 1 {
		t.Fatalf("surviving workflow has %d attempts after restart, want exactly 1 (no duplicate dispatch)", got)
	}
	if st := inst1.Attempts[0].Status; st != workflow.AttemptRunning && st != workflow.AttemptStarting {
		t.Fatalf("surviving attempt status = %q, want still live", st)
	}
	if got := len(fb.exec.started); got != 1 {
		t.Fatalf("restarted supervisor dispatched %d workflows, want only the new one", got)
	}
}

// TestControllerRunnerCollidingRuntimeIDsIndependentProgress proves two
// supervisors whose runtimes both expose the colliding rt_1 id do not
// cross-correlate in-flight views: each supervisor's runner dispatches and
// tracks only its own controller's workflow, and both progress concurrently.
func TestControllerRunnerCollidingRuntimeIDsIndependentProgress(t *testing.T) {
	root := filepath.Join(t.TempDir(), "wfroot")
	f1 := newRunnerFixture(t, "runner-rt-a", root, 2, 4, true)
	f2 := newRunnerFixture(t, "runner-rt-b", root, 2, 4, true)
	f2.controllerID = "c2"
	wf1 := f1.addWorkflow(t, "rt-1", "c1", "")
	wf2 := f2.addWorkflow(t, "rt-2", "c2", "")
	f1.enableController(t, "c1", 1)
	f2.enableController(t, "c2", 1)

	waitFor(t, "both supervisors dispatched their own workflow", func() bool {
		return f1.sup.activeRuntimeCount() == 1 && f2.sup.activeRuntimeCount() == 1
	})

	// Both runtimes exist under the colliding id rt_1.
	for _, f := range []*runnerFixture{f1, f2} {
		rts := f.sup.listRuntimes()
		if len(rts) != 1 || rts[0]["runtime_id"] != "rt_1" {
			t.Fatalf("runtime list = %+v, want exactly one runtime with the colliding id rt_1", rts)
		}
	}

	// Each attempt records its own workflow identity with the colliding
	// runtime id, and neither supervisor's runner touches the other's
	// workflow across several reconcile passes.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		for _, tc := range []struct {
			f  *runnerFixture
			wf string
		}{{f1, wf1}, {f2, wf2}} {
			att := tc.f.runningAttempts(t, tc.wf)
			if len(att) != 1 {
				t.Fatalf("workflow %s has %d live attempts, want exactly its own 1", tc.wf, len(att))
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, tc := range []struct {
		f  *runnerFixture
		wf string
	}{{f1, wf1}, {f2, wf2}} {
		att := tc.f.workflowInstance(t, tc.wf).Attempts
		if len(att) != 1 || att[0].Identity.RuntimeID != "rt_1" {
			t.Fatalf("workflow %s attempts = %+v, want one attempt identified as rt_1", tc.wf, att)
		}
	}

	// Each supervisor's in-flight view comes from its own candidate index;
	// after a refresh (what the runner's anti-entropy pass performs) both
	// live attempts are visible, keyed by full identity: two distinct
	// workflow ids with their own controller ids, no cross-workflow merge.
	if err := f1.mgr.RebuildCandidateIndex(f1.sup.supervisorIdentity()); err != nil {
		t.Fatalf("RebuildCandidateIndex: %v", err)
	}
	live := f1.mgr.LiveAttempts()
	byWorkflow := map[string]string{}
	for _, la := range live {
		byWorkflow[string(la.Identity.WorkflowID)] = la.ControllerID
	}
	if byWorkflow[wf1] != "c1" || byWorkflow[wf2] != "c2" {
		t.Fatalf("live attempts after refresh = %+v, want wf1 under c1 and wf2 under c2 (independent identities)", byWorkflow)
	}

	// Releasing one supervisor's runtime does not disturb the other's
	// progress.
	_ = f1.sup.cancelRuntime("rt_1")
	waitFor(t, "wf1 attempt terminal", func() bool {
		return len(f1.runningAttempts(t, wf1)) == 0
	})
	if got := len(f2.runningAttempts(t, wf2)); got != 1 {
		t.Fatalf("wf2 live attempts after wf1 termination = %d, want its own attempt untouched", got)
	}
}

// TestControllerRunnerCapacityBlockedEventDedup proves capacity_blocked
// controller events are appended once per reason change: many blocked passes
// under one reason append a single event, a reason change local→tree appends
// the second, and a successful dispatch clears the block with one
// capacity_cleared event.
func TestControllerRunnerCapacityBlockedEventDedup(t *testing.T) {
	f := newRunnerFixture(t, "runner-cap-event", "", 1, 2, true)
	wf := f.addWorkflow(t, "cap-event-1", "c1", "")
	// Occupy the only local slot; the tree budget keeps one free slot so the
	// first block reason is local.
	res, err := f.sup.spawn(SpawnParams{Prompt: "occupy", Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("occupy spawn: %v", err)
	}
	f.enableController(t, "c1", 5)

	waitBlocked := func(reason string) {
		t.Helper()
		waitFor(t, "capacity blocked as "+reason, func() bool {
			rec, ok, err := f.cstore.Get("c1")
			return err == nil && ok && rec.CapacityBlocked == reason
		})
	}
	capacityEvents := func(t *testing.T) (blocked []string, cleared int) {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(f.cstore.Root(), "controllers", "c1", "events.ndjson"))
		if err != nil {
			t.Fatalf("read controller events: %v", err)
		}
		for _, line := range strings.Split(strings.TrimRight(string(data), "\n"), "\n") {
			var e workflowcontroller.ControllerEvent
			if err := json.Unmarshal([]byte(line), &e); err != nil {
				t.Fatalf("unmarshal event: %v", err)
			}
			switch e.Kind {
			case workflowcontroller.EventCapacityBlocked:
				blocked = append(blocked, e.BlockedReason)
			case workflowcontroller.EventCapacityCleared:
				cleared++
			}
		}
		return blocked, cleared
	}

	waitBlocked("local")
	// Several fast passes later, the log still holds exactly one local
	// capacity_blocked event (deduped across passes).
	time.Sleep(200 * time.Millisecond)
	blocked, _ := capacityEvents(t)
	if len(blocked) != 1 || blocked[0] != "local" {
		t.Fatalf("capacity_blocked events = %v, want exactly [local]", blocked)
	}
	if got := len(f.runningAttempts(t, wf)); got != 0 {
		t.Fatalf("workflow dispatched despite exhausted admission")
	}

	// Exhaust the tree budget too: the composed capacity changed, so the
	// capacity-change signal fires (the budget notifier only announces
	// releases) and the next pass re-attempts dispatch, changing the reason
	// to tree and appending the second event.
	token, err := f.sup.acquireTreeAdmission()
	if err != nil {
		t.Fatalf("exhaust tree: %v", err)
	}
	f.sup.signalCapacityChange()
	waitBlocked("tree")
	blocked, _ = capacityEvents(t)
	if len(blocked) != 2 || blocked[0] != "local" || blocked[1] != "tree" {
		t.Fatalf("capacity_blocked events = %v, want [local tree]", blocked)
	}

	// Free everything: the next dispatch succeeds, clears the block, and
	// appends exactly one capacity_cleared event.
	f.sup.treeBudgetMu.Lock()
	f.sup.treeBudget.Release(token)
	f.sup.treeBudgetMu.Unlock()
	_ = f.sup.cancelRuntime(res.RuntimeID)
	f.waitRunning(t, wf)
	waitFor(t, "capacity cleared", func() bool {
		rec, ok, err := f.cstore.Get("c1")
		return err == nil && ok && rec.CapacityBlocked == ""
	})
	blocked, cleared := capacityEvents(t)
	if len(blocked) != 2 {
		t.Fatalf("capacity_blocked events = %v, want still [local tree]", blocked)
	}
	if cleared != 1 {
		t.Fatalf("capacity_cleared events = %d, want exactly 1", cleared)
	}
}
