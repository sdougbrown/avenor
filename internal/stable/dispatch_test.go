package stable

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/admission"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/workflow"
	"github.com/sdougbrown/avenor/internal/workflowcontroller"
)

// dispatchTemplateJSON returns a one-node run template whose entry node is
// auto-dispatched to controllerID under concurrencyKey.
func dispatchTemplateJSON(t *testing.T, templateID, controllerID, concurrencyKey string) []byte {
	return dispatchTemplateJSONTTL(t, templateID, controllerID, concurrencyKey, 900)
}

func dispatchTemplateJSONTTL(t *testing.T, templateID, controllerID, concurrencyKey string, ttlSeconds int) []byte {
	policy := map[string]any{"mode": "auto", "controller_id": controllerID}
	if concurrencyKey != "" {
		policy["concurrency_key"] = concurrencyKey
	}
	template := map[string]any{
		"schema_version":   1,
		"template_id":      templateID,
		"template_version": "1",
		"entry_nodes":      []string{"start"},
		"nodes": []any{map[string]any{
			"id":           "start",
			"action":       map[string]any{"type": "run", "prompt": "do the thing"},
			"dispatch":     policy,
			"retry_policy": map[string]any{"max_attempts": 3, "exhaustion": "block"},
		}},
		"terminal_outcomes":    []string{"done"},
		"default_lease_policy": map[string]any{"ttl_seconds": ttlSeconds},
	}
	return mustJSON(t, template)
}

// mustJSON marshals v or fails the test.
func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}

// newDispatchSupervisor builds a supervisor with a workflow root, registers
// the dispatch template, instantiates it, and creates + leases controller c1.
type dispatchFixture struct {
	sup        *Supervisor
	mgr        *workflow.Manager
	cstore     *workflowcontroller.ControllerStore
	wf         string
	activation string
	revision   int64
	leaseID    string
	ownerEpoch int64
	release    chan struct{}
	provider   *blockingAdmissionProvider
	failStart  chan struct{} // when non-nil, provider creation fails
}

func newDispatchFixture(t *testing.T, name string, maxRuntimes, maxTreeBudget int, concurrencyKey string) *dispatchFixture {
	return newDispatchFixtureTTL(t, name, maxRuntimes, maxTreeBudget, concurrencyKey, 900)
}

func newDispatchFixtureTTL(t *testing.T, name string, maxRuntimes, maxTreeBudget int, concurrencyKey string, ttlSeconds int) *dispatchFixture {
	t.Helper()
	sup := NewSupervisor(Config{
		ControlSocket:   newStableSocketPath(t, name),
		MaxRuntimes:     maxRuntimes,
		MaxTreeBudget:   maxTreeBudget,
		ShutdownTimeout: 0,
		WorkflowRoot:    filepath.Join(t.TempDir(), "wfroot"),
	})
	mgr, cstore, err := sup.workflowBarrierResult()
	if err != nil {
		t.Fatalf("workflow barrier: %v", err)
	}
	if _, err := mgr.WorkflowCreate(dispatchTemplateJSONTTL(t, "tmpl-dispatch-"+name, "c1", concurrencyKey, ttlSeconds)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	out, err := mgr.WorkflowInstantiate(mustJSON(t, map[string]string{"template_id": "tmpl-dispatch-" + name, "template_version": "1"}))
	if err != nil {
		t.Fatalf("WorkflowInstantiate: %v", err)
	}
	wf, ok := out.(map[string]any)["workflow_id"].(string)
	if !ok || wf == "" {
		t.Fatalf("instantiate result missing workflow_id: %#v", out)
	}
	if _, err := cstore.Create("c1", 10); err != nil {
		t.Fatalf("controller create: %v", err)
	}
	if _, err := cstore.SetDesiredState("c1", workflowcontroller.DesiredEnabled, "test fixture"); err != nil {
		t.Fatalf("controller enable: %v", err)
	}
	rec, ok, err := cstore.AcquireLease("c1", sup.supervisorIdentity())
	if err != nil || !ok {
		t.Fatalf("acquire leader lease: ok=%v err=%v", ok, err)
	}
	cands, err := mgr.CandidatesForController("c1", 1)
	if err != nil || len(cands) != 1 {
		t.Fatalf("candidates: n=%d err=%v", len(cands), err)
	}
	f := &dispatchFixture{
		sup:        sup,
		mgr:        mgr,
		cstore:     cstore,
		wf:         wf,
		activation: string(cands[0].Identity.ActivationID),
		revision:   cands[0].Revision,
		leaseID:    rec.Leader.LeaseID,
		ownerEpoch: rec.Leader.OwnerEpoch,
		release:    make(chan struct{}),
	}
	f.provider = &blockingAdmissionProvider{release: f.release}
	sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		if f.failStart != nil {
			<-f.failStart
			return nil, fmt.Errorf("intentional provider creation failure")
		}
		return f.provider, nil
	}
	t.Cleanup(func() {
		close(f.release)
		for _, rt := range sup.listRuntimes() {
			if id, ok := rt["runtime_id"].(string); ok {
				_ = sup.cancelRuntime(id)
			}
		}
		waitFor(t, "runtimes terminal at cleanup", func() bool { return sup.activeRuntimeCount() == 0 })
		_ = sup.broker.Stop()
		sup.stopReaper()
	})
	return f
}

func (f *dispatchFixture) dispatchRequest(selection *workflow.ExecutionSelection) DispatchRequest {
	return DispatchRequest{
		WorkflowID:       f.wf,
		NodeID:           "start",
		ActivationID:     f.activation,
		ExpectedRevision: f.revision,
		ControllerID:     "c1",
		LeaderLeaseID:    f.leaseID,
		OwnerEpoch:       f.ownerEpoch,
		Selection:        selection,
	}
}

func (f *dispatchFixture) treeActive(t *testing.T) int {
	t.Helper()
	active, _, _ := f.sup.treeBudgetStatusValues()
	return active
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestDispatchConsumesExactlyOneReservation proves one controller dispatch
// consumes exactly one tree slot and one local slot: no second acquire inside
// spawn.
func TestDispatchConsumesExactlyOneReservation(t *testing.T) {
	f := newDispatchFixture(t, "dispatch-one", 4, 4, "")
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != Dispatched {
		t.Fatalf("outcome = %s (%s), want dispatched", out.Kind, out.Detail)
	}
	waitFor(t, "runtime registration", func() bool { return f.sup.activeRuntimeCount() == 1 })
	if got := f.treeActive(t); got != 1 {
		t.Fatalf("tree slots active = %d, want exactly 1", got)
	}
	if got := f.sup.activeRuntimeCount(); got != 1 {
		t.Fatalf("active runtimes = %d, want 1", got)
	}
	f.sup.controlMu.Lock()
	outstanding := f.sup.outstandingReservations
	f.sup.controlMu.Unlock()
	if outstanding != 0 {
		t.Fatalf("outstanding reservations = %d, want 0 after conversion", outstanding)
	}
	if out.RuntimeID == "" {
		t.Fatal("dispatched outcome missing runtime id")
	}
}

// TestDispatchReservationReleasedOnPreStartFailure proves a provider-start
// failure releases the reservation, records the terminal attempt fact, and
// leaves capacity reclaimable.
func TestDispatchReservationReleasedOnPreStartFailure(t *testing.T) {
	f := newDispatchFixture(t, "dispatch-fail", 1, 2, "")
	f.failStart = make(chan struct{})
	close(f.failStart)
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != DispatchStartFailed {
		t.Fatalf("outcome = %s, want start_failed", out.Kind)
	}
	if got := f.treeActive(t); got != 0 {
		t.Fatalf("tree slots active = %d, want 0 after pre-start failure", got)
	}
	if got := f.sup.activeRuntimeCount(); got != 0 {
		t.Fatalf("active runtimes = %d, want 0", got)
	}
	f.sup.controlMu.Lock()
	outstanding := f.sup.outstandingReservations
	f.sup.controlMu.Unlock()
	if outstanding != 0 {
		t.Fatalf("reservation leaked: outstanding = %d", outstanding)
	}
	// Re-arm the provider so a plain spawn after the failure succeeds.
	f.failStart = nil
	if _, err := f.sup.spawn(SpawnParams{Prompt: "after failure", Dir: t.TempDir()}); err != nil {
		t.Fatalf("spawn after failed dispatch: %v", err)
	}
}

// TestDispatchCapacityBlockedLocal proves a local capacity denial is a
// no-event, no-revision-change result and the node stays a candidate.
func TestDispatchCapacityBlockedLocal(t *testing.T) {
	f := newDispatchFixture(t, "dispatch-local-full", 1, 4, "")
	// Occupy the only local slot with an ordinary spawn.
	if _, err := f.sup.spawn(SpawnParams{Prompt: "occupy", Dir: t.TempDir()}); err != nil {
		t.Fatalf("occupy spawn: %v", err)
	}
	snapBefore := f.workflowRevision(t)
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != DispatchCapacityBlocked || out.Source != "local" {
		t.Fatalf("outcome = %s source=%s, want capacity_blocked/local", out.Kind, out.Source)
	}
	if f.workflowRevision(t) != snapBefore {
		t.Fatal("capacity denial changed the workflow revision")
	}
	cands, err := f.mgr.CandidatesForController("c1", 10)
	if err != nil || len(cands) != 1 {
		t.Fatalf("node did not remain a candidate: n=%d err=%v", len(cands), err)
	}
}

// workflowRevision reads the workflow's current revision through inspect.
func (f *dispatchFixture) workflowRevision(t *testing.T) int64 {
	t.Helper()
	out, err := f.mgr.WorkflowStatus(f.wf)
	if err != nil {
		t.Fatalf("WorkflowStatus: %v", err)
	}
	rev, ok := out.(map[string]any)["revision"].(int64)
	if !ok {
		if n, ok := out.(map[string]any)["revision"].(float64); ok {
			return int64(n)
		}
		t.Fatalf("status missing revision: %#v", out)
	}
	return rev
}

// TestDispatchCapacityBlockedTree proves a tree-budget denial reports source
// tree and leaves no workflow state behind.
func TestDispatchCapacityBlockedTree(t *testing.T) {
	f := newDispatchFixture(t, "dispatch-tree-full", 4, 1, "")
	// Exhaust the one tree slot directly (simulating another process).
	if _, err := f.sup.acquireTreeAdmission(); err != nil {
		t.Fatalf("exhaust tree: %v", err)
	}
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != DispatchCapacityBlocked || out.Source != "tree" {
		t.Fatalf("outcome = %s source=%s, want capacity_blocked/tree", out.Kind, out.Source)
	}
	if f.workflowRevision(t) != f.revision {
		t.Fatal("tree capacity denial changed the workflow revision")
	}
}

// TestDispatchStaleCandidateReleasesReservation proves a stale candidate
// releases its reservation and appends no events.
func TestDispatchStaleCandidateReleasesReservation(t *testing.T) {
	f := newDispatchFixture(t, "dispatch-stale", 4, 4, "")
	req := f.dispatchRequest(nil)
	req.ExpectedRevision = f.revision + 100
	out, err := f.sup.dispatchWorkflowNode(t.Context(), req)
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != DispatchStale {
		t.Fatalf("outcome = %s, want stale", out.Kind)
	}
	f.sup.controlMu.Lock()
	outstanding := f.sup.outstandingReservations
	f.sup.controlMu.Unlock()
	if outstanding != 0 {
		t.Fatalf("reservation leaked on stale: outstanding = %d", outstanding)
	}
	if got := f.treeActive(t); got != 0 {
		t.Fatalf("tree slots active = %d, want 0", got)
	}
}

// TestDispatchNotLeaderReleasesReservation proves a lost/expired leader lease
// or disabled controller yields not_leader with no leaked slot.
func TestDispatchNotLeaderReleasesReservation(t *testing.T) {
	f := newDispatchFixture(t, "dispatch-not-leader", 4, 4, "")
	req := f.dispatchRequest(nil)
	req.OwnerEpoch = f.ownerEpoch + 1
	out, err := f.sup.dispatchWorkflowNode(t.Context(), req)
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != DispatchNotLeader {
		t.Fatalf("outcome = %s, want not_leader", out.Kind)
	}
	// Disabled controller also reports not_leader.
	if _, err := f.cstore.SetDesiredState("c1", workflowcontroller.DesiredDisabled, "test"); err != nil {
		t.Fatalf("disable: %v", err)
	}
	rec, ok, err := f.cstore.Get("c1")
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if rec.Leader == nil {
		// Disabling released the live lease: dispatching with the now-gone
		// lease pair must still report not_leader.
		req2 := f.dispatchRequest(nil)
		out2, err := f.sup.dispatchWorkflowNode(t.Context(), req2)
		if err != nil {
			t.Fatalf("dispatchWorkflowNode (disabled): %v", err)
		}
		if out2.Kind != DispatchNotLeader {
			t.Fatalf("outcome = %s, want not_leader for disabled controller", out2.Kind)
		}
	} else {
		req2 := f.dispatchRequest(nil)
		req2.LeaderLeaseID = rec.Leader.LeaseID
		req2.OwnerEpoch = rec.Leader.OwnerEpoch
		out2, err := f.sup.dispatchWorkflowNode(t.Context(), req2)
		if err != nil {
			t.Fatalf("dispatchWorkflowNode (disabled): %v", err)
		}
		if out2.Kind != DispatchNotLeader {
			t.Fatalf("outcome = %s, want not_leader for disabled controller", out2.Kind)
		}
	}
	f.sup.controlMu.Lock()
	outstanding := f.sup.outstandingReservations
	f.sup.controlMu.Unlock()
	if outstanding != 0 {
		t.Fatalf("reservation leaked on not_leader: outstanding = %d", outstanding)
	}
}

// TestDispatchPanicReleasesReservation proves a panicking executor start
// still releases the reservation through the panic unwinding.
func TestDispatchPanicReleasesReservation(t *testing.T) {
	f := newDispatchFixture(t, "dispatch-panic", 4, 4, "")
	f.sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) {
		panic("provider start panic")
	}
	var out DispatchOutcome
	var err error
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic to propagate from executor start")
			}
		}()
		out, err = f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	}()
	_ = out
	_ = err
	f.sup.controlMu.Lock()
	outstanding := f.sup.outstandingReservations
	f.sup.controlMu.Unlock()
	if outstanding != 0 {
		t.Fatalf("reservation leaked on panic: outstanding = %d", outstanding)
	}
	if got := f.treeActive(t); got != 0 {
		t.Fatalf("tree slots active = %d, want 0 after panic", got)
	}
}

// TestDispatchKeyHeldAcrossControllersAndManual proves one concurrency key
// admits exactly one live attempt across two controller IDs and a manual
// actor on one shared root.
func TestDispatchKeyHeldAcrossControllersAndManual(t *testing.T) {
	f := newDispatchFixture(t, "dispatch-key", 4, 4, "deploys")
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	if out.Kind != Dispatched {
		t.Fatalf("first outcome = %s (%s), want dispatched", out.Kind, out.Detail)
	}
	waitFor(t, "first runtime live", func() bool { return f.sup.activeRuntimeCount() == 1 })

	// A second supervisor on the same root (separate store handle, like a
	// separate process) must see the key as held.
	sup2 := NewSupervisor(Config{
		ControlSocket:   newStableSocketPath(t, "dispatch-key-2"),
		MaxRuntimes:     4,
		MaxTreeBudget:   4,
		ShutdownTimeout: 0,
		WorkflowRoot:    f.sup.config.WorkflowRoot,
	})
	mgr2, cstore2, err := sup2.workflowBarrierResult()
	if err != nil {
		t.Fatalf("second barrier: %v", err)
	}
	t.Cleanup(func() { _ = sup2.broker.Stop(); sup2.stopReaper() })
	if _, err := cstore2.Create("c2", 10); err != nil {
		t.Fatalf("controller c2 create: %v", err)
	}
	if _, err := cstore2.SetDesiredState("c2", workflowcontroller.DesiredEnabled, "test fixture"); err != nil {
		t.Fatalf("controller c2 enable: %v", err)
	}
	rec2, ok, err := cstore2.AcquireLease("c2", sup2.supervisorIdentity())
	if err != nil || !ok {
		t.Fatalf("c2 lease: ok=%v err=%v", ok, err)
	}
	// Instantiate a second workflow with the same key on the shared root.
	if _, err := mgr2.WorkflowCreate(dispatchTemplateJSONTTL(t, "tmpl-dispatch-key-2", "c2", "deploys", 1)); err != nil {
		t.Fatalf("WorkflowCreate 2: %v", err)
	}
	out2, err := mgr2.WorkflowInstantiate(mustJSON(t, map[string]string{"template_id": "tmpl-dispatch-key-2", "template_version": "1"}))
	if err != nil {
		t.Fatalf("WorkflowInstantiate 2: %v", err)
	}
	wf2 := out2.(map[string]any)["workflow_id"].(string)
	cands2, err := mgr2.CandidatesForController("c2", 1)
	if err != nil || len(cands2) != 1 {
		t.Fatalf("candidates 2: n=%d err=%v", len(cands2), err)
	}

	// Controller dispatch on the held key bounces without events.
	res2, err := sup2.reserveAdmission()
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	_, beginErr := mgr2.BeginDispatch(workflow.BeginDispatchRequest{
		WorkflowID:       workflow.WorkflowID(wf2),
		NodeID:           "start",
		ActivationID:     cands2[0].Identity.ActivationID,
		ExpectedRevision: cands2[0].Revision,
		ControllerID:     "c2",
		LeaderLeaseID:    rec2.Leader.LeaseID,
	})
	if !errors.Is(beginErr, workflow.ErrConcurrencyKeyHeld) {
		t.Fatalf("BeginDispatch error = %v, want ErrConcurrencyKeyHeld", beginErr)
	}
	res2.Release()

	// A manual actor on the same key is rejected at the start boundary too.
	// Its claim leaves the activation leased, so the lease-expiry sweep below
	// must recover it before the key releases.
	claimOut, err := mgr2.WorkflowCommand(wf2, mustJSON(t, map[string]any{
		"op": "claim", "node_id": "start", "activation_id": string(cands2[0].Identity.ActivationID), "actor": "manual-1",
	}))
	if err != nil {
		t.Fatalf("manual claim: %v", err)
	}
	claim := claimOut.(map[string]any)
	startPayload := map[string]any{
		"op":            "start",
		"node_id":       "start",
		"activation_id": string(cands2[0].Identity.ActivationID),
		"lease_id":      claim["lease_id"],
		"owner_token":   claim["owner_token"],
	}
	sup2.newProviderFunc = f.sup.newProviderFunc
	if _, err := mgr2.WorkflowCommand(wf2, mustJSON(t, startPayload)); !errors.Is(err, workflow.ErrConcurrencyKeyHeld) {
		t.Fatalf("manual start error = %v, want ErrConcurrencyKeyHeld", err)
	}

	// After the first attempt terminates, the key releases and the second
	// workflow dispatches. The manual claim's stranded lease is recovered by
	// the same sweep.
	_ = f.sup.cancelRuntime(out.RuntimeID)
	waitFor(t, "first runtime terminal", func() bool { return f.sup.activeRuntimeCount() == 0 })
	waitFor(t, "manual lease recovery re-arms the candidate", func() bool {
		if _, err := mgr2.ResumeAwaitingChildren(); err != nil {
			return false
		}
		if err := mgr2.RebuildCandidateIndex("test"); err != nil {
			return false
		}
		cands2, err = mgr2.CandidatesForController("c2", 1)
		return err == nil && len(cands2) == 1
	})
	out3, err := sup2.dispatchWorkflowNode(t.Context(), DispatchRequest{
		WorkflowID:       wf2,
		NodeID:           "start",
		ActivationID:     string(cands2[0].Identity.ActivationID),
		ExpectedRevision: cands2[0].Revision,
		ControllerID:     "c2",
		LeaderLeaseID:    rec2.Leader.LeaseID,
		OwnerEpoch:       rec2.Leader.OwnerEpoch,
	})
	if err != nil {
		t.Fatalf("second dispatch: %v", err)
	}
	if out3.Kind != Dispatched {
		t.Fatalf("second outcome = %s (%s), want dispatched", out3.Kind, out3.Detail)
	}
}

// TestDispatchPinnedSelectionInheritance proves a re-armed candidate keeps
// its pinned selection and conflicting selections are rejected.
func TestDispatchPinnedSelectionInheritance(t *testing.T) {
	f := newDispatchFixture(t, "dispatch-pin", 4, 4, "")
	sel := &workflow.ExecutionSelection{Backend: "codex", Agent: "agent-a", Model: "m1"}
	// Fail the first start pre-provider so the activation re-arms with the
	// selection pinned.
	f.failStart = make(chan struct{})
	close(f.failStart)
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(sel))
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if out.Kind != DispatchStartFailed {
		t.Fatalf("outcome = %s (%s), want start_failed", out.Kind, out.Detail)
	}
	waitFor(t, "retry re-arm", func() bool {
		if err := f.mgr.RebuildCandidateIndex("test"); err != nil {
			return false
		}
		cands, err := f.mgr.CandidatesForController("c1", 10)
		return err == nil && len(cands) == 1
	})
	cands, err := f.mgr.CandidatesForController("c1", 1)
	if err != nil || len(cands) != 1 {
		t.Fatalf("re-armed candidates: n=%d err=%v", len(cands), err)
	}
	// Conflicting selection at BeginDispatch is rejected.
	conflict := &workflow.ExecutionSelection{Backend: "codex", Agent: "agent-b", Model: "m2"}
	if _, err := f.mgr.BeginDispatch(workflow.BeginDispatchRequest{
		WorkflowID:       workflow.WorkflowID(f.wf),
		NodeID:           "start",
		ActivationID:     cands[0].Identity.ActivationID,
		ExpectedRevision: cands[0].Revision,
		ControllerID:     "c1",
		LeaderLeaseID:    f.leaseID,
		Selection:        conflict,
	}); !errors.Is(err, workflow.ErrSelectionConflict) {
		t.Fatalf("conflicting BeginDispatch error = %v, want ErrSelectionConflict", err)
	}
	// A nil selection inherits the pinned one; the reservation is released
	// unused because the executor start is not performed here.
	res, err := f.sup.reserveAdmission()
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	begin, err := f.mgr.BeginDispatch(workflow.BeginDispatchRequest{
		WorkflowID:       workflow.WorkflowID(f.wf),
		NodeID:           "start",
		ActivationID:     cands[0].Identity.ActivationID,
		ExpectedRevision: cands[0].Revision,
		ControllerID:     "c1",
		LeaderLeaseID:    f.leaseID,
	})
	if err != nil {
		res.Release()
		t.Fatalf("inheriting BeginDispatch: %v", err)
	}
	if begin.Selection == nil || begin.Selection.Agent != "agent-a" {
		res.Release()
		t.Fatalf("inherited selection = %+v, want pinned agent-a", begin.Selection)
	}
	res.Release()
}

// TestDispatchCrashAfterBeginDispatchRecovers proves a process death after
// the attempt intent leaves an expiring lease that the lease-expiry sweep
// recovers without creating a second live attempt.
func TestDispatchCrashAfterBeginDispatchRecovers(t *testing.T) {
	f := newDispatchFixtureTTL(t, "dispatch-crash", 4, 4, "", 1)
	res, err := f.sup.reserveAdmission()
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
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
	// Simulate the process dying before the executor starts: drop the
	// reservation on the floor (admission's PID reaper reclaims it) and never
	// finalize.
	_ = res
	_ = begin.AttemptID

	// The lease-expiry recovery sweep expires the stranded lease.
	var expired bool
	waitFor(t, "lease expiry sweep", func() bool {
		if _, err := f.mgr.ResumeAwaitingChildren(); err != nil {
			return false
		}
		if err := f.mgr.RebuildCandidateIndex("test"); err != nil {
			return false
		}
		cands, err := f.mgr.CandidatesForController("c1", 10)
		if err != nil {
			return false
		}
		for _, c := range cands {
			if c.Identity.ActivationID == workflow.ActivationID(f.activation) {
				expired = true
			}
		}
		return expired
	})

	// A replacement dispatch on the recovered activation appends a new
	// attempt to the same activation instead of a second live one.
	cands, err := f.mgr.CandidatesForController("c1", 10)
	if err != nil || len(cands) != 1 {
		t.Fatalf("recovered candidates: n=%d err=%v", len(cands), err)
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
	// Exactly one live runtime exists — the replacement attempt. The
	// abandoned reservation's tree token stays held until admission's PID
	// reaper reclaims it (here: never, same process), which is the documented
	// crash behavior: no second live attempt, capacity reclaimable by the
	// reaper, and no completion inferred from the released reservation.
	waitFor(t, "single live runtime", func() bool { return f.sup.activeRuntimeCount() == 1 })
	if got := f.treeActive(t); got < 1 {
		t.Fatalf("tree slots active = %d, want at least the replacement's 1", got)
	}
}

// TestReservationDegradedLocalOnlyMode proves a reservation without a tree
// budget still holds exactly the local slot.
func TestReservationDegradedLocalOnlyMode(t *testing.T) {
	// A supervisor whose Avenor-owned runtime-state directory cannot be
	// created runs in degraded local-only mode. Point HOME at a regular file
	// so creating ~/.avenor/sockets fails.
	unwritable := filepath.Join(t.TempDir(), "home-file")
	if err := os.WriteFile(unwritable, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", unwritable)
	var sup *Supervisor
	stderr := captureStderr(t, func() {
		sup = NewSupervisor(Config{
			ControlSocket:   filepath.Join(t.TempDir(), "control.sock"),
			MaxRuntimes:     2,
			MaxTreeBudget:   8,
			ShutdownTimeout: 0,
			WorkflowRoot:    filepath.Join(t.TempDir(), "wfroot"),
		})
	})
	if !strings.Contains(stderr, "degraded local-only mode") {
		t.Fatalf("stderr = %q, want degraded-mode warning", stderr)
	} // matches supervisor's "tree budget unavailable; using degraded local-only mode"
	t.Cleanup(func() { _ = sup.broker.Stop(); sup.stopReaper() })
	if sup.treeBudget != nil {
		t.Fatal("expected degraded mode (no tree budget)")
	}
	res, err := sup.reserveAdmission()
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	sup.controlMu.Lock()
	outstanding := sup.outstandingReservations
	sup.controlMu.Unlock()
	if outstanding != 1 {
		t.Fatalf("outstanding = %d, want 1", outstanding)
	}
	res.Release()
	res.Release() // idempotent
	sup.controlMu.Lock()
	outstanding = sup.outstandingReservations
	sup.controlMu.Unlock()
	if outstanding != 0 {
		t.Fatalf("outstanding after double release = %d, want 0", outstanding)
	}
}

// TestReservationHoldsLocalSlotAgainstSpawn proves outstanding reservations
// count against the local limit.
func TestReservationHoldsLocalSlotAgainstSpawn(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket:   newStableSocketPath(t, "dispatch-hold"),
		MaxRuntimes:     1,
		MaxTreeBudget:   4,
		ShutdownTimeout: 0,
	})
	t.Cleanup(func() { _ = sup.broker.Stop(); sup.stopReaper() })
	res, err := sup.reserveAdmission()
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	t.Cleanup(res.Release)
	_, err = sup.spawn(SpawnParams{Prompt: "blocked", Dir: t.TempDir()})
	if err == nil {
		t.Fatal("spawn should be locally capacity-blocked while a reservation holds the slot")
	}
	var ce *admission.CapacityError
	if !errors.As(err, &ce) || ce.Source != "local" {
		t.Fatalf("spawn error = %v, want local CapacityError", err)
	}
}

// TestSpawnConsumesReservationOnSuccess proves the ordinary spawn path
// converts its reservation into the registered runtime (tree token included).
func TestSpawnConsumesReservationOnSuccess(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket:   newStableSocketPath(t, "dispatch-spawn-convert"),
		MaxRuntimes:     2,
		MaxTreeBudget:   2,
		ShutdownTimeout: 0,
	})
	f := &dispatchFixture{release: make(chan struct{})}
	provider := &blockingAdmissionProvider{release: f.release}
	sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) { return provider, nil }
	t.Cleanup(func() {
		close(f.release)
		_ = sup.broker.Stop()
		sup.stopReaper()
	})
	if _, err := sup.spawn(SpawnParams{Prompt: "hello", Dir: t.TempDir()}); err != nil {
		t.Fatalf("spawn: %v", err)
	}
	waitFor(t, "runtime registration", func() bool { return sup.activeRuntimeCount() == 1 })
	if active, _, _ := sup.treeBudgetStatusValues(); active != 1 {
		t.Fatalf("tree slots active = %d, want 1 after successful spawn", active)
	}
	sup.controlMu.Lock()
	outstanding := sup.outstandingReservations
	sup.controlMu.Unlock()
	if outstanding != 0 {
		t.Fatalf("outstanding = %d, want 0 after conversion", outstanding)
	}
}
