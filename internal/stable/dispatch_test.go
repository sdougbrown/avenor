package stable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/admission"
	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/events"
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

// dispatchActionTemplateJSON returns a one-node template whose entry node
// runs the given action type, auto-dispatched to controllerID.
func dispatchActionTemplateJSON(t *testing.T, templateID, controllerID, actionType, filePath string) []byte {
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
		"default_lease_policy": map[string]any{"ttl_seconds": 900},
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
	return newDispatchFixtureTemplate(t, name, maxRuntimes, maxTreeBudget,
		dispatchTemplateJSONTTL(t, "tmpl-dispatch-"+name, "c1", concurrencyKey, ttlSeconds))
}

// newDispatchFixtureTemplate is newDispatchFixture with a caller-supplied
// template instead of the default auto run template.
func newDispatchFixtureTemplate(t *testing.T, name string, maxRuntimes, maxTreeBudget int, template []byte) *dispatchFixture {
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
	if _, err := mgr.WorkflowCreate(template); err != nil {
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
	// Disabled controller also reports not_leader — whether or not disable
	// released the live leader lease — and never leaks the reservation.
	if _, err := f.cstore.SetDesiredState("c1", workflowcontroller.DesiredDisabled, "test"); err != nil {
		t.Fatalf("disable: %v", err)
	}
	req2 := f.dispatchRequest(nil)
	out2, err := f.sup.dispatchWorkflowNode(t.Context(), req2)
	if err != nil {
		t.Fatalf("dispatchWorkflowNode (disabled): %v", err)
	}
	if out2.Kind != DispatchNotLeader {
		t.Fatalf("outcome = %s, want not_leader for disabled controller", out2.Kind)
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
	t.Cleanup(func() {
		// sup2's dispatched runtime blocks in the shared release-gated provider;
		// it must be canceled and driven to its terminal write before the
		// TempDir cleanup below removes the workflow root, or its final
		// attempt-termination write races the directory removal.
		for _, rt := range sup2.listRuntimes() {
			if id, ok := rt["runtime_id"].(string); ok {
				_ = sup2.cancelRuntime(id)
			}
		}
		waitFor(t, "sup2 runtimes terminal at cleanup", func() bool { return sup2.activeRuntimeCount() == 0 })
		_ = sup2.broker.Stop()
		sup2.stopReaper()
	})
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

// TestResolveAssignmentSelectionAutoApprove proves the assignment's
// auto_approve flag resolves into the pinned selection, including an
// auto_approve-only assignment (which is not an empty assignment).
func TestResolveAssignmentSelectionAutoApprove(t *testing.T) {
	sel, err := resolveAssignmentSelection(&workflow.Assignment{Backend: "codex", AutoApprove: true})
	if err != nil {
		t.Fatalf("resolveAssignmentSelection: %v", err)
	}
	if !sel.AutoApprove {
		t.Fatal("selection.AutoApprove = false, want true")
	}
	sel, err = resolveAssignmentSelection(&workflow.Assignment{AutoApprove: true})
	if err != nil {
		t.Fatalf("resolveAssignmentSelection(auto_approve only): %v", err)
	}
	if !sel.AutoApprove || sel.Backend != "" || sel.Agent != "" || sel.Model != "" {
		t.Fatalf("selection = %+v, want AutoApprove true with no identity", sel)
	}
}

// TestDispatchAssignmentAutoApproveRunsUnattended proves a dispatched node
// whose assignment declares auto_approve runs unattended: the runtime status
// surfaces auto_approve true, and a scripted permission.request event on that
// runtime resolves on its own — the CLI auto-answer path marks the claim
// resolved (source "avenor") and emits permission.response, so the request
// never stays pending until the claim timeout and no decider run is spawned
// (no permdec: label ever appears).
func TestDispatchAssignmentAutoApproveRunsUnattended(t *testing.T) {
	template := mustJSON(t, map[string]any{
		"schema_version":   1,
		"template_id":      "tmpl-dispatch-dispatch-auto-approve",
		"template_version": "1",
		"entry_nodes":      []string{"start"},
		"nodes": []any{map[string]any{
			"id":           "start",
			"action":       map[string]any{"type": "run", "prompt": "do the thing"},
			"assignment":   map[string]any{"auto_approve": true},
			"dispatch":     map[string]any{"mode": "auto", "controller_id": "c1"},
			"retry_policy": map[string]any{"max_attempts": 3, "exhaustion": "block"},
		}},
		"terminal_outcomes":    []string{"done"},
		"default_lease_policy": map[string]any{"ttl_seconds": 900},
	})
	f := newDispatchFixtureTemplate(t, "dispatch-auto-approve", 4, 4, template)
	requestID := "perm_req_auto"
	// Hold the worker prompt open after the permission request so the runtime
	// stays alive while the auto-answer is observed.
	gate := make(chan struct{})
	defer close(gate)
	var deciderSpawned atomic.Bool
	f.sup.newProviderFunc = func(startOpts runtime.StartOptions, _ string) (runtime.Provider, error) {
		if strings.HasPrefix(startOpts.Label, "permdec:") {
			deciderSpawned.Store(true)
		}
		return &stableScriptedProvider{attempt: -1, scripts: []stableScriptedAttempt{{
			sessionID: "ses_auto_worker",
			events: []stableScriptedEvent{
				{event: events.Event{
					Event:     "permission.request",
					SessionID: "ses_auto_worker",
					Fields: map[string]any{
						"request_id": requestID,
						"tool":       "bash",
						"command":    "rm -rf /tmp/build",
						"options":    []any{map[string]any{"optionId": "allow_once", "kind": "allow"}},
					},
				}},
				{release: gate},
			},
		}}}, nil
	}
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != Dispatched {
		t.Fatalf("outcome = %s (%s), want dispatched", out.Kind, out.Detail)
	}
	waitFor(t, "runtime registration", func() bool { return f.sup.activeRuntimeCount() == 1 })
	runtimes := f.sup.listRuntimes()
	if len(runtimes) != 1 {
		t.Fatalf("runtimes = %d, want 1", len(runtimes))
	}
	if got, ok := runtimes[0]["auto_approve"].(bool); !ok || !got {
		t.Fatalf("runtime auto_approve = %T %v, want bool true", runtimes[0]["auto_approve"], runtimes[0]["auto_approve"])
	}
	// The auto-approve runtime must resolve the pending request on its own:
	// the claim is marked resolved (no human, no decider) and the runtime
	// status clears pending_permission via the permission.response event.
	waitFor(t, "auto-resolved permission", func() bool {
		if f.sup.control.PermissionResolverState(out.RuntimeID, requestID) != control.PermissionResolverResolved {
			return false
		}
		status, err := f.sup.RuntimeStatus(out.RuntimeID)
		if err != nil {
			return false
		}
		rt, _ := status.(map[string]any)
		pending, _ := rt["pending_permission"].(bool)
		return !pending
	})
	if deciderSpawned.Load() {
		t.Fatal("a permdec: decider run was spawned on an auto-approve runtime, want none")
	}
}

// TestResolveAssignmentSelectionPermissionDecider proves the assignment's
// permission_policy resolves onto the pinned selection: the decider roster
// entry resolves to the decider identity with the entry's default thinking
// level (validated against the decider's own backend, like the node
// identity's level), a decider without a roster file is rejected, and
// auto_approve plus permission_policy are mutually exclusive.
func TestResolveAssignmentSelectionPermissionDecider(t *testing.T) {
	rosterPath := writeStage5Roster(t, t.TempDir(), `{
		"worker":  {"backend": "pi", "agent": "worker", "model": "m"},
		"rm-guard": {"backend": "pi", "agent": "rm-guard", "model": "guard-m"},
		"thinking-guard": {"backend": "pi", "agent": "guard", "model": "m", "thinking": "high"},
		"bad-thinking": {"backend": "agy", "agent": "guard", "model": "m", "thinking": "low"}
	}`)
	sel, err := resolveAssignmentSelection(&workflow.Assignment{
		RosterFile:       rosterPath,
		RosterEntry:      "worker",
		PermissionPolicy: &workflow.PermissionPolicy{Decider: "rm-guard"},
	})
	if err != nil {
		t.Fatalf("resolveAssignmentSelection: %v", err)
	}
	if sel.PermissionDecider == nil || sel.PermissionDecider.Entry != "rm-guard" ||
		sel.PermissionDecider.Agent != "rm-guard" || sel.PermissionDecider.Backend != "pi" {
		t.Fatalf("selection.PermissionDecider = %+v, want rm-guard/pi", sel.PermissionDecider)
	}
	if sel.Agent != "worker" || sel.RosterDigest == "" {
		t.Fatalf("selection identity = %+v, want the worker entry and a roster digest", sel)
	}
	sel, err = resolveAssignmentSelection(&workflow.Assignment{
		RosterFile:       rosterPath,
		PermissionPolicy: &workflow.PermissionPolicy{Decider: "thinking-guard"},
	})
	if err != nil {
		t.Fatalf("resolveAssignmentSelection(decider thinking): %v", err)
	}
	if sel.PermissionDecider == nil || sel.PermissionDecider.Thinking != "high" {
		t.Fatalf("selection.PermissionDecider = %+v, want the entry's high thinking level", sel.PermissionDecider)
	}
	if _, err := resolveAssignmentSelection(&workflow.Assignment{
		RosterFile:       rosterPath,
		PermissionPolicy: &workflow.PermissionPolicy{Decider: "bad-thinking"},
	}); err == nil || !strings.Contains(err.Error(), `does not support parameter "thinking"`) {
		t.Fatalf("resolveAssignmentSelection(bad thinking) error = %v, want unsupported thinking", err)
	}
	if _, err := resolveAssignmentSelection(&workflow.Assignment{
		Backend:          "pi",
		PermissionPolicy: &workflow.PermissionPolicy{Decider: "rm-guard"},
	}); err == nil || !strings.Contains(err.Error(), "requires roster_file") {
		t.Fatalf("resolveAssignmentSelection(no roster) error = %v, want requires roster_file", err)
	}
	if _, err := resolveAssignmentSelection(&workflow.Assignment{
		RosterFile:       rosterPath,
		PermissionPolicy: &workflow.PermissionPolicy{Decider: "missing"},
	}); err == nil || !strings.Contains(err.Error(), "permission_policy.decider") {
		t.Fatalf("resolveAssignmentSelection(unknown decider) error = %v, want permission_policy.decider", err)
	}
	if _, err := resolveAssignmentSelection(&workflow.Assignment{
		RosterFile:       rosterPath,
		AutoApprove:      true,
		PermissionPolicy: &workflow.PermissionPolicy{Decider: "rm-guard"},
	}); err == nil || !strings.Contains(err.Error(), "cannot declare both auto_approve and permission_policy") {
		t.Fatalf("resolveAssignmentSelection(both) error = %v, want mutual exclusion", err)
	}
}

// TestResolveAssignmentSelectionRosterFileOnlyRejected proves a node
// assignment that sets roster_file without a roster_entry (and without a
// permission policy) is rejected at dispatch: the roster file remains an
// identity selector, so the spawn-selection rules fire and the assignment
// cannot silently run on the default backend.
func TestResolveAssignmentSelectionRosterFileOnlyRejected(t *testing.T) {
	rosterPath := writeStage5Roster(t, t.TempDir(), `{
		"worker": {"backend": "pi", "agent": "worker", "model": "m"}
	}`)
	_, err := resolveAssignmentSelection(&workflow.Assignment{RosterFile: rosterPath})
	if err == nil || !strings.Contains(err.Error(), "roster_file requires roster_entry") {
		t.Fatalf("resolveAssignmentSelection(roster file without entry) error = %v, want roster_file requires roster_entry", err)
	}
	if _, err := resolveAssignmentSelection(&workflow.Assignment{
		RosterFile:       rosterPath,
		PermissionPolicy: &workflow.PermissionPolicy{Decider: "worker"},
	}); err != nil {
		t.Fatalf("resolveAssignmentSelection(decider without roster entry): %v", err)
	}
}

// TestSpawnRejectsAutoApproveWithPermissionDecider pins the spawn guard: a
// runtime cannot carry both unattended permission policies — auto-approve
// resolves every request as allow, a permission decider routes requests to a
// judging agent — and the guard must fire before any provider work happens.
func TestSpawnRejectsAutoApproveWithPermissionDecider(t *testing.T) {
	sup := NewSupervisor(Config{
		ControlSocket: "/tmp/test-permdec-exclusive.sock",
		MaxRuntimes:   1,
	})
	_, err := sup.spawn(SpawnParams{
		Prompt:            "hello",
		Dir:               "/tmp",
		AutoApprove:       true,
		PermissionDecider: &workflow.PermissionDecider{Entry: "rm-guard"},
	})
	if err == nil || !strings.Contains(err.Error(), "auto_approve and permission deciders are mutually exclusive") {
		t.Fatalf("err = %v, want auto_approve/permission_decider mutual exclusion", err)
	}
	_, err = sup.spawn(SpawnParams{
		Prompt:                 "hello",
		Dir:                    "/tmp",
		AutoApprove:            true,
		PermissionModelDecider: &workflow.PermissionModelDecider{Provider: "groq", Model: "m"},
	})
	if err == nil || !strings.Contains(err.Error(), "auto_approve and permission deciders are mutually exclusive") {
		t.Fatalf("err = %v, want auto_approve/permission_model_decider mutual exclusion", err)
	}
	_, err = sup.spawn(SpawnParams{
		Prompt:                 "hello",
		Dir:                    "/tmp",
		PermissionDecider:      &workflow.PermissionDecider{Entry: "rm-guard"},
		PermissionModelDecider: &workflow.PermissionModelDecider{Provider: "groq", Model: "m"},
	})
	if err == nil || !strings.Contains(err.Error(), "permission_decider and permission_model_decider are mutually exclusive") {
		t.Fatalf("err = %v, want decider/decider_model mutual exclusion", err)
	}
	if got := sup.activeRuntimeCount(); got != 0 {
		t.Fatalf("activeRuntimeCount() = %d, want 0 after rejected spawns", got)
	}
}

// TestDispatchAssignmentPermissionDeciderRoutesRequest proves the routing
// half of the judgment-gate policy: a dispatched node whose assignment
// declares permission_policy spawns its runtime with no file permission
// handler, and a pending permission request on that runtime spawns the
// declared decider run (auto-approved so the decider itself cannot stall)
// while the request stays pending — never silently allowed.
func TestDispatchAssignmentPermissionDeciderRoutesRequest(t *testing.T) {
	rosterPath := writeStage5Roster(t, t.TempDir(), `{
		"worker":  {"backend": "pi", "agent": "worker", "model": "m"},
		"rm-guard": {"backend": "pi", "agent": "rm-guard", "model": "guard-m", "thinking": "high"}
	}`)
	template := mustJSON(t, dispatchPermdecTemplate(rosterPath, "tmpl-dispatch-dispatch-permdec"))
	f := newDispatchFixtureTemplate(t, "dispatch-permdec", 4, 4, template)
	requestID := "perm_req_1"
	var providerSeq int64
	// Capture the spawn dirs so the test can prove the decider runs in a
	// neutral scratch directory instead of the worker's workspace.
	var workerDir, deciderDir string
	f.sup.newProviderFunc = func(startOpts runtime.StartOptions, _ string) (runtime.Provider, error) {
		if strings.HasPrefix(startOpts.Label, "permdec:") {
			deciderDir = startOpts.Dir
		} else {
			workerDir = startOpts.Dir
		}
		n := atomic.AddInt64(&providerSeq, 1)
		return &stableScriptedProvider{attempt: -1, scripts: []stableScriptedAttempt{{
			sessionID: fmt.Sprintf("ses_permdec_%d", n),
			events: []stableScriptedEvent{{event: events.Event{
				Event:     "permission.request",
				SessionID: fmt.Sprintf("ses_permdec_%d", n),
				Fields: map[string]any{
					"request_id": requestID,
					"tool":       "bash",
					"command":    "rm -rf /tmp/build",
					"options":    []any{map[string]any{"optionId": "allow_once", "kind": "allow"}},
				},
			}, release: nil}},
		}}}, nil
	}
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != Dispatched {
		t.Fatalf("outcome = %s (%s), want dispatched", out.Kind, out.Detail)
	}
	// The request must surface as pending on the worker runtime, with the
	// declared decider, and no file permission handler may have been derived.
	// Wait on the runtime's pending_permission status rather than the active
	// runtime count: the scripted worker finishes its turn right after
	// emitting permission.request, so the runtime can tear down (leaving a
	// status tombstone that still reports the pending request) before an
	// active-count poll ever observes it.
	var worker map[string]any
	waitFor(t, "pending permission", func() bool {
		status, err := f.sup.RuntimeStatus(out.RuntimeID)
		if err != nil {
			return false
		}
		rt, _ := status.(map[string]any)
		if pending, _ := rt["pending_permission"].(bool); pending {
			worker = rt
			return true
		}
		return false
	})
	if got, _ := worker["permission_decider"].(string); got != "rm-guard" {
		t.Fatalf("worker permission_decider = %q, want rm-guard", got)
	}
	if worker["auto_approve"] != false {
		t.Fatalf("worker auto_approve = %v, want false", worker["auto_approve"])
	}

	// The declared decider run spawns for the pending request, under the
	// decider identity, auto-approved so the decider itself cannot stall.
	var decider map[string]any
	waitFor(t, "decider runtime", func() bool {
		runtimes := f.sup.listRuntimes()
		for _, rt := range runtimes {
			if label, _ := rt["label"].(string); strings.HasPrefix(label, "permdec:") {
				decider = rt
				return true
			}
		}
		return false
	})
	if got, _ := decider["agent"].(string); got != "rm-guard" {
		t.Fatalf("decider agent = %q, want rm-guard", got)
	}
	if got, _ := decider["auto_approve"].(bool); !got {
		t.Fatalf("decider auto_approve = %v, want true", decider["auto_approve"])
	}
	if got, _ := decider["effective_model"].(string); got != "guard-m" {
		t.Fatalf("decider model = %q, want guard-m", got)
	}
	if got, _ := decider["thinking"].(string); got != "high" {
		t.Fatalf("decider thinking = %q, want the roster entry's high", got)
	}
	if got, _ := decider["backend"].(string); got != "pi" {
		t.Fatalf("decider backend = %q, want pi", got)
	}
	// The decider must not run in the worker's workspace: it is spawned in a
	// neutral per-request scratch directory under the system temp dir, then
	// that directory is removed when the decider run tears down.
	if deciderDir == "" || deciderDir == workerDir {
		t.Fatalf("decider dir = %q, want a scratch dir distinct from the worker dir %q", deciderDir, workerDir)
	}
	if !strings.HasPrefix(deciderDir, filepath.Clean(os.TempDir())+string(os.PathSeparator)) {
		t.Fatalf("decider dir = %q, want a scratch dir under %s", deciderDir, os.TempDir())
	}
	waitFor(t, "decider scratch dir removed", func() bool {
		_, err := os.Stat(deciderDir)
		return os.IsNotExist(err)
	})
	// The request stays pending: the kernel routed it, it did not answer it.
	if pending, _ := worker["pending_permission"].(bool); !pending {
		t.Fatal("worker pending_permission = false, want the request to stay pending")
	}
}

// dispatchPermdecTemplate builds the single-node dispatch template whose
// assignment routes the node's permission requests to the "rm-guard" decider
// declared in the roster at rosterPath. The decider dispatch tests differ
// only in their template ID.
func dispatchPermdecTemplate(rosterPath, templateID string) map[string]any {
	return map[string]any{
		"schema_version":   1,
		"template_id":      templateID,
		"template_version": "1",
		"entry_nodes":      []string{"start"},
		"nodes": []any{map[string]any{
			"id":           "start",
			"action":       map[string]any{"type": "run", "prompt": "do the thing"},
			"assignment":   map[string]any{"roster_file": rosterPath, "roster_entry": "worker", "permission_policy": map[string]any{"decider": "rm-guard"}},
			"dispatch":     map[string]any{"mode": "auto", "controller_id": "c1"},
			"retry_policy": map[string]any{"max_attempts": 3, "exhaustion": "block"},
		}},
		"terminal_outcomes":    []string{"done"},
		"default_lease_policy": map[string]any{"ttl_seconds": 900},
	}
}

// TestDispatchPermissionDeciderSpawnFailureReroutesRequest proves a failed
// decider spawn re-routes the request internally: the backend sent its
// permission.request event once and is blocked waiting for the answer, so the
// retry happens from the cached options without a further event instead of
// stalling pending with only a stderr line.
func TestDispatchPermissionDeciderSpawnFailureReroutesRequest(t *testing.T) {
	rosterPath := writeStage5Roster(t, t.TempDir(), `{
		"worker":  {"backend": "pi", "agent": "worker", "model": "m"},
		"rm-guard": {"backend": "pi", "agent": "rm-guard", "model": "guard-m"}
	}`)
	template := mustJSON(t, dispatchPermdecTemplate(rosterPath, "tmpl-dispatch-permdec-retry"))
	f := newDispatchFixtureTemplate(t, "permdec-retry", 4, 4, template)
	requestID := "perm_req_retry"
	// Hold the worker prompt open after the permission request so the run
	// stays alive while the internal re-route is observed.
	gate := make(chan struct{})
	defer close(gate)
	permissionEvent := func() events.Event {
		return events.Event{
			Event:     "permission.request",
			SessionID: "ses_worker_retry",
			Fields: map[string]any{
				"request_id": requestID,
				"tool":       "bash",
				"command":    "rm -rf /tmp/build",
				"options":    []any{map[string]any{"optionId": "allow_once", "kind": "allow"}},
			},
		}
	}
	var deciderSpawns atomic.Int64
	f.sup.newProviderFunc = func(startOpts runtime.StartOptions, _ string) (runtime.Provider, error) {
		if strings.HasPrefix(startOpts.Label, "permdec:") {
			if deciderSpawns.Add(1) == 1 {
				return nil, fmt.Errorf("decider spawn failed")
			}
			return &stableScriptedProvider{attempt: -1, scripts: []stableScriptedAttempt{{
				sessionID: "ses_decider_retry",
			}}}, nil
		}
		return &stableScriptedProvider{attempt: -1, scripts: []stableScriptedAttempt{{
			sessionID: "ses_worker_retry",
			events:    []stableScriptedEvent{{event: permissionEvent()}, {release: gate}},
		}}}, nil
	}
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != Dispatched {
		t.Fatalf("outcome = %s (%s), want dispatched", out.Kind, out.Detail)
	}
	// The failed spawn must re-route the request on its own: the retry runs
	// internally from the cached options, with no further permission.request
	// event (the CLI session loop would refuse a concurrent second one).
	var decider map[string]any
	waitFor(t, "retried decider runtime", func() bool {
		for _, rt := range f.sup.listRuntimes() {
			if label, _ := rt["label"].(string); strings.HasPrefix(label, "permdec:") {
				decider = rt
				return true
			}
		}
		return false
	})
	if deciderSpawns.Load() < 2 {
		t.Fatalf("decider spawns = %d, want the retry to spawn a second time", deciderSpawns.Load())
	}
	if got, _ := decider["agent"].(string); got != "rm-guard" {
		t.Fatalf("decider agent = %q, want rm-guard", got)
	}
}

// TestDispatchPermissionDeciderSpawnFailuresBounded proves the decider spawn
// retries are bounded per request: each failed spawn re-routes the request
// internally from the cached options, but past maxPermissionDeciderSpawnFailures
// the routed mark is kept, no further spawns fire, the exhaustion surfaces in
// runtime status, and the request stays pending for a human gate — never
// silently rejected.
func TestDispatchPermissionDeciderSpawnFailuresBounded(t *testing.T) {
	rosterPath := writeStage5Roster(t, t.TempDir(), `{
		"worker":  {"backend": "pi", "agent": "worker", "model": "m"},
		"rm-guard": {"backend": "pi", "agent": "rm-guard", "model": "guard-m"}
	}`)
	f := newDispatchFixtureTemplate(t, "permdec-bounded", 4, 4, mustJSON(t, dispatchPermdecTemplate(rosterPath, "tmpl-dispatch-permdec-bounded")))
	requestID := "perm_req_bounded"
	// Hold the worker prompt open after the permission request so the run
	// stays alive while the bounded retries play out.
	gate := make(chan struct{})
	defer close(gate)
	var deciderSpawns atomic.Int64
	f.sup.newProviderFunc = func(startOpts runtime.StartOptions, _ string) (runtime.Provider, error) {
		if strings.HasPrefix(startOpts.Label, "permdec:") {
			deciderSpawns.Add(1)
			return nil, fmt.Errorf("decider spawn failed")
		}
		return &stableScriptedProvider{attempt: -1, scripts: []stableScriptedAttempt{{
			sessionID: "ses_worker_bounded",
			events: []stableScriptedEvent{{event: events.Event{
				Event:     "permission.request",
				SessionID: "ses_worker_bounded",
				Fields: map[string]any{
					"request_id": requestID,
					"tool":       "bash",
					"command":    "rm -rf /tmp/build",
					"options":    []any{map[string]any{"optionId": "allow_once", "kind": "allow"}},
				},
			}}, {release: gate}},
		}}}, nil
	}
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != Dispatched {
		t.Fatalf("outcome = %s (%s), want dispatched", out.Kind, out.Detail)
	}
	// The retries are internal: each failed spawn re-routes the request from
	// the cached options without a further permission.request event, so
	// exactly maxPermissionDeciderSpawnFailures decider spawns fire before
	// the exhaustion sticks.
	var worker map[string]any
	waitFor(t, "decider spawn retries exhausted", func() bool {
		status, err := f.sup.RuntimeStatus(out.RuntimeID)
		if err == nil {
			if rt, ok := status.(map[string]any); ok {
				if exhausted, _ := rt["permission_decider_exhausted"].(bool); exhausted {
					worker = rt
					return true
				}
			}
		}
		return false
	})
	if got := deciderSpawns.Load(); got != maxPermissionDeciderSpawnFailures {
		t.Fatalf("decider spawns = %d, want exactly the %d-attempt bound", got, maxPermissionDeciderSpawnFailures)
	}
	// A further routing attempt must not spawn another decider: the routed
	// mark stays once the bound is exhausted.
	f.sup.routePermissionRequest(out.RuntimeID, requestID, []any{map[string]any{"optionId": "allow_once", "kind": "allow"}})
	if got := deciderSpawns.Load(); got != maxPermissionDeciderSpawnFailures {
		t.Fatalf("decider spawns after exhaustion = %d, want no further spawns", got)
	}
	// The request stays pending for the human gate: no auto-reject, no local
	// resolution, and the pending permission still surfaces in status.
	if state := f.sup.control.PermissionResolverState(out.RuntimeID, requestID); state == control.PermissionResolverResolved {
		t.Fatalf("permission resolver state = %v, want the request left unresolved", state)
	}
	if pending, _ := worker["pending_permission"].(bool); !pending {
		t.Fatal("worker pending_permission = false, want the request to stay pending")
	}
}

// TestDispatchPermissionDeciderConcurrencyCapHolds proves the supervisor caps
// concurrent permission-decider runs: with the cap full, a new distinct
// pending request is routed but spawns no decider run — it stays pending for
// a human gate instead of crowding other runtimes out of the shared MaxRuntimes
// admission — and once a decider run fails and frees capacity, the blocked
// request is re-routed internally from its cached options without a further
// permission.request event.
func TestDispatchPermissionDeciderConcurrencyCapHolds(t *testing.T) {
	rosterPath := writeStage5Roster(t, t.TempDir(), `{
		"worker":  {"backend": "pi", "agent": "worker", "model": "m"},
		"rm-guard": {"backend": "pi", "agent": "rm-guard", "model": "guard-m"}
	}`)
	f := newDispatchFixtureTemplate(t, "permdec-cap", 8, 8, mustJSON(t, dispatchPermdecTemplate(rosterPath, "tmpl-dispatch-permdec-cap")))
	blockedID := "perm_req_cap_blocked"
	// Hold the decider runs open so the decider concurrency counter stays at
	// the cap, and hold the worker prompt open so its routing state stays alive.
	gate := make(chan struct{})
	workerGate := make(chan struct{})
	var gateOnce sync.Once
	closeGate := func() { gateOnce.Do(func() { close(gate) }) }
	defer closeGate()
	defer close(workerGate)
	var deciderSpawns atomic.Int64
	f.sup.newProviderFunc = func(startOpts runtime.StartOptions, _ string) (runtime.Provider, error) {
		if strings.HasPrefix(startOpts.Label, "permdec:") {
			deciderSpawns.Add(1)
			<-gate
			return nil, fmt.Errorf("decider spawn released")
		}
		return &stableScriptedProvider{attempt: -1, scripts: []stableScriptedAttempt{{
			sessionID: "ses_worker_cap",
			events:    []stableScriptedEvent{{release: workerGate}},
		}}}, nil
	}
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != Dispatched {
		t.Fatalf("outcome = %s (%s), want dispatched", out.Kind, out.Detail)
	}
	waitFor(t, "worker runtime", func() bool {
		_, err := f.sup.RuntimeStatus(out.RuntimeID)
		return err == nil
	})
	options := []any{map[string]any{"optionId": "allow_once", "kind": "allow"}}
	// Fill the cap with distinct pending requests, waiting for each spawn so
	// the routing order is deterministic. Routing goes through the production
	// fanout entry (cachePermissionOptions), which caches the options a re-route
	// needs after a failed spawn or a freed slot.
	for i := 0; i < maxConcurrentPermissionDeciders; i++ {
		f.sup.cachePermissionOptions(out.RuntimeID, fmt.Sprintf("perm_req_cap_%d", i), options)
		waitFor(t, fmt.Sprintf("decider spawn %d", i+1), func() bool { return deciderSpawns.Load() >= int64(i+1) })
	}
	// A further distinct request hits the cap: no spawn fires, the routed mark
	// stays, and the request stays pending for a human gate.
	f.sup.cachePermissionOptions(out.RuntimeID, blockedID, options)
	// Quiesce before the exact-count assert: a broken cap launches the
	// overflow spawn asynchronously, and the count read could otherwise race
	// ahead of the spawn's own counter increment.
	time.Sleep(50 * time.Millisecond)
	if got := deciderSpawns.Load(); got != int64(maxConcurrentPermissionDeciders) {
		t.Fatalf("decider spawns = %d, want the cap of %d held with no overflow spawn", got, maxConcurrentPermissionDeciders)
	}
	for _, rt := range f.sup.listRuntimes() {
		if label, _ := rt["label"].(string); label == "permdec:"+out.RuntimeID+":"+blockedID {
			t.Fatalf("capped request spawned a decider run %q, want it left pending", label)
		}
	}
	if state := f.sup.control.PermissionResolverState(out.RuntimeID, blockedID); state == control.PermissionResolverResolved {
		t.Fatalf("capped request resolver state = %v, want the request left unresolved", state)
	}
	// Free the held decider runs: each spawn fails, releases its slot, and
	// re-routes the blocked request from its cached options — no further
	// permission.request event is needed.
	closeGate()
	waitFor(t, "blocked request re-routed after capacity freed", func() bool {
		return deciderSpawns.Load() > int64(maxConcurrentPermissionDeciders)
	})
}

// TestPermissionDeciderMarshalFailureDoesNotSpawnBlind proves options that
// cannot be encoded never reach a decider as empty options: the failed encode
// is a failed attempt against the retry bound, no decider run starts, and the
// request stays pending for a human gate once the bound is exhausted.
func TestPermissionDeciderMarshalFailureDoesNotSpawnBlind(t *testing.T) {
	rosterPath := writeStage5Roster(t, t.TempDir(), `{
		"worker":  {"backend": "pi", "agent": "worker", "model": "m"},
		"rm-guard": {"backend": "pi", "agent": "rm-guard", "model": "guard-m"}
	}`)
	f := newDispatchFixtureTemplate(t, "permdec-marshal", 4, 4, mustJSON(t, dispatchPermdecTemplate(rosterPath, "tmpl-dispatch-permdec-marshal")))
	requestID := "perm_req_marshal"
	var deciderSpawns atomic.Int64
	f.sup.newProviderFunc = func(startOpts runtime.StartOptions, _ string) (runtime.Provider, error) {
		if strings.HasPrefix(startOpts.Label, "permdec:") {
			deciderSpawns.Add(1)
			return &stableScriptedProvider{attempt: -1, scripts: []stableScriptedAttempt{{
				sessionID: "ses_decider_marshal",
			}}}, nil
		}
		return &stableScriptedProvider{attempt: -1, scripts: []stableScriptedAttempt{{
			sessionID: "ses_worker_marshal",
		}}}, nil
	}
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != Dispatched {
		t.Fatalf("outcome = %s (%s), want dispatched", out.Kind, out.Detail)
	}
	decider := workflow.PermissionDecider{Entry: "rm-guard", Backend: "pi", Agent: "rm-guard", Model: "guard-m"}
	// A channel value cannot marshal: every attempt must fail before any
	// provider call instead of spawning a decider that judges blind.
	for i := 0; i < maxPermissionDeciderSpawnFailures; i++ {
		f.sup.spawnPermissionDecider(decider, out.RuntimeID, requestID, []any{make(chan int)}, "worker")
	}
	if got := deciderSpawns.Load(); got != 0 {
		t.Fatalf("decider spawns = %d, want none from marshal failures", got)
	}
	if state := f.sup.control.PermissionResolverState(out.RuntimeID, requestID); state == control.PermissionResolverResolved {
		t.Fatal("permission resolver state resolved, want the request left pending")
	}
	// Each failed encode counts against the bound, so the exhaustion marker
	// surfaces in runtime status after the bound is reached.
	waitFor(t, "exhaustion surfaced", func() bool {
		status, err := f.sup.RuntimeStatus(out.RuntimeID)
		if err != nil {
			return false
		}
		rt, _ := status.(map[string]any)
		exhausted, _ := rt["permission_decider_exhausted"].(bool)
		return exhausted
	})
}

// TestPermissionDeciderSkipsAlreadyResolvedRequest proves the decider spawn
// re-checks the control plane before starting: a request that was already
// resolved (for example by a human answering through the control plane while
// the spawn was being prepared) spawns no decider run.
func TestPermissionDeciderSkipsAlreadyResolvedRequest(t *testing.T) {
	rosterPath := writeStage5Roster(t, t.TempDir(), `{
		"worker":  {"backend": "pi", "agent": "worker", "model": "m"},
		"rm-guard": {"backend": "pi", "agent": "rm-guard", "model": "guard-m"}
	}`)
	template := mustJSON(t, dispatchPermdecTemplate(rosterPath, "tmpl-dispatch-permdec-resolved"))
	f := newDispatchFixtureTemplate(t, "permdec-resolved", 4, 4, template)
	requestID := "perm_req_resolved"
	gate := make(chan struct{})
	// Release the held spawn and the held worker prompt on every exit path so
	// teardown never waits on them, even when an assertion below fails.
	defer close(gate)
	var deciderSpawns atomic.Int64
	f.sup.newProviderFunc = func(startOpts runtime.StartOptions, _ string) (runtime.Provider, error) {
		if strings.HasPrefix(startOpts.Label, "permdec:") {
			deciderSpawns.Add(1)
			// Hold the automatic spawn so only the explicit re-check below is
			// under test; release it as a failure so no decider run lingers.
			<-gate
			return nil, fmt.Errorf("decider spawn failed")
		}
		return &stableScriptedProvider{attempt: -1, scripts: []stableScriptedAttempt{{
			sessionID: "ses_worker_resolved",
			events: []stableScriptedEvent{
				{event: events.Event{
					Event:     "permission.request",
					SessionID: "ses_worker_resolved",
					Fields: map[string]any{
						"request_id": requestID,
						"tool":       "bash",
						"command":    "rm -rf /tmp/build",
						"options":    []any{map[string]any{"optionId": "allow_once", "kind": "allow"}},
					},
				}},
				// Hold the worker prompt open after the permission request so
				// the child run never completes: its teardown would clear the
				// runtime's control-plane claims, and the claim below must stay
				// observable for the whole test.
				{release: gate},
			},
		}}}, nil
	}
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != Dispatched {
		t.Fatalf("outcome = %s (%s), want dispatched", out.Kind, out.Detail)
	}
	waitFor(t, "decider spawn blocked", func() bool { return deciderSpawns.Load() >= 1 })
	// The claim is prepared before the event reaches the fanout writer that
	// triggers the decider spawn, and the held worker prompt keeps it alive,
	// so the claim must already be observable here.
	waitFor(t, "permission claim", func() bool {
		return f.sup.control.PermissionResolverState(out.RuntimeID, requestID) != control.PermissionResolverUnknown
	})
	if !f.sup.control.MarkPermissionClaimResolved(out.RuntimeID, requestID, "test", "allow_once", "") {
		t.Fatal("MarkPermissionClaimResolved returned false")
	}
	// The already-resolved request must not start another decider run.
	f.sup.spawnPermissionDecider(workflow.PermissionDecider{Entry: "rm-guard", Backend: "pi", Agent: "rm-guard", Model: "guard-m"},
		out.RuntimeID, requestID, nil, "worker")
	if got := deciderSpawns.Load(); got != 1 {
		t.Fatalf("decider spawns = %d, want the resolved request to spawn none", got)
	}
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
	stderr := captureStderr(t, func(stderr io.Writer) {
		sup = NewSupervisor(Config{
			ControlSocket:   filepath.Join(t.TempDir(), "control.sock"),
			MaxRuntimes:     2,
			MaxTreeBudget:   8,
			ShutdownTimeout: 0,
			WorkflowRoot:    filepath.Join(t.TempDir(), "wfroot"),
			Stderr:          stderr,
		})
	})
	if !strings.Contains(stderr, "tree budget unavailable; using degraded local-only mode") {
		t.Fatalf("stderr = %q, want degraded-mode warning", stderr)
	}
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

// cancelAfterContext taps ctx.Err() so a test can cancel a dispatch at its
// nth internal checkpoint: the first after Err calls report the parent's
// result, every later one reports context.Canceled. The dispatch checkpoints
// are ordered (before the reservation, after it, after BeginDispatch), so a
// count selects the exact boundary under test.
type cancelAfterContext struct {
	context.Context
	after int
	calls int
}

func (c *cancelAfterContext) Err() error {
	c.calls++
	if c.calls > c.after {
		return context.Canceled
	}
	return c.Context.Err()
}

// TestDispatchCanceledBeforeReservation proves a cancel arriving before the
// admission reservation reports canceled with nothing reserved and no
// workflow state appended.
func TestDispatchCanceledBeforeReservation(t *testing.T) {
	f := newDispatchFixture(t, "dispatch-cancel-pre", 4, 4, "")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out, err := f.sup.dispatchWorkflowNode(ctx, f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != DispatchCanceled {
		t.Fatalf("outcome = %s, want canceled", out.Kind)
	}
	f.sup.controlMu.Lock()
	outstanding := f.sup.outstandingReservations
	f.sup.controlMu.Unlock()
	if outstanding != 0 {
		t.Fatalf("outstanding reservations = %d, want 0", outstanding)
	}
	if got := f.treeActive(t); got != 0 {
		t.Fatalf("tree slots active = %d, want 0", got)
	}
	if f.workflowRevision(t) != f.revision {
		t.Fatal("canceled dispatch changed the workflow revision")
	}
}

// TestDispatchCanceledAfterReservation proves a cancel arriving after the
// reservation but before leader validation releases the reservation unused.
func TestDispatchCanceledAfterReservation(t *testing.T) {
	f := newDispatchFixture(t, "dispatch-cancel-resv", 4, 4, "")
	out, err := f.sup.dispatchWorkflowNode(&cancelAfterContext{Context: t.Context(), after: 1}, f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != DispatchCanceled {
		t.Fatalf("outcome = %s, want canceled", out.Kind)
	}
	f.sup.controlMu.Lock()
	outstanding := f.sup.outstandingReservations
	f.sup.controlMu.Unlock()
	if outstanding != 0 {
		t.Fatalf("reservation leaked: outstanding = %d", outstanding)
	}
	if got := f.treeActive(t); got != 0 {
		t.Fatalf("tree slots active = %d, want 0", got)
	}
	if f.workflowRevision(t) != f.revision {
		t.Fatal("canceled dispatch changed the workflow revision")
	}
}

// TestDispatchCanceledAfterBeginDispatch proves a cancel arriving after the
// attempt intent was committed keeps the granted lease authoritative: the
// attempt is finalized as a pre-start failure (not a cancellation), so the
// node's retry policy re-arms the activation, the reservation is released,
// and the outcome reports canceled.
func TestDispatchCanceledAfterBeginDispatch(t *testing.T) {
	f := newDispatchFixture(t, "dispatch-cancel-post", 4, 4, "")
	out, err := f.sup.dispatchWorkflowNode(&cancelAfterContext{Context: t.Context(), after: 2}, f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != DispatchCanceled {
		t.Fatalf("outcome = %s, want canceled", out.Kind)
	}
	if out.AttemptID == "" {
		t.Fatal("canceled-after-begin outcome missing attempt id")
	}
	f.sup.controlMu.Lock()
	outstanding := f.sup.outstandingReservations
	f.sup.controlMu.Unlock()
	if outstanding != 0 {
		t.Fatalf("reservation leaked: outstanding = %d", outstanding)
	}
	if got := f.treeActive(t); got != 0 {
		t.Fatalf("tree slots active = %d, want 0", got)
	}
	waitFor(t, "runtime absent after cancel", func() bool { return f.sup.activeRuntimeCount() == 0 })
	insp, err := f.mgr.WorkflowInspect(f.wf)
	if err != nil {
		t.Fatalf("WorkflowInspect: %v", err)
	}
	ins := insp.(map[string]any)
	activations := ins["activations"].([]workflow.Activation)
	attempts := ins["attempts"].([]workflow.Attempt)
	if len(attempts) != 1 || attempts[0].Status != workflow.AttemptFailed {
		t.Fatalf("attempts = %+v, want one failed attempt", attempts)
	}
	if attempts[0].MarkerKind != "dispatch" || attempts[0].MarkerLabel != "dispatch_canceled" {
		t.Fatalf("attempt marker = %q/%q, want dispatch/dispatch_canceled", attempts[0].MarkerKind, attempts[0].MarkerLabel)
	}
	act := activationByNodeStable(activations, "start")
	if act == nil || act.Status != workflow.ActivationReady || act.ActiveLease != nil {
		t.Fatalf("activation = %+v, want ready with no lease", act)
	}
}

// activationByNodeStable returns the first activation for a node, or nil.
func activationByNodeStable(acts []workflow.Activation, nodeID string) *workflow.Activation {
	for i := range acts {
		if string(acts[i].NodeID) == nodeID {
			return &acts[i]
		}
	}
	return nil
}

// TestDispatchKeyHeldOutcomeReleasesReservation proves a key-held bounce
// through dispatchWorkflowNode reports key_held and leaves no reservation or
// extra tree token behind.
func TestDispatchKeyHeldOutcomeReleasesReservation(t *testing.T) {
	f := newDispatchFixture(t, "dispatch-key-outcome", 4, 4, "deploys")
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	if out.Kind != Dispatched {
		t.Fatalf("first outcome = %s (%s), want dispatched", out.Kind, out.Detail)
	}
	waitFor(t, "first runtime live", func() bool { return f.sup.activeRuntimeCount() == 1 })

	// A second workflow sharing the key under the same controller.
	if _, err := f.mgr.WorkflowCreate(dispatchTemplateJSONTTL(t, "tmpl-dispatch-key-outcome-2", "c1", "deploys", 900)); err != nil {
		t.Fatalf("WorkflowCreate 2: %v", err)
	}
	inst, err := f.mgr.WorkflowInstantiate(mustJSON(t, map[string]string{"template_id": "tmpl-dispatch-key-outcome-2", "template_version": "1"}))
	if err != nil {
		t.Fatalf("WorkflowInstantiate 2: %v", err)
	}
	wf2 := inst.(map[string]any)["workflow_id"].(string)
	if err := f.mgr.RebuildCandidateIndex("test"); err != nil {
		t.Fatalf("RebuildCandidateIndex: %v", err)
	}
	cands, err := f.mgr.CandidatesForController("c1", 10)
	if err != nil {
		t.Fatalf("CandidatesForController: %v", err)
	}
	var cand *workflow.ReadyCandidate
	for i := range cands {
		if string(cands[i].Identity.WorkflowID) == wf2 {
			cand = &cands[i]
		}
	}
	if cand == nil {
		t.Fatalf("second workflow candidate missing: %+v", cands)
	}
	held, err := f.sup.dispatchWorkflowNode(t.Context(), DispatchRequest{
		WorkflowID:       wf2,
		NodeID:           string(cand.Identity.NodeID),
		ActivationID:     string(cand.Identity.ActivationID),
		ExpectedRevision: cand.Revision,
		ControllerID:     "c1",
		LeaderLeaseID:    f.leaseID,
		OwnerEpoch:       f.ownerEpoch,
	})
	if err != nil {
		t.Fatalf("held dispatch: %v", err)
	}
	if held.Kind != DispatchKeyHeld {
		t.Fatalf("held outcome = %s (%s), want key_held", held.Kind, held.Detail)
	}
	f.sup.controlMu.Lock()
	outstanding := f.sup.outstandingReservations
	f.sup.controlMu.Unlock()
	if outstanding != 0 {
		t.Fatalf("outstanding reservations = %d, want 0 after key-held bounce", outstanding)
	}
	if got := f.treeActive(t); got != 1 {
		t.Fatalf("tree slots active = %d, want exactly the first runtime's 1", got)
	}
}

// TestDispatchSingleAcquirePerAction proves one controller dispatch consumes
// exactly one reservation even at the tightest budget: with one free tree slot
// and one local slot, the run, loop, and team executors all start without a
// release-and-reacquire, which would fail the local limit.
func TestDispatchSingleAcquirePerAction(t *testing.T) {
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
			template := dispatchActionTemplateJSON(t, "tmpl-dispatch-"+action.name, "c1", action.name, filePath)
			f := newDispatchFixtureTemplate(t, action.name, 1, 1, template)
			out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
			if err != nil {
				t.Fatalf("dispatchWorkflowNode: %v", err)
			}
			if out.Kind != Dispatched {
				t.Fatalf("outcome = %s (%s), want dispatched", out.Kind, out.Detail)
			}
			waitFor(t, "runtime registration", func() bool { return f.sup.activeRuntimeCount() == 1 })
			if got := f.treeActive(t); got != 1 {
				t.Fatalf("tree slots active = %d, want exactly 1 (no second acquire)", got)
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
		})
	}
}

// TestDispatchPersistsRuntimeIdentityOnSnapshot proves a successful dispatch
// records the attempt_identified runtime identity in the persisted snapshot,
// not only in the in-memory outcome.
func TestDispatchPersistsRuntimeIdentityOnSnapshot(t *testing.T) {
	f := newDispatchFixture(t, "dispatch-identity", 4, 4, "")
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != Dispatched || out.RuntimeID == "" {
		t.Fatalf("outcome = %s runtime=%q, want dispatched with a runtime id", out.Kind, out.RuntimeID)
	}
	insp, err := f.mgr.WorkflowInspect(f.wf)
	if err != nil {
		t.Fatalf("WorkflowInspect: %v", err)
	}
	attempts := insp.(map[string]any)["attempts"].([]workflow.Attempt)
	if len(attempts) != 1 {
		t.Fatalf("attempts = %+v, want exactly one", attempts)
	}
	if attempts[0].Identity.RuntimeID != out.RuntimeID {
		t.Fatalf("persisted runtime id = %q, want %q", attempts[0].Identity.RuntimeID, out.RuntimeID)
	}
	if attempts[0].Identity.SessionID == "" {
		t.Fatal("persisted attempt identity missing session id")
	}
}

// TestDispatchStaleThroughRunnerDeps proves a stale expected revision is
// translated to ResultStale through the runner deps and leaves no
// admission reservation held.
func TestDispatchStaleThroughRunnerDeps(t *testing.T) {
	f := newDispatchFixture(t, "stale-deps", 4, 4, "")
	deps := &stableRunnerDeps{s: f.sup, controllerID: "c1"}
	dec := workflowcontroller.Decision{Candidate: workflowcontroller.Candidate{
		Identity: workflow.ExecutionIdentity{
			WorkflowID:   workflow.WorkflowID(f.wf),
			NodeID:       "start",
			ActivationID: workflow.ActivationID(f.activation),
		},
		ControllerID: "c1",
		Revision:     f.revision + 1, // stale: the candidate was ready at f.revision
	}}
	res, err := deps.Dispatch(t.Context(), dec, workflowcontroller.LeaderLease{LeaseID: f.leaseID, OwnerEpoch: f.ownerEpoch})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if res.Kind != workflowcontroller.ResultStale {
		t.Fatalf("result kind = %s, want stale", res.Kind)
	}
	f.sup.controlMu.Lock()
	outstanding := f.sup.outstandingReservations
	f.sup.controlMu.Unlock()
	if outstanding != 0 {
		t.Fatalf("reservation leaked: outstanding = %d", outstanding)
	}
	if got := f.treeActive(t); got != 0 {
		t.Fatalf("tree slots active = %d, want 0", got)
	}
}

// TestDispatchKeyHeldThroughRunnerDeps proves a candidate whose
// concurrency key is held by another live attempt is translated to
// ResultKeyHeld through the runner deps and leaves no admission
// reservation held.
func TestDispatchKeyHeldThroughRunnerDeps(t *testing.T) {
	f := newDispatchFixture(t, "keyheld-deps", 4, 4, "ck")
	// A second workflow on the same template (and the same concurrency key).
	out, err := f.mgr.WorkflowInstantiate(mustJSON(t, map[string]string{"template_id": "tmpl-dispatch-keyheld-deps", "template_version": "1"}))
	if err != nil {
		t.Fatalf("WorkflowInstantiate: %v", err)
	}
	wf2, ok := out.(map[string]any)["workflow_id"].(string)
	if !ok || wf2 == "" {
		t.Fatalf("instantiate result missing workflow_id: %#v", out)
	}

	// Hold the key: dispatch the first workflow and block its runtime.
	if _, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil)); err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	waitFor(t, "first runtime live", func() bool { return f.sup.activeRuntimeCount() == 1 })

	// The second workflow's candidate is ready, but its key is held.
	cands, err := f.mgr.CandidatesForController("c1", 10)
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	var target *workflow.ReadyCandidate
	for i := range cands {
		if cands[i].Identity.WorkflowID == workflow.WorkflowID(wf2) {
			target = &cands[i]
		}
	}
	if target == nil {
		t.Fatalf("candidate for %s not found: %v", wf2, cands)
	}

	deps := &stableRunnerDeps{s: f.sup, controllerID: "c1"}
	dec := workflowcontroller.Decision{Candidate: workflowcontroller.Candidate{
		Identity:       target.Identity,
		ControllerID:   target.ControllerID,
		Revision:       target.Revision,
		ConcurrencyKey: target.ConcurrencyKey,
	}}
	res, err := deps.Dispatch(t.Context(), dec, workflowcontroller.LeaderLease{LeaseID: f.leaseID, OwnerEpoch: f.ownerEpoch})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if res.Kind != workflowcontroller.ResultKeyHeld {
		t.Fatalf("result kind = %s, want key_held", res.Kind)
	}
	f.sup.controlMu.Lock()
	outstanding := f.sup.outstandingReservations
	f.sup.controlMu.Unlock()
	if outstanding != 0 {
		t.Fatalf("reservation leaked: outstanding = %d", outstanding)
	}
	if got := f.treeActive(t); got != 1 {
		t.Fatalf("tree slots active = %d, want 1 (only the key-holding runtime)", got)
	}
}

// TestTranslateDispatchOutcome covers every DispatchOutcomeKind to
// DispatchResultKind mapping, including the default case.
func TestTranslateDispatchOutcome(t *testing.T) {
	tests := []struct {
		in   DispatchOutcomeKind
		want workflowcontroller.DispatchResultKind
	}{
		{Dispatched, workflowcontroller.ResultDispatched},
		{DispatchCapacityBlocked, workflowcontroller.ResultCapacityBlocked},
		{DispatchStale, workflowcontroller.ResultStale},
		{DispatchKeyHeld, workflowcontroller.ResultKeyHeld},
		{DispatchNotLeader, workflowcontroller.ResultNotLeader},
		{DispatchStartFailed, workflowcontroller.ResultStartFailed},
		{DispatchCanceled, workflowcontroller.ResultCanceled},
		{DispatchOutcomeKind("unknown_kind"), workflowcontroller.ResultCanceled},
	}
	for _, tt := range tests {
		if got := translateDispatchOutcome(tt.in); got != tt.want {
			t.Errorf("translateDispatchOutcome(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// writeProviderRegistry installs a user-level provider registry for a test:
// XDG_CONFIG_HOME points at a scratch dir holding the given providers.json.
func writeProviderRegistry(t *testing.T, providersJSON string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "avenor"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "avenor", "providers.json"), []byte(providersJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", dir)
}

// dispatchModelDeciderTemplate builds the single-node dispatch template whose
// assignment routes the node's permission requests to a direct-model decider
// (provider + model, no roster file).
func dispatchModelDeciderTemplate(provider, model, templateID string) map[string]any {
	return map[string]any{
		"schema_version":   1,
		"template_id":      templateID,
		"template_version": "1",
		"entry_nodes":      []string{"start"},
		"nodes": []any{map[string]any{
			"id":         "start",
			"action":     map[string]any{"type": "run", "prompt": "clean up the build directory"},
			"assignment": map[string]any{"permission_policy": map[string]any{"decider_model": map[string]any{"provider": provider, "model": model}}},
			"dispatch":   map[string]any{"mode": "auto", "controller_id": "c1"},
			"retry_policy": map[string]any{
				"max_attempts": 3,
				"exhaustion":   "block",
			},
		}},
		"terminal_outcomes":    []string{"done"},
		"default_lease_policy": map[string]any{"ttl_seconds": 900},
	}
}

// TestResolveAssignmentSelectionModelDecider proves the direct-model decider
// resolves onto the pinned selection from the user-level provider registry —
// without a roster file — and pins the endpoint and key variable name while
// never carrying a credential. An unknown provider and combined decider
// variants are rejected.
func TestResolveAssignmentSelectionModelDecider(t *testing.T) {
	writeProviderRegistry(t, `{
		"providers": {
			"groq": {"base_url": "http://model.example/openai/v1", "api_key_env": "MODEL_DECIDER_TEST_KEY"},
			"lit":  {"base_url": "http://model.example/openai/v1", "api_key": "literal"}
		}
	}`)
	sel, err := resolveAssignmentSelection(&workflow.Assignment{
		PermissionPolicy: &workflow.PermissionPolicy{DeciderModel: &workflow.PermissionDeciderModel{Provider: "groq", Model: "llama-3.3-70b"}},
	})
	if err != nil {
		t.Fatalf("resolveAssignmentSelection: %v", err)
	}
	if sel.PermissionModelDecider == nil ||
		sel.PermissionModelDecider.Provider != "groq" ||
		sel.PermissionModelDecider.Model != "llama-3.3-70b" ||
		sel.PermissionModelDecider.BaseURL != "http://model.example/openai/v1" ||
		sel.PermissionModelDecider.APIKeyEnv != "MODEL_DECIDER_TEST_KEY" {
		t.Fatalf("selection.PermissionModelDecider = %+v, want the pinned groq endpoint", sel.PermissionModelDecider)
	}
	if sel.AutoApprove || sel.PermissionDecider != nil {
		t.Fatalf("selection = %+v, want no other permission policy", sel)
	}
	// A literal api_key provider pins the endpoint but never the credential.
	sel, err = resolveAssignmentSelection(&workflow.Assignment{
		PermissionPolicy: &workflow.PermissionPolicy{DeciderModel: &workflow.PermissionDeciderModel{Provider: "lit", Model: "m"}},
	})
	if err != nil {
		t.Fatalf("resolveAssignmentSelection(literal key): %v", err)
	}
	if sel.PermissionModelDecider == nil || sel.PermissionModelDecider.APIKeyEnv != "" {
		t.Fatalf("selection.PermissionModelDecider = %+v, want no api_key_env and no credential", sel.PermissionModelDecider)
	}
	if _, err := resolveAssignmentSelection(&workflow.Assignment{
		PermissionPolicy: &workflow.PermissionPolicy{DeciderModel: &workflow.PermissionDeciderModel{Provider: "missing", Model: "m"}},
	}); err == nil || !strings.Contains(err.Error(), `provider "missing" is not configured`) {
		t.Fatalf("resolveAssignmentSelection(unknown provider) error = %v, want not configured", err)
	}
	rosterPath := writeStage5Roster(t, t.TempDir(), `{
		"worker":  {"backend": "pi", "agent": "worker", "model": "m"},
		"rm-guard": {"backend": "pi", "agent": "rm-guard", "model": "guard-m"}
	}`)
	if _, err := resolveAssignmentSelection(&workflow.Assignment{
		RosterFile:       rosterPath,
		RosterEntry:      "worker",
		PermissionPolicy: &workflow.PermissionPolicy{Decider: "rm-guard", DeciderModel: &workflow.PermissionDeciderModel{Provider: "groq", Model: "m"}},
	}); err == nil || !strings.Contains(err.Error(), "cannot declare both decider and decider_model") {
		t.Fatalf("resolveAssignmentSelection(both variants) error = %v, want mutual exclusion", err)
	}
	if _, err := resolveAssignmentSelection(&workflow.Assignment{
		AutoApprove:      true,
		PermissionPolicy: &workflow.PermissionPolicy{DeciderModel: &workflow.PermissionDeciderModel{Provider: "groq", Model: "m"}},
	}); err == nil || !strings.Contains(err.Error(), "cannot declare both auto_approve and permission_policy") {
		t.Fatalf("resolveAssignmentSelection(auto + model) error = %v, want mutual exclusion", err)
	}
	// A decider_model policy does not make the roster file an identity
	// selector: a bare roster_file with only a model decider is still
	// rejected by the spawn-selection rules.
	if _, err := resolveAssignmentSelection(&workflow.Assignment{
		RosterFile:       rosterPath,
		PermissionPolicy: &workflow.PermissionPolicy{DeciderModel: &workflow.PermissionDeciderModel{Provider: "groq", Model: "m"}},
	}); err == nil || !strings.Contains(err.Error(), "roster_file requires roster_entry") {
		t.Fatalf("resolveAssignmentSelection(bare roster file) error = %v, want identity-selector rejection", err)
	}
}

// modelDeciderCapture records what the decision endpoint saw.
type modelDeciderCapture struct {
	mu    sync.Mutex
	calls int
	auth  string
	body  string
}

func (c *modelDeciderCapture) record(auth string, body []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	c.auth = auth
	c.body = string(body)
}

func (c *modelDeciderCapture) snapshot() (int, string, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls, c.auth, c.body
}

// newModelDeciderServer serves OpenAI-compatible decision completions with a
// fixed response body.
func newModelDeciderServer(capture *modelDeciderCapture, respond func(w http.ResponseWriter)) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		capture.record(r.Header.Get("Authorization"), body)
		respond(w)
	}))
	return srv
}

// scriptedWorkerRequest holds the worker runtime open after its scripted
// permission.request so the decision and answer can be observed on a live
// runtime.
func scriptedWorkerRequest(requestID string, gate <-chan struct{}) *stableScriptedProvider {
	return &stableScriptedProvider{attempt: -1, scripts: []stableScriptedAttempt{{
		sessionID: "ses_modeldec_worker",
		events: []stableScriptedEvent{
			{event: events.Event{
				Event:     "permission.request",
				SessionID: "ses_modeldec_worker",
				Fields: map[string]any{
					"request_id": requestID,
					"tool":       "bash",
					"command":    "rm -rf /tmp/build",
					"options": []any{
						map[string]any{"optionId": "allow_once", "kind": "allow"},
						map[string]any{"optionId": "reject_once", "kind": "reject"},
					},
				},
			}},
			{release: gate},
		},
	}}}
}

// newModelDeciderFixture builds the shared setup for the dispatched-node
// model-decider tests: a decision server serving respond, a provider
// registry pointing at it, the decision key in the environment, and a
// fixture whose single node declares the decider_model policy. The caller
// wires newProviderFunc and holds the worker open as its test needs.
func newModelDeciderFixture(t *testing.T, name string, respond func(http.ResponseWriter)) (*dispatchFixture, *modelDeciderCapture) {
	t.Helper()
	capture := &modelDeciderCapture{}
	srv := newModelDeciderServer(capture, respond)
	t.Cleanup(srv.Close)
	writeProviderRegistry(t, fmt.Sprintf(`{
		"providers": {"groq": {"base_url": %q, "api_key_env": "MODEL_DECIDER_TEST_KEY"}}
	}`, srv.URL))
	t.Setenv("MODEL_DECIDER_TEST_KEY", "sk-model-test")
	template := mustJSON(t, dispatchModelDeciderTemplate("groq", "llama-3.3-70b", "tmpl-dispatch-"+name))
	f := newDispatchFixtureTemplate(t, name, 4, 4, template)
	return f, capture
}

// TestDispatchModelDeciderAnswersRequest proves the direct-model resolver end
// to end: a dispatched node with a decider_model assignment spawns its worker
// with no file permission handler and no decider run; the pending request is
// answered by a single model call to the configured provider (authenticated
// with the registry key, judged against the node task and the request's
// tool/command), and the model's allow decision applies the matching pending
// option with its reason as the answer message.
func TestDispatchModelDeciderAnswersRequest(t *testing.T) {
	requestID := "perm_req_model_allow"
	f, capture := newModelDeciderFixture(t, "dispatch-modeldec-allow", func(w http.ResponseWriter) {
		w.Write([]byte(`{"choices":[{"message":{"content":"{\"decision\":\"allow\",\"reason\":\"cleanup is plainly needed for the task\"}"}}]}`))
	})
	gate := make(chan struct{})
	defer close(gate)
	var deciderSpawned atomic.Bool
	f.sup.newProviderFunc = func(startOpts runtime.StartOptions, _ string) (runtime.Provider, error) {
		if strings.HasPrefix(startOpts.Label, "permdec:") {
			deciderSpawned.Store(true)
		}
		return scriptedWorkerRequest(requestID, gate), nil
	}
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != Dispatched {
		t.Fatalf("outcome = %s (%s), want dispatched", out.Kind, out.Detail)
	}
	waitFor(t, "runtime registration", func() bool { return f.sup.activeRuntimeCount() == 1 })
	status, err := f.sup.RuntimeStatus(out.RuntimeID)
	if err != nil {
		t.Fatalf("RuntimeStatus: %v", err)
	}
	rtStatus, _ := status.(map[string]any)
	if got, _ := rtStatus["permission_model_decider"].(string); got != "groq/llama-3.3-70b" {
		t.Fatalf("worker permission_model_decider = %q, want groq/llama-3.3-70b", got)
	}
	if approve, _ := rtStatus["auto_approve"].(bool); approve {
		t.Fatal("worker auto_approve = true, want false")
	}
	// The resolved key must never reach a status surface: the registry holds
	// it, the pinned selection carries only its variable name.
	if strings.Contains(fmt.Sprintf("%v", rtStatus), "sk-model-test") {
		t.Fatal("runtime status leaked the provider API key")
	}
	// The model's allow decision answers the request: the claim resolves with
	// the chosen option and the reason as the message, the status clears, and
	// no decider run was ever spawned.
	waitFor(t, "model-answered permission", func() bool {
		resolved, matches := f.sup.control.ResolvedPermissionMatches(out.RuntimeID, requestID, "allow_once", "cleanup is plainly needed for the task")
		return resolved && matches
	})
	waitFor(t, "pending cleared", func() bool {
		status, err := f.sup.RuntimeStatus(out.RuntimeID)
		if err != nil {
			return false
		}
		rt, _ := status.(map[string]any)
		pending, _ := rt["pending_permission"].(bool)
		return !pending
	})
	if deciderSpawned.Load() {
		t.Fatal("a permdec: decider run was spawned for a direct-model policy, want none")
	}
	calls, auth, body := capture.snapshot()
	if calls != 1 {
		t.Fatalf("decision calls = %d, want 1", calls)
	}
	if auth != "Bearer sk-model-test" {
		t.Fatalf("decision auth = %q, want the registry key", auth)
	}
	if !strings.Contains(body, `"model":"llama-3.3-70b"`) {
		t.Fatalf("decision request body missing model: %s", body)
	}
	if !strings.Contains(body, "clean up the build directory") {
		t.Fatalf("decision request body missing the node task: %s", body)
	}
	if !strings.Contains(body, "rm -rf /tmp/build") {
		t.Fatalf("decision request body missing the pending command: %s", body)
	}
	if !strings.Contains(body, "allow_once") {
		t.Fatalf("decision request body missing the options: %s", body)
	}
}

// TestDispatchModelDeciderDeferStaysPending proves a defer decision is a
// judgment, not a failure: the request stays pending for a human gate, no
// retry fires, and the decider-exhausted flag stays clear.
func TestDispatchModelDeciderDeferStaysPending(t *testing.T) {
	requestID := "perm_req_model_defer"
	f, capture := newModelDeciderFixture(t, "dispatch-modeldec-defer", func(w http.ResponseWriter) {
		w.Write([]byte(`{"choices":[{"message":{"content":"{\"decision\":\"defer\",\"reason\":\"unclear scope\"}"}}]}`))
	})
	gate := make(chan struct{})
	defer close(gate)
	f.sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		return scriptedWorkerRequest(requestID, gate), nil
	}
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != Dispatched {
		t.Fatalf("outcome = %s (%s), want dispatched", out.Kind, out.Detail)
	}
	waitFor(t, "decision call", func() bool {
		calls, _, _ := capture.snapshot()
		return calls >= 1
	})
	waitFor(t, "request mirrored pending", func() bool {
		status, err := f.sup.RuntimeStatus(out.RuntimeID)
		if err != nil {
			return false
		}
		rt, _ := status.(map[string]any)
		pending, _ := rt["pending_permission"].(bool)
		return pending
	})
	// The deferred request stays pending: one call, no retry, not exhausted.
	calls, _, _ := capture.snapshot()
	if calls != 1 {
		t.Fatalf("decision calls = %d, want 1 (a defer must not retry)", calls)
	}
	status, err := f.sup.RuntimeStatus(out.RuntimeID)
	if err != nil {
		t.Fatalf("RuntimeStatus: %v", err)
	}
	rt, _ := status.(map[string]any)
	if pending, _ := rt["pending_permission"].(bool); !pending {
		t.Fatal("deferred request must stay pending for a human gate")
	}
	if exhausted, _ := rt["permission_decider_exhausted"].(bool); exhausted {
		t.Fatal("a defer must not mark the decider exhausted")
	}
	if state := f.sup.control.PermissionResolverState(out.RuntimeID, requestID); state == control.PermissionResolverResolved {
		t.Fatal("a deferred request must not be resolved")
	}
}

// TestDispatchModelDeciderFailuresBounded proves failed decision calls share
// the harness decider's bounded failure semantics: each failure re-routes the
// request internally, past the bound no further calls fire, and the request
// stays pending for a human gate with the exhausted flag surfaced.
func TestDispatchModelDeciderFailuresBounded(t *testing.T) {
	requestID := "perm_req_model_fail"
	f, capture := newModelDeciderFixture(t, "dispatch-modeldec-fail", func(w http.ResponseWriter) {
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
	})
	gate := make(chan struct{})
	defer close(gate)
	f.sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		return scriptedWorkerRequest(requestID, gate), nil
	}
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != Dispatched {
		t.Fatalf("outcome = %s (%s), want dispatched", out.Kind, out.Detail)
	}
	waitFor(t, "decider retries exhausted", func() bool {
		status, err := f.sup.RuntimeStatus(out.RuntimeID)
		if err != nil {
			return false
		}
		rt, _ := status.(map[string]any)
		exhausted, _ := rt["permission_decider_exhausted"].(bool)
		return exhausted
	})
	calls, _, _ := capture.snapshot()
	if calls != maxPermissionDeciderSpawnFailures {
		t.Fatalf("decision calls = %d, want %d (bounded retries, then a human gate)", calls, maxPermissionDeciderSpawnFailures)
	}
	status, err := f.sup.RuntimeStatus(out.RuntimeID)
	if err != nil {
		t.Fatalf("RuntimeStatus: %v", err)
	}
	rt, _ := status.(map[string]any)
	if pending, _ := rt["pending_permission"].(bool); !pending {
		t.Fatal("request must stay pending after exhausted retries")
	}
	// Every acquire must be paired with exactly one release: a leaked slot
	// would permanently shrink the concurrency pool (four exhausted requests
	// would kill the decider for the process lifetime).
	f.sup.permDeciderMu.Lock()
	active := f.sup.modelDeciderActive
	f.sup.permDeciderMu.Unlock()
	if active != 0 {
		t.Fatalf("modelDeciderActive = %d after exhausted retries, want 0 (no leaked slots)", active)
	}
}

// TestSpawnModelDeciderRequiresResolvableKey pins the fail-fast contract: a
// direct-model decider whose provider key cannot be resolved at spawn fails
// the spawn before any runtime starts, surfacing the misconfiguration to the
// operator instead of stalling the first permission request.
func TestSpawnModelDeciderRequiresResolvableKey(t *testing.T) {
	writeProviderRegistry(t, `{
		"providers": {"groq": {"base_url": "http://model.example/openai/v1", "api_key_env": "MODEL_DECIDER_TEST_UNSET_KEY"}}
	}`)
	sup := NewSupervisor(Config{
		ControlSocket: newStableSocketPath(t, "modeldec-nokey"),
		MaxRuntimes:   1,
	})
	_, err := sup.spawn(SpawnParams{
		Prompt:                 "hello",
		Dir:                    "/tmp",
		PermissionModelDecider: &workflow.PermissionModelDecider{Provider: "groq", Model: "llama-3.3-70b"},
	})
	if err == nil || !strings.Contains(err.Error(), "MODEL_DECIDER_TEST_UNSET_KEY") {
		t.Fatalf("err = %v, want unset api_key_env failure", err)
	}
	if got := sup.activeRuntimeCount(); got != 0 {
		t.Fatalf("activeRuntimeCount() = %d, want 0 after rejected spawn", got)
	}
}

// TestDispatchModelDeciderBudgetBoundsCalls proves the per-runtime decision
// budget: a worker raising a stream of distinct permission requests gets at
// most maxModelDeciderCallsPerRuntime billable model calls; further requests
// stay pending for a human without a call.
func TestDispatchModelDeciderBudgetBoundsCalls(t *testing.T) {
	requestID := "perm_req_model_budget_0"
	f, capture := newModelDeciderFixture(t, "dispatch-modeldec-budget", func(w http.ResponseWriter) {
		w.Write([]byte(`{"choices":[{"message":{"content":"{\"decision\":\"defer\"}"}}]}`))
	})
	gate := make(chan struct{})
	defer close(gate)
	f.sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		return scriptedWorkerRequest(requestID, gate), nil
	}
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != Dispatched {
		t.Fatalf("outcome = %s (%s), want dispatched", out.Kind, out.Detail)
	}
	waitFor(t, "worker runtime", func() bool {
		_, err := f.sup.RuntimeStatus(out.RuntimeID)
		return err == nil
	})
	// The worker's own request is the first call; route further distinct
	// fabricated requests up to and past the budget.
	seedPendingPermission(t, f.sup, out.RuntimeID)
	options := []any{map[string]any{"optionId": "allow_once", "kind": "allow"}}
	for i := 1; i <= maxModelDeciderCallsPerRuntime; i++ {
		f.sup.cachePermissionOptions(out.RuntimeID, fmt.Sprintf("perm_req_model_budget_%d", i), options)
	}
	waitFor(t, "budget exhausted", func() bool {
		calls, _, _ := capture.snapshot()
		return calls >= maxModelDeciderCallsPerRuntime
	})
	time.Sleep(100 * time.Millisecond)
	if calls, _, _ := capture.snapshot(); calls != maxModelDeciderCallsPerRuntime {
		t.Fatalf("decision calls = %d, want the budget of %d with no overflow call", calls, maxModelDeciderCallsPerRuntime)
	}
}

// TestDispatchModelDeciderHumanAnswerWinsRace proves a human answering a
// request while the decision call is in flight resolves cleanly: the model's
// late answer is dropped as already-resolved, no retry fires, and the
// resolved claim carries the human's option and message — never the model's.
func TestDispatchModelDeciderHumanAnswerWinsRace(t *testing.T) {
	requestID := "perm_req_model_race"
	capture := &modelDeciderCapture{}
	srv := newModelDeciderServer(capture, func(w http.ResponseWriter) {
		// Hold the decision call in flight so the human answer lands first.
		time.Sleep(800 * time.Millisecond)
		w.Write([]byte(`{"choices":[{"message":{"content":"{\"decision\":\"allow\",\"reason\":\"too late\"}"}}]}`))
	})
	defer srv.Close()
	writeProviderRegistry(t, fmt.Sprintf(`{
		"providers": {"groq": {"base_url": %q, "api_key_env": "MODEL_DECIDER_TEST_KEY"}}
	}`, srv.URL))
	t.Setenv("MODEL_DECIDER_TEST_KEY", "sk-model-test")

	template := mustJSON(t, dispatchModelDeciderTemplate("groq", "llama-3.3-70b", "tmpl-dispatch-dispatch-modeldec-race"))
	f := newDispatchFixtureTemplate(t, "dispatch-modeldec-race", 4, 4, template)
	gate := make(chan struct{})
	defer close(gate)
	f.sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		return scriptedWorkerRequest(requestID, gate), nil
	}
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != Dispatched {
		t.Fatalf("outcome = %s (%s), want dispatched", out.Kind, out.Detail)
	}
	waitFor(t, "runtime registration", func() bool { return f.sup.activeRuntimeCount() == 1 })
	waitFor(t, "request pending", func() bool {
		status, err := f.sup.RuntimeStatus(out.RuntimeID)
		if err != nil {
			return false
		}
		rt, _ := status.(map[string]any)
		pending, _ := rt["pending_permission"].(bool)
		return pending
	})
	waitFor(t, "decision call in flight", func() bool {
		calls, _, _ := capture.snapshot()
		return calls >= 1
	})
	// The human answers while the model call is still in flight.
	if err := f.sup.answerPermission(out.RuntimeID, requestID, "reject_once", "human override"); err != nil {
		t.Fatalf("human answerPermission: %v", err)
	}
	// Past the in-flight decision: the model's allow must not be applied and
	// must not burn a retry.
	time.Sleep(1200 * time.Millisecond)
	calls, _, _ := capture.snapshot()
	if calls != 1 {
		t.Fatalf("decision calls = %d, want 1 (the late model answer must not retry)", calls)
	}
	if resolved, matches := f.sup.control.ResolvedPermissionMatches(out.RuntimeID, requestID, "reject_once", "human override"); !resolved || !matches {
		t.Fatalf("resolved claim matches = %v/%v, want the human's option and message", resolved, matches)
	}
	status, err := f.sup.RuntimeStatus(out.RuntimeID)
	if err != nil {
		t.Fatalf("RuntimeStatus: %v", err)
	}
	rt, _ := status.(map[string]any)
	if exhausted, _ := rt["permission_decider_exhausted"].(bool); exhausted {
		t.Fatal("a race the human won must not mark the decider exhausted")
	}
}

// TestResolveAssignmentSelectionModelDeciderRequiresModel pins the
// dispatch-time model check: an assignment whose decider_model names no
// model fails at dispatch with a clear error, before any runtime spawns and
// before the first permission request can burn bounded retries on an
// uncallable provider.
func TestResolveAssignmentSelectionModelDeciderRequiresModel(t *testing.T) {
	writeProviderRegistry(t, `{
		"providers": {"groq": {"base_url": "http://model.example/openai/v1", "api_key_env": "MODEL_DECIDER_TEST_KEY"}}
	}`)
	_, err := resolveAssignmentSelection(&workflow.Assignment{
		PermissionPolicy: &workflow.PermissionPolicy{DeciderModel: &workflow.PermissionDeciderModel{Provider: "groq", Model: " "}},
	})
	if err == nil || !strings.Contains(err.Error(), "model is required") {
		t.Fatalf("resolveAssignmentSelection(blank model) error = %v, want model is required", err)
	}
}

// TestDispatchModelDeciderConcurrencyCapHolds proves the direct-model cap
// behaves like the harness decider's: while maxConcurrentModelDeciders calls
// are in flight, a further distinct request gets no decision call and stays
// pending; when capacity frees, the blocked request re-routes from its
// cached options and receives exactly one call — no further
// permission.request event needed.
func TestDispatchModelDeciderConcurrencyCapHolds(t *testing.T) {
	writeProviderRegistry(t, `{
		"providers": {"groq": {"base_url": "http://model.example/openai/v1", "api_key_env": "MODEL_DECIDER_TEST_KEY"}}
	}`)
	t.Setenv("MODEL_DECIDER_TEST_KEY", "sk-model-test")
	template := mustJSON(t, dispatchModelDeciderTemplate("groq", "llama-3.3-70b", "tmpl-dispatch-dispatch-modeldec-cap"))
	f := newDispatchFixtureTemplate(t, "dispatch-modeldec-cap", 8, 8, template)
	blockedID := "perm_req_model_cap_blocked"
	// Hold every in-flight decision call on the gate so the cap stays full;
	// each call resolves as a defer so no retry churn muddies the counts.
	gate := make(chan struct{})
	closeGate := sync.OnceFunc(func() { close(gate) })
	defer closeGate()
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		<-gate
		w.Write([]byte(`{"choices":[{"message":{"content":"{\"decision\":\"defer\"}"}}]}`))
	}))
	defer srv.Close()
	writeProviderRegistry(t, fmt.Sprintf(`{
		"providers": {"groq": {"base_url": %q, "api_key_env": "MODEL_DECIDER_TEST_KEY"}}
	}`, srv.URL))
	workerGate := make(chan struct{})
	defer close(workerGate)
	f.sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		return &stableScriptedProvider{attempt: -1, scripts: []stableScriptedAttempt{{
			sessionID: "ses_modeldec_cap",
			events:    []stableScriptedEvent{{release: workerGate}},
		}}}, nil
	}
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != Dispatched {
		t.Fatalf("outcome = %s (%s), want dispatched", out.Kind, out.Detail)
	}
	waitFor(t, "worker runtime", func() bool {
		_, err := f.sup.RuntimeStatus(out.RuntimeID)
		return err == nil
	})
	options := []any{map[string]any{"optionId": "allow_once", "kind": "allow"}}
	// Seed the runtime's pending-permission mirror so each fabricated
	// request's decision goroutine skips its mirror-wait deadline: without
	// this, every call would land ~2s late and a broken cap would still pass
	// the exact-count assertions. Routing goes through the production fanout
	// entry (cachePermissionOptions), which caches the options a re-route
	// needs after a freed slot.
	seedPendingPermission(t, f.sup, out.RuntimeID)
	for i := 0; i < maxConcurrentModelDeciders; i++ {
		f.sup.cachePermissionOptions(out.RuntimeID, fmt.Sprintf("perm_req_model_cap_%d", i), options)
		want := int64(i + 1)
		waitFor(t, fmt.Sprintf("in-flight decision call %d", i+1), func() bool { return calls.Load() >= want })
	}
	// A further distinct request hits the cap: no decision call fires, the
	// routed mark stays, and the request stays pending.
	f.sup.cachePermissionOptions(out.RuntimeID, blockedID, options)
	time.Sleep(50 * time.Millisecond)
	if got := calls.Load(); got != int64(maxConcurrentModelDeciders) {
		t.Fatalf("decision calls = %d, want the cap of %d held with no overflow call", got, maxConcurrentModelDeciders)
	}
	if state := f.sup.control.PermissionResolverState(out.RuntimeID, blockedID); state == control.PermissionResolverResolved {
		t.Fatalf("capped request resolver state = %v, want the request left unresolved", state)
	}
	// Free the in-flight calls: each completes as a defer, and the blocked
	// request re-routes from its cached options and gets its call.
	closeGate()
	waitFor(t, "blocked request re-routed after capacity freed", func() bool {
		return calls.Load() > int64(maxConcurrentModelDeciders)
	})
	time.Sleep(50 * time.Millisecond)
	if got := calls.Load(); got != int64(maxConcurrentModelDeciders+1) {
		t.Fatalf("decision calls after release = %d, want exactly %d", got, maxConcurrentModelDeciders+1)
	}
	status, err := f.sup.RuntimeStatus(out.RuntimeID)
	if err != nil {
		t.Fatalf("RuntimeStatus: %v", err)
	}
	rt, _ := status.(map[string]any)
	if exhausted, _ := rt["permission_decider_exhausted"].(bool); exhausted {
		t.Fatal("cap blocking must not mark the decider exhausted")
	}
}

// seedPendingPermission marks the runtime as holding a pending permission
// request with a non-empty structured mirror, so a fabricated request's
// decision goroutine proceeds without waiting out the mirror-wait deadline.
// Same-package test hook for the state the production event fanout writes.
func seedPendingPermission(t *testing.T, sup *Supervisor, runtimeID string) {
	t.Helper()
	sup.controlMu.Lock()
	rt := sup.runtimes[runtimeID]
	sup.controlMu.Unlock()
	if rt == nil {
		t.Fatalf("runtime %q not found", runtimeID)
	}
	rt.mu.Lock()
	rt.pendingPermission = true
	rt.permission = map[string]any{"tool": "bash", "command": "rm -rf /tmp/build"}
	rt.mu.Unlock()
}

// TestDispatchModelDeciderUnappliableDecisionStaysPending proves decisions
// that no pending option can express are judgments the resolver cannot
// apply, not failures: the request stays pending for a human gate, no retry
// fires, and the exhausted flag stays clear. An option whose kind matches
// but which requires a message the model did not supply stays pending too.
func TestDispatchModelDeciderUnappliableDecisionStaysPending(t *testing.T) {
	requestID := "perm_req_model_unappliable"
	// The first call decides reject with only an allow option pending; the
	// second decides allow with no reason against an option that requires a
	// message. Both are decisions the resolver cannot apply.
	var callNum atomic.Int64
	f, capture := newModelDeciderFixture(t, "dispatch-modeldec-unappliable", func(w http.ResponseWriter) {
		if callNum.Add(1) == 1 {
			w.Write([]byte(`{"choices":[{"message":{"content":"{\"decision\":\"reject\",\"reason\":\"looks bad\"}"}}]}`))
			return
		}
		w.Write([]byte(`{"choices":[{"message":{"content":"{\"decision\":\"allow\"}"}}]}`))
	})
	gate := make(chan struct{})
	defer close(gate)
	// The worker's scripted request offers only an allow option; the model
	// decides reject — no pending option can express it.
	f.sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		return &stableScriptedProvider{attempt: -1, scripts: []stableScriptedAttempt{{
			sessionID: "ses_modeldec_unappliable",
			events: []stableScriptedEvent{
				{event: events.Event{
					Event:     "permission.request",
					SessionID: "ses_modeldec_unappliable",
					Fields: map[string]any{
						"request_id": requestID,
						"tool":       "bash",
						"command":    "rm -rf /tmp/build",
						"options":    []any{map[string]any{"optionId": "allow_once", "kind": "allow"}},
					},
				}},
				{release: gate},
			},
		}}}, nil
	}
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != Dispatched {
		t.Fatalf("outcome = %s (%s), want dispatched", out.Kind, out.Detail)
	}
	waitFor(t, "decision call", func() bool {
		calls, _, _ := capture.snapshot()
		return calls >= 1
	})
	// A second routed request offers only an option that requires a message;
	// the model's allow carries no reason the resolver could apply.
	messageOption := []any{map[string]any{"optionId": "allow", "kind": "allow", "requiresMessage": true}}
	f.sup.cachePermissionOptions(out.RuntimeID, "perm_req_model_msg", messageOption)
	waitFor(t, "second decision call", func() bool {
		calls, _, _ := capture.snapshot()
		return calls >= 2
	})
	time.Sleep(50 * time.Millisecond)
	if calls, _, _ := capture.snapshot(); calls != 2 {
		t.Fatalf("decision calls = %d, want 2 (unappliable decisions must not retry)", calls)
	}
	for _, id := range []string{requestID, "perm_req_model_msg"} {
		if state := f.sup.control.PermissionResolverState(out.RuntimeID, id); state == control.PermissionResolverResolved {
			t.Fatalf("request %s must stay pending for a human gate", id)
		}
	}
	status, err := f.sup.RuntimeStatus(out.RuntimeID)
	if err != nil {
		t.Fatalf("RuntimeStatus: %v", err)
	}
	rt, _ := status.(map[string]any)
	if exhausted, _ := rt["permission_decider_exhausted"].(bool); exhausted {
		t.Fatal("an unappliable decision must not mark the decider exhausted")
	}
}
