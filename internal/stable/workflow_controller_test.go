package stable

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/workflow"
	"github.com/sdougbrown/avenor/internal/workflowcontroller"
)

func assertAbsent(t *testing.T, paths ...string) {
	t.Helper()
	for _, p := range paths {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("path %s exists (or stat failed: %v); expected it to stay absent", p, err)
		}
	}
}

func createControllerParams(t *testing.T, id string, maxInflight int) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"controller_id": id, "max_inflight": maxInflight})
	if err != nil {
		t.Fatalf("marshal create params: %v", err)
	}
	return raw
}

// waitForControllerLeader polls the store until the controller holds a lease
// matching match, failing after a bounded deadline.
func waitForControllerLeader(t *testing.T, store *workflowcontroller.ControllerStore, id string, match func(rec workflowcontroller.ControllerRecord) bool) workflowcontroller.ControllerRecord {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		rec, ok, err := store.Get(id)
		if err != nil {
			t.Fatalf("store.Get(%s): %v", id, err)
		}
		if ok && rec.Leader != nil && match(rec) {
			return rec
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for leader on controller %s; last record: %+v", id, rec)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func stopLoopsAtCleanup(t *testing.T, sups ...*Supervisor) {
	t.Helper()
	for _, sup := range sups {
		sup := sup
		t.Cleanup(sup.stopControllerLoops)
	}
}

// TestControllerBarrierIdleCreatesNothing proves a supervisor with a
// nonexistent workflow root stays side-effect free: construction, idle time,
// and even a lazily-triggered barrier (via a controller status of an unknown
// id) never create the root, instances/, or controllers/.
func TestControllerBarrierIdleCreatesNothing(t *testing.T) {
	root := filepath.Join(t.TempDir(), "wfroot")
	sup := NewSupervisor(Config{ControlSocket: newStableSocketPath(t, "wfctl-noroot"), WorkflowRoot: root})
	stopLoopsAtCleanup(t, sup)

	assertAbsent(t, root, filepath.Join(root, "instances"), filepath.Join(root, "controllers"))

	_, err := sup.WorkflowControllerStatus("no-such-controller")
	if err == nil || !errors.Is(err, workflowcontroller.ErrNotFound) {
		t.Fatalf("status of unknown controller err = %v, want not found", err)
	}

	// The barrier ran lazily (status is a controller command) but must not
	// have created anything.
	assertAbsent(t, root, filepath.Join(root, "instances"), filepath.Join(root, "controllers"))
	if sup.workflowMgr == nil {
		t.Fatal("barrier did not construct the workflow manager")
	}
}

// TestControllerBarrierConcurrentWorkflowAndController races the first
// workflow RPC against the first controller command and proves both funnel
// through the same once-barrier: both succeed and exactly one leader loop per
// enabled controller exists.
func TestControllerBarrierConcurrentWorkflowAndController(t *testing.T) {
	root := filepath.Join(t.TempDir(), "wfroot")
	if err := workflow.New(root).CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	sup := NewSupervisor(Config{ControlSocket: newStableSocketPath(t, "wfctl-race"), WorkflowRoot: root})
	stopLoopsAtCleanup(t, sup)

	type rpcResult struct {
		statusErr error
		createRes any
		createErr error
		enableRes any
		enableErr error
	}
	done := make(chan rpcResult, 1)
	go func() {
		var r rpcResult
		_, r.statusErr = (lazyWorkflowHandler{sup}).WorkflowStatus("wf-missing")
		r.createRes, r.createErr = sup.WorkflowControllerCreate(createControllerParams(t, "c1", 5))
		r.enableRes, r.enableErr = sup.WorkflowControllerEnable("c1")
		done <- r
	}()
	_, wfErr := (lazyWorkflowHandler{sup}).WorkflowStatus("wf-missing-too")

	r := <-done
	if wfErr == nil || !strings.Contains(wfErr.Error(), "workflow not found") {
		t.Fatalf("workflow status err = %v, want workflow not found", wfErr)
	}
	if r.statusErr == nil || !strings.Contains(r.statusErr.Error(), "workflow not found") {
		t.Fatalf("concurrent workflow status err = %v, want workflow not found", r.statusErr)
	}
	if r.createErr != nil {
		t.Fatalf("controller create: %v", r.createErr)
	}
	rec, ok := r.createRes.(workflowcontroller.ControllerRecord)
	if !ok || rec.ControllerID != "c1" || rec.MaxInflight != 5 {
		t.Fatalf("create result = %#v, want c1 record with max_inflight 5", r.createRes)
	}
	if r.enableErr != nil {
		t.Fatalf("controller enable: %v", r.enableErr)
	}
	if sup.workflowMgr == nil {
		t.Fatal("barrier did not construct the workflow manager")
	}
	sup.controllerLoopsMu.Lock()
	loops := len(sup.controllerLoops)
	sup.controllerLoopsMu.Unlock()
	if loops != 1 {
		t.Fatalf("leader loops = %d, want exactly 1 after enable", loops)
	}
}

// TestControllerBarrierCatalogFailureDisablesControllers corrupts an
// instance's event log so catalog recovery fails: controller methods must
// return the barrier error while workflow RPCs keep serving.
func TestControllerBarrierCatalogFailureDisablesControllers(t *testing.T) {
	root := filepath.Join(t.TempDir(), "wfroot")
	parentID, childID := stageTerminalChildComposition(t, root)

	// Corrupt the child's event log at a non-final line: recovery must fail.
	eventsPath := filepath.Join(root, "instances", childID, "events.ndjson")
	corrupted := []byte("not-json\n" + `{"kind":"created","seq":1}` + "\n")
	if err := os.WriteFile(eventsPath, corrupted, 0o644); err != nil {
		t.Fatalf("corrupt event log: %v", err)
	}

	sup := NewSupervisor(Config{ControlSocket: newStableSocketPath(t, "wfctl-catalog-fail"), WorkflowRoot: root})
	stopLoopsAtCleanup(t, sup)

	if _, err := sup.WorkflowControllerCreate(createControllerParams(t, "c1", 5)); err == nil {
		t.Fatal("controller create succeeded despite failed catalog recovery")
	}
	if _, err := sup.WorkflowControllerList(); err == nil {
		t.Fatal("controller list succeeded despite failed catalog recovery")
	}
	if _, err := sup.WorkflowReady("c1", 10); err == nil {
		t.Fatal("workflow.ready succeeded despite failed catalog recovery")
	}

	// Workflow RPCs keep serving: the parent instance's own state is intact.
	res, err := (lazyWorkflowHandler{sup}).WorkflowStatus(parentID)
	if err != nil {
		t.Fatalf("workflow status still served after barrier failure: %v", err)
	}
	if res == nil {
		t.Fatal("workflow status returned nil result")
	}
}

// TestControllerRestartReacquiresLease simulates a restart handover: A holds
// the lease, dies (lease expired via a future-clock recovery), and B on the
// same root re-acquires with owner epoch+1 while A never leads again.
func TestControllerRestartReacquiresLease(t *testing.T) {
	root := filepath.Join(t.TempDir(), "wfroot")
	if err := workflow.New(root).CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	store := workflowcontroller.NewStore(root)

	supA := NewSupervisor(Config{ControlSocket: newStableSocketPath(t, "wfctl-restart-a"), WorkflowRoot: root})
	stopLoopsAtCleanup(t, supA)
	if _, err := supA.WorkflowControllerCreate(createControllerParams(t, "c1", 5)); err != nil {
		t.Fatalf("A create: %v", err)
	}
	if _, err := supA.WorkflowControllerEnable("c1"); err != nil {
		t.Fatalf("A enable: %v", err)
	}
	recA := waitForControllerLeader(t, store, "c1", func(rec workflowcontroller.ControllerRecord) bool {
		return rec.Leader.OwnerID == supA.supervisorIdentity()
	})
	if recA.Leader.OwnerEpoch != 1 {
		t.Fatalf("A leader epoch = %d, want 1", recA.Leader.OwnerEpoch)
	}

	// The old process dies: expire its lease via a future-clock recovery.
	future := time.Now().Add(2 * workflowcontroller.LeaseTTL)
	if _, err := workflowcontroller.NewStoreWithClock(root, func() time.Time { return future }).Recover(); err != nil {
		t.Fatalf("future-clock recovery: %v", err)
	}
	// Abandon A: stop its loop so it can no longer renew or re-acquire.
	supA.stopControllerLoop("c1")
	if rec, _, err := store.Get("c1"); err != nil {
		t.Fatalf("store.Get after abandoning A: %v", err)
	} else if rec.Leader != nil {
		t.Fatalf("lease still held after abandoning A: %+v", rec.Leader)
	}

	// B starts on the same root; its barrier recovers the controller and its
	// leader loop re-acquires the lease with owner epoch+1.
	supB := NewSupervisor(Config{ControlSocket: newStableSocketPath(t, "wfctl-restart-b"), WorkflowRoot: root})
	stopLoopsAtCleanup(t, supB)
	if _, err := supB.WorkflowControllerList(); err != nil {
		t.Fatalf("B list (barrier trigger): %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		res, err := supB.WorkflowControllerStatus("c1")
		if err != nil {
			t.Fatalf("B status: %v", err)
		}
		status := res.(map[string]any)
		leader, _ := status["leader"].(map[string]any)
		if leader != nil && leader["is_this_process"] == true {
			if got := leader["owner_id"]; got != supB.supervisorIdentity() {
				t.Fatalf("B leader owner_id = %v, want %v", got, supB.supervisorIdentity())
			}
			if epoch := leader["owner_epoch"].(int64); epoch <= recA.Leader.OwnerEpoch {
				t.Fatalf("B leader epoch = %d, want > %d", epoch, recA.Leader.OwnerEpoch)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for B to become leader; last status: %#v", status)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// A must not be leader afterward.
	if rec, _, err := store.Get("c1"); err != nil {
		t.Fatalf("store.Get: %v", err)
	} else if rec.Leader != nil && rec.Leader.OwnerID == supA.supervisorIdentity() {
		t.Fatalf("A is leader after B took over: %+v", rec.Leader)
	}
}

// TestControllerDisableReleasesLiveLease proves disabling a live leader
// persists desired=disabled with the reason, durably releases the lease,
// removes the loop, and refuses further acquisition.
func TestControllerDisableReleasesLiveLease(t *testing.T) {
	root := filepath.Join(t.TempDir(), "wfroot")
	store := workflowcontroller.NewStore(root)
	sup := NewSupervisor(Config{ControlSocket: newStableSocketPath(t, "wfctl-disable"), WorkflowRoot: root})
	stopLoopsAtCleanup(t, sup)

	if _, err := sup.WorkflowControllerCreate(createControllerParams(t, "c1", 5)); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := sup.WorkflowControllerEnable("c1"); err != nil {
		t.Fatalf("enable: %v", err)
	}
	waitForControllerLeader(t, store, "c1", func(rec workflowcontroller.ControllerRecord) bool {
		return rec.Leader.OwnerID == sup.supervisorIdentity()
	})

	if _, err := sup.WorkflowControllerDisable("c1", nil); err == nil {
		t.Fatal("disable without reason succeeded")
	}

	raw, err := json.Marshal(map[string]string{"reason": "ops pause"})
	if err != nil {
		t.Fatalf("marshal disable params: %v", err)
	}
	if _, err := sup.WorkflowControllerDisable("c1", raw); err != nil {
		t.Fatalf("disable: %v", err)
	}

	rec, ok, err := store.Get("c1")
	if err != nil || !ok {
		t.Fatalf("store.Get after disable: ok=%v err=%v", ok, err)
	}
	if rec.DesiredState != workflowcontroller.DesiredDisabled {
		t.Fatalf("desired state after disable = %q, want disabled", rec.DesiredState)
	}
	if rec.DisabledReason != "ops pause" {
		t.Fatalf("disabled reason = %q, want ops pause", rec.DisabledReason)
	}
	if rec.Leader != nil {
		t.Fatalf("lease still held after disable: %+v", rec.Leader)
	}
	sup.controllerLoopsMu.Lock()
	_, loopRunning := sup.controllerLoops["c1"]
	sup.controllerLoopsMu.Unlock()
	if loopRunning {
		t.Fatal("leader loop still tracked after disable")
	}
	if _, _, err := store.AcquireLease("c1", "someone"); !errors.Is(err, workflowcontroller.ErrDisabled) {
		t.Fatalf("AcquireLease while disabled err = %v, want ErrDisabled", err)
	}
}

// TestControllerEnableTwiceSingleLoop proves enabling the same controller
// twice keeps exactly one leader loop and one stable lease in this process.
func TestControllerEnableTwiceSingleLoop(t *testing.T) {
	root := filepath.Join(t.TempDir(), "wfroot")
	store := workflowcontroller.NewStore(root)
	sup := NewSupervisor(Config{ControlSocket: newStableSocketPath(t, "wfctl-enable-twice"), WorkflowRoot: root})
	stopLoopsAtCleanup(t, sup)

	if _, err := sup.WorkflowControllerCreate(createControllerParams(t, "c1", 5)); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := sup.WorkflowControllerEnable("c1"); err != nil {
		t.Fatalf("enable 1: %v", err)
	}
	if _, err := sup.WorkflowControllerEnable("c1"); err != nil {
		t.Fatalf("enable 2: %v", err)
	}
	sup.controllerLoopsMu.Lock()
	loops := len(sup.controllerLoops)
	sup.controllerLoopsMu.Unlock()
	if loops != 1 {
		t.Fatalf("leader loops after double enable = %d, want 1", loops)
	}

	rec := waitForControllerLeader(t, store, "c1", func(rec workflowcontroller.ControllerRecord) bool {
		return rec.Leader.OwnerID == sup.supervisorIdentity()
	})
	leaseID := rec.Leader.LeaseID
	time.Sleep(300 * time.Millisecond)
	rec, _, err := store.Get("c1")
	if err != nil {
		t.Fatalf("store.Get: %v", err)
	}
	if rec.Leader == nil || rec.Leader.LeaseID != leaseID {
		t.Fatalf("lease churned from duplicate loops: was %q, now %+v", leaseID, rec.Leader)
	}
}
