package stable

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

	// The first controller command triggers the barrier and retains its error.
	if _, err := sup.WorkflowControllerCreate(createControllerParams(t, "c1", 5)); err == nil {
		t.Fatal("controller create succeeded despite failed catalog recovery")
	}
	if sup.workflowBarrierErr == nil {
		t.Fatal("barrier error not retained after failed catalog recovery")
	}
	barrierErr := sup.workflowBarrierErr

	raw, err := json.Marshal(map[string]string{"reason": "ops pause"})
	if err != nil {
		t.Fatalf("marshal disable params: %v", err)
	}
	checks := []struct {
		name string
		fn   func() error
	}{
		{"create", func() error { _, err := sup.WorkflowControllerCreate(createControllerParams(t, "c1", 5)); return err }},
		{"enable", func() error { _, err := sup.WorkflowControllerEnable("c1"); return err }},
		{"disable", func() error { _, err := sup.WorkflowControllerDisable("c1", raw); return err }},
		{"status", func() error { _, err := sup.WorkflowControllerStatus("c1"); return err }},
		{"list", func() error { _, err := sup.WorkflowControllerList(); return err }},
		{"ready", func() error { _, err := sup.WorkflowReady("c1", 10); return err }},
	}
	for _, check := range checks {
		if err := check.fn(); !errors.Is(err, barrierErr) {
			t.Fatalf("controller %s after barrier failure: err = %v, want retained barrier error", check.name, err)
		}
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
			epoch, ok := leader["owner_epoch"].(int64)
			if !ok {
				t.Fatalf("B leader owner_epoch = %#v, want int64", leader["owner_epoch"])
			}
			if epoch <= recA.Leader.OwnerEpoch {
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

// TestControllerStatusSurfacesNextPollReadError proves a failed poll-time
// read is reported as next_poll_error instead of silently rendering
// next_poll_at: nil. The status reads the controller record twice — once for
// the record and once inside NextPollTime — so the test corrupts the
// snapshot concurrently until the flip lands in the window between the two
// reads: a corrupt first read fails the whole call, only a corrupt second
// read reaches the surfaced error.
func TestControllerStatusSurfacesNextPollReadError(t *testing.T) {
	root := filepath.Join(t.TempDir(), "wfroot")
	sup := NewSupervisor(Config{ControlSocket: newStableSocketPath(t, "wfctl-nextpoll-err"), WorkflowRoot: root})
	stopLoopsAtCleanup(t, sup)
	if _, err := sup.WorkflowControllerCreate(createControllerParams(t, "c1", 2)); err != nil {
		t.Fatalf("create: %v", err)
	}
	snapshot := filepath.Join(root, "controllers", "c1", "controller.json")
	valid, err := os.ReadFile(snapshot)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	corrupt := valid[:len(valid)/2]

	stop := make(chan struct{})
	flipped := make(chan struct{})
	go func() {
		defer close(flipped)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = os.WriteFile(snapshot, corrupt, 0o600)
			time.Sleep(200 * time.Microsecond)
			_ = os.WriteFile(snapshot, valid, 0o600)
			time.Sleep(200 * time.Microsecond)
		}
	}()
	t.Cleanup(func() {
		close(stop)
		<-flipped
		_ = os.WriteFile(snapshot, valid, 0o600)
	})

	deadline := time.Now().Add(10 * time.Second)
	for {
		st, err := sup.WorkflowControllerStatus("c1")
		if err != nil {
			if time.Now().After(deadline) {
				t.Fatalf("status never got past a corrupt first read: %v", err)
			}
			continue // first read hit the corrupt bytes; flip again
		}
		status := st.(map[string]any)
		if got := status["next_poll_error"]; got != nil {
			msg, ok := got.(string)
			if !ok || !strings.Contains(msg, "controller c1 snapshot") {
				t.Fatalf("next_poll_error = %#v, want the snapshot read error", got)
			}
			if status["next_poll_at"] != nil {
				t.Fatalf("next_poll_at = %#v, want nil on a failed read", status["next_poll_at"])
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("next_poll_error never surfaced despite a concurrently corrupt snapshot")
		}
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

// TestControllerSelfExitThenReenable proves a leader loop that exits on its
// own (it observes a desired state of disabled, changed out-of-band as by
// another process) removes its own map entry, so a later enable in the same
// process starts a fresh loop that acquires leadership instead of finding a
// stale entry and no-oping.
func TestControllerSelfExitThenReenable(t *testing.T) {
	root := filepath.Join(t.TempDir(), "wfroot")
	store := workflowcontroller.NewStore(root)
	sup := NewSupervisor(Config{ControlSocket: newStableSocketPath(t, "wfctl-self-exit"), WorkflowRoot: root})
	sup.controllerRenewInterval = 20 * time.Millisecond
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

	// Disable out-of-band, bypassing the supervisor handler: the loop must
	// observe the desired state, exit on its own, and remove its entry.
	if _, err := store.SetDesiredState("c1", workflowcontroller.DesiredDisabled, "out-of-band"); err != nil {
		t.Fatalf("out-of-band disable: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		sup.controllerLoopsMu.Lock()
		loops := len(sup.controllerLoops)
		sup.controllerLoopsMu.Unlock()
		if loops == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("self-exited loop still tracked after disable; loops = %d", loops)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Re-enabling in the same process must start a fresh loop that acquires
	// leadership.
	if _, err := sup.WorkflowControllerEnable("c1"); err != nil {
		t.Fatalf("re-enable: %v", err)
	}
	rec := waitForControllerLeader(t, store, "c1", func(rec workflowcontroller.ControllerRecord) bool {
		return rec.Leader.OwnerID == sup.supervisorIdentity()
	})
	sup.controllerLoopsMu.Lock()
	loops := len(sup.controllerLoops)
	sup.controllerLoopsMu.Unlock()
	if loops != 1 {
		t.Fatalf("leader loops after re-enable = %d, want 1", loops)
	}
	if rec.Leader.OwnerEpoch != 2 {
		t.Fatalf("leader epoch after re-enable = %d, want 2", rec.Leader.OwnerEpoch)
	}
}

// TestControllerEnableDisableRaceMatchesDesiredState hammers concurrent
// enable/disable RPCs and proves the serialization invariant: when both
// goroutines finish, loop presence matches the last persisted desired state
// and exactly one loop serves an enabled controller.
func TestControllerEnableDisableRaceMatchesDesiredState(t *testing.T) {
	root := filepath.Join(t.TempDir(), "wfroot")
	store := workflowcontroller.NewStore(root)
	sup := NewSupervisor(Config{ControlSocket: newStableSocketPath(t, "wfctl-race-hammer"), WorkflowRoot: root})
	stopLoopsAtCleanup(t, sup)

	if _, err := sup.WorkflowControllerCreate(createControllerParams(t, "c1", 5)); err != nil {
		t.Fatalf("create: %v", err)
	}
	raw, err := json.Marshal(map[string]string{"reason": "race"})
	if err != nil {
		t.Fatalf("marshal disable params: %v", err)
	}

	const iterations = 50
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			if _, err := sup.WorkflowControllerEnable("c1"); err != nil {
				t.Errorf("enable %d: %v", i, err)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			if _, err := sup.WorkflowControllerDisable("c1", raw); err != nil {
				t.Errorf("disable %d: %v", i, err)
			}
		}
	}()
	wg.Wait()

	rec, ok, err := store.Get("c1")
	if err != nil || !ok {
		t.Fatalf("final get: ok=%v err=%v", ok, err)
	}
	sup.controllerLoopsMu.Lock()
	loops := len(sup.controllerLoops)
	sup.controllerLoopsMu.Unlock()
	if rec.DesiredState == workflowcontroller.DesiredEnabled {
		if loops != 1 {
			t.Fatalf("durably enabled but leader loops = %d, want 1", loops)
		}
		waitForControllerLeader(t, store, "c1", func(rec workflowcontroller.ControllerRecord) bool {
			return rec.Leader.OwnerID == sup.supervisorIdentity()
		})
	} else {
		if loops != 0 {
			t.Fatalf("durably disabled but leader loops = %d, want 0", loops)
		}
		if _, _, err := store.AcquireLease("c1", "someone"); !errors.Is(err, workflowcontroller.ErrDisabled) {
			t.Fatalf("acquire while disabled: err = %v, want ErrDisabled", err)
		}
	}
}

// TestControllerEagerBarrierStartsLeaderWithoutRPC pre-creates durable
// controller state (an enabled controller record) and starts a Supervisor
// through Run, its normal startup path: the eager startup barrier recovers
// state and the enabled controller gains a leader without any workflow or
// controller RPC being issued, and the candidate index is recovered.
func TestControllerEagerBarrierStartsLeaderWithoutRPC(t *testing.T) {
	root := filepath.Join(t.TempDir(), "wfroot")
	prev := workflowcontroller.NewStore(root)
	if _, err := prev.Create("c1", 5); err != nil {
		t.Fatalf("previous-process create: %v", err)
	}
	if _, err := prev.SetDesiredState("c1", workflowcontroller.DesiredEnabled, ""); err != nil {
		t.Fatalf("previous-process enable: %v", err)
	}

	sup := NewSupervisor(Config{ControlSocket: newStableSocketPath(t, "wfctl-eager"), WorkflowRoot: root})
	stopLoopsAtCleanup(t, sup)
	t.Cleanup(func() { _ = sup.Shutdown("graceful") })
	done := make(chan int, 1)
	go func() { done <- sup.Run() }()

	store := workflowcontroller.NewStore(root)
	waitForControllerLeader(t, store, "c1", func(rec workflowcontroller.ControllerRecord) bool {
		return rec.Leader.OwnerID == sup.supervisorIdentity()
	})
	if sup.workflowMgr == nil {
		t.Fatal("eager barrier did not construct the workflow manager")
	}
	if _, err := sup.workflowMgr.CandidatesForController("c1", 10); err != nil {
		t.Fatalf("candidate index not recovered by eager barrier: %v", err)
	}
}

// TestControllerLeaseRenewalObserved proves the leader loop renews its lease:
// with a small injectable renew interval, RenewedAt advances across at least
// two renewals and ExpiresAt moves forward.
func TestControllerLeaseRenewalObserved(t *testing.T) {
	root := filepath.Join(t.TempDir(), "wfroot")
	store := workflowcontroller.NewStore(root)
	sup := NewSupervisor(Config{ControlSocket: newStableSocketPath(t, "wfctl-renew"), WorkflowRoot: root})
	sup.controllerRenewInterval = 25 * time.Millisecond
	stopLoopsAtCleanup(t, sup)

	if _, err := sup.WorkflowControllerCreate(createControllerParams(t, "c1", 5)); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := sup.WorkflowControllerEnable("c1"); err != nil {
		t.Fatalf("enable: %v", err)
	}
	rec := waitForControllerLeader(t, store, "c1", func(rec workflowcontroller.ControllerRecord) bool {
		return rec.Leader.OwnerID == sup.supervisorIdentity()
	})
	baseRenewedAt := rec.Leader.RenewedAt
	baseExpiresAt := rec.Leader.ExpiresAt

	// Consecutive renewals are at least one interval apart, so two observed
	// advances of RenewedAt prove two renewals happened.
	deadline := time.Now().Add(10 * time.Second)
	renewals := 0
	last := baseRenewedAt
	var err error
	for renewals < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for two renewals; observed %d after %s", renewals, baseRenewedAt)
		}
		time.Sleep(5 * time.Millisecond)
		rec, _, err = store.Get("c1")
		if err != nil {
			t.Fatalf("store.Get during renewals: %v", err)
		}
		if rec.Leader == nil {
			t.Fatal("lease lost while waiting for renewals")
		}
		if rec.Leader.RenewedAt.After(last) {
			last = rec.Leader.RenewedAt
			renewals++
		}
	}
	if !rec.Leader.ExpiresAt.After(baseExpiresAt) {
		t.Fatalf("expires_at did not advance across renewals: %s -> %s", baseExpiresAt, rec.Leader.ExpiresAt)
	}
}

// TestWorkflowReadyAdvisoryCandidates proves the success path of
// Supervisor.WorkflowReady: after the barrier recovers the candidate index,
// instantiating auto-dispatch workflows surfaces them as advisory candidates
// for the owning controller, and the limit is honored.
func TestWorkflowReadyAdvisoryCandidates(t *testing.T) {
	root := filepath.Join(t.TempDir(), "wfroot")
	if err := workflow.New(root).CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	sup := NewSupervisor(Config{ControlSocket: newStableSocketPath(t, "wfctl-ready"), WorkflowRoot: root})
	stopLoopsAtCleanup(t, sup)

	if _, err := sup.WorkflowControllerCreate(createControllerParams(t, "c1", 5)); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := sup.WorkflowControllerEnable("c1"); err != nil {
		t.Fatalf("enable: %v", err)
	}

	// The barrier ran (create is a controller command) and recovered the
	// candidate index; new instances enter it via the store commit hook.
	template := map[string]any{
		"schema_version":   1,
		"template_id":      "ready-tmpl",
		"template_version": "1",
		"entry_nodes":      []string{"start"},
		"nodes": []any{map[string]any{
			"id":       "start",
			"action":   map[string]any{"type": "run", "prompt": "do the thing"},
			"dispatch": map[string]any{"mode": "auto", "controller_id": "c1", "concurrency_key": "deploys"},
		}},
		"terminal_outcomes": []string{"done"},
	}
	tmplJSON, err := json.Marshal(template)
	if err != nil {
		t.Fatalf("marshal template: %v", err)
	}
	if _, err := (lazyWorkflowHandler{sup}).WorkflowCreate(tmplJSON); err != nil {
		t.Fatalf("workflow create: %v", err)
	}

	instantiate := func() workflow.WorkflowID {
		t.Helper()
		payload, err := json.Marshal(map[string]string{"template_id": "ready-tmpl", "template_version": "1"})
		if err != nil {
			t.Fatalf("marshal instantiate: %v", err)
		}
		out, err := (lazyWorkflowHandler{sup}).WorkflowInstantiate(payload)
		if err != nil {
			t.Fatalf("workflow instantiate: %v", err)
		}
		id, ok := out.(map[string]any)["workflow_id"].(string)
		if !ok || id == "" {
			t.Fatalf("instantiate result = %#v, want workflow_id", out)
		}
		return workflow.WorkflowID(id)
	}
	wf1 := instantiate()
	wf2 := instantiate()

	ready := func(limit int) (map[string]any, error) {
		res, err := sup.WorkflowReady("c1", limit)
		if err != nil {
			return nil, err
		}
		m, ok := res.(map[string]any)
		if !ok {
			t.Fatalf("ready result = %#v, want map", res)
		}
		return m, nil
	}

	// limit=0 returns every ready candidate (both instances).
	resAll, err := ready(0)
	if err != nil {
		t.Fatalf("ready(0): %v", err)
	}
	if got := resAll["advisory"]; got != true {
		t.Fatalf("advisory = %v, want true", got)
	}
	if got := resAll["controller_id"]; got != "c1" {
		t.Fatalf("controller_id = %v, want c1", got)
	}
	candsAll, ok := resAll["candidates"].([]workflow.ReadyCandidate)
	if !ok {
		t.Fatalf("candidates = %#v, want []workflow.ReadyCandidate", resAll["candidates"])
	}
	if len(candsAll) != 2 {
		t.Fatalf("candidates(0) = %d, want 2", len(candsAll))
	}
	seen := map[workflow.WorkflowID]workflow.NodeID{}
	for _, c := range candsAll {
		if c.ControllerID != "c1" {
			t.Fatalf("candidate controller_id = %q, want c1", c.ControllerID)
		}
		if c.Identity.NodeID != "start" {
			t.Fatalf("candidate node = %q, want start", c.Identity.NodeID)
		}
		if c.Identity.ActivationID == "" {
			t.Fatal("candidate activation id is empty")
		}
		seen[c.Identity.WorkflowID] = c.Identity.NodeID
	}
	if seen[wf1] != "start" || seen[wf2] != "start" {
		t.Fatalf("candidates do not cover both workflows: %#v", seen)
	}

	// limit=1 returns at most one candidate.
	resOne, err := ready(1)
	if err != nil {
		t.Fatalf("ready(1): %v", err)
	}
	candsOne, ok := resOne["candidates"].([]workflow.ReadyCandidate)
	if !ok {
		t.Fatalf("candidates = %#v, want []workflow.ReadyCandidate", resOne["candidates"])
	}
	if len(candsOne) != 1 {
		t.Fatalf("candidates(1) = %d, want 1 (limit honored)", len(candsOne))
	}
}

// TestControllerRenewFailureYieldsAndReacquires proves the leader loop's
// renew-failure branch: when the lease is taken out-of-band (a CAS conflict
// on the next renew), the loop yields (holding=false) instead of exiting,
// keeps its map entry, and re-acquires once the lease is released.
func TestControllerRenewFailureYieldsAndReacquires(t *testing.T) {
	root := filepath.Join(t.TempDir(), "wfroot")
	if err := workflow.New(root).CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	store := workflowcontroller.NewStore(root)
	sup := NewSupervisor(Config{ControlSocket: newStableSocketPath(t, "wfctl-renew-fail"), WorkflowRoot: root})
	sup.controllerRenewInterval = 20 * time.Millisecond
	stopLoopsAtCleanup(t, sup)

	if _, err := sup.WorkflowControllerCreate(createControllerParams(t, "c1", 5)); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := sup.WorkflowControllerEnable("c1"); err != nil {
		t.Fatalf("enable: %v", err)
	}
	recA := waitForControllerLeader(t, store, "c1", func(rec workflowcontroller.ControllerRecord) bool {
		return rec.Leader.OwnerID == sup.supervisorIdentity()
	})
	if recA.Leader.OwnerEpoch != 1 {
		t.Fatalf("A leader epoch = %d, want 1", recA.Leader.OwnerEpoch)
	}

	// Out-of-band: a second store handle with an advanced clock expires A's
	// lease and acquires it as owner B, bumping the epoch.
	advanced := time.Now().Add(2 * workflowcontroller.LeaseTTL)
	ooStore := workflowcontroller.NewStoreWithClock(root, func() time.Time { return advanced })
	recB, ok, err := ooStore.AcquireLease("c1", "owner-b")
	if err != nil {
		t.Fatalf("B acquire: %v", err)
	}
	if !ok {
		t.Fatalf("B acquire: not granted; rec=%+v", recB)
	}
	if recB.Leader.OwnerEpoch != recA.Leader.OwnerEpoch+1 {
		t.Fatalf("B leader epoch = %d, want %d", recB.Leader.OwnerEpoch, recA.Leader.OwnerEpoch+1)
	}
	leaseB := recB.Leader.LeaseID
	epochB := recB.Leader.OwnerEpoch

	// A's next renew is a CAS conflict: the loop yields (holding=false) and
	// falls back to acquisition retry. Wait for the yield to settle: B is
	// the leader and A's loop entry is still present (A did not exit).
	deadline := time.Now().Add(10 * time.Second)
	for {
		rec, _, err := store.Get("c1")
		if err != nil {
			t.Fatalf("store.Get: %v", err)
		}
		if rec.Leader != nil && rec.Leader.OwnerID == "owner-b" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for B to be leader; last=%+v", rec.Leader)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Give A's loop a renew cycle to hit the renew-failure branch and yield.
	time.Sleep(50 * time.Millisecond)
	sup.controllerLoopsMu.Lock()
	_, loopPresent := sup.controllerLoops["c1"]
	sup.controllerLoopsMu.Unlock()
	if !loopPresent {
		t.Fatal("A's loop entry is gone after the renew-failure yield; A exited instead of yielding")
	}

	// Release B's lease: A's acquisition retry re-acquires with epoch+1.
	if _, err := store.ReleaseLease("c1", leaseB, epochB); err != nil {
		t.Fatalf("release B lease: %v", err)
	}
	recA2 := waitForControllerLeader(t, store, "c1", func(rec workflowcontroller.ControllerRecord) bool {
		return rec.Leader.OwnerID == sup.supervisorIdentity()
	})
	if recA2.Leader.OwnerEpoch != epochB+1 {
		t.Fatalf("A re-acquired epoch = %d, want %d", recA2.Leader.OwnerEpoch, epochB+1)
	}
}
