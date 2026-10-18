package stable

// Test lifecycle helpers. Every fixture that drives a lifecycle transition
// initializes its synthetic child explicitly — options declare the lifecycle
// state, attempt class and presentation data, never "derive from flags" — and
// every supervisor-owning test registers the post-teardown assertion that
// waits for owned child completion before asserting lifecycleErrors is zero.

import (
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runstate"
)

// testLifecycleOptions declares the explicit lifecycle initialization for a
// synthetic test child. Later mutations use signals or field commits under
// the required locks.
type testLifecycleOptions struct {
	id             string
	exec           execState
	attempts       int
	completion     completionStage
	shutdown       shutdownStage
	writer         writerStage
	class          attemptClass
	phase          string
	phaseLabel     string
	exitCode       int
	lastStopReason string
}

// initializeTestLifecycle explicitly initializes a synthetic child's lifecycle.
// Fixtures declare the starting state and its attempt class here instead of
// relying on zero values. Whole-state initialization is allowed only for a
// new, unpublished child with no concurrent users; the invariant check keeps
// test declarations honest.
func initializeTestLifecycle(t testing.TB, child *childRuntime, opts testLifecycleOptions) {
	t.Helper()
	if child.done == nil {
		// Fixture setup: a synthetic child always needs a completable done
		// channel so cleanup can join it.
		child.done = make(chan struct{})
	}
	if child.runDone == nil {
		// The run-goroutine join point is created alongside done; closeRunDone
		// makes closing it idempotent for fixtures without a run goroutine.
		child.runDone = make(chan struct{})
	}
	class := opts.class
	if class == directAttemptClass && opts.attempts > 1 {
		t.Fatalf("test child %s: direct attempt class cannot carry %d attempts", child.id, opts.attempts)
	}
	st := lifecycleState{
		exec:       opts.exec,
		attempts:   opts.attempts,
		shutdown:   opts.shutdown,
		writer:     opts.writer,
		completion: opts.completion,
		class:      class,
	}
	if err := validateLifecycleState(st); err != nil {
		t.Fatalf("test child %s: invalid declared lifecycle state: %v", child.id, err)
	}
	child.lc = st
	child.phase = runstate.Phase(opts.phase)
	child.phaseLabel = opts.phaseLabel
	child.exitCode = opts.exitCode
	child.lastStopReason = opts.lastStopReason
}

// registerSyntheticChild registers a synthetic child with its owning
// supervisor in one ordered step: explicit lifecycle initialization, then
// owned-completion tracking, then registry insertion. initializeTestLifecycle
// also creates the done and run-goroutine join points when the fixture omitted
// them, so no separate done guard is needed before the call.
func registerSyntheticChild(t *testing.T, sup *Supervisor, child *childRuntime, opts testLifecycleOptions) {
	t.Helper()
	initializeTestLifecycle(t, child, opts)
	trackTestChildCompletion(t, child)
	sup.runtimes[child.id] = child
}

// newTestChildLifecycle constructs a synthetic child with an explicitly
// initialized lifecycle and registers the owned-child cleanup tracking: the
// cleanup joins every owned child (done channel and run-goroutine join point)
// before the lifecycle error counter is read after teardown.
func newTestChildLifecycle(t *testing.T, sup *Supervisor, opts testLifecycleOptions) *childRuntime {
	t.Helper()
	if sup == nil {
		t.Fatal("newTestChildLifecycle requires an explicit owning supervisor")
	}
	child := &childRuntime{
		id:       opts.id,
		done:     make(chan struct{}),
		runDone:  make(chan struct{}),
		promptCh: make(chan struct{}, 1),
	}
	initializeTestLifecycle(t, child, opts)
	trackTestChildCompletion(t, child)
	return child
}

// trackTestChildCompletion registers the owned-child cleanup tracking for a
// child constructed elsewhere: wait for its done channel and run-goroutine
// join. A join failure fails the cleanup fatally, so the lifecycle counters
// are never read while a child is still tearing down; the counter assertion
// itself belongs to the owner-level cleanup.
func trackTestChildCompletion(t *testing.T, child *childRuntime) {
	t.Helper()
	t.Cleanup(func() {
		joinTestChildCompletionFatal(t, child)
	})
}

// newLifecycleTestOwner returns an explicit bare Supervisor owner for
// synthetic fixtures that drive lifecycle transitions without a full
// supervisor. It is nonnil by construction: there is no nil-owner fallback.
func newLifecycleTestOwner() *Supervisor {
	return &Supervisor{}
}

// assertNoLifecycleErrors asserts that the supervisor recorded zero illegal
// lifecycle transitions.
func assertNoLifecycleErrors(t testing.TB, sup *Supervisor) {
	t.Helper()
	if errs := sup.lifecycleErrors.Load(); errs != 0 {
		t.Errorf("lifecycleErrors = %d, want 0", errs)
	}
}

// assertSupervisorLifecycleClean waits for every child completion channel
// currently owned by the supervisor and then asserts the lifecycle error
// counter is zero. It must run after the test's teardown actions (shutdown, control
// stop, cancel, joins), never before them. Children deleted from the registry
// before completion (spawn rollback) complete inside the rollback path, so
// their counters still land here.
func assertSupervisorLifecycleClean(t *testing.T, sup *Supervisor) {
	t.Helper()
	sup.controlMu.Lock()
	children := make([]*childRuntime, 0, len(sup.runtimes))
	for _, c := range sup.runtimes {
		children = append(children, c)
	}
	sup.controlMu.Unlock()
	for _, c := range children {
		joinTestChildCompletion(t, c)
	}
	assertNoLifecycleErrors(t, sup)
}

// joinTestChildCompletion waits for a child's done channel and then for its
// run-goroutine join point. done alone is insufficient: direct runs do work
// after complete (the capacity release) and the run/writer/answer goroutines
// must be joined before any lifecycle counter is read. Owner-level cleanups
// use this variant so every owned child is attempted before the counters are
// read once.
func joinTestChildCompletion(t testing.TB, child *childRuntime) {
	joinTestChild(t, child, false)
}

// joinTestChildCompletionFatal waits like joinTestChildCompletion but fails
// the test fatally on a join timeout or a missing join point: a cleanup that
// would be followed by counter reads must never continue past a failed join.
func joinTestChildCompletionFatal(t testing.TB, child *childRuntime) {
	joinTestChild(t, child, true)
}

func joinTestChild(t testing.TB, child *childRuntime, fatal bool) {
	fail := t.Errorf
	if fatal {
		fail = t.Fatalf
	}
	select {
	case <-child.done:
	case <-time.After(5 * time.Second):
		fail("lifecycle cleanup: child %s done channel still open after teardown", child.id)
		// One stuck join is enough evidence; do not wait out a second timeout
		// per child on the non-fatal path.
		return
	}
	if child.runDone != nil {
		select {
		case <-child.runDone:
		case <-time.After(5 * time.Second):
			fail("lifecycle cleanup: child %s run goroutine still running after teardown", child.id)
		}
	} else {
		fail("lifecycle cleanup: child %s has no run-goroutine join point", child.id)
	}
}

// newTestSupervisor is the explicit owner constructor for tests: it registers
// the post-teardown lifecycle assertion as a cleanup that runs after the
// test's own deferred teardown actions. It must be called before the test
// registers its own teardown cleanups: t.Cleanup runs last-in-first-out, so a
// cleanup registered later would run before this one and race the assertion.
// The cleanup stops admission by cancelling every owned runtime, waits for
// all child completion channels and run-goroutine joins, then asserts
// lifecycleErrors is zero.
func newTestSupervisor(t *testing.T, cfg Config) *Supervisor {
	t.Helper()
	sup := NewSupervisor(cfg)
	t.Cleanup(func() {
		stopAllTestRuntimes(sup)
		assertSupervisorLifecycleClean(t, sup)
	})
	return sup
}

// stopAllTestRuntimes cancels every runtime the supervisor still owns so
// teardown-driven children complete before the lifecycle counter assertion.
// Cancellation is idempotent for already-ended children.
func stopAllTestRuntimes(sup *Supervisor) {
	sup.controlMu.Lock()
	ids := make([]string, 0, len(sup.runtimes))
	for id := range sup.runtimes {
		ids = append(ids, id)
	}
	sup.controlMu.Unlock()
	for _, id := range ids {
		_ = sup.cancelRuntime(id)
	}
}

// finishSyntheticRunChild finishes a synthetic child's modeled lifecycle at
// the afterRunTeardown boundary: it closes the child's event writer through
// the owning supervisor (writerCloseBegun/Finished), then sends runEnded and
// completes with afterRunTeardown. It must not re-enter beginChildShutdown:
// the caller's cancel/shutdown action already established the shutdown
// boundary.
func finishSyntheticRunChild(s *Supervisor, child *childRuntime) {
	s.closeChildEventWriter(child)
	child.mu.Lock()
	_, eff, err := s.applyLifecycle(child, lifecycleSignal{
		kind: signalRunEnded, class: child.lc.class, boundary: afterRunTeardown,
	})
	child.mu.Unlock()
	if err != nil {
		s.reportLifecycleError(child, err)
		// A failed transition still publishes, so external waiters are never
		// stranded on a machine error.
		child.complete(s, afterRunTeardown)
	} else {
		s.executeDeferredLifecycleEffects(child, eff)
	}
	closeTestRunJoin(child)
}

// closeTestRunJoin closes a synthetic child's run-goroutine join point: the
// fixture has no run goroutine, so the finish helper owns the close.
func closeTestRunJoin(child *childRuntime) {
	child.closeRunDone()
}

// finishSyntheticChildLifecycle explicitly finishes the modeled lifecycle of a
// park-wait-only synthetic fixture (no run goroutine): shutdown boundary,
// writer close, runEnded and publication with the afterRunTeardown boundary.
func finishSyntheticChildLifecycle(s *Supervisor, child *childRuntime) {
	s.beginChildShutdown(child)
	finishSyntheticRunChild(s, child)
}

// finishSyntheticRollback finishes a reserved synthetic child through the
// spawn-rollback boundary: spawnRolledBack then publication with
// afterSpawnRollback.
func finishSyntheticRollback(s *Supervisor, child *childRuntime) {
	child.mu.Lock()
	_, eff, err := s.applyLifecycle(child, lifecycleSignal{
		kind: signalSpawnRolledBack, class: child.lc.class, boundary: afterSpawnRollback,
	})
	child.mu.Unlock()
	if err != nil {
		s.reportLifecycleError(child, err)
		// A failed transition still publishes, so external waiters are never
		// stranded on a machine error.
		child.complete(s, afterSpawnRollback)
	} else {
		s.executeDeferredLifecycleEffects(child, eff)
	}
	closeTestRunJoin(child)
}

// finishUnrunChildAtCleanup registers an explicit modeled-lifecycle finish for
// a synthetic child driven only through private entry points (such as
// runChildAttempt) that never execute the production teardown defer. The
// finish runs after the test's assertions but before the completion tracking
// cleanup that waits on done.
func finishUnrunChildAtCleanup(t *testing.T, sup *Supervisor, child *childRuntime) {
	t.Helper()
	t.Cleanup(func() {
		child.mu.Lock()
		attempts := child.lc.attempts
		child.mu.Unlock()
		if attempts > 0 {
			finishActiveSyntheticChild(sup, child)
			return
		}
		finishSyntheticChildLifecycle(sup, child)
	})
}

// finishActiveSyntheticChild finishes the modeled lifecycle of an active
// status fixture (machine attempts=1, no run goroutine): it settles the
// modeled attempt, then establishes the shutdown boundary, closes the writer
// and publishes completion. The adapter owns the settled signal's waiter
// release.
func finishActiveSyntheticChild(s *Supervisor, child *childRuntime) {
	child.mu.Lock()
	_, _, err := s.applyLifecycle(child, lifecycleSignal{kind: signalAttemptSettled, class: child.lc.class})
	child.mu.Unlock()
	if err != nil {
		s.reportLifecycleError(child, err)
	}
	s.beginChildShutdown(child)
	finishSyntheticRunChild(s, child)
}

// initializeCompletedTestChild declares a fully published synthetic child
// (execEnded/completionPublished) and closes its done channel as the explicit
// teardown setup; a completed tombstone has no run goroutine to complete it.
func initializeCompletedTestChild(t *testing.T, sup *Supervisor, child *childRuntime) {
	t.Helper()
	initializeTestLifecycle(t, child, testLifecycleOptions{exec: execEnded, completion: completionPublished, class: directAttemptClass})
	close(child.done)
	closeTestRunJoin(child)
	trackTestChildCompletion(t, child)
}
