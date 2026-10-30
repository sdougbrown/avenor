package stable

// Isolated lifecycle adapter tests: diagnostic propagation is exercised with
// explicit expected counts on an isolated owner, not by whitelisting real
// lifecycle paths in suite-wide zero-error assertions.

import (
	"io"
	"testing"

	"github.com/sdougbrown/avenor/internal/runstate"
)

// TestEffectExecutionPartitionCoversEveryKind pins the effectKind partition:
// every defined kind must map to exactly one executor side — owned kinds run
// under the transaction's locks, deferred kinds after every lock is released.
// A kind in neither set is a machine bug both executors would report at
// runtime, so the table must never return effectSideUnknown for a defined
// kind.
func TestEffectExecutionPartitionCoversEveryKind(t *testing.T) {
	t.Parallel()
	owned := []effectKind{
		effectClearPresentation, effectSetAgentPresentation, effectSetSuccessPresentation,
		effectSetSessionEndPresentation, effectSetIdleEndData, effectRecordOutcomeExit,
		effectSettleTurnWaiters,
	}
	deferred := []effectKind{
		effectWriteSentinel, effectSignalCapacity, effectReleaseTreeToken,
		effectEmitSessionEnd, effectConsiderProviderCancel, effectCancelContext,
		effectPublishCompletion, effectCloseDone,
	}
	sides := make(map[effectKind]effectSide, len(owned)+len(deferred))
	for _, k := range owned {
		if side := effectExecution(k); side != effectSideOwned {
			t.Errorf("effectExecution(%d) = %d, want effectSideOwned", int(k), int(side))
		}
		sides[k] = effectSideOwned
	}
	for _, k := range deferred {
		if side := effectExecution(k); side != effectSideDeferred {
			t.Errorf("effectExecution(%d) = %d, want effectSideDeferred", int(k), int(side))
		}
		if _, dup := sides[k]; dup {
			t.Errorf("effectKind %d is in both the owned and deferred sets", int(k))
		}
		sides[k] = effectSideDeferred
	}
	// The effectKind const block is a contiguous iota run ending at
	// effectKindEnd, so walking its range reaches every defined kind,
	// including kinds added later, exactly once.
	for k := effectClearPresentation; k < effectKindEnd; k++ {
		if effectExecution(k) == effectSideUnknown {
			t.Errorf("effectExecution(%d) = effectSideUnknown: the kind has no executor", int(k))
		}
		if _, ok := sides[k]; !ok {
			t.Errorf("effectKind %d is missing from this test's owned/deferred lists", int(k))
		}
	}
}

func TestReportLifecycleErrorCountsIllegalTransitions(t *testing.T) {
	t.Parallel()
	s := newLifecycleTestOwner()
	s.errWriter = io.Discard // keep the expected-error log out of test output
	child := &childRuntime{id: "rt_report", done: make(chan struct{})}

	// A deliberately invalid state (positive attempt count outside running)
	// makes next reject the signal; the adapter must return the error, leave
	// lc unchanged, and the report must count exactly one error.
	child.mu.Lock()
	child.lc = lifecycleState{exec: execStarting, attempts: 1}
	_, _, err := s.applyLifecycle(child, lifecycleSignal{kind: signalAttemptPreparing, class: directAttemptClass})
	child.mu.Unlock()
	if err == nil {
		t.Fatal("applyLifecycle accepted a signal on an invalid state")
	}
	if child.lc.exec != execStarting || child.lc.attempts != 1 {
		t.Fatalf("invalid prediction mutated lc: exec=%d attempts=%d", child.lc.exec, child.lc.attempts)
	}
	s.reportLifecycleError(child, err)
	if got := s.lifecycleErrors.Load(); got != 1 {
		t.Fatalf("lifecycleErrors = %d, want 1 after one reported transition error", got)
	}
	s.reportLifecycleError(child, err)
	if got := s.lifecycleErrors.Load(); got != 2 {
		t.Fatalf("lifecycleErrors = %d, want 2 after a second report", got)
	}
}

// TestApplyLifecycleCommitsOwnedEffects drives the signals whose effects the
// adapter owns and asserts what it commits and executes: activity and attempt
// count for both attempt classes, the presentation writes, the idle-end data,
// the settled/finalized waiter release, parking and completion.
func TestApplyLifecycleCommitsOwnedEffects(t *testing.T) {
	t.Parallel()
	apply := func(t *testing.T, s *Supervisor, child *childRuntime, sig lifecycleSignal) {
		t.Helper()
		child.mu.Lock()
		_, _, err := s.applyLifecycle(child, sig)
		child.mu.Unlock()
		if err != nil {
			t.Fatalf("applyLifecycle(%v): %v", sig.kind, err)
		}
	}
	lifecycleSnapshot := func(t *testing.T, child *childRuntime) (active bool, attempts int, phase runstate.Phase, phaseLabel string) {
		t.Helper()
		child.mu.Lock()
		defer child.mu.Unlock()
		return child.lc.isActive(), child.lc.attempts, child.phase, child.phaseLabel
	}

	t.Run("direct activity and presentation", func(t *testing.T) {
		s := newLifecycleTestOwner()
		child := &childRuntime{id: "rt_owned_direct", done: make(chan struct{})}
		initializeTestLifecycle(t, child, testLifecycleOptions{exec: execIdle, class: directAttemptClass, phase: "stale", phaseLabel: "old"})

		// Preparing clears the presentation without touching the count.
		apply(t, s, child, lifecycleSignal{kind: signalAttemptPreparing, class: directAttemptClass})
		if active, attempts, phase, label := lifecycleSnapshot(t, child); active || attempts != 0 || phase != "" || label != "" {
			t.Fatalf("after preparing: active=%v attempts=%d phase=%q label=%q", active, attempts, phase, label)
		}

		// Starting increments the count and marks activity.
		apply(t, s, child, lifecycleSignal{kind: signalAttemptStarted, class: directAttemptClass})
		if active, attempts, _, _ := lifecycleSnapshot(t, child); !active || attempts != 1 {
			t.Fatalf("after start: active=%v attempts=%d", active, attempts)
		}

		// Settling decrements back to zero, keeps the presentation clear and
		// releases the registered waiter.
		child.mu.Lock()
		waiter := make(chan struct{})
		child.turnWaiters = append(child.turnWaiters, waiter)
		child.mu.Unlock()
		apply(t, s, child, lifecycleSignal{kind: signalAttemptSettled, class: directAttemptClass})
		if active, attempts, phase, label := lifecycleSnapshot(t, child); active || attempts != 0 || phase != "" || label != "" {
			t.Fatalf("after settle: active=%v attempts=%d phase=%q label=%q", active, attempts, phase, label)
		}
		select {
		case <-waiter:
		default:
			t.Fatal("registered waiter was not released by the settle")
		}
	})

	t.Run("workflow parallel attempts and finalization", func(t *testing.T) {
		s := newLifecycleTestOwner()
		child := &childRuntime{id: "rt_owned_workflow", done: make(chan struct{})}
		initializeTestLifecycle(t, child, testLifecycleOptions{exec: execRunning, attempts: 2, class: workflowAttemptClass, phase: "working", phaseLabel: "phase two"})

		// The first of two parallel settles keeps the last attempt active and
		// leaves the presentation alone.
		apply(t, s, child, lifecycleSignal{kind: signalAttemptSettled, class: workflowAttemptClass})
		if active, attempts, phase, label := lifecycleSnapshot(t, child); !active || attempts != 1 || phase != "working" || label != "phase two" {
			t.Fatalf("after parallel settle: active=%v attempts=%d phase=%q label=%q", active, attempts, phase, label)
		}

		// Finalization resets the count, releases the waiters unconditionally
		// and never clears the presentation.
		child.mu.Lock()
		waiter := make(chan struct{})
		child.turnWaiters = append(child.turnWaiters, waiter)
		child.mu.Unlock()
		apply(t, s, child, lifecycleSignal{kind: signalWorkflowFinalized, class: workflowAttemptClass})
		if active, attempts, phase, label := lifecycleSnapshot(t, child); active || attempts != 0 || phase != "working" || label != "phase two" {
			t.Fatalf("after finalize: active=%v attempts=%d phase=%q label=%q", active, attempts, phase, label)
		}
		select {
		case <-waiter:
		default:
			t.Fatal("registered waiter was not released by the finalization")
		}
	})

	t.Run("parking transitions move exec between parked and idle", func(t *testing.T) {
		s := newLifecycleTestOwner()
		child := &childRuntime{id: "rt_owned_parked", done: make(chan struct{})}
		initializeTestLifecycle(t, child, testLifecycleOptions{exec: execIdle, class: directAttemptClass})
		parkedState := func(t *testing.T) execState {
			t.Helper()
			child.mu.Lock()
			defer child.mu.Unlock()
			return child.lc.exec
		}

		// ParkRequested parks; localSlotAcquired and parkWaitEnded return exec
		// to idle.
		apply(t, s, child, lifecycleSignal{kind: signalParkRequested, class: directAttemptClass, runKind: runKindOrdinaryDirect})
		if exec := parkedState(t); exec != execParked {
			t.Fatalf("after park: exec=%d", exec)
		}
		apply(t, s, child, lifecycleSignal{kind: signalLocalSlotAcquired, class: directAttemptClass})
		if exec := parkedState(t); exec != execIdle {
			t.Fatalf("after slot acquire: exec=%d", exec)
		}
		apply(t, s, child, lifecycleSignal{kind: signalParkRequested, class: directAttemptClass, runKind: runKindOrdinaryDirect})
		if exec := parkedState(t); exec != execParked {
			t.Fatalf("after second park: exec=%d", exec)
		}
		apply(t, s, child, lifecycleSignal{kind: signalParkWaitEnded, class: directAttemptClass})
		if exec := parkedState(t); exec != execIdle {
			t.Fatalf("after wait ended: exec=%d", exec)
		}
	})

	t.Run("completion transitions close admission and publication", func(t *testing.T) {
		s := newLifecycleTestOwner()
		child := &childRuntime{id: "rt_owned_completion", done: make(chan struct{})}
		initializeTestLifecycle(t, child, testLifecycleOptions{exec: execIdle, class: directAttemptClass})

		// The park timeout closes admission but leaves done open: no waiter
		// settle, no done close.
		apply(t, s, child, lifecycleSignal{kind: signalParkRequested, class: directAttemptClass, runKind: runKindOrdinaryDirect})
		apply(t, s, child, lifecycleSignal{kind: signalParkTimedOut, class: directAttemptClass})
		child.mu.Lock()
		completion := child.lc.completion
		child.mu.Unlock()
		if completion != completionAdmissionClosed {
			t.Fatalf("after park timeout: completion=%d", completion)
		}
		select {
		case <-child.done:
			t.Fatal("park timeout closed done")
		default:
		}

		// The final publication settles the registered waiter. The finish
		// helper owns the shutdown/writer stages (nil sink, so the writer
		// stays closing only).
		apply(t, s, child, lifecycleSignal{kind: signalParkWaitEnded, class: directAttemptClass})
		apply(t, s, child, lifecycleSignal{kind: signalIdleContextEnded, class: directAttemptClass, idleExitCode: 124, idleStopReason: "timeout"})
		child.mu.Lock()
		waiter := make(chan struct{})
		child.turnWaiters = append(child.turnWaiters, waiter)
		child.mu.Unlock()
		finishSyntheticChildLifecycle(s, child)
		child.mu.Lock()
		completed, waiting := child.lc.isCompleted(), len(child.turnWaiters)
		child.mu.Unlock()
		if !completed || waiting != 0 {
			t.Fatalf("after publication: completed=%v waiters=%d", completed, waiting)
		}
		select {
		case <-waiter:
		default:
			t.Fatal("registered waiter was not released by the publication")
		}
	})

	t.Run("presentation and idle-end data", func(t *testing.T) {
		s := newLifecycleTestOwner()
		child := &childRuntime{id: "rt_owned_presentation", done: make(chan struct{})}
		initializeTestLifecycle(t, child, testLifecycleOptions{exec: execIdle, class: directAttemptClass, phaseLabel: "kept"})

		// An agent status replaces the phase; an empty label retains the stored one.
		apply(t, s, child, lifecycleSignal{kind: signalAgentStatusObserved, class: directAttemptClass, phase: "working"})
		if _, _, phase, label := lifecycleSnapshot(t, child); phase != "working" || label != "kept" {
			t.Fatalf("after agent status: phase=%q label=%q", phase, label)
		}

		// A successful presentation marks the phase done.
		apply(t, s, child, lifecycleSignal{kind: signalSuccessPresented, class: directAttemptClass, exitCode: 0})
		if _, _, phase, _ := lifecycleSnapshot(t, child); phase != "done" {
			t.Fatalf("after success: phase=%q", phase)
		}

		// A session end marks the phase done and records a nonempty stop
		// reason; an empty one leaves the stored reason alone.
		apply(t, s, child, lifecycleSignal{kind: signalSessionEndObserved, class: directAttemptClass, stopReason: "end_turn"})
		child.mu.Lock()
		reason, phase := child.lastStopReason, child.phase
		child.mu.Unlock()
		if reason != "end_turn" || phase != "done" {
			t.Fatalf("after session end: lastStopReason=%q phase=%q", reason, phase)
		}
		apply(t, s, child, lifecycleSignal{kind: signalSessionEndObserved, class: directAttemptClass})
		child.mu.Lock()
		reason, phase = child.lastStopReason, child.phase
		child.mu.Unlock()
		if reason != "end_turn" || phase != "done" {
			t.Fatalf("after empty session end: lastStopReason=%q phase=%q", reason, phase)
		}

		// An idle-context end overwrites the terminal data.
		apply(t, s, child, lifecycleSignal{kind: signalIdleContextEnded, class: directAttemptClass, idleExitCode: 2, idleStopReason: "timeout"})
		child.mu.Lock()
		exitCode, reason, phase := child.exitCode, child.lastStopReason, child.phase
		child.mu.Unlock()
		if exitCode != 2 || reason != "timeout" || phase != "done" {
			t.Fatalf("after idle end: exitCode=%d reason=%q phase=%q", exitCode, reason, phase)
		}
	})
}

func TestFinishSyntheticRunChildDrivesFullTeardown(t *testing.T) {
	t.Parallel()
	owner := newLifecycleTestOwner()
	child := newTestChildLifecycle(t, owner, testLifecycleOptions{id: "rt_finish_helper", exec: execStarting, class: directAttemptClass})
	finishSyntheticChildLifecycle(owner, child)

	child.mu.Lock()
	state := child.lc
	child.mu.Unlock()
	if state.exec != execEnded || state.completion != completionPublished || !state.isShuttingDown() || !state.isWriterClosing() {
		t.Fatalf("post-finish lifecycle = %+v, want ended/published/shutdown/writer-closing", state)
	}
	select {
	case <-child.done:
	default:
		t.Fatal("done channel not closed by the finish helper")
	}
	assertNoLifecycleErrors(t, owner)
}
