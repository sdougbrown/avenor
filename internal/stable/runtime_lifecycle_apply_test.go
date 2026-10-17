package stable

// Isolated reporter/diagnostic tests for the Stage 2 lifecycle adapter
// (plan Stage 2 change 6): diagnostic propagation is exercised with explicit
// expected counts on an isolated owner, not by whitelisting real lifecycle
// paths in suite-wide zero-error assertions.

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestReportLifecycleErrorCountsIllegalTransitions(t *testing.T) {
	s := newLifecycleTestOwner()
	s.errWriter = io.Discard // keep the expected-error log out of test output
	child := &childRuntime{id: "rt_report", done: make(chan struct{})}

	// A deliberately invalid state (positive attempt count outside running)
	// makes next reject the signal; the adapter must return the error, leave
	// lc unchanged, and the report must count exactly one error.
	child.mu.Lock()
	child.lc = lifecycleState{exec: execStarting, attempts: 1}
	_, _, err := s.applyLifecycle(child, lifecycleSignal{kind: signalAttemptPreparing, class: directAttemptClass}, nil)
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
	if s.lifecycleShadowMismatches.Load() != 0 {
		t.Fatalf("lifecycleShadowMismatches = %d, want 0", s.lifecycleShadowMismatches.Load())
	}
}

func TestCountShadowMismatchCountsIndependentComparisons(t *testing.T) {
	s := newLifecycleTestOwner()
	s.errWriter = io.Discard
	child := &childRuntime{id: "rt_mismatch", done: make(chan struct{})}
	initializeTestLifecycle(t, child, testLifecycleOptions{exec: execStarting, class: directAttemptClass})

	s.countShadowMismatch(child, signalPublished, "synthetic comparison failure", nil)
	if got := s.lifecycleShadowMismatches.Load(); got != 1 {
		t.Fatalf("lifecycleShadowMismatches = %d, want 1", got)
	}
	if s.lifecycleErrors.Load() != 0 {
		t.Fatalf("lifecycleErrors = %d, want 0 (mismatches are counted separately)", s.lifecycleErrors.Load())
	}
	// The isolated owner never contributed to any real supervisor's counters.
	assertNoLifecycleDivergence(t, &Supervisor{})
}

func TestCompareDeferredLifecycleEffectsCountsSequenceMismatch(t *testing.T) {
	s := newLifecycleTestOwner()
	s.errWriter = io.Discard
	child := &childRuntime{id: "rt_seq", done: make(chan struct{})}
	initializeTestLifecycle(t, child, testLifecycleOptions{exec: execStarting, class: directAttemptClass})
	child.sentinelFile = "/tmp/rt_seq_sentinel"

	predicted := []lifecycleEffect{
		{kind: effectWriteSentinel, exitCode: 3, reason: "boom", sessionID: "ses_x"},
		{kind: effectEmitSessionEnd, exitCode: 3, reason: "boom"},
	}
	witnessed := []lifecycleEffect{
		{kind: effectEmitSessionEnd, exitCode: 3, reason: "boom"},
	}
	s.compareDeferredLifecycleEffects(child, predicted, witnessed, signalIdleContextEnded)
	if got := s.lifecycleShadowMismatches.Load(); got != 1 {
		t.Fatalf("lifecycleShadowMismatches = %d, want 1 after a dropped sentinel witness", got)
	}

	// A matching sequence counts nothing.
	s.lifecycleShadowMismatches.Store(0)
	s.compareDeferredLifecycleEffects(child, predicted, predicted, signalIdleContextEnded)
	if got := s.lifecycleShadowMismatches.Load(); got != 0 {
		t.Fatalf("lifecycleShadowMismatches = %d, want 0 for a matching sequence", got)
	}
}

// TestTransactLifecycleCollectsOneSyntheticMismatch drives one divergent
// still-legacy-owned mirror through the real transaction and asserts the
// mismatch is counted without an illegal-transition error, with the detail
// line emitted only into the captured stderr writer.
func TestTransactLifecycleCollectsOneSyntheticMismatch(t *testing.T) {
	var buf bytes.Buffer
	s := newLifecycleTestOwner()
	s.errWriter = &buf
	child := &childRuntime{id: "rt_mismatch_transact", done: make(chan struct{})}
	initializeTestLifecycle(t, child, testLifecycleOptions{exec: execIdle, class: directAttemptClass})

	child.mu.Lock()
	_, _, err := s.applyLifecycle(child, lifecycleSignal{kind: signalAttemptPreparing, class: directAttemptClass}, func() legacyLifecycleWitness {
		// The legacy mutation writes a divergent still-legacy-owned mirror.
		child.shuttingDown = true
		return legacyLifecycleWitness{}
	})
	child.mu.Unlock()

	if err == nil {
		t.Fatal("transactLifecycle accepted a divergent legacy mirror without a mismatch")
	}
	if lifecycleTransitionFailed(err) {
		t.Fatalf("a shadow mismatch must not surface as a transition error: %v", err)
	}
	if got := s.lifecycleErrors.Load(); got != 0 {
		t.Fatalf("lifecycleErrors = %d, want 0 (a mismatch is not an illegal transition)", got)
	}
	if got := s.lifecycleShadowMismatches.Load(); got != 1 {
		t.Fatalf("lifecycleShadowMismatches = %d, want 1", got)
	}
	if out := buf.String(); out != "" {
		t.Fatalf("stderr written before reportLifecycleError: %q", out)
	}
	s.reportLifecycleError(child, err)
	// A shadow-diag error reports per detail line and leaves the transition
	// counter untouched; only the bundled fallback increments it.
	if got := s.lifecycleErrors.Load(); got != 0 {
		t.Fatalf("lifecycleErrors = %d after reportLifecycleError, want 0", got)
	}
	if out := buf.String(); !strings.Contains(out, "lifecycle shadow mismatch (") || !strings.Contains(out, "rt_mismatch_transact") || strings.Contains(out, "lifecycle error (runtime") {
		t.Fatalf("captured stderr = %q, want the per-detail shadow mismatch lines for the child", out)
	}
}

// TestTransactLifecycleLockedWitnessIsSoleDivergence pins the locked-effect
// detail routing: the only divergence is a locked-effect witness entry with no
// predicted counterpart, so the collected stderr detail comes solely from the
// in-transaction sink, not from any field or presentation comparison.
func TestTransactLifecycleLockedWitnessIsSoleDivergence(t *testing.T) {
	var buf bytes.Buffer
	s := newLifecycleTestOwner()
	s.errWriter = &buf
	child := &childRuntime{id: "rt_locked_witness", done: make(chan struct{})}
	initializeTestLifecycle(t, child, testLifecycleOptions{exec: execIdle, class: directAttemptClass})

	child.mu.Lock()
	_, _, err := s.applyLifecycle(child, lifecycleSignal{
		kind: signalAttemptOutcome, class: directAttemptClass,
		runKind: runKindOrdinaryDirect, exitCode: 7, sentinelReason: "error", resultSessionID: "ses_x",
	}, func() legacyLifecycleWitness {
		// The exit write matches the prediction (the adapter overwrites the
		// mirror anyway). The witnessed locked effect is the adapter-owned
		// record-exit, which the legacy comparison pool excludes, so the
		// witnessed entry has no predicted counterpart: that unpaired witness
		// is the sole divergence.
		child.exitCode = 7
		w := legacyLifecycleWitness{}
		w.locked = append(w.locked, lifecycleEffect{kind: effectRecordOutcomeExit, exitCode: 7})
		return w
	})
	child.mu.Unlock()

	if err == nil || lifecycleTransitionFailed(err) {
		t.Fatalf("applyLifecycle err = %v, want a shadow-diag error", err)
	}
	if got := s.lifecycleShadowMismatches.Load(); got != 1 {
		t.Fatalf("lifecycleShadowMismatches = %d, want 1 (locked-effect witness is the sole divergence)", got)
	}
	if got := s.lifecycleErrors.Load(); got != 0 {
		t.Fatalf("lifecycleErrors = %d, want 0 (a mismatch is not an illegal transition)", got)
	}
	if out := buf.String(); out != "" {
		t.Fatalf("stderr written before reportLifecycleError: %q", out)
	}
	s.reportLifecycleError(child, err)
	if got := s.lifecycleErrors.Load(); got != 0 {
		t.Fatalf("lifecycleErrors = %d after reportLifecycleError, want 0", got)
	}
	if out := buf.String(); !strings.Contains(out, "lifecycle shadow mismatch (") || !strings.Contains(out, "rt_locked_witness") || strings.Contains(out, "lifecycle error (runtime") {
		t.Fatalf("captured stderr = %q, want the per-detail locked-effect line", out)
	}
}

// TestCompareShadowTransactionCountsDivergentMirrors runs a real transaction
// per compared dimension with one divergent mirror on an otherwise valid
// fixture, on an isolated owner whose counters are asserted exactly. The
// adapter writes the mirrors it owns (activity, presentation, idle-end data,
// recorded exit, parked) itself, so a rogue write on a signal that does not
// own that mirror still shows up as a mismatch, while a rogue write the
// adapter overwrites does not.
func TestCompareShadowTransactionCountsDivergentMirrors(t *testing.T) {
	// prepareLegacy leaves the mirrors to the adapter and performs no locked
	// effects: attemptPreparing's owned clear runs in the adapter.
	prepare := func(diverge func(*childRuntime)) func(*childRuntime) legacyLifecycleWitness {
		return func(c *childRuntime) legacyLifecycleWitness {
			if diverge != nil {
				diverge(c)
			}
			return legacyLifecycleWitness{}
		}
	}

	cases := []struct {
		name   string
		opts   testLifecycleOptions
		signal lifecycleSignal
		legacy func(*childRuntime) legacyLifecycleWitness
		want   int64
	}{
		{
			name:   "no divergence counts nothing",
			opts:   testLifecycleOptions{exec: execIdle, class: directAttemptClass},
			signal: lifecycleSignal{kind: signalAttemptPreparing, class: directAttemptClass},
			legacy: prepare(nil),
			want:   0,
		},
		{
			name:   "parked",
			opts:   testLifecycleOptions{exec: execIdle, class: directAttemptClass},
			signal: lifecycleSignal{kind: signalAttemptPreparing, class: directAttemptClass},
			legacy: prepare(func(c *childRuntime) { c.parked = true }),
			want:   2, // parked and effective-capacity comparisons both fire
		},
		{
			name:   "completed",
			opts:   testLifecycleOptions{exec: execIdle, class: directAttemptClass},
			signal: lifecycleSignal{kind: signalAttemptPreparing, class: directAttemptClass},
			legacy: prepare(func(c *childRuntime) { c.completed = true }),
			want:   2, // completed and effective-capacity comparisons both fire
		},
		{
			name:   "shutting down",
			opts:   testLifecycleOptions{exec: execIdle, class: directAttemptClass},
			signal: lifecycleSignal{kind: signalAttemptPreparing, class: directAttemptClass},
			legacy: prepare(func(c *childRuntime) { c.shuttingDown = true }),
			want:   1,
		},
		{
			name:   "writer closing",
			opts:   testLifecycleOptions{exec: execIdle, class: directAttemptClass},
			signal: lifecycleSignal{kind: signalAttemptPreparing, class: directAttemptClass},
			legacy: prepare(func(c *childRuntime) { c.writerClosing = true }),
			want:   1,
		},
		{
			name:   "recorded exit code",
			opts:   testLifecycleOptions{exec: execIdle, class: directAttemptClass},
			signal: lifecycleSignal{kind: signalAttemptOutcome, class: directAttemptClass, runKind: runKindOrdinaryDirect, exitCode: 7, sentinelReason: "error", resultSessionID: "ses_x"},
			legacy: func(c *childRuntime) legacyLifecycleWitness {
				// The adapter overwrites the exit mirror itself, so a divergent
				// legacy write is invisible; the spurious locked-effect witness
				// is the only divergence left.
				w := legacyLifecycleWitness{}
				c.exitCode = 9
				w.locked = append(w.locked, lifecycleEffect{kind: effectRecordOutcomeExit, exitCode: 9})
				return w
			},
			want: 1, // locked-effect witness fires
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newLifecycleTestOwner()
			s.errWriter = io.Discard
			child := &childRuntime{id: "rt_mirror", done: make(chan struct{})}
			initializeTestLifecycle(t, child, tc.opts)

			var legacy func() legacyLifecycleWitness
			if tc.legacy != nil {
				c := child
				legacy = func() legacyLifecycleWitness { return tc.legacy(c) }
			}
			child.mu.Lock()
			_, _, err := s.applyLifecycle(child, tc.signal, legacy)
			child.mu.Unlock()
			if err != nil && lifecycleTransitionFailed(err) {
				t.Fatalf("fixture produced an illegal transition, not a mismatch: %v", err)
			}
			if got := s.lifecycleErrors.Load(); got != 0 {
				t.Fatalf("lifecycleErrors = %d, want 0", got)
			}
			if got := s.lifecycleShadowMismatches.Load(); got != tc.want {
				t.Fatalf("lifecycleShadowMismatches = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestApplyLifecycleWritesOwnedMirrors drives the signals whose effects the
// adapter owns and asserts the mirrors it writes: the activity mirrors for
// both attempt classes, the presentation writes, the idle-end data and the
// settled/finalized waiter release.
func TestApplyLifecycleWritesOwnedMirrors(t *testing.T) {
	apply := func(t *testing.T, s *Supervisor, child *childRuntime, sig lifecycleSignal) {
		t.Helper()
		child.mu.Lock()
		_, _, err := s.applyLifecycle(child, sig, nil)
		child.mu.Unlock()
		if err != nil {
			t.Fatalf("applyLifecycle(%v): %v", sig.kind, err)
		}
	}
	mirrorState := func(t *testing.T, child *childRuntime) (active bool, activeAttempts int, phase, phaseLabel string) {
		t.Helper()
		child.mu.Lock()
		defer child.mu.Unlock()
		return child.active, child.activeAttempts, child.phase, child.phaseLabel
	}

	t.Run("direct activity and presentation", func(t *testing.T) {
		s := newLifecycleTestOwner()
		child := &childRuntime{id: "rt_owned_direct", done: make(chan struct{})}
		initializeTestLifecycle(t, child, testLifecycleOptions{exec: execIdle, class: directAttemptClass, phase: "stale", phaseLabel: "old"})

		// Preparing clears the presentation without touching the count.
		apply(t, s, child, lifecycleSignal{kind: signalAttemptPreparing, class: directAttemptClass})
		if active, attempts, phase, label := mirrorState(t, child); active || attempts != 0 || phase != "" || label != "" {
			t.Fatalf("after preparing: active=%v attempts=%d phase=%q label=%q", active, attempts, phase, label)
		}

		// Starting increments the count and marks activity.
		apply(t, s, child, lifecycleSignal{kind: signalAttemptStarted, class: directAttemptClass})
		if active, attempts, _, _ := mirrorState(t, child); !active || attempts != 1 {
			t.Fatalf("after start: active=%v attempts=%d", active, attempts)
		}

		// Settling decrements back to zero, keeps the presentation clear and
		// releases the registered waiter.
		child.mu.Lock()
		waiter := make(chan struct{})
		child.turnWaiters = append(child.turnWaiters, waiter)
		child.mu.Unlock()
		apply(t, s, child, lifecycleSignal{kind: signalAttemptSettled, class: directAttemptClass})
		if active, attempts, phase, label := mirrorState(t, child); active || attempts != 0 || phase != "" || label != "" {
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
		if active, attempts, phase, label := mirrorState(t, child); !active || attempts != 1 || phase != "working" || label != "phase two" {
			t.Fatalf("after parallel settle: active=%v attempts=%d phase=%q label=%q", active, attempts, phase, label)
		}

		// Finalization resets the count, releases the waiters unconditionally
		// and never clears the presentation.
		child.mu.Lock()
		waiter := make(chan struct{})
		child.turnWaiters = append(child.turnWaiters, waiter)
		child.mu.Unlock()
		apply(t, s, child, lifecycleSignal{kind: signalWorkflowFinalized, class: workflowAttemptClass})
		if active, attempts, phase, label := mirrorState(t, child); active || attempts != 0 || phase != "working" || label != "phase two" {
			t.Fatalf("after finalize: active=%v attempts=%d phase=%q label=%q", active, attempts, phase, label)
		}
		select {
		case <-waiter:
		default:
			t.Fatal("registered waiter was not released by the finalization")
		}
	})

	t.Run("parking transitions write the parked mirror", func(t *testing.T) {
		s := newLifecycleTestOwner()
		child := &childRuntime{id: "rt_owned_parked", done: make(chan struct{})}
		initializeTestLifecycle(t, child, testLifecycleOptions{exec: execIdle, class: directAttemptClass})
		parkedState := func(t *testing.T) (execState, bool) {
			t.Helper()
			child.mu.Lock()
			defer child.mu.Unlock()
			return child.lc.exec, child.parked
		}

		// ParkRequested parks; localSlotAcquired and parkWaitEnded clear the
		// parked mirror again.
		apply(t, s, child, lifecycleSignal{kind: signalParkRequested, class: directAttemptClass, runKind: runKindOrdinaryDirect})
		if exec, parked := parkedState(t); exec != execParked || !parked {
			t.Fatalf("after park: exec=%d parked=%v", exec, parked)
		}
		apply(t, s, child, lifecycleSignal{kind: signalLocalSlotAcquired, class: directAttemptClass})
		if exec, parked := parkedState(t); exec != execIdle || parked {
			t.Fatalf("after slot acquire: exec=%d parked=%v", exec, parked)
		}
		apply(t, s, child, lifecycleSignal{kind: signalParkRequested, class: directAttemptClass, runKind: runKindOrdinaryDirect})
		if exec, parked := parkedState(t); exec != execParked || !parked {
			t.Fatalf("after second park: exec=%d parked=%v", exec, parked)
		}
		apply(t, s, child, lifecycleSignal{kind: signalParkWaitEnded, class: directAttemptClass})
		if exec, parked := parkedState(t); exec != execIdle || parked {
			t.Fatalf("after wait ended: exec=%d parked=%v", exec, parked)
		}
	})

	t.Run("presentation and idle-end data", func(t *testing.T) {
		s := newLifecycleTestOwner()
		child := &childRuntime{id: "rt_owned_presentation", done: make(chan struct{})}
		initializeTestLifecycle(t, child, testLifecycleOptions{exec: execIdle, class: directAttemptClass, phaseLabel: "kept"})

		// An agent status replaces the phase; an empty label retains the stored one.
		apply(t, s, child, lifecycleSignal{kind: signalAgentStatusObserved, class: directAttemptClass, phase: "working"})
		if _, _, phase, label := mirrorState(t, child); phase != "working" || label != "kept" {
			t.Fatalf("after agent status: phase=%q label=%q", phase, label)
		}

		// A successful presentation marks the phase done.
		apply(t, s, child, lifecycleSignal{kind: signalSuccessPresented, class: directAttemptClass, exitCode: 0})
		if _, _, phase, _ := mirrorState(t, child); phase != "done" {
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
	owner := newLifecycleTestOwner()
	child := newTestChildLifecycle(t, owner, testLifecycleOptions{id: "rt_finish_helper", exec: execStarting, class: directAttemptClass})
	finishSyntheticChildLifecycle(owner, child)

	child.mu.Lock()
	state := child.lc
	child.mu.Unlock()
	if state.exec != execEnded || state.completion != completionPublished || !state.isShuttingDown() || !state.isWriterClosing() {
		t.Fatalf("post-finish lifecycle = %+v, want ended/published/shutdown/writer-closing", state)
	}
	if !child.completed {
		t.Fatal("legacy completed mirror not set by the finish helper")
	}
	select {
	case <-child.done:
	default:
		t.Fatal("done channel not closed by the finish helper")
	}
	assertNoLifecycleDivergence(t, owner)
}
