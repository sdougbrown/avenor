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
// legacy mirror through the real transaction and asserts the mismatch is
// counted without an illegal-transition error, with the detail lines emitted
// only into the captured stderr writer.
func TestTransactLifecycleCollectsOneSyntheticMismatch(t *testing.T) {
	var buf bytes.Buffer
	s := newLifecycleTestOwner()
	s.errWriter = &buf
	child := &childRuntime{id: "rt_mismatch_transact", done: make(chan struct{})}
	initializeTestLifecycle(t, child, testLifecycleOptions{exec: execIdle, class: directAttemptClass})

	child.mu.Lock()
	_, _, err := s.applyLifecycle(child, lifecycleSignal{kind: signalAttemptPreparing, class: directAttemptClass}, func() legacyLifecycleWitness {
		// The legacy mutation writes a divergent mirror and records the
		// locked effect it actually performed.
		w := legacyLifecycleWitness{}
		child.phase = "done"
		w.locked = append(w.locked, lifecycleEffect{kind: effectClearPresentation})
		return w
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
// detail routing: the only divergence is a missing locked effect in the
// witness, so the collected stderr detail comes solely from the in-transaction
// sink, not from any field or presentation comparison.
func TestTransactLifecycleLockedWitnessIsSoleDivergence(t *testing.T) {
	var buf bytes.Buffer
	s := newLifecycleTestOwner()
	s.errWriter = &buf
	child := &childRuntime{id: "rt_locked_witness", done: make(chan struct{})}
	initializeTestLifecycle(t, child, testLifecycleOptions{exec: execRunning, attempts: 1, class: directAttemptClass})

	child.mu.Lock()
	_, _, err := s.applyLifecycle(child, lifecycleSignal{kind: signalAttemptSettled, class: directAttemptClass}, func() legacyLifecycleWitness {
		// The legacy settle performs only the waiter release; the predicted
		// clear-presentation effect is absent from the locked witness while
		// every mirror already matches the prediction.
		w := legacyLifecycleWitness{}
		child.active = false
		child.settleTurnLocked()
		w.locked = append(w.locked, lifecycleEffect{kind: effectSettleTurnWaiters})
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
// per compared dimension with one divergent legacy mirror on an otherwise
// valid fixture, on an isolated owner whose counters are asserted exactly.
func TestCompareShadowTransactionCountsDivergentMirrors(t *testing.T) {
	// prepareLegacy performs the locked presentation clear the legacy
	// attemptPreparing path performs, records the effect it performed, and
	// applies an optional divergence to the legacy mirrors.
	prepare := func(diverge func(*childRuntime)) func(*childRuntime) legacyLifecycleWitness {
		return func(c *childRuntime) legacyLifecycleWitness {
			w := legacyLifecycleWitness{}
			c.phase = ""
			c.phaseLabel = ""
			if diverge != nil {
				diverge(c)
			}
			w.locked = append(w.locked, lifecycleEffect{kind: effectClearPresentation})
			return w
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
			name:   "direct attempts",
			opts:   testLifecycleOptions{exec: execIdle, class: directAttemptClass},
			signal: lifecycleSignal{kind: signalAttemptPreparing, class: directAttemptClass},
			legacy: prepare(func(c *childRuntime) { c.active = true }),
			want:   1,
		},
		{
			name:   "workflow attempts",
			opts:   testLifecycleOptions{exec: execRunning, attempts: 1, class: workflowAttemptClass},
			signal: lifecycleSignal{kind: signalWorkflowFinalized, class: workflowAttemptClass},
			legacy: func(c *childRuntime) legacyLifecycleWitness {
				w := legacyLifecycleWitness{}
				c.activeAttempts = 2
				c.active = true
				c.settleTurnLocked()
				w.locked = append(w.locked, lifecycleEffect{kind: effectSettleTurnWaiters})
				return w
			},
			want: 1,
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
			name:   "presentation",
			opts:   testLifecycleOptions{exec: execIdle, class: directAttemptClass},
			signal: lifecycleSignal{kind: signalAttemptPreparing, class: directAttemptClass},
			legacy: prepare(func(c *childRuntime) { c.phase = "done" }),
			want:   1,
		},
		{
			name:   "recorded exit code",
			opts:   testLifecycleOptions{exec: execIdle, class: directAttemptClass},
			signal: lifecycleSignal{kind: signalAttemptOutcome, class: directAttemptClass, runKind: runKindOrdinaryDirect, exitCode: 7, sentinelReason: "error", resultSessionID: "ses_x"},
			legacy: func(c *childRuntime) legacyLifecycleWitness {
				w := legacyLifecycleWitness{}
				c.exitCode = 9
				w.locked = append(w.locked, lifecycleEffect{kind: effectRecordOutcomeExit, exitCode: 9})
				return w
			},
			want: 2, // exit-code field and locked-effect payload both fire
		},
		{
			name:   "idle end data",
			opts:   testLifecycleOptions{exec: execIdle, class: directAttemptClass},
			signal: lifecycleSignal{kind: signalIdleContextEnded, class: directAttemptClass, idleExitCode: 2, idleStopReason: "deadline"},
			legacy: func(c *childRuntime) legacyLifecycleWitness {
				w := legacyLifecycleWitness{}
				c.exitCode = 3
				c.lastStopReason = "other"
				w.locked = append(w.locked, lifecycleEffect{kind: effectSetIdleEndData, exitCode: 3, reason: "other"})
				return w
			},
			want: 4, // exit code, stop reason, locked-effect payload and presentation all fire
		},
		{
			name:   "session end stop reason",
			opts:   testLifecycleOptions{exec: execIdle, class: directAttemptClass},
			signal: lifecycleSignal{kind: signalSessionEndObserved, class: directAttemptClass, stopReason: "end_turn"},
			legacy: func(c *childRuntime) legacyLifecycleWitness {
				w := legacyLifecycleWitness{}
				c.lastStopReason = "other"
				w.locked = append(w.locked, lifecycleEffect{kind: effectSetSessionEndPresentation, reason: "other"})
				return w
			},
			want: 2, // stop reason and presentation both fire
		},
		{
			name:   "locked effect witness",
			opts:   testLifecycleOptions{exec: execRunning, attempts: 1, class: directAttemptClass},
			signal: lifecycleSignal{kind: signalAttemptSettled, class: directAttemptClass},
			legacy: func(c *childRuntime) legacyLifecycleWitness {
				w := legacyLifecycleWitness{}
				c.active = false
				c.settleTurnLocked()
				w.locked = append(w.locked, lifecycleEffect{kind: effectSettleTurnWaiters})
				return w
			},
			want: 1, // the predicted clear-presentation effect is missing from the witness
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
