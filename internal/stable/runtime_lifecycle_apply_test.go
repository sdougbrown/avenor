package stable

// Isolated reporter/diagnostic tests for the Stage 2 lifecycle adapter
// (plan Stage 2 change 6): diagnostic propagation is exercised with explicit
// expected counts on an isolated owner, not by whitelisting real lifecycle
// paths in suite-wide zero-error assertions.

import (
	"io"
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

	s.countShadowMismatch(child, signalPublished, "synthetic comparison failure")
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
