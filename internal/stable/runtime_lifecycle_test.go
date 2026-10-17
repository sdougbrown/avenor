package stable

// Table and sequence tests for the pure childRuntime lifecycle machine
// (issue #316, Stage 1). These tests drive only next() and the field-specific
// predicates; no production call site or fixture is touched.

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func directState(exec execState) lifecycleState {
	return lifecycleState{exec: exec, class: directAttemptClass}
}

func workflowState(exec execState, attempts int) lifecycleState {
	return lifecycleState{exec: exec, attempts: attempts, class: workflowAttemptClass}
}

// publishedRunning is a representative live direct state after publication
// and a first attempt start: S→R via attemptStarted.
func runningDirect() lifecycleState {
	return lifecycleState{exec: execRunning, attempts: 1, class: directAttemptClass}
}

func idleDirect() lifecycleState {
	return lifecycleState{exec: execIdle, class: directAttemptClass}
}

func applyOK(t *testing.T, st lifecycleState, sig lifecycleSignal) (lifecycleState, continuation, []lifecycleEffect) {
	t.Helper()
	out, cont, eff, err := next(st, sig)
	if err != nil {
		t.Fatalf("next(signal %d) on %+v: unexpected error: %v", sig.kind, st, err)
	}
	if err := validateLifecycleState(out); err != nil {
		t.Fatalf("next(signal %d) produced invalid state %+v: %v", sig.kind, out, err)
	}
	return out, cont, eff
}

func expectErr(t *testing.T, st lifecycleState, sig lifecycleSignal) {
	t.Helper()
	out, cont, eff, err := next(st, sig)
	if err == nil {
		t.Fatalf("next(signal %d) on %+v: expected error, got state %+v, continuation %d, effects %v", sig.kind, st, out, cont.kind, eff)
	}
	if out != st {
		t.Fatalf("next(signal %d) error path changed state: got %+v want %+v", sig.kind, out, st)
	}
	if cont.kind != contNone {
		t.Fatalf("next(signal %d) error path returned continuation %d", sig.kind, cont.kind)
	}
	if eff != nil {
		t.Fatalf("next(signal %d) error path returned effects %v", sig.kind, eff)
	}
}

func effectKinds(eff []lifecycleEffect) []effectKind {
	kinds := make([]effectKind, 0, len(eff))
	for _, e := range eff {
		kinds = append(kinds, e.kind)
	}
	return kinds
}

func effectsEqual(a, b []lifecycleEffect) bool {
	return reflect.DeepEqual(a, b)
}

func TestRuntimeLifecyclePredicateTruthTables(t *testing.T) {
	// isCompleted: true for both admissionClosed and published (the park
	// timeout sets completed before done closes).
	for completion, want := range map[completionStage]bool{
		completionOpen:            false,
		completionAdmissionClosed: true,
		completionPublished:       true,
	} {
		st := lifecycleState{completion: completion}
		if got := st.isCompleted(); got != want {
			t.Errorf("isCompleted(completion=%d) = %v, want %v", completion, got, want)
		}
	}

	// isWriterClosing: true for both closing and closed (writerClosing stays
	// true after writerClosed is set; every admission gate keys on it).
	for writer, want := range map[writerStage]bool{
		writerOpen:    false,
		writerClosing: true,
		writerClosed:  true,
	} {
		st := lifecycleState{writer: writer}
		if got := st.isWriterClosing(); got != want {
			t.Errorf("isWriterClosing(writer=%d) = %v, want %v", writer, got, want)
		}
	}

	// isWriterClosed: true only for writerClosed.
	for writer, want := range map[writerStage]bool{
		writerOpen:    false,
		writerClosing: false,
		writerClosed:  true,
	} {
		st := lifecycleState{writer: writer}
		if got := st.isWriterClosed(); got != want {
			t.Errorf("isWriterClosed(writer=%d) = %v, want %v", writer, got, want)
		}
	}

	// isParked: recognizes both parked states.
	if !(&lifecycleState{exec: execParked}).isParked() {
		t.Error("isParked(execParked) = false, want true")
	}
	if !(&lifecycleState{exec: execParkedEnding}).isParked() {
		t.Error("isParked(execParkedEnding) = false, want true")
	}
	for _, exec := range []execState{execReserved, execStarting, execRunning, execIdle, execEnded} {
		if (&lifecycleState{exec: exec}).isParked() {
			t.Errorf("isParked(%d) = true, want false", exec)
		}
	}

	// isActive: running only.
	for _, exec := range []execState{execReserved, execStarting, execIdle, execParked, execParkedEnding, execEnded} {
		if (&lifecycleState{exec: exec}).isActive() {
			t.Errorf("isActive(%d) = true, want false", exec)
		}
	}
	if !(&lifecycleState{exec: execRunning}).isActive() {
		t.Error("isActive(execRunning) = false, want true")
	}

	// isShuttingDown: requested only.
	if (&lifecycleState{}).isShuttingDown() {
		t.Error("isShuttingDown(none) = true, want false")
	}
	if !(&lifecycleState{shutdown: shutdownRequested}).isShuttingDown() {
		t.Error("isShuttingDown(requested) = false, want true")
	}
}

func TestRuntimeLifecycleMatrixRows(t *testing.T) {
	successSentinel := lifecycleEffect{kind: effectWriteSentinel, exitCode: 0, reason: "end_turn", sessionID: "s1"}
	cases := []struct {
		name    string
		start   lifecycleState
		signal  lifecycleSignal
		want    lifecycleState
		cont    continuationKind
		effects []lifecycleEffect
	}{
		{
			name:   "published from reserved",
			start:  lifecycleState{},
			signal: lifecycleSignal{kind: signalPublished},
			want:   lifecycleState{exec: execStarting},
		},
		{
			name:    "preparing clears presentation",
			start:   idleDirect(),
			signal:  lifecycleSignal{kind: signalAttemptPreparing, class: directAttemptClass},
			want:    idleDirect(),
			effects: []lifecycleEffect{{kind: effectClearPresentation}},
		},
		{
			name:   "started increments and runs",
			start:  idleDirect(),
			signal: lifecycleSignal{kind: signalAttemptStarted, class: directAttemptClass},
			want:   runningDirect(),
		},
		{
			name:   "start failed from starting returns to idle without effects",
			start:  directState(execStarting),
			signal: lifecycleSignal{kind: signalAttemptStartFailed, class: directAttemptClass},
			want:   idleDirect(),
		},
		{
			// A prepared workflow attempt can fail out of a running state without
			// touching the count (parallel preparation keeps another attempt live).
			name:   "start failed in running with workflow class leaves the attempt running",
			start:  workflowState(execRunning, 1),
			signal: lifecycleSignal{kind: signalAttemptStartFailed, class: workflowAttemptClass},
			want:   workflowState(execRunning, 1),
		},
		{
			name:  "settled to zero clears and settles",
			start: runningDirect(),
			signal: lifecycleSignal{
				kind: signalAttemptSettled, class: directAttemptClass,
				exitCode: 0, stopReason: "end_turn",
			},
			want: idleDirect(),
			effects: []lifecycleEffect{
				{kind: effectClearPresentation},
				{kind: effectSettleTurnWaiters},
			},
		},
		{
			name:   "finalized resets count and settles unconditionally",
			start:  workflowState(execRunning, 2),
			signal: lifecycleSignal{kind: signalWorkflowFinalized, class: workflowAttemptClass},
			want:   workflowState(execIdle, 0),
			effects: []lifecycleEffect{
				{kind: effectSettleTurnWaiters},
			},
		},
		{
			name:   "finalized records the exit code the legacy path writes",
			start:  workflowState(execRunning, 2),
			signal: lifecycleSignal{kind: signalWorkflowFinalized, class: workflowAttemptClass, exitCode: 3, workflowExitRecords: true},
			want:   workflowState(execIdle, 0),
			effects: []lifecycleEffect{
				{kind: effectSettleTurnWaiters},
				{kind: effectRecordWorkflowExit, exitCode: 3},
			},
		},
		{
			name:    "returned releases tree token",
			start:   idleDirect(),
			signal:  lifecycleSignal{kind: signalAttemptReturned, class: directAttemptClass},
			want:    idleDirect(),
			effects: []lifecycleEffect{{kind: effectReleaseTreeToken}},
		},
		{
			name:  "outcome success ordinary direct parks after successPresented",
			start: lifecycleState{exec: execIdle, class: directAttemptClass},
			signal: lifecycleSignal{
				kind: signalAttemptOutcome, class: directAttemptClass,
				runKind: runKindOrdinaryDirect, exitCode: 0,
				sentinelReason: "end_turn", resultSessionID: "s1",
				retryable: false,
			},
			want: lifecycleState{exec: execIdle, class: directAttemptClass},
			cont: contPark,
			effects: []lifecycleEffect{
				{kind: effectRecordOutcomeExit, exitCode: 0},
				successSentinel,
			},
		},
		{
			name:  "outcome success workflow direct ends",
			start: idleDirect(),
			signal: lifecycleSignal{
				kind: signalAttemptOutcome, class: directAttemptClass,
				runKind: runKindWorkflowDirect, exitCode: 0,
				sentinelReason: "end_turn", resultSessionID: "s1",
			},
			want: idleDirect(),
			cont: contEnd,
			effects: []lifecycleEffect{
				{kind: effectRecordOutcomeExit, exitCode: 0},
				successSentinel,
			},
		},
		{
			name:  "outcome success decider ends",
			start: idleDirect(),
			signal: lifecycleSignal{
				kind: signalAttemptOutcome, class: directAttemptClass,
				runKind: runKindDecider, exitCode: 0,
				sentinelReason: "end_turn", resultSessionID: "s1",
			},
			want: idleDirect(),
			cont: contEnd,
			effects: []lifecycleEffect{
				{kind: effectRecordOutcomeExit, exitCode: 0},
				successSentinel,
			},
		},
		{
			name:  "outcome terminal failure selects terminal probe, defers sentinel",
			start: idleDirect(),
			signal: lifecycleSignal{
				kind: signalAttemptOutcome, class: directAttemptClass,
				runKind: runKindOrdinaryDirect, exitCode: 1,
				stopReason: "error", sentinelReason: "error", resultSessionID: "s1",
				retryable: false, retriesRemaining: false,
			},
			want: lifecycleState{exec: execIdle, class: directAttemptClass, pendingProbe: probeTerminalPrompt},
			cont: contProbeTerminalPrompt,
			effects: []lifecycleEffect{
				{kind: effectRecordOutcomeExit, exitCode: 1},
			},
		},
		{
			// The workflow gate applies only to the exit-0 branch: a
			// workflow-direct nonretryable nonzero outcome still probes the
			// terminal prompt queue (supervisor.go:2272-2294; the workflowID
			// gate at 2230-2240 is success-only).
			name:  "outcome workflow-direct terminal failure probes the terminal prompt",
			start: idleDirect(),
			signal: lifecycleSignal{
				kind: signalAttemptOutcome, class: directAttemptClass,
				runKind: runKindWorkflowDirect, exitCode: 1,
				stopReason: "error", sentinelReason: "error", resultSessionID: "s1",
				retryable: false,
			},
			want: lifecycleState{exec: execIdle, class: directAttemptClass, pendingProbe: probeTerminalPrompt},
			cont: contProbeTerminalPrompt,
			effects: []lifecycleEffect{
				{kind: effectRecordOutcomeExit, exitCode: 1},
			},
		},
		{
			name:  "outcome terminal failure with conflict ends with sentinel",
			start: idleDirect(),
			signal: lifecycleSignal{
				kind: signalAttemptOutcome, class: directAttemptClass,
				runKind: runKindOrdinaryDirect, exitCode: 1,
				stopReason: "session_id_conflict", sentinelReason: "session_id_conflict",
				resultSessionID:   "s1",
				sessionIDConflict: true, retryable: false,
			},
			want: idleDirect(),
			cont: contEnd,
			effects: []lifecycleEffect{
				{kind: effectRecordOutcomeExit, exitCode: 1},
				{kind: effectWriteSentinel, exitCode: 1, reason: "session_id_conflict", sessionID: "s1"},
			},
		},
		{
			name:  "outcome terminal decider failure ends with sentinel",
			start: idleDirect(),
			signal: lifecycleSignal{
				kind: signalAttemptOutcome, class: directAttemptClass,
				runKind: runKindDecider, exitCode: 1,
				stopReason: "error", sentinelReason: "error", resultSessionID: "s1",
				retryable: false,
			},
			want: idleDirect(),
			cont: contEnd,
			effects: []lifecycleEffect{
				{kind: effectRecordOutcomeExit, exitCode: 1},
				{kind: effectWriteSentinel, exitCode: 1, reason: "error", sessionID: "s1"},
			},
		},
		{
			name:  "outcome retryable decider records and ends with sentinel",
			start: idleDirect(),
			signal: lifecycleSignal{
				kind: signalAttemptOutcome, class: directAttemptClass,
				runKind: runKindDecider, exitCode: 1,
				stopReason: "error", sentinelReason: "error", resultSessionID: "s1",
				retryable: true, retriesRemaining: true,
			},
			want: idleDirect(),
			cont: contEnd,
			effects: []lifecycleEffect{
				{kind: effectRecordOutcomeExit, exitCode: 1},
				{kind: effectWriteSentinel, exitCode: 1, reason: "error", sessionID: "s1"},
			},
		},
		{
			name:  "outcome retryable with retries selects retry probe without effects",
			start: idleDirect(),
			signal: lifecycleSignal{
				kind: signalAttemptOutcome, class: directAttemptClass,
				runKind: runKindOrdinaryDirect, exitCode: 1,
				stopReason: "error", sentinelReason: "error",
				retryable: true, retriesRemaining: true,
			},
			want: lifecycleState{exec: execIdle, class: directAttemptClass, pendingProbe: probeRetryPrompt},
			cont: contProbeRetryPrompt,
		},
		{
			name: "continuation terminal probe resume keeps sentinel regardless of consumed",
			// Mid-protocol state: attemptOutcome selected the terminal probe.
			start: lifecycleState{exec: execIdle, class: directAttemptClass, pendingProbe: probeTerminalPrompt},
			signal: lifecycleSignal{
				kind: signalPromptContinuation, class: directAttemptClass,
				probeKind: probeTerminalPrompt, promptConsumed: true,
				exitCode: 1, sentinelReason: "error", resultSessionID: "s1",
			},
			want: idleDirect(),
			cont: contResumeQueued,
			effects: []lifecycleEffect{
				{kind: effectWriteSentinel, exitCode: 1, reason: "error", sessionID: "s1"},
			},
		},
		{
			name:  "continuation terminal probe no prompt ends with sentinel",
			start: lifecycleState{exec: execIdle, class: directAttemptClass, pendingProbe: probeTerminalPrompt},
			signal: lifecycleSignal{
				kind: signalPromptContinuation, class: directAttemptClass,
				probeKind: probeTerminalPrompt, promptConsumed: false,
				exitCode: 1, sentinelReason: "error", resultSessionID: "s1",
			},
			want: idleDirect(),
			cont: contEnd,
			effects: []lifecycleEffect{
				{kind: effectWriteSentinel, exitCode: 1, reason: "error", sessionID: "s1"},
			},
		},
		{
			name: "continuation retry probe consumed and live resumes without sentinel",
			// Mid-protocol state: attemptOutcome selected the retry probe.
			start: lifecycleState{exec: execIdle, class: directAttemptClass, pendingProbe: probeRetryPrompt},
			signal: lifecycleSignal{
				kind: signalPromptContinuation, class: directAttemptClass,
				probeKind: probeRetryPrompt, promptConsumed: true,
				resumeContextErr: nil,
			},
			want: idleDirect(),
			cont: contRetry,
		},
		{
			name:  "continuation retry probe consumed but context dead ends idle",
			start: lifecycleState{exec: execIdle, class: directAttemptClass, pendingProbe: probeRetryPrompt},
			signal: lifecycleSignal{
				kind: signalPromptContinuation, class: directAttemptClass,
				probeKind: probeRetryPrompt, promptConsumed: true,
				resumeContextErr: context.Canceled,
			},
			want: idleDirect(),
			cont: contIdleContextEnd,
		},
		{
			name:  "continuation retry probe no prompt and live context backs off",
			start: lifecycleState{exec: execIdle, class: directAttemptClass, pendingProbe: probeRetryPrompt},
			signal: lifecycleSignal{
				kind: signalPromptContinuation, class: directAttemptClass,
				probeKind: probeRetryPrompt, promptConsumed: false,
			},
			want: idleDirect(),
			cont: contBackoff,
		},
		{
			name:   "success presented writes done presentation",
			start:  idleDirect(),
			signal: lifecycleSignal{kind: signalSuccessPresented, class: directAttemptClass, exitCode: 0},
			want:   idleDirect(),
			effects: []lifecycleEffect{
				{kind: effectSetSuccessPresentation},
			},
		},
		{
			name:   "park requested parks and signals capacity",
			start:  idleDirect(),
			signal: lifecycleSignal{kind: signalParkRequested, class: directAttemptClass, runKind: runKindOrdinaryDirect},
			want:   lifecycleState{exec: execParked, class: directAttemptClass},
			effects: []lifecycleEffect{
				{kind: effectSignalCapacity},
			},
		},
		{
			name:   "prompt dequeued is observational",
			start:  lifecycleState{exec: execParked, class: directAttemptClass},
			signal: lifecycleSignal{kind: signalPromptDequeued, class: directAttemptClass},
			want:   lifecycleState{exec: execParked, class: directAttemptClass},
		},
		{
			name:   "local slot acquired resumes to idle",
			start:  lifecycleState{exec: execParked, class: directAttemptClass},
			signal: lifecycleSignal{kind: signalLocalSlotAcquired, class: directAttemptClass},
			want:   idleDirect(),
		},
		{
			name:   "park timed out closes admission without settle or close",
			start:  lifecycleState{exec: execParked, class: directAttemptClass},
			signal: lifecycleSignal{kind: signalParkTimedOut, class: directAttemptClass},
			want:   lifecycleState{exec: execParked, completion: completionAdmissionClosed, class: directAttemptClass},
		},
		{
			// The not-ok park wait (context canceled, supervisor.go:2956-2957)
			// resumes to idle with completion still open.
			name:   "park wait ended from parked with open completion resumes to idle",
			start:  lifecycleState{exec: execParked, class: directAttemptClass},
			signal: lifecycleSignal{kind: signalParkWaitEnded, class: directAttemptClass},
			want:   idleDirect(),
		},
		{
			name:   "park wait ended resumes to idle",
			start:  lifecycleState{exec: execParked, completion: completionAdmissionClosed, class: directAttemptClass},
			signal: lifecycleSignal{kind: signalParkWaitEnded, class: directAttemptClass},
			want:   lifecycleState{exec: execIdle, completion: completionAdmissionClosed, class: directAttemptClass},
		},
		{
			// Park-window rule (decision 4): the idle-context end while parked
			// after the admission closed (local-slot failure trace) must retain
			// the parked accounting exactly.
			name:  "idle context ended while parked after admission closed retains parked",
			start: lifecycleState{exec: execParked, completion: completionAdmissionClosed, class: directAttemptClass},
			signal: lifecycleSignal{
				kind: signalIdleContextEnded, class: directAttemptClass,
				idleExitCode: 130, idleStopReason: "cancelled",
			},
			want: lifecycleState{exec: execParked, completion: completionAdmissionClosed, class: directAttemptClass},
			effects: []lifecycleEffect{
				{kind: effectSetIdleEndData, exitCode: 130, reason: "cancelled"},
				{kind: effectWriteSentinel, exitCode: 130, reason: "cancelled"},
				{kind: effectEmitSessionEnd, exitCode: 130, reason: "cancelled"},
			},
		},
		{
			name:  "idle context ended overwrites data and emits sentinel and session end",
			start: idleDirect(),
			signal: lifecycleSignal{
				kind: signalIdleContextEnded, class: directAttemptClass,
				idleExitCode: 130, idleStopReason: "cancelled",
			},
			want: idleDirect(),
			effects: []lifecycleEffect{
				{kind: effectSetIdleEndData, exitCode: 130, reason: "cancelled"},
				{kind: effectWriteSentinel, exitCode: 130, reason: "cancelled"},
				{kind: effectEmitSessionEnd, exitCode: 130, reason: "cancelled"},
			},
		},
		{
			// The idle sentinel carries the session ID the caller resolved
			// outside mu; an empty one keeps the payload empty.
			name:  "idle context ended sentinel carries the resolved session id",
			start: idleDirect(),
			signal: lifecycleSignal{
				kind: signalIdleContextEnded, class: directAttemptClass,
				idleExitCode: 124, idleStopReason: "timeout", resultSessionID: "ses_idle",
			},
			want: idleDirect(),
			effects: []lifecycleEffect{
				{kind: effectSetIdleEndData, exitCode: 124, reason: "timeout"},
				{kind: effectWriteSentinel, exitCode: 124, reason: "timeout", sessionID: "ses_idle"},
				{kind: effectEmitSessionEnd, exitCode: 124, reason: "timeout"},
			},
		},
		{
			name:  "run ended from idle publishes at teardown boundary",
			start: idleDirect(),
			signal: lifecycleSignal{
				kind: signalRunEnded, class: directAttemptClass, boundary: afterRunTeardown,
			},
			want: lifecycleState{exec: execEnded, class: directAttemptClass},
			effects: []lifecycleEffect{
				{kind: effectPublishCompletion, boundary: afterRunTeardown},
			},
		},
		{
			name:   "run ended from parked parks the ending boundary",
			start:  lifecycleState{exec: execParked, class: directAttemptClass},
			signal: lifecycleSignal{kind: signalRunEnded, class: directAttemptClass},
			want:   lifecycleState{exec: execParkedEnding, class: directAttemptClass},
			effects: []lifecycleEffect{
				{kind: effectPublishCompletion, boundary: afterRunTeardown},
			},
		},
		{
			// The rollback cue leaves the child in V/S: the publication itself
			// ends a rollback child (supervisor.go:1475-1480).
			name:   "rollback from reserved emits the publication cue unchanged",
			start:  lifecycleState{},
			signal: lifecycleSignal{kind: signalSpawnRolledBack, boundary: afterSpawnRollback},
			want:   lifecycleState{},
			effects: []lifecycleEffect{
				{kind: effectPublishCompletion, boundary: afterSpawnRollback},
			},
		},
		{
			name:  "rollback from starting emits the publication cue unchanged",
			start: directState(execStarting),
			signal: lifecycleSignal{
				kind: signalSpawnRolledBack, boundary: afterSpawnRollback,
			},
			want: directState(execStarting),
			effects: []lifecycleEffect{
				{kind: effectPublishCompletion, boundary: afterSpawnRollback},
			},
		},
		{
			// Panic-recovery teardown: a starting child (attempt never
			// incremented) ends through the teardown boundary
			// (supervisor.go:2141-2167).
			name:   "run ended from starting publishes at teardown boundary",
			start:  directState(execStarting),
			signal: lifecycleSignal{kind: signalRunEnded, class: directAttemptClass, boundary: afterRunTeardown},
			want:   lifecycleState{exec: execEnded, class: directAttemptClass},
			effects: []lifecycleEffect{
				{kind: effectPublishCompletion, boundary: afterRunTeardown},
			},
		},
		{
			name:   "shutdown requested cancels context once",
			start:  runningDirect(),
			signal: lifecycleSignal{kind: signalShutdownRequested},
			want:   lifecycleState{exec: execRunning, attempts: 1, shutdown: shutdownRequested, class: directAttemptClass},
			effects: []lifecycleEffect{
				{kind: effectConsiderProviderCancel},
				{kind: effectCancelContext},
			},
		},
		{
			name:   "writer close begun marks closing",
			start:  idleDirect(),
			signal: lifecycleSignal{kind: signalWriterCloseBegun},
			want:   lifecycleState{exec: execIdle, writer: writerClosing, class: directAttemptClass},
		},
		{
			name:   "writer close finished marks closed",
			start:  lifecycleState{exec: execIdle, writer: writerClosing, class: directAttemptClass},
			signal: lifecycleSignal{kind: signalWriterCloseFinished, closeCalled: true},
			want:   lifecycleState{exec: execIdle, writer: writerClosed, class: directAttemptClass},
		},
		{
			name: "completion published from parked-ending normalizes to ended",
			start: lifecycleState{
				exec: execParkedEnding, shutdown: shutdownRequested, writer: writerClosing,
				completion: completionAdmissionClosed, class: directAttemptClass,
			},
			signal: lifecycleSignal{kind: signalCompletionPublished, boundary: afterRunTeardown},
			want: lifecycleState{
				exec: execEnded, shutdown: shutdownRequested, writer: writerClosing,
				completion: completionPublished, class: directAttemptClass,
			},
			effects: []lifecycleEffect{
				{kind: effectSettleTurnWaiters},
				{kind: effectCloseDone},
			},
		},
		{
			// A rollback publication runs while the child is still reserved with
			// completion open, with writer open and no shutdown, and the
			// publication itself ends the child (supervisor.go:1475-1480).
			name:   "rollback publication from reserved ends the child",
			start:  lifecycleState{},
			signal: lifecycleSignal{kind: signalCompletionPublished, boundary: afterSpawnRollback},
			want:   lifecycleState{exec: execEnded, completion: completionPublished},
			effects: []lifecycleEffect{
				{kind: effectSettleTurnWaiters},
				{kind: effectCloseDone},
			},
		},
		{
			name:  "agent status observed with phase and done presentation emits nothing",
			start: idleDirect(),
			signal: lifecycleSignal{
				kind: signalAgentStatusObserved, phase: "running", label: "next",
				presentationWasDone: true,
			},
			want: idleDirect(),
		},
		{
			name:  "agent status observed with phase emits presentation write",
			start: idleDirect(),
			signal: lifecycleSignal{
				kind: signalAgentStatusObserved, phase: "running", label: "next",
			},
			want: idleDirect(),
			effects: []lifecycleEffect{
				{kind: effectSetAgentPresentation, phase: "running", label: "next"},
			},
		},
		{
			// An empty label carries an empty payload label: the caller keeps the
			// previous label (supervisor.go:4860-4864).
			name:  "agent status observed with phase and empty label carries empty label",
			start: idleDirect(),
			signal: lifecycleSignal{
				kind: signalAgentStatusObserved, phase: "running", label: "",
			},
			want: idleDirect(),
			effects: []lifecycleEffect{
				{kind: effectSetAgentPresentation, phase: "running"},
			},
		},
		{
			name:  "agent status observed with empty phase emits nothing",
			start: idleDirect(),
			signal: lifecycleSignal{
				kind: signalAgentStatusObserved, phase: "", label: "kept",
			},
			want: idleDirect(),
		},
		{
			// An empty stop reason is absent from the payload: the effect leaves
			// the stored reason untouched (supervisor.go:4895-4897).
			name:   "session end observed sets done and keeps empty reason",
			start:  runningDirect(),
			signal: lifecycleSignal{kind: signalSessionEndObserved, stopReason: ""},
			want:   runningDirect(),
			effects: []lifecycleEffect{
				{kind: effectSetSessionEndPresentation},
			},
		},
		{
			name:   "session end observed with reason carries it",
			start:  runningDirect(),
			signal: lifecycleSignal{kind: signalSessionEndObserved, stopReason: "end_turn"},
			want:   runningDirect(),
			effects: []lifecycleEffect{
				{kind: effectSetSessionEndPresentation, reason: "end_turn"},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, cont, eff := applyOK(t, tc.start, tc.signal)
			if out != tc.want {
				t.Errorf("state = %+v, want %+v", out, tc.want)
			}
			if cont.kind != tc.cont {
				t.Errorf("continuation = %d, want %d", cont.kind, tc.cont)
			}
			if !effectsEqual(eff, tc.effects) {
				t.Errorf("effects = %+v, want %+v", eff, tc.effects)
			}
		})
	}
}

func TestRuntimeLifecycleRepeats(t *testing.T) {
	t.Run("allowed repeats", func(t *testing.T) {
		// preparing: repeat allowed, clears presentation each time.
		st := workflowState(execRunning, 1)
		for i := 0; i < 2; i++ {
			out, _, eff := applyOK(t, st, lifecycleSignal{kind: signalAttemptPreparing, class: workflowAttemptClass})
			if out != st {
				t.Fatalf("preparing repeat changed state: %+v", out)
			}
			if !effectsEqual(eff, []lifecycleEffect{{kind: effectClearPresentation}}) {
				t.Fatalf("preparing repeat effects = %v", eff)
			}
		}

		// startFailed: repeat allowed, S→I once then unchanged, no settle.
		st = directState(execStarting)
		out, _, eff := applyOK(t, st, lifecycleSignal{kind: signalAttemptStartFailed, class: directAttemptClass})
		if out.exec != execIdle || len(eff) != 0 {
			t.Fatalf("first startFailed: state %+v effects %v", out, eff)
		}
		out, _, eff = applyOK(t, out, lifecycleSignal{kind: signalAttemptStartFailed, class: directAttemptClass})
		if out.exec != execIdle || len(eff) != 0 {
			t.Fatalf("repeat startFailed: state %+v effects %v", out, eff)
		}

		// workflowFinalized: repeat allowed, settles unconditionally, no clear.
		st = workflowState(execRunning, 2)
		out, _, eff = applyOK(t, st, lifecycleSignal{kind: signalWorkflowFinalized, class: workflowAttemptClass})
		if out.exec != execIdle || out.attempts != 0 {
			t.Fatalf("first finalize: %+v", out)
		}
		if !effectsEqual(eff, []lifecycleEffect{{kind: effectSettleTurnWaiters}}) {
			t.Fatalf("first finalize effects = %v", eff)
		}
		out, _, eff = applyOK(t, out, lifecycleSignal{kind: signalWorkflowFinalized, class: workflowAttemptClass})
		if out.exec != execIdle || out.attempts != 0 {
			t.Fatalf("repeat finalize: %+v", out)
		}
		if !effectsEqual(eff, []lifecycleEffect{{kind: effectSettleTurnWaiters}}) {
			t.Fatalf("repeat finalize effects = %v", eff)
		}
		// The exit-record variant carries the exit code the legacy path writes
		// after finalization.
		out, _, eff = applyOK(t, out, lifecycleSignal{kind: signalWorkflowFinalized, class: workflowAttemptClass, exitCode: 2, workflowExitRecords: true})
		if !effectsEqual(eff, []lifecycleEffect{
			{kind: effectSettleTurnWaiters},
			{kind: effectRecordWorkflowExit, exitCode: 2},
		}) {
			t.Fatalf("finalize exit-record effects = %v", eff)
		}

		// attemptReturned: repeated release is safe; effect emitted per signal.
		st = idleDirect()
		out, _, eff = applyOK(t, st, lifecycleSignal{kind: signalAttemptReturned, class: directAttemptClass})
		if out != st || !effectsEqual(eff, []lifecycleEffect{{kind: effectReleaseTreeToken}}) {
			t.Fatalf("first returned: %+v %v", out, eff)
		}
		out, _, eff = applyOK(t, out, lifecycleSignal{kind: signalAttemptReturned, class: directAttemptClass})
		if out != st || !effectsEqual(eff, []lifecycleEffect{{kind: effectReleaseTreeToken}}) {
			t.Fatalf("repeat returned: %+v %v", out, eff)
		}

		// successPresented: repeat sets phase done again, no label clear.
		st = idleDirect()
		for i := 0; i < 2; i++ {
			out, _, eff := applyOK(t, st, lifecycleSignal{kind: signalSuccessPresented, class: directAttemptClass, exitCode: 0})
			if out != st || !effectsEqual(eff, []lifecycleEffect{{kind: effectSetSuccessPresentation}}) {
				t.Fatalf("successPresented repeat %d: %+v %v", i, out, eff)
			}
		}

		// promptDequeued: observational repeat.
		st = lifecycleState{exec: execParked, class: directAttemptClass}
		for i := 0; i < 2; i++ {
			out, _, eff := applyOK(t, st, lifecycleSignal{kind: signalPromptDequeued, class: directAttemptClass})
			if out != st || len(eff) != 0 {
				t.Fatalf("promptDequeued repeat %d: %+v %v", i, out, eff)
			}
		}

		// idleContextEnded: repeat overwrites exit/phase/reason and emits
		// sentinel then session.end; parked is never cleared.
		st = lifecycleState{exec: execParked, class: directAttemptClass}
		sig := lifecycleSignal{kind: signalIdleContextEnded, idleExitCode: 124, idleStopReason: "timeout"}
		for i := 0; i < 2; i++ {
			out, _, eff := applyOK(t, st, sig)
			if out != st {
				t.Fatalf("idleContextEnded repeat %d changed state: %+v", i, out)
			}
			if !out.isParked() {
				t.Fatalf("idleContextEnded repeat %d cleared parked", i)
			}
			want := []lifecycleEffect{
				{kind: effectSetIdleEndData, exitCode: 124, reason: "timeout"},
				{kind: effectWriteSentinel, exitCode: 124, reason: "timeout"},
				{kind: effectEmitSessionEnd, exitCode: 124, reason: "timeout"},
			}
			if !effectsEqual(eff, want) {
				t.Fatalf("idleContextEnded repeat %d effects = %v", i, eff)
			}
		}

		// shutdownRequested: repeats allowed; every call considers provider
		// cancel; context cancel only on first.
		st = idleDirect()
		out, _, eff = applyOK(t, st, lifecycleSignal{kind: signalShutdownRequested})
		if want := []effectKind{effectConsiderProviderCancel, effectCancelContext}; !reflect.DeepEqual(effectKinds(eff), want) {
			t.Fatalf("first shutdown effects = %v, want %v", effectKinds(eff), want)
		}
		out, _, eff = applyOK(t, out, lifecycleSignal{kind: signalShutdownRequested})
		if want := []effectKind{effectConsiderProviderCancel}; !reflect.DeepEqual(effectKinds(eff), want) {
			t.Fatalf("repeat shutdown effects = %v, want %v", effectKinds(eff), want)
		}

		// writerCloseBegun: repeat allowed even after closed; writer unchanged.
		st = lifecycleState{exec: execIdle, writer: writerClosed, class: directAttemptClass}
		out, _, eff = applyOK(t, st, lifecycleSignal{kind: signalWriterCloseBegun})
		if out != st || len(eff) != 0 {
			t.Fatalf("close begun after closed: %+v %v", out, eff)
		}

		// runEnded: terminal repeat carries no effects. A rollback signal from
		// a teardown-ended child is rejected: only a reserved or starting child
		// with open completion may roll back.
		st = lifecycleState{exec: execEnded}
		out, _, eff = applyOK(t, st, lifecycleSignal{kind: signalRunEnded})
		if out != st || len(eff) != 0 {
			t.Fatalf("runEnded repeat: %+v %v", out, eff)
		}
		expectErr(t, st, lifecycleSignal{kind: signalSpawnRolledBack, boundary: afterSpawnRollback})

		// spawnRolledBack: the cue leaves the state unchanged and repeats
		// re-emit it; the publication's CP repeat guards double publication.
		st = lifecycleState{}
		out, _, eff = applyOK(t, st, lifecycleSignal{kind: signalSpawnRolledBack, boundary: afterSpawnRollback})
		if out != st || !effectsEqual(eff, []lifecycleEffect{{kind: effectPublishCompletion, boundary: afterSpawnRollback}}) {
			t.Fatalf("first rollback: %+v %v", out, eff)
		}
		out, _, eff = applyOK(t, out, lifecycleSignal{kind: signalSpawnRolledBack, boundary: afterSpawnRollback})
		if out != st || !effectsEqual(eff, []lifecycleEffect{{kind: effectPublishCompletion, boundary: afterSpawnRollback}}) {
			t.Fatalf("repeat rollback: %+v %v", out, eff)
		}

		// shutdownRequested repeats from parked-ending and ended: state
		// unchanged, only the provider-cancel consideration repeats (the repeat
		// may request provider cancel again; the parent cancel is once-only).
		st = lifecycleState{exec: execParkedEnding, shutdown: shutdownRequested, class: directAttemptClass}
		out, _, eff = applyOK(t, st, lifecycleSignal{kind: signalShutdownRequested})
		if out != st || !effectsEqual(eff, []lifecycleEffect{{kind: effectConsiderProviderCancel}}) {
			t.Fatalf("repeat shutdown from parked-ending: %+v %v", out, eff)
		}
		st = lifecycleState{exec: execEnded, shutdown: shutdownRequested}
		out, _, eff = applyOK(t, st, lifecycleSignal{kind: signalShutdownRequested})
		if out != st || !effectsEqual(eff, []lifecycleEffect{{kind: effectConsiderProviderCancel}}) {
			t.Fatalf("repeat shutdown from ended: %+v %v", out, eff)
		}
		// runEnded repeat from parked-ending: no effects, state unchanged.
		st = lifecycleState{exec: execParkedEnding, class: directAttemptClass}
		out, _, eff = applyOK(t, st, lifecycleSignal{kind: signalRunEnded, class: directAttemptClass})
		if out != st || len(eff) != 0 {
			t.Fatalf("runEnded repeat from parked-ending: %+v %v", out, eff)
		}

		// completionPublished: CP repeat is a no-op.
		st = lifecycleState{exec: execEnded, completion: completionPublished}
		out, _, eff = applyOK(t, st, lifecycleSignal{kind: signalCompletionPublished, boundary: afterRunTeardown})
		if out != st || len(eff) != 0 {
			t.Fatalf("completionPublished repeat: %+v %v", out, eff)
		}
	})

	t.Run("duplicates rejected", func(t *testing.T) {
		// published: duplicate rejected.
		expectErr(t, directState(execStarting), lifecycleSignal{kind: signalPublished})

		// parkRequested: duplicate rejected.
		expectErr(t, lifecycleState{exec: execParked, class: directAttemptClass},
			lifecycleSignal{kind: signalParkRequested, runKind: runKindOrdinaryDirect})

		// localSlotAcquired: duplicate rejected.
		expectErr(t, idleDirect(), lifecycleSignal{kind: signalLocalSlotAcquired})

		// parkTimedOut: duplicate rejected (CA not a valid source).
		expectErr(t, lifecycleState{exec: execParked, completion: completionAdmissionClosed},
			lifecycleSignal{kind: signalParkTimedOut})

		// parkWaitEnded: duplicate rejected (source must be parked).
		expectErr(t, lifecycleState{exec: execIdle, completion: completionAdmissionClosed},
			lifecycleSignal{kind: signalParkWaitEnded})

		// attemptSettled: extra settle rejected.
		expectErr(t, idleDirect(), lifecycleSignal{kind: signalAttemptSettled, class: directAttemptClass})
		expectErr(t, workflowState(execIdle, 0), lifecycleSignal{kind: signalAttemptSettled, class: workflowAttemptClass})

		// writerCloseFinished: repeated finished rejected.
		expectErr(t, lifecycleState{writer: writerClosed}, lifecycleSignal{kind: signalWriterCloseFinished, closeCalled: true})
	})
}

func TestRuntimeLifecycleInvalidSources(t *testing.T) {
	cases := []struct {
		name   string
		start  lifecycleState
		signal lifecycleSignal
	}{
		{"preparing after admission closed", lifecycleState{exec: execIdle, completion: completionAdmissionClosed},
			lifecycleSignal{kind: signalAttemptPreparing}},
		{"started after published", lifecycleState{exec: execEnded, completion: completionPublished},
			lifecycleSignal{kind: signalAttemptStarted}},
		{"started in parked", lifecycleState{exec: execParked},
			lifecycleSignal{kind: signalAttemptStarted}},
		{"direct parallel start rejected", runningDirect(),
			lifecycleSignal{kind: signalAttemptStarted, class: directAttemptClass}},
		{"settled from idle", idleDirect(),
			lifecycleSignal{kind: signalAttemptSettled, class: directAttemptClass}},
		{"settled direct with two attempts", lifecycleState{exec: execRunning, attempts: 2, class: directAttemptClass},
			lifecycleSignal{kind: signalAttemptSettled, class: directAttemptClass}},
		{"workflow finalized with direct class", runningDirect(),
			lifecycleSignal{kind: signalWorkflowFinalized, class: directAttemptClass}},
		// Class is immutable: a signal whose class differs from the state's
		// class is rejected regardless of the signal kind.
		{"direct class signal on workflow child", workflowState(execIdle, 0),
			lifecycleSignal{kind: signalShutdownRequested, class: directAttemptClass}},
		{"workflow class signal on direct child", idleDirect(),
			lifecycleSignal{kind: signalShutdownRequested, class: workflowAttemptClass}},
		{"returned with workflow class", idleDirect(),
			lifecycleSignal{kind: signalAttemptReturned, class: workflowAttemptClass}},
		{"returned from running", runningDirect(),
			lifecycleSignal{kind: signalAttemptReturned, class: directAttemptClass}},
		{"outcome from running", runningDirect(),
			lifecycleSignal{kind: signalAttemptOutcome, class: directAttemptClass}},
		{"continuation from running", runningDirect(),
			lifecycleSignal{kind: signalPromptContinuation, class: directAttemptClass, probeKind: probeTerminalPrompt}},
		{"outcome with workflow class", idleDirect(),
			lifecycleSignal{kind: signalAttemptOutcome, class: workflowAttemptClass}},
		{"park requested after admission closed", lifecycleState{exec: execIdle, completion: completionAdmissionClosed},
			lifecycleSignal{kind: signalParkRequested, runKind: runKindOrdinaryDirect}},
		{"park requested from running", runningDirect(),
			lifecycleSignal{kind: signalParkRequested, runKind: runKindOrdinaryDirect}},
		// Only runChild parks (supervisor.go:2243); loop/team children never
		// park, so a workflow-class park signal is rejected from every source.
		{"park requested from workflow child in starting", workflowState(execStarting, 0),
			lifecycleSignal{kind: signalParkRequested, class: workflowAttemptClass, runKind: runKindOrdinaryDirect}},
		{"park requested from workflow child after finalization", workflowState(execIdle, 0),
			lifecycleSignal{kind: signalParkRequested, class: workflowAttemptClass, runKind: runKindOrdinaryDirect}},
		{"prompt dequeued from idle", idleDirect(),
			lifecycleSignal{kind: signalPromptDequeued}},
		{"prompt dequeued after admission closed", lifecycleState{exec: execParked, completion: completionAdmissionClosed},
			lifecycleSignal{kind: signalPromptDequeued}},
		// The gate at runtime_lifecycle.go: preparing in a running state is
		// workflow-only (parallel preparation).
		{"preparing in running with direct class", runningDirect(),
			lifecycleSignal{kind: signalAttemptPreparing, class: directAttemptClass}},
		{"start failed in running with direct class", runningDirect(),
			lifecycleSignal{kind: signalAttemptStartFailed, class: directAttemptClass}},
		{"park timed out from idle", idleDirect(), lifecycleSignal{kind: signalParkTimedOut}},
		{"park wait ended from idle", idleDirect(), lifecycleSignal{kind: signalParkWaitEnded}},
		{"idle end from running", runningDirect(), lifecycleSignal{kind: signalIdleContextEnded}},
		{"idle end after parked ending", lifecycleState{exec: execParkedEnding}, lifecycleSignal{kind: signalIdleContextEnded}},
		{"idle end after published", lifecycleState{exec: execEnded, completion: completionPublished},
			lifecycleSignal{kind: signalIdleContextEnded}},
		{"run ended from running with attempts", runningDirect(), lifecycleSignal{kind: signalRunEnded}},
		{"run ended after published", lifecycleState{exec: execEnded, completion: completionPublished},
			lifecycleSignal{kind: signalRunEnded}},
		{"rollback from idle", idleDirect(),
			lifecycleSignal{kind: signalSpawnRolledBack, boundary: afterSpawnRollback}},
		{"rollback after admission closed", lifecycleState{exec: execIdle, completion: completionAdmissionClosed},
			lifecycleSignal{kind: signalSpawnRolledBack, boundary: afterSpawnRollback}},
		{"run ended with rollback boundary", idleDirect(),
			lifecycleSignal{kind: signalRunEnded, class: directAttemptClass, boundary: afterSpawnRollback}},
		{"rollback with teardown boundary", lifecycleState{},
			lifecycleSignal{kind: signalSpawnRolledBack, boundary: afterRunTeardown}},
		{"close finished from open", lifecycleState{}, lifecycleSignal{kind: signalWriterCloseFinished, closeCalled: true}},
		{"close finished without close call", lifecycleState{writer: writerClosing},
			lifecycleSignal{kind: signalWriterCloseFinished, closeCalled: false}},
		{"publication from starting", lifecycleState{exec: execStarting},
			lifecycleSignal{kind: signalCompletionPublished, boundary: afterRunTeardown}},
		{"publication with attempts", lifecycleState{exec: execRunning, attempts: 1},
			lifecycleSignal{kind: signalCompletionPublished, boundary: afterRunTeardown}},
		{"teardown publication without shutdown", lifecycleState{exec: execEnded, writer: writerClosing},
			lifecycleSignal{kind: signalCompletionPublished, boundary: afterRunTeardown}},
		{"teardown publication with open writer", lifecycleState{exec: execEnded, shutdown: shutdownRequested},
			lifecycleSignal{kind: signalCompletionPublished, boundary: afterRunTeardown}},
		// Rollback publication is only valid from a reserved or starting child
		// with open completion (supervisor.go:1475-1480).
		{"rollback publication from parked ending", lifecycleState{exec: execParkedEnding, completion: completionAdmissionClosed, class: directAttemptClass},
			lifecycleSignal{kind: signalCompletionPublished, boundary: afterSpawnRollback}},
		{"rollback publication from ended", lifecycleState{exec: execEnded},
			lifecycleSignal{kind: signalCompletionPublished, boundary: afterSpawnRollback}},
		{"rollback publication after admission closed", lifecycleState{exec: execIdle, completion: completionAdmissionClosed},
			lifecycleSignal{kind: signalCompletionPublished, boundary: afterSpawnRollback}},
		{"unknown signal kind", idleDirect(), lifecycleSignal{kind: signalKind(999)}},
		{"success presented with nonzero exit", idleDirect(),
			lifecycleSignal{kind: signalSuccessPresented, exitCode: 1}},
		{"park requested for decider run", idleDirect(),
			lifecycleSignal{kind: signalParkRequested, runKind: runKindDecider}},
		{"continuation without probe kind", idleDirect(),
			lifecycleSignal{kind: signalPromptContinuation, probeKind: probeNone}},
		{"invalid run kind on outcome", idleDirect(),
			lifecycleSignal{kind: signalAttemptOutcome, runKind: runKind(42)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expectErr(t, tc.start, tc.signal)
		})
	}
}

func TestRuntimeLifecycleCrossDimensionInvariants(t *testing.T) {
	cases := []struct {
		name  string
		state lifecycleState
	}{
		{"negative attempts", lifecycleState{exec: execRunning, attempts: -1}},
		{"running with zero attempts", lifecycleState{exec: execRunning}},
		{"attempts while idle", lifecycleState{exec: execIdle, attempts: 1}},
		{"attempts while starting", lifecycleState{exec: execStarting, attempts: 1}},
		{"attempts while parked", lifecycleState{exec: execParked, attempts: 1}},
		{"attempts after admission closed", lifecycleState{exec: execIdle, attempts: 1, completion: completionAdmissionClosed}},
		{"admission closed while starting", lifecycleState{exec: execStarting, completion: completionAdmissionClosed}},
		{"admission closed while reserved", lifecycleState{completion: completionAdmissionClosed}},
		{"published while parked ending", lifecycleState{exec: execParkedEnding, completion: completionPublished}},
		{"reserved with attempts", lifecycleState{attempts: 1}},
		{"reserved with admission closed", lifecycleState{completion: completionAdmissionClosed}},
		{"reserved with published", lifecycleState{completion: completionPublished}},
		{"direct with two attempts", lifecycleState{exec: execRunning, attempts: 2, class: directAttemptClass}},
		{"invalid exec state", lifecycleState{exec: execState(99)}},
		{"invalid completion state", lifecycleState{completion: completionStage(99)}},
		{"invalid writer state", lifecycleState{writer: writerStage(99)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateLifecycleState(tc.state); err == nil {
				t.Fatalf("validateLifecycleState(%+v) expected error", tc.state)
			}
			// next must also reject it on input, regardless of the signal.
			expectErr(t, tc.state, lifecycleSignal{kind: signalShutdownRequested})
		})
	}

	t.Run("reachable combinations stay valid", func(t *testing.T) {
		valid := []lifecycleState{
			// running+shutdown is orthogonal.
			{exec: execRunning, attempts: 1, shutdown: shutdownRequested},
			// parked+shutdown is orthogonal.
			{exec: execParked, shutdown: shutdownRequested},
			// parked+writerClosed is reachable via the local-slot failure path.
			{exec: execParked, writer: writerClosed},
			// parked-ending with closed writer and admission closed.
			{exec: execParkedEnding, writer: writerClosed, completion: completionAdmissionClosed},
		}
		for i, st := range valid {
			if err := validateLifecycleState(st); err != nil {
				t.Errorf("case %d: validateLifecycleState(%+v) = %v, want nil", i, st, err)
			}
		}
	})
}

// TestRuntimeLifecycleDirectParkResume drives the ordinary direct cycle:
// success parks, a queued prompt resumes, a second success parks, the park
// times out (admission closed before done), the wait ends, and teardown
// publishes completion.
func TestRuntimeLifecycleDirectParkResume(t *testing.T) {
	st := lifecycleState{}
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalPublished})

	// First attempt.
	out, _, eff := applyOK(t, st, lifecycleSignal{kind: signalAttemptPreparing, class: directAttemptClass})
	if !effectsEqual(eff, []lifecycleEffect{{kind: effectClearPresentation}}) {
		t.Fatalf("preparing effects = %v", eff)
	}
	st = out
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptStarted, class: directAttemptClass})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptSettled, class: directAttemptClass})

	// Success: record exit → sentinel → (separate) successPresented → park.
	out, cont, eff := applyOK(t, st, lifecycleSignal{
		kind: signalAttemptOutcome, class: directAttemptClass,
		runKind: runKindOrdinaryDirect, exitCode: 0,
		sentinelReason: "end_turn", resultSessionID: "s1",
	})
	if cont.kind != contPark {
		t.Fatalf("success continuation = %d, want %d", cont.kind, contPark)
	}
	if want := []effectKind{effectRecordOutcomeExit, effectWriteSentinel}; !reflect.DeepEqual(effectKinds(eff), want) {
		t.Fatalf("success effects = %v, want %v", effectKinds(eff), want)
	}
	st = out
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalSuccessPresented, class: directAttemptClass, exitCode: 0})
	st, _, eff = applyOK(t, st, lifecycleSignal{kind: signalParkRequested, class: directAttemptClass, runKind: runKindOrdinaryDirect})
	if !effectsEqual(eff, []lifecycleEffect{{kind: effectSignalCapacity}}) {
		t.Fatalf("park effects = %v", eff)
	}
	if !st.isParked() {
		t.Fatalf("state after park: %+v", st)
	}

	// Parked prompt dequeues, then the local slot is re-acquired and a second
	// attempt runs.
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalPromptDequeued, class: directAttemptClass})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalLocalSlotAcquired, class: directAttemptClass})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptPreparing, class: directAttemptClass})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptStarted, class: directAttemptClass})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptSettled, class: directAttemptClass})
	st, _, _ = applyOK(t, st, lifecycleSignal{
		kind: signalAttemptOutcome, class: directAttemptClass,
		runKind: runKindOrdinaryDirect, exitCode: 0, sentinelReason: "end_turn",
	})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalSuccessPresented, class: directAttemptClass, exitCode: 0})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalParkRequested, class: directAttemptClass, runKind: runKindOrdinaryDirect})

	// Park timeout: admission closes before done, no settle, no closeDone.
	out, _, eff = applyOK(t, st, lifecycleSignal{kind: signalParkTimedOut})
	if len(eff) != 0 {
		t.Fatalf("parkTimedOut effects = %v, want none", eff)
	}
	st = out
	if !st.isCompleted() {
		t.Fatal("parkTimedOut did not close admission")
	}
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalParkWaitEnded})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalIdleContextEnded, idleExitCode: 124, idleStopReason: "timeout"})

	// Teardown: shutdown, writer close, run end, publication.
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalShutdownRequested})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalWriterCloseBegun})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalWriterCloseFinished, closeCalled: true})
	out, _, eff = applyOK(t, st, lifecycleSignal{kind: signalRunEnded})
	if !effectsEqual(eff, []lifecycleEffect{{kind: effectPublishCompletion, boundary: afterRunTeardown}}) {
		t.Fatalf("runEnded effects = %v", eff)
	}
	st = out
	st, _, eff = applyOK(t, st, lifecycleSignal{kind: signalCompletionPublished, boundary: afterRunTeardown})
	if want := []effectKind{effectSettleTurnWaiters, effectCloseDone}; !reflect.DeepEqual(effectKinds(eff), want) {
		t.Fatalf("publication effects = %v, want %v", effectKinds(eff), want)
	}
	if st.exec != execEnded || st.completion != completionPublished {
		t.Fatalf("final state: %+v", st)
	}
}

// TestRuntimeLifecycleAdmissionClosedBeforeDone pins the window the park
// timeout opens: admission is closed (prompt admission must reject) while done
// is still open (no completionPublished yet, so no closeDone effect).
func TestRuntimeLifecycleAdmissionClosedBeforeDone(t *testing.T) {
	st := lifecycleState{exec: execParked, class: directAttemptClass}
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalParkTimedOut})
	if !st.isCompleted() {
		t.Fatal("RuntimePrompt must reject after the park timeout")
	}
	if st.completion == completionPublished {
		t.Fatal("done must not be published by the park timeout")
	}
	// The direct teardown path in this window: wait ends, teardown, publication.
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalParkWaitEnded})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalShutdownRequested})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalWriterCloseBegun})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalWriterCloseFinished, closeCalled: true})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalRunEnded})
	st, _, eff := applyOK(t, st, lifecycleSignal{kind: signalCompletionPublished, boundary: afterRunTeardown})
	if want := []effectKind{effectSettleTurnWaiters, effectCloseDone}; !reflect.DeepEqual(effectKinds(eff), want) {
		t.Fatalf("publication effects = %v", effectKinds(eff))
	}
	if !st.isCompleted() {
		t.Fatal("final state must be completed")
	}
}

// TestRuntimeLifecycleRetryExhaustionBackoff drives the retry loop: a
// retryable failure with retries remaining probes the queue (backoff when
// empty), the retried attempt starts and settles, and an exhausted retry
// budget ends with the terminal sentinel.
func TestRuntimeLifecycleRetryExhaustionBackoff(t *testing.T) {
	st := lifecycleState{exec: execStarting, class: directAttemptClass}
	// Startup failure before any activity: preparing cleared, startFailed
	// returns to idle with no settle.
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptPreparing, class: directAttemptClass})
	st, _, eff := applyOK(t, st, lifecycleSignal{kind: signalAttemptStartFailed, class: directAttemptClass})
	if len(eff) != 0 {
		t.Fatalf("startFailed effects = %v, want none", eff)
	}

	// Retryable failure with retries remaining: retry probe, no prompt, live
	// context → backoff.
	st, cont, eff := applyOK(t, st, lifecycleSignal{
		kind: signalAttemptOutcome, class: directAttemptClass,
		runKind: runKindOrdinaryDirect, exitCode: 1,
		stopReason: "error", sentinelReason: "error",
		retryable: true, retriesRemaining: true,
	})
	if cont.kind != contProbeRetryPrompt {
		t.Fatalf("continuation = %d, want %d", cont.kind, contProbeRetryPrompt)
	}
	if len(eff) != 0 {
		t.Fatalf("retry probe outcome effects = %v, want none", eff)
	}
	st, cont, _ = applyOK(t, st, lifecycleSignal{
		kind: signalPromptContinuation, class: directAttemptClass,
		probeKind: probeRetryPrompt, promptConsumed: false,
	})
	if cont.kind != contBackoff {
		t.Fatalf("continuation = %d, want %d", cont.kind, contBackoff)
	}

	// Retried attempt runs and settles.
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptPreparing, class: directAttemptClass})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptStarted, class: directAttemptClass})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptSettled, class: directAttemptClass})

	// Retries exhausted: terminal, no prompt, sentinel, end.
	out, cont, eff := applyOK(t, st, lifecycleSignal{
		kind: signalAttemptOutcome, class: directAttemptClass,
		runKind: runKindOrdinaryDirect, exitCode: 1,
		stopReason: "error", sentinelReason: "error", resultSessionID: "s1",
		retryable: true, retriesRemaining: false,
	})
	if cont.kind != contProbeTerminalPrompt {
		t.Fatalf("exhausted continuation = %d, want %d", cont.kind, contProbeTerminalPrompt)
	}
	if want := []effectKind{effectRecordOutcomeExit}; !reflect.DeepEqual(effectKinds(eff), want) {
		t.Fatalf("exhausted outcome effects = %v, want %v", effectKinds(eff), want)
	}
	// Terminal probe: no prompt, live context → end, sentinel still written.
	st = out
	st, cont, eff = applyOK(t, st, lifecycleSignal{
		kind: signalPromptContinuation, class: directAttemptClass,
		probeKind: probeTerminalPrompt, promptConsumed: false,
		exitCode: 1, sentinelReason: "error", resultSessionID: "s1",
	})
	if cont.kind != contEnd {
		t.Fatalf("terminal probe continuation = %d, want %d", cont.kind, contEnd)
	}
	if want := []effectKind{effectWriteSentinel}; !reflect.DeepEqual(effectKinds(eff), want) {
		t.Fatalf("terminal probe effects = %v, want %v", effectKinds(eff), want)
	}

	// Teardown.
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalShutdownRequested})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalWriterCloseBegun})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalWriterCloseFinished, closeCalled: true})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalRunEnded})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalCompletionPublished, boundary: afterRunTeardown})
}

// TestRuntimeLifecycleTerminalProbeContextTiming covers the terminal probe's
// context sampling order: the context is sampled inside the probe after the
// exit was recorded; a dead context ends without dequeuing, and the sentinel
// is written regardless of consumed status.
func TestRuntimeLifecycleTerminalProbeContextTiming(t *testing.T) {
	for _, tc := range []struct {
		name     string
		consumed bool
		ctxErr   error
		wantCont continuationKind
	}{
		{"live and consumed resumes", true, nil, contResumeQueued},
		{"live but queue empty ends", false, nil, contEnd},
		{"dead context ends", false, context.Canceled, contEnd},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := idleDirect()
			st, _, _ = applyOK(t, st, lifecycleSignal{
				kind: signalAttemptOutcome, class: directAttemptClass,
				runKind: runKindOrdinaryDirect, exitCode: 1,
				stopReason: "error", sentinelReason: "error", resultSessionID: "s1",
				retryable: false, retriesRemaining: false,
			})
			st, cont, eff := applyOK(t, st, lifecycleSignal{
				kind: signalPromptContinuation, class: directAttemptClass,
				probeKind: probeTerminalPrompt, promptConsumed: tc.consumed,
				terminalContextErr: tc.ctxErr,
				exitCode:           1, sentinelReason: "error", resultSessionID: "s1",
			})
			if cont.kind != tc.wantCont {
				t.Fatalf("continuation = %d, want %d", cont.kind, tc.wantCont)
			}
			if want := []effectKind{effectWriteSentinel}; !reflect.DeepEqual(effectKinds(eff), want) {
				t.Fatalf("terminal probe effects = %v, want sentinel always", effectKinds(eff))
			}
		})
	}
}

// TestRuntimeLifecycleRetryProbeContextTiming covers the retry probe's
// dequeue-before-context order: a consumed prompt with a live context resumes
// (no sentinel), a consumed prompt with a dead context is discarded and the
// idle-context end runs immediately, and an empty queue with a live context
// backs off.
func TestRuntimeLifecycleRetryProbeContextTiming(t *testing.T) {
	for _, tc := range []struct {
		name      string
		consumed  bool
		resumeErr error
		idleErr   error
		wantCont  continuationKind
		wantEff   []effectKind
	}{
		{"consumed and live resumes without sentinel", true, nil, nil, contRetry, nil},
		{"consumed but context dead ends idle", true, context.Canceled, nil, contIdleContextEnd, nil},
		{"not consumed and dead ends idle", false, nil, context.DeadlineExceeded, contIdleContextEnd, nil},
		{"not consumed and live backs off", false, nil, nil, contBackoff, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := idleDirect()
			// The retry probe is only reachable after attemptOutcome selected it.
			st, _, _ = applyOK(t, st, lifecycleSignal{
				kind: signalAttemptOutcome, class: directAttemptClass,
				runKind: runKindOrdinaryDirect, exitCode: 1,
				stopReason: "error", sentinelReason: "error",
				retryable: true, retriesRemaining: true,
			})
			st, cont, eff := applyOK(t, st, lifecycleSignal{
				kind: signalPromptContinuation, class: directAttemptClass,
				probeKind:        probeRetryPrompt,
				promptConsumed:   tc.consumed,
				resumeContextErr: tc.resumeErr,
				idleContextErr:   tc.idleErr,
			})
			if cont.kind != tc.wantCont {
				t.Fatalf("continuation = %d, want %d", cont.kind, tc.wantCont)
			}
			if got := effectKinds(eff); len(got) != len(tc.wantEff) {
				t.Fatalf("retry probe effects = %v, want %v", got, tc.wantEff)
			}
			// The retry probe never emits a result sentinel.
			for _, k := range effectKinds(eff) {
				if k == effectWriteSentinel {
					t.Fatal("retry probe emitted a sentinel")
				}
			}
		})
	}
}

// TestRuntimeLifecycleTwoStepContinuationProtocol binds the two-step
// continuation protocol (decision 5, inventory amendment 5): attemptOutcome
// arms pendingProbe with the probe it selects, the matching promptContinuation
// consumes it, a mismatched probe kind is rejected with unchanged state, a
// continuation without a pending probe is rejected, and a second attemptOutcome
// while a probe is pending is rejected.
func TestRuntimeLifecycleTwoStepContinuationProtocol(t *testing.T) {
	retryOutcome := lifecycleSignal{
		kind: signalAttemptOutcome, class: directAttemptClass,
		runKind: runKindOrdinaryDirect, exitCode: 1,
		stopReason: "error", sentinelReason: "error",
		retryable: true, retriesRemaining: true,
	}

	t.Run("matching retry pair accepted and clears the pending probe", func(t *testing.T) {
		st := idleDirect()
		st, cont, _ := applyOK(t, st, retryOutcome)
		if cont.kind != contProbeRetryPrompt || st.pendingProbe != probeRetryPrompt {
			t.Fatalf("outcome: continuation %d pendingProbe %d", cont.kind, st.pendingProbe)
		}
		st, cont, _ = applyOK(t, st, lifecycleSignal{
			kind: signalPromptContinuation, class: directAttemptClass,
			probeKind: probeRetryPrompt, promptConsumed: true,
		})
		if cont.kind != contRetry {
			t.Fatalf("continuation = %d, want %d", cont.kind, contRetry)
		}
		if st.pendingProbe != probeNone {
			t.Fatalf("pending probe not cleared: %d", st.pendingProbe)
		}
	})

	t.Run("matching terminal pair accepted and clears the pending probe", func(t *testing.T) {
		st := idleDirect()
		st, _, _ = applyOK(t, st, lifecycleSignal{
			kind: signalAttemptOutcome, class: directAttemptClass,
			runKind: runKindOrdinaryDirect, exitCode: 1,
			stopReason: "error", sentinelReason: "error", resultSessionID: "s1",
			retryable: false, retriesRemaining: false,
		})
		if st.pendingProbe != probeTerminalPrompt {
			t.Fatalf("pendingProbe = %d, want %d", st.pendingProbe, probeTerminalPrompt)
		}
		st, cont, _ := applyOK(t, st, lifecycleSignal{
			kind: signalPromptContinuation, class: directAttemptClass,
			probeKind: probeTerminalPrompt, promptConsumed: false,
			terminalContextErr: context.Canceled,
			exitCode:           1, sentinelReason: "error", resultSessionID: "s1",
		})
		if cont.kind != contEnd || st.pendingProbe != probeNone {
			t.Fatalf("continuation %d pendingProbe %d", cont.kind, st.pendingProbe)
		}
	})

	t.Run("mismatched probe kind rejected with unchanged state", func(t *testing.T) {
		st := idleDirect()
		st, _, _ = applyOK(t, st, retryOutcome)
		before := st
		expectErr(t, st, lifecycleSignal{
			kind: signalPromptContinuation, class: directAttemptClass,
			probeKind: probeTerminalPrompt, promptConsumed: true,
		})
		if st != before {
			t.Fatalf("rejected mismatch changed state: %+v", st)
		}
		// The pending probe survives the rejection: the matching continuation
		// is still accepted afterwards.
		st, cont, _ := applyOK(t, st, lifecycleSignal{
			kind: signalPromptContinuation, class: directAttemptClass,
			probeKind: probeRetryPrompt, promptConsumed: false,
		})
		if cont.kind != contBackoff || st.pendingProbe != probeNone {
			t.Fatalf("after mismatch: continuation %d pendingProbe %d", cont.kind, st.pendingProbe)
		}
	})

	t.Run("continuation without attemptOutcome rejected", func(t *testing.T) {
		expectErr(t, idleDirect(), lifecycleSignal{
			kind: signalPromptContinuation, class: directAttemptClass,
			probeKind: probeRetryPrompt,
		})
	})

	t.Run("attemptOutcome twice rejected while probe pending", func(t *testing.T) {
		st := idleDirect()
		st, _, _ = applyOK(t, st, retryOutcome)
		expectErr(t, st, retryOutcome)
		if st.pendingProbe != probeRetryPrompt {
			t.Fatalf("pending probe disturbed by rejected outcome: %d", st.pendingProbe)
		}
	})
}

// TestRuntimeLifecyclePendingProbeGuard covers the broadened pending-probe
// guard: while a probe is pending, the signals that prepare, start or park a
// new attempt are rejected with unchanged state, and validateLifecycleState
// enforces the pending-probe invariant (S/I with open completion).
func TestRuntimeLifecyclePendingProbeGuard(t *testing.T) {
	probes := []struct {
		name  string
		probe probeKind
	}{
		{"terminal probe pending", probeTerminalPrompt},
		{"retry probe pending", probeRetryPrompt},
	}
	blocked := []struct {
		name   string
		signal lifecycleSignal
	}{
		{"preparing", lifecycleSignal{kind: signalAttemptPreparing, class: directAttemptClass}},
		{"started", lifecycleSignal{kind: signalAttemptStarted, class: directAttemptClass}},
		{"park requested", lifecycleSignal{kind: signalParkRequested, class: directAttemptClass, runKind: runKindOrdinaryDirect}},
		{"idle context ended", lifecycleSignal{kind: signalIdleContextEnded}},
	}
	for _, p := range probes {
		for _, b := range blocked {
			t.Run(p.name+"/"+b.name, func(t *testing.T) {
				st := lifecycleState{exec: execIdle, class: directAttemptClass, pendingProbe: p.probe}
				expectErr(t, st, b.signal)
				if st.pendingProbe != p.probe {
					t.Fatalf("rejected signal changed pendingProbe: %d", st.pendingProbe)
				}
			})
		}
	}

	t.Run("pending probe invariant", func(t *testing.T) {
		// A pending probe requires S/I with open completion.
		invalid := []lifecycleState{
			{exec: execReserved, pendingProbe: probeRetryPrompt},
			{exec: execRunning, attempts: 1, class: directAttemptClass, pendingProbe: probeRetryPrompt},
			{exec: execParked, class: directAttemptClass, pendingProbe: probeRetryPrompt},
			{exec: execParkedEnding, class: directAttemptClass, pendingProbe: probeRetryPrompt},
			{exec: execIdle, completion: completionAdmissionClosed, class: directAttemptClass, pendingProbe: probeRetryPrompt},
			{exec: execEnded, completion: completionPublished, class: directAttemptClass, pendingProbe: probeRetryPrompt},
		}
		for i, st := range invalid {
			if err := validateLifecycleState(st); err == nil {
				t.Errorf("case %d: validateLifecycleState(%+v) expected error", i, st)
			}
			// next must reject it on input regardless of the signal.
			expectErr(t, st, lifecycleSignal{kind: signalShutdownRequested})
		}
		valid := []lifecycleState{
			{exec: execStarting, class: directAttemptClass, pendingProbe: probeTerminalPrompt},
			{exec: execIdle, class: directAttemptClass, pendingProbe: probeRetryPrompt},
		}
		for i, st := range valid {
			if err := validateLifecycleState(st); err != nil {
				t.Errorf("case %d: validateLifecycleState(%+v) = %v, want nil", i, st, err)
			}
		}
	})
}

// TestRuntimeLifecycleShutdownSlotAcquisition pins that a requested child
// shutdown does not gate parking or local-slot acquisition: the slot gate
// checks only the supervisor's shutdown (supervisor.go:1199-1216), so the
// park/slot path moves parked→idle with shutdown requested.
func TestRuntimeLifecycleShutdownSlotAcquisition(t *testing.T) {
	st := idleDirect()
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalShutdownRequested})
	if !st.isShuttingDown() {
		t.Fatal("shutdown must be requested before parking")
	}
	st, _, eff := applyOK(t, st, lifecycleSignal{kind: signalParkRequested, class: directAttemptClass, runKind: runKindOrdinaryDirect})
	if st.exec != execParked || !st.isShuttingDown() {
		t.Fatalf("park with shutdown requested: %+v", st)
	}
	if !effectsEqual(eff, []lifecycleEffect{{kind: effectSignalCapacity}}) {
		t.Fatalf("park effects = %v", eff)
	}
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalLocalSlotAcquired, class: directAttemptClass})
	if st.exec != execIdle || !st.isShuttingDown() {
		t.Fatalf("slot acquisition with shutdown requested: %+v", st)
	}
}

// TestRuntimeLifecycleConflictAndDeciderExclusions covers the branch
// exclusions: a session-ID conflict cannot resume a failed terminal turn, a
// decider run is terminal on every path (including decider+workflow
// classification, where decider wins), and none of them parks.
func TestRuntimeLifecycleConflictAndDeciderExclusions(t *testing.T) {
	cases := []struct {
		name     string
		runKind  runKind
		conflict bool
	}{
		{"session conflict", runKindOrdinaryDirect, true},
		// Decider wins when decider and workflow classifications coincide; the
		// caller resolves that before runKind reaches the machine, so a single
		// decider row covers both (the former duplicate row is removed).
		{"decider", runKindDecider, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Terminal failure: decider/conflict excludes the terminal probe.
			st := idleDirect()
			st, cont, eff := applyOK(t, st, lifecycleSignal{
				kind: signalAttemptOutcome, class: directAttemptClass,
				runKind: tc.runKind, exitCode: 1,
				stopReason: "session_id_conflict", sentinelReason: "session_id_conflict",
				resultSessionID:   "s1",
				sessionIDConflict: tc.conflict, retryable: false,
			})
			if cont.kind != contEnd {
				t.Fatalf("continuation = %d, want %d", cont.kind, contEnd)
			}
			if want := []effectKind{effectRecordOutcomeExit, effectWriteSentinel}; !reflect.DeepEqual(effectKinds(eff), want) {
				t.Fatalf("effects = %v, want %v", effectKinds(eff), want)
			}
			// A retryable failure with retries remaining reaches the record-and-
			// return path only for a decider run; a session-ID conflict is never
			// retryable (the caller computes retryable), so that row is omitted
			// here.
			if tc.runKind == runKindDecider {
				st, cont, eff = applyOK(t, st, lifecycleSignal{
					kind: signalAttemptOutcome, class: directAttemptClass,
					runKind: tc.runKind, exitCode: 1,
					stopReason: "error", sentinelReason: "error", resultSessionID: "s1",
					retryable: true, retriesRemaining: true,
				})
				if cont.kind != contEnd {
					t.Fatalf("retryable decider continuation = %d, want %d", cont.kind, contEnd)
				}
				if want := []effectKind{effectRecordOutcomeExit, effectWriteSentinel}; !reflect.DeepEqual(effectKinds(eff), want) {
					t.Fatalf("retryable decider effects = %v, want %v", effectKinds(eff), want)
				}
			}
			// A decider's successful end_turn is terminal too: never parks.
			if tc.runKind == runKindDecider {
				st, cont, _ = applyOK(t, st, lifecycleSignal{
					kind: signalAttemptOutcome, class: directAttemptClass,
					runKind: tc.runKind, exitCode: 0,
					sentinelReason: "end_turn", resultSessionID: "s1",
				})
				if cont.kind != contEnd {
					t.Fatalf("decider success continuation = %d, want %d", cont.kind, contEnd)
				}
			}
		})
	}
}

// TestRuntimeLifecycleParallelPreparation drives 0→1→2→1→0→1→0 with a phase
// gap between the settled first phase and the next start, asserting settles
// happen only on →0 and finalization settles unconditionally.
func TestRuntimeLifecycleParallelPreparation(t *testing.T) {
	st := workflowState(execStarting, 0)
	// First phase.
	st, _, eff := applyOK(t, st, lifecycleSignal{kind: signalAttemptPreparing, class: workflowAttemptClass})
	if !effectsEqual(eff, []lifecycleEffect{{kind: effectClearPresentation}}) {
		t.Fatalf("preparing effects = %v", eff)
	}
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptStarted, class: workflowAttemptClass})
	if st.attempts != 1 || !st.isActive() {
		t.Fatalf("after first start: %+v", st)
	}
	// A second phase prepares while the first is still running.
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptPreparing, class: workflowAttemptClass})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptStarted, class: workflowAttemptClass})
	if st.attempts != 2 || !st.isActive() {
		t.Fatalf("after parallel start: %+v", st)
	}
	// First phase settles: count drops but presentation is kept and no settle.
	st, _, eff = applyOK(t, st, lifecycleSignal{kind: signalAttemptSettled, class: workflowAttemptClass})
	if st.attempts != 1 || st.exec != execRunning || len(eff) != 0 {
		t.Fatalf("first settle: %+v effects %v", st, eff)
	}
	// Phase gap: prepare then start the replacement phase.
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptPreparing, class: workflowAttemptClass})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptStarted, class: workflowAttemptClass})
	if st.attempts != 2 {
		t.Fatalf("after replacement start: %+v", st)
	}
	// Second settle: count drops, still running.
	st, _, eff = applyOK(t, st, lifecycleSignal{kind: signalAttemptSettled, class: workflowAttemptClass})
	if st.attempts != 1 || len(eff) != 0 {
		t.Fatalf("second settle: %+v effects %v", st, eff)
	}
	// Final settle: →0 clears presentation and settles waiters.
	st, _, eff = applyOK(t, st, lifecycleSignal{kind: signalAttemptSettled, class: workflowAttemptClass})
	if st.attempts != 0 || st.exec != execIdle {
		t.Fatalf("final settle: %+v", st)
	}
	if want := []effectKind{effectClearPresentation, effectSettleTurnWaiters}; !reflect.DeepEqual(effectKinds(eff), want) {
		t.Fatalf("final settle effects = %v, want %v", effectKinds(eff), want)
	}
	// A later phase starts and settles again (0→1→0), then the run finalizes:
	// repeat allowed, settles unconditionally, no presentation clear.
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptPreparing, class: workflowAttemptClass})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptStarted, class: workflowAttemptClass})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptSettled, class: workflowAttemptClass})
	st, _, eff = applyOK(t, st, lifecycleSignal{kind: signalWorkflowFinalized, class: workflowAttemptClass})
	if want := []effectKind{effectSettleTurnWaiters}; !reflect.DeepEqual(effectKinds(eff), want) {
		t.Fatalf("finalize effects = %v", effectKinds(eff))
	}
	if st.attempts != 0 || st.exec != execIdle {
		t.Fatalf("after finalize: %+v", st)
	}
}

// TestRuntimeLifecycleAdmissionRejectionAfterShutdown covers attempt starts
// after shutdown (allowed; provider admission is a separate gate) and the
// rejected-admission path, which ends the started attempt without an extra
// startFailed/settled pair beyond the single settle.
func TestRuntimeLifecycleAdmissionRejectionAfterShutdown(t *testing.T) {
	st := workflowState(execStarting, 0)
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalShutdownRequested, class: workflowAttemptClass})
	// A closing writer does not block attempt bookkeeping either.
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalWriterCloseBegun, class: workflowAttemptClass})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptPreparing, class: workflowAttemptClass})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptStarted, class: workflowAttemptClass})
	if !st.isActive() || !st.isShuttingDown() {
		t.Fatalf("started after shutdown: %+v", st)
	}
	// Provider admission rejects: the started attempt is settled (count →0).
	st, _, eff := applyOK(t, st, lifecycleSignal{kind: signalAttemptSettled, class: workflowAttemptClass})
	if st.attempts != 0 || st.exec != execIdle {
		t.Fatalf("rejected admission settle: %+v", st)
	}
	if want := []effectKind{effectClearPresentation, effectSettleTurnWaiters}; !reflect.DeepEqual(effectKinds(eff), want) {
		t.Fatalf("rejected admission settle effects = %v, want %v", effectKinds(eff), want)
	}
	// The rejected-admission path uses the single settle; the caller never
	// sends an additional startFailed/settled pair for the same attempt. The
	// machine still accepts startFailed from idle for the pre-activity direct
	// failure placement, so no machine-level rejection is asserted here.
}

// TestRuntimeLifecycleWriterCloseStages covers the nil-sink close (begun
// only, stays closing, no finished signal), repeated begun after closed, and
// the open→closed jump rejection.
func TestRuntimeLifecycleWriterCloseStages(t *testing.T) {
	t.Run("nil sink stays closing", func(t *testing.T) {
		st := idleDirect()
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalWriterCloseBegun})
		if !st.isWriterClosing() || st.isWriterClosed() {
			t.Fatalf("after begun: writer=%d", st.writer)
		}
		// No signal when the sink is nil: the caller's guard never sends the
		// finished signal here, and the machine's payload rule (closeCalled)
		// is exercised in the invalid-source table. Teardown can still publish
		// with a closing writer (sink nil).
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalShutdownRequested})
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalRunEnded})
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalCompletionPublished, boundary: afterRunTeardown})
		if st.writer != writerClosing {
			t.Fatalf("writer after nil-sink publication: %d", st.writer)
		}
	})
	t.Run("close called marks closed and blocks repeats", func(t *testing.T) {
		st := idleDirect()
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalWriterCloseBegun})
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalWriterCloseFinished, closeCalled: true})
		if !st.isWriterClosed() {
			t.Fatalf("after finished: writer=%d", st.writer)
		}
		// Repeated begun succeeds without regression...
		out, _, _ := applyOK(t, st, lifecycleSignal{kind: signalWriterCloseBegun})
		if out.writer != writerClosed {
			t.Fatalf("begun after closed: writer=%d", out.writer)
		}
		// ...and a repeated finished is rejected (the caller's closed guard
		// avoids sending it).
		expectErr(t, st, lifecycleSignal{kind: signalWriterCloseFinished, closeCalled: true})
		// The open→closed jump is rejected without a begun transition.
		expectErr(t, idleDirect(), lifecycleSignal{kind: signalWriterCloseFinished, closeCalled: true})
	})
}

// TestRuntimeLifecycleRollback covers spawn rollback from reserved and from a
// starting child: the cleanup signal emits the publication cue with the state
// unchanged, and the publication — valid only from V/S with completion open,
// with writer open and no shutdown — is what ends the child
// (supervisor.go:1475-1480).
func TestRuntimeLifecycleRollback(t *testing.T) {
	for _, tc := range []struct {
		name  string
		start lifecycleState
	}{
		{"from reserved", lifecycleState{}},
		{"from starting", lifecycleState{exec: execStarting}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := tc.start
			st, _, eff := applyOK(t, st, lifecycleSignal{kind: signalSpawnRolledBack, boundary: afterSpawnRollback})
			if st != tc.start {
				t.Fatalf("rollback changed state: %+v", st)
			}
			if !effectsEqual(eff, []lifecycleEffect{{kind: effectPublishCompletion, boundary: afterSpawnRollback}}) {
				t.Fatalf("rollback effects = %v", eff)
			}
			// Publication runs while the child is still V/S with completion
			// open, with writer open and shutdown none, and normalizes exec to
			// ended.
			st, _, eff = applyOK(t, st, lifecycleSignal{kind: signalCompletionPublished, boundary: afterSpawnRollback})
			if want := []effectKind{effectSettleTurnWaiters, effectCloseDone}; !reflect.DeepEqual(effectKinds(eff), want) {
				t.Fatalf("publication effects = %v", effectKinds(eff))
			}
			if st.exec != execEnded || st.completion != completionPublished {
				t.Fatalf("final state: %+v", st)
			}
		})
	}
}

// TestRuntimeLifecycleIdleEndOverwrites asserts that repeated idle-context
// ends overwrite the exit/phase/reason payload each time and never clear
// parked.
func TestRuntimeLifecycleIdleEndOverwrites(t *testing.T) {
	st := lifecycleState{exec: execParked, class: directAttemptClass}
	for i, want := range []struct {
		exit   int
		reason string
	}{
		{130, "cancelled"},
		{124, "timeout"},
	} {
		out, _, eff := applyOK(t, st, lifecycleSignal{
			kind: signalIdleContextEnded, idleExitCode: want.exit, idleStopReason: want.reason,
		})
		if !out.isParked() {
			t.Fatalf("idle end %d cleared parked: %+v", i, out)
		}
		wantEff := []lifecycleEffect{
			{kind: effectSetIdleEndData, exitCode: want.exit, reason: want.reason},
			{kind: effectWriteSentinel, exitCode: want.exit, reason: want.reason},
			{kind: effectEmitSessionEnd, exitCode: want.exit, reason: want.reason},
		}
		if !effectsEqual(eff, wantEff) {
			t.Fatalf("idle end %d effects = %+v, want %+v", i, eff, wantEff)
		}
		st = out
	}
}

// TestRuntimeLifecycleLocalSlotFailure drives the verified local-slot failure
// trace: success → parked → dequeue → local-slot acquisition fails (no
// signal) → idle-context metadata → shutdown → writer close → runEnded at the
// complete boundary (still parked) → publication normalizes to ended. Parked
// must be preserved exactly until final publication, and the effective
// capacity exclusion (completed || parked) must hold at every step.
func TestRuntimeLifecycleLocalSlotFailure(t *testing.T) {
	st := lifecycleState{exec: execStarting, class: directAttemptClass}
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptPreparing, class: directAttemptClass})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptStarted, class: directAttemptClass})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptSettled, class: directAttemptClass})
	st, _, _ = applyOK(t, st, lifecycleSignal{
		kind: signalAttemptOutcome, class: directAttemptClass,
		runKind: runKindOrdinaryDirect, exitCode: 0, sentinelReason: "end_turn",
	})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalSuccessPresented, class: directAttemptClass, exitCode: 0})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalParkRequested, class: directAttemptClass, runKind: runKindOrdinaryDirect})
	if !st.isParked() {
		t.Fatal("child must be parked before the resume attempt")
	}

	// A queued prompt is dequeued; the local-slot acquisition then fails.
	// Failure sends no parkWaitEnded and does not clear parked.
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalPromptDequeued, class: directAttemptClass})
	if !st.isParked() {
		t.Fatal("dequeue must not clear parked")
	}
	// No localSlotAcquired: slot acquisition failed. Optional idle-context
	// metadata while still parked.
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalIdleContextEnded, idleExitCode: 130, idleStopReason: "cancelled"})
	if !st.isParked() {
		t.Fatal("idle-context end must not clear parked")
	}

	// Teardown: shutdown and writer close, still parked.
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalShutdownRequested})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalWriterCloseBegun})
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalWriterCloseFinished, closeCalled: true})
	if !st.isParked() {
		t.Fatal("child must stay parked through teardown")
	}
	if excluded := st.isCompleted() || st.isParked(); !excluded {
		t.Fatal("effective capacity must exclude the child before publication")
	}

	// runEnded at the complete boundary: parked-ending, still reporting parked.
	out, _, eff := applyOK(t, st, lifecycleSignal{kind: signalRunEnded})
	if !effectsEqual(eff, []lifecycleEffect{{kind: effectPublishCompletion, boundary: afterRunTeardown}}) {
		t.Fatalf("runEnded effects = %v", eff)
	}
	st = out
	if !st.isParked() {
		t.Fatal("parked must be preserved until final publication")
	}
	// Local-slot failure leaves completion open: publication still validates.
	st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalCompletionPublished, boundary: afterRunTeardown})
	if st.completion != completionPublished {
		t.Fatalf("completion after publication: %d", st.completion)
	}
	// After publication the scoped exception applies: exec normalized to ended,
	// and the capacity reader's completed||parked exclusion still holds.
	if st.exec != execEnded {
		t.Fatalf("exec after publication: %d", st.exec)
	}
	if excluded := st.isCompleted() || st.isParked(); !excluded {
		t.Fatal("effective capacity must exclude the child after publication")
	}
}

// TestRuntimeLifecycleSentinelBeforeSuccessPresentation pins the ordered
// interpreter contract at the success site: the outcome signal's effects are
// record-exit then sentinel, and the success presentation is emitted only by
// the separate successPresented signal at the old post-sentinel site.
func TestRuntimeLifecycleSentinelBeforeSuccessPresentation(t *testing.T) {
	st := idleDirect()
	st, _, eff := applyOK(t, st, lifecycleSignal{
		kind: signalAttemptOutcome, class: directAttemptClass,
		runKind: runKindOrdinaryDirect, exitCode: 0,
		sentinelReason: "end_turn", resultSessionID: "s1",
	})
	want := []effectKind{effectRecordOutcomeExit, effectWriteSentinel}
	if !reflect.DeepEqual(effectKinds(eff), want) {
		t.Fatalf("outcome effects = %v, want %v", effectKinds(eff), want)
	}
	for _, e := range eff {
		if e.kind == effectSetSuccessPresentation {
			t.Fatal("outcome must not present success; the separate signal does")
		}
	}
	st, _, eff = applyOK(t, st, lifecycleSignal{kind: signalSuccessPresented, class: directAttemptClass, exitCode: 0})
	if !effectsEqual(eff, []lifecycleEffect{{kind: effectSetSuccessPresentation}}) {
		t.Fatalf("successPresented effects = %v", eff)
	}
	// The presentation order is sentinel → success presentation; only the
	// separate signal moves the presentation.
}

// TestRuntimeLifecycleWorkflowDirectAndLoopTraces covers a workflow-direct
// success (ends instead of parking, direct attempt class) and a loop child
// trace (workflow class, phases, finalization) through publication.
func TestRuntimeLifecycleWorkflowDirectAndLoopTraces(t *testing.T) {
	t.Run("workflow direct success ends", func(t *testing.T) {
		// A workflow-direct run still uses the direct attempt class.
		st := lifecycleState{exec: execStarting, class: directAttemptClass}
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptPreparing, class: directAttemptClass})
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptStarted, class: directAttemptClass})
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptSettled, class: directAttemptClass})
		out, cont, _ := applyOK(t, st, lifecycleSignal{
			kind: signalAttemptOutcome, class: directAttemptClass,
			runKind: runKindWorkflowDirect, exitCode: 0, sentinelReason: "end_turn",
		})
		if cont.kind != contEnd {
			t.Fatalf("workflow-direct success continuation = %d, want %d", cont.kind, contEnd)
		}
		st = out
		// No park: the run ends through teardown.
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalShutdownRequested})
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalWriterCloseBegun})
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalWriterCloseFinished, closeCalled: true})
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalRunEnded})
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalCompletionPublished, boundary: afterRunTeardown})
	})

	t.Run("loop child phases and finalization", func(t *testing.T) {
		st := lifecycleState{exec: execStarting, class: workflowAttemptClass}
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptPreparing, class: workflowAttemptClass})
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptStarted, class: workflowAttemptClass})
		// Session end observed mid-attempt: phase done while active.
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalSessionEndObserved, class: workflowAttemptClass, stopReason: "end_turn"})
		if !st.isActive() {
			t.Fatal("session end must not settle the attempt")
		}
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalAttemptSettled, class: workflowAttemptClass})
		// Run error: exit recorded by the imperative path, then finalization.
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalWorkflowFinalized, class: workflowAttemptClass})
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalShutdownRequested, class: workflowAttemptClass})
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalWriterCloseBegun, class: workflowAttemptClass})
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalWriterCloseFinished, class: workflowAttemptClass, closeCalled: true})
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalRunEnded, class: workflowAttemptClass})
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalCompletionPublished, class: workflowAttemptClass, boundary: afterRunTeardown})
	})
}

// TestRuntimeLifecycleErrorCarriesSignalContext asserts errors are
// descriptive: they identify the signal and never reconstruct or recover.
func TestRuntimeLifecycleErrorCarriesSignalContext(t *testing.T) {
	_, _, _, err := next(lifecycleState{}, lifecycleSignal{kind: signalPromptDequeued})
	if err == nil {
		t.Fatal("expected error for promptDequeued outside parked")
	}
	if got := err.Error(); !strings.Contains(got, "prompt dequeued") {
		t.Fatalf("error %q does not identify the signal", got)
	}
	_, _, _, err = next(idleDirect(), lifecycleSignal{kind: signalKind(4242)})
	if err == nil {
		t.Fatal("expected error for unknown signal kind")
	}
}

// TestRuntimeLifecycleFaultSignal covers the fault fail-safe signal: it is
// legal from every exec state except a published completion, moves exec to
// the ended form teardown accepts, clears a pending probe, zeroes the
// attempts, settles the waiters, and leaves completion, shutdown and writer
// untouched so the teardown sequence still validates.
func TestRuntimeLifecycleFaultSignal(t *testing.T) {
	cases := []struct {
		name     string
		start    lifecycleState
		wantExec execState
	}{
		{"reserved", directState(execReserved), execEnded},
		{"starting", directState(execStarting), execEnded},
		{"idle", idleDirect(), execEnded},
		{"running", workflowState(execRunning, 1), execEnded},
		{"parked", directState(execParked), execParkedEnding},
		{"parked with admission closed", lifecycleState{exec: execParked, completion: completionAdmissionClosed, class: directAttemptClass}, execParkedEnding},
		{"parked ending", directState(execParkedEnding), execParkedEnding},
		{"ended", directState(execEnded), execEnded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, cont, eff := applyOK(t, tc.start, lifecycleSignal{kind: signalFault, class: tc.start.class})
			if out.exec != tc.wantExec {
				t.Fatalf("fault exec = %d, want %d", out.exec, tc.wantExec)
			}
			if out.attempts != 0 {
				t.Fatalf("fault attempts = %d, want 0", out.attempts)
			}
			if out.pendingProbe != probeNone {
				t.Fatalf("fault left probe %d pending", out.pendingProbe)
			}
			if out.completion != tc.start.completion || out.shutdown != tc.start.shutdown || out.writer != tc.start.writer {
				t.Fatalf("fault changed a protected dimension: %+v", out)
			}
			if cont.kind != contEnd {
				t.Fatalf("fault continuation = %d, want %d", cont.kind, contEnd)
			}
			if want := []lifecycleEffect{{kind: effectSettleTurnWaiters}}; !effectsEqual(eff, want) {
				t.Fatalf("fault effects = %v, want %v", eff, want)
			}
		})
	}

	t.Run("fault clears a pending probe", func(t *testing.T) {
		for _, probe := range []probeKind{probeTerminalPrompt, probeRetryPrompt} {
			for _, exec := range []execState{execStarting, execIdle} {
				st := lifecycleState{exec: exec, class: directAttemptClass, pendingProbe: probe}
				out, cont, eff := applyOK(t, st, lifecycleSignal{kind: signalFault, class: directAttemptClass})
				if out.exec != execEnded || out.pendingProbe != probeNone {
					t.Fatalf("fault from probe %d in exec %d: %+v", probe, exec, out)
				}
				if cont.kind != contEnd {
					t.Fatalf("fault continuation = %d, want %d", cont.kind, contEnd)
				}
				if want := []lifecycleEffect{{kind: effectSettleTurnWaiters}}; !effectsEqual(eff, want) {
					t.Fatalf("fault effects = %v, want %v", eff, want)
				}
			}
		}
	})

	t.Run("fault rejected after completion published", func(t *testing.T) {
		st := lifecycleState{exec: execEnded, completion: completionPublished}
		expectErr(t, st, lifecycleSignal{kind: signalFault})
	})

	t.Run("teardown sequence after a fault validates cleanly", func(t *testing.T) {
		// A failed promptContinuation leaves the probe pending; the fault
		// clears it and the teardown transactions all validate.
		st := lifecycleState{exec: execIdle, class: directAttemptClass, pendingProbe: probeRetryPrompt}
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalFault, class: directAttemptClass})
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalShutdownRequested, class: directAttemptClass})
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalWriterCloseBegun, class: directAttemptClass})
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalWriterCloseFinished, class: directAttemptClass, closeCalled: true})
		out, _, eff := applyOK(t, st, lifecycleSignal{kind: signalRunEnded, class: directAttemptClass})
		if len(eff) != 0 {
			t.Fatalf("runEnded after a fault must be an effect-free repeat, got %v", eff)
		}
		st = out
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalCompletionPublished, class: directAttemptClass, boundary: afterRunTeardown})
		if st.completion != completionPublished || st.exec != execEnded {
			t.Fatalf("publication after a fault: %+v", st)
		}
	})

	t.Run("parked teardown sequence after a fault validates cleanly", func(t *testing.T) {
		// A failed local-slot acquisition leaves the child parked; the fault
		// keeps the parked form until the publication normalizes it.
		st := directState(execParked)
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalFault, class: directAttemptClass})
		if st.exec != execParkedEnding {
			t.Fatalf("fault from parked: %d", st.exec)
		}
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalShutdownRequested, class: directAttemptClass})
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalWriterCloseBegun, class: directAttemptClass})
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalWriterCloseFinished, class: directAttemptClass, closeCalled: true})
		out, _, eff := applyOK(t, st, lifecycleSignal{kind: signalRunEnded, class: directAttemptClass})
		if len(eff) != 0 {
			t.Fatalf("runEnded after a fault must be an effect-free repeat, got %v", eff)
		}
		st = out
		st, _, _ = applyOK(t, st, lifecycleSignal{kind: signalCompletionPublished, class: directAttemptClass, boundary: afterRunTeardown})
		if st.completion != completionPublished || st.exec != execEnded {
			t.Fatalf("publication after a parked fault: %+v", st)
		}
	})
}
