package stable

// Pure childRuntime lifecycle machine.
//
// This file owns the closed lifecycle dimensions (exec, attempts, shutdown,
// writer, completion, attempt class). It also owns the signal/effect/
// continuation enums, the field-specific predicates, and the pure transition
// function next. The
// imperative interpreter (applyLifecycle in runtime_lifecycle_apply.go)
// commits its predictions and executes the effects at the supervisor's call
// sites.
//
// Invariants encoded here match the supervisor behavior they preserve:
//   - The park timeout marks admission closed before done closes. The
//     RuntimePrompt admission gate therefore rejects completed runtimes:
//     isCompleted() is true for both completionAdmissionClosed and
//     completionPublished.
//   - writerClosing stays true after writerClosed is set. Every admission gate
//     (beginChildProviderTurn, prepareChildPermissionClaim and
//     beginDirectPermissionAnswer) keys on writerClosing, so isWriterClosing()
//     is true for both writer stages.
//   - The local-slot failure window keeps the child parked until final
//     publication. A failed acquireLocalRuntimeSlot wait in runChild leaves
//     execParked. The complete boundary normalizes execParkedEnding to
//     execEnded only at completionPublished.

import (
	"errors"
	"fmt"
)

// execState is the execution dimension of the lifecycle. V (execReserved) is
// the pre-publication state; S/I/R/P/PE/E (starting/idle/running/parked/
// parked-ending/ended) are the transition matrix states below.
type execState int

const (
	execReserved execState = iota
	execStarting
	execRunning
	execIdle
	execParked
	execParkedEnding
	execEnded
)

func (e execState) valid() bool { return e >= execReserved && e <= execEnded }

// shutdownStage is the child-scoped shutdown dimension.
type shutdownStage int

const (
	shutdownNone shutdownStage = iota
	shutdownRequested
)

func (s shutdownStage) valid() bool { return s >= shutdownNone && s <= shutdownRequested }

// writerStage is the event-writer dimension.
type writerStage int

const (
	writerOpen writerStage = iota
	writerClosing
	writerClosed
)

func (w writerStage) valid() bool { return w >= writerOpen && w <= writerClosed }

// completionStage is the completion dimension: open (still admitting), admission
// closed (park timeout marked it completed; done not yet closed), or published
// (complete ran inside doneOnce).
type completionStage int

const (
	completionOpen completionStage = iota
	completionAdmissionClosed
	completionPublished
)

func (c completionStage) valid() bool { return c >= completionOpen && c <= completionPublished }

// attemptClass is the immutable classification set at child initialization:
// workflow class when either LoopFile or TeamFile is nonempty, otherwise
// direct. Workflow-direct runs still use the direct attempt class.
type attemptClass int

const (
	directAttemptClass attemptClass = iota
	workflowAttemptClass
)

func (a attemptClass) valid() bool { return a >= directAttemptClass && a <= workflowAttemptClass }

// runKind is the result-policy classification of a direct run: decider first,
// then workflow-direct when workflowID is nonempty, otherwise ordinary direct.
type runKind int

const (
	runKindOrdinaryDirect runKind = iota
	runKindWorkflowDirect
	runKindDecider
)

func (k runKind) valid() bool { return k >= runKindOrdinaryDirect && k <= runKindDecider }

// probeKind selects which branch protocol the promptContinuation signal
// reports on in the two-step continuation protocol.
type probeKind int

const (
	probeNone probeKind = iota
	probeTerminalPrompt
	probeRetryPrompt
)

func (p probeKind) valid() bool { return p >= probeTerminalPrompt && p <= probeRetryPrompt }

// completionBoundary attests which caller boundary the completion publication
// runs at.
type completionBoundary int

const (
	afterRunTeardown completionBoundary = iota
	afterSpawnRollback
)

func (b completionBoundary) valid() bool { return b >= afterRunTeardown && b <= afterSpawnRollback }

// lifecycleState is the closed lifecycle state struct. Dimensions outside it
// remain on the child. They cover phase/label presentation, exitCode,
// lastStopReason, prompt queues and channels, waiters, provider resources,
// output, permission state, and decider data. Effects write presentation and
// the direct-run exit data; the workflow runners write their exit code after
// finalization.
type lifecycleState struct {
	exec       execState
	attempts   int
	shutdown   shutdownStage
	writer     writerStage
	completion completionStage
	// class is immutable after initialization; no transition changes it.
	class attemptClass
	// pendingProbe guards the two-step continuation protocol:
	// attemptOutcome sets it to the probe it selects and the matching
	// promptContinuation clears it. It is an internal guard dimension, not a
	// lifecycle predicate. validateLifecycleState requires a pending probe to
	// sit in S/I with open completion. The signals blocked while it is set are
	// exactly signalAttemptPreparing, signalAttemptStarted,
	// signalParkRequested and signalIdleContextEnded. Those are the signals
	// that would prepare, start or park a new attempt, plus the idle-context
	// end. No attempt lifecycle therefore interleaves with the protocol.
	pendingProbe probeKind
}

// shutdownRequestedEffects returns the deferred effects a shutdownRequested
// transaction emits: the provider-cancel consideration on every request, plus
// the first-only parent context cancel. Both next() and the beginChildShutdown
// error path emit the same list.
func shutdownRequestedEffects(alreadyRequested bool) []lifecycleEffect {
	eff := []lifecycleEffect{{kind: effectConsiderProviderCancel}}
	if !alreadyRequested {
		eff = append(eff, lifecycleEffect{kind: effectCancelContext})
	}
	return eff
}

// Pointer-receiver, nonlocking predicates. Each reads only its named field;
// they are range checks, not equality checks.

func (l *lifecycleState) isActive() bool { return l.exec == execRunning }

// isParked recognizes both parked states: execParked and execParkedEnding.
// In the second state the child is still parked at the final completion
// boundary, after the local-slot failure window ran through teardown.
func (l *lifecycleState) isParked() bool {
	return l.exec == execParked || l.exec == execParkedEnding
}

// isCompleted is true for both completionAdmissionClosed and completionPublished.
// The park timeout sets completed before done closes, and the RuntimePrompt
// admission gate must reject in that window.
func (l *lifecycleState) isCompleted() bool {
	return l.completion == completionAdmissionClosed || l.completion == completionPublished
}

func (l *lifecycleState) isShuttingDown() bool { return l.shutdown == shutdownRequested }

// isWriterClosing is true for both writerClosing and writerClosed. Today
// writerClosing stays true after writerClosed is set, and every admission
// gate keys on writerClosing (beginChildProviderTurn,
// prepareChildPermissionClaim and beginDirectPermissionAnswer).
func (l *lifecycleState) isWriterClosing() bool {
	return l.writer == writerClosing || l.writer == writerClosed
}

// isWriterClosed is true only for writerClosed.
func (l *lifecycleState) isWriterClosed() bool { return l.writer == writerClosed }

// continuationKind enumerates the final continuations the two-step decision
// protocol (attemptOutcome then promptContinuation) can select. The selected
// prompt string stays in a caller-local variable, never in lifecycle state.
type continuationKind int

const (
	contNone continuationKind = iota
	// attemptOutcome continuations.
	contPark                // ordinary direct success: caller sends successPresented, then parkRequested
	contEnd                 // terminal: return through teardown
	contProbeTerminalPrompt // terminal probe: sample context, dequeue once
	contProbeRetryPrompt    // retry probe: dequeue before checking context
	// promptContinuation continuations.
	contResumeQueued   // terminal probe dequeued a prompt: resume with attempt=0
	contRetry          // retry probe consumed a prompt with live context: resume with attempt=0
	contBackoff        // retry probe found no prompt, context live: run the backoff timer
	contIdleContextEnd // context dead: caller runs the idle-context end, then returns
)

type continuation struct{ kind continuationKind }

// signalKind enumerates every lifecycle signal the matrix accepts. Matrix
// names are symbolic; Go constants carry the signal prefix to avoid
// collisions with state constants such as shutdownRequested and
// completionPublished.
type signalKind int

const (
	signalPublished signalKind = iota
	signalAttemptPreparing
	signalAttemptStarted
	signalAttemptStartFailed
	signalAttemptSettled
	signalWorkflowFinalized
	signalAttemptReturned
	signalAttemptOutcome
	signalPromptContinuation
	signalSuccessPresented
	signalParkRequested
	signalPromptDequeued
	signalLocalSlotAcquired
	signalParkTimedOut
	signalParkWaitEnded
	signalIdleContextEnded
	signalRunEnded
	signalSpawnRolledBack
	signalShutdownRequested
	signalWriterCloseBegun
	signalWriterCloseFinished
	signalCompletionPublished
	signalAgentStatusObserved
	signalSessionEndObserved
	signalFault
)

// lifecycleSignal carries immutable snapshots only — no resource pointers,
// channels or closures. Resources and closures are bound exclusively by the
// imperative interpreter.
type lifecycleSignal struct {
	kind signalKind

	// Immutable attempt classification, copied into every signal.
	class attemptClass

	// Attempt-outcome and continuation result data. stopReason
	// is the raw result stop reason, preserved for retry/conflict selection;
	// sentinelReason is the caller-normalized sentinel reason (stop-reason
	// fallback already applied). sessionIDConflict is computed by the caller
	// from the raw stop reason. resultSessionID is the session ID written into
	// a result sentinel. It is the attempt result's session ID for
	// attemptOutcome and promptContinuation, and the caller-resolved session
	// ID (read outside mu) for idleContextEnded.
	runKind           runKind
	exitCode          int
	stopReason        string
	sentinelReason    string
	resultSessionID   string
	sessionIDConflict bool
	retryable         bool
	retriesRemaining  bool

	// promptContinuation inputs. terminalContextErr is sampled inside the
	// terminal probe after the exit code is recorded and unlocked.
	// resumeContextErr is sampled only after an actual dequeue. The retry
	// probe dequeues before checking context.
	//
	// The retry probe's idle-context decision uses a single context sample.
	// The run loop reads ctx.Err() once after the dequeue attempt (the retry
	// probe's dequeue in runChild). A consumed prompt with a dead context is
	// therefore an idle-context end, and an empty queue with a dead context is
	// one too.
	// resumeContextErr and idleContextErr therefore describe the same single
	// sample; the caller must not resample between the dequeue and the
	// idle-end decision.
	probeKind          probeKind
	promptConsumed     bool // an actual successful dequeue, not queue nonemptiness
	terminalContextErr error
	resumeContextErr   error
	idleContextErr     error

	// agentStatusObserved snapshot, sampled under child.mu.
	phase string
	label string

	// idleContextEnded carries caller-normalized reason/exitCode computed with
	// the existing deadline/error mapping.
	idleExitCode   int
	idleStopReason string

	// fault carries the terminal writes the fault fail-safe replays through
	// the machine's locked effects. recordExit selects the recorded outcome
	// exit: attemptOutcome's effectRecordOutcomeExit, with the code in
	// exitCode. idleEnd selects the idle-end presentation:
	// signalIdleContextEnded's effectSetIdleEndData, with the data in
	// idleExitCode and idleStopReason. The bare fault sets neither.
	recordExit bool
	idleEnd    bool

	// writerCloseFinished: Close was actually called on a nonnil sink. No
	// signal is sent when the sink is nil; a Close error still counts as called.
	closeCalled bool

	// completionPublished attests the boundary it publishes at.
	boundary completionBoundary
}

// effectKind enumerates the effect descriptions next returns. These are
// descriptions, not operations: the interpreter in runtime_lifecycle_apply.go
// binds resources and executes them at the supervisor's call sites. Locked effects
// execute within applyLifecycle under mu; deferred ones return to the outer
// lock owner. The retained imperative orchestration is deliberately NOT
// modeled as effects; the interpreter keeps those at their exact sites. It
// covers writer drain and sink Close, provider closing/cancelAnswers,
// registry/provider/broker cleanup, rollback cancel/delete, and post-complete
// capacity signaling.
type effectKind int

const (
	effectClearPresentation         effectKind = iota // preparing; settled→0
	effectSetAgentPresentation                        // agentStatusObserved
	effectSetSuccessPresentation                      // successPresented
	effectSetSessionEndPresentation                   // sessionEndObserved
	effectSetIdleEndData                              // idleContextEnded; signalFault idle end
	effectRecordOutcomeExit                           // attemptOutcome terminal/decider cases; signalFault
	effectSettleTurnWaiters                           // settled→0; workflowFinalized; completionPublished
	effectReleaseTreeToken                            // attemptReturned
	effectWriteSentinel                               // attemptOutcome success/end/decider; promptContinuation terminal probe; idleContextEnded
	effectSignalCapacity                              // parkRequested
	effectEmitSessionEnd                              // idleContextEnded
	effectConsiderProviderCancel                      // shutdownRequested, including repeats
	effectCancelContext                               // first shutdownRequested only
	effectPublishCompletion                           // runEnded / spawnRolledBack
	effectCloseDone                                   // completionPublished first publication
)

// lifecycleEffect is a single effect description with its immutable payload.
// Only the fields named for a signal's effects are meaningful.
type lifecycleEffect struct {
	kind effectKind

	exitCode  int    // recordOutcomeExit, writeSentinel, setIdleEndData
	reason    string // writeSentinel (normalized), setIdleEndData; setSessionEndPresentation may be empty
	sessionID string // writeSentinel

	phase string // setAgentPresentation
	label string // setAgentPresentation

	boundary completionBoundary // publishCompletion
}

// validateLifecycleState enforces the cross-dimension contract. It rejects
// negative counts, running with count 0, and positive counts with any
// nonrunning exec. It rejects non-open completion with positive attempts and
// admission closed outside parked/idle/parked-ending/ended. It rejects
// published outside ended and reserved with non-open completion or nonzero
// attempts. It rejects direct attempt counts above one and a pending probe
// outside S/I with open completion. It
// deliberately does NOT reject running+shutdown, parked+shutdown,
// parked+writerClosed, or running+phase done. The first two are orthogonal;
// the local-slot failure explains the third. session.end precedes active
// cleanup for the fourth.
func validateLifecycleState(l lifecycleState) error {
	if !l.exec.valid() {
		return fmt.Errorf("lifecycle: invalid exec state %d", l.exec)
	}
	if !l.shutdown.valid() {
		return fmt.Errorf("lifecycle: invalid shutdown stage %d", l.shutdown)
	}
	if !l.writer.valid() {
		return fmt.Errorf("lifecycle: invalid writer stage %d", l.writer)
	}
	if !l.completion.valid() {
		return fmt.Errorf("lifecycle: invalid completion stage %d", l.completion)
	}
	if !l.class.valid() {
		return fmt.Errorf("lifecycle: invalid attempt class %d", l.class)
	}
	if l.attempts < 0 {
		return fmt.Errorf("lifecycle: negative attempt count %d", l.attempts)
	}
	if l.exec == execRunning && l.attempts == 0 {
		return errors.New("lifecycle: running with zero attempts")
	}
	if l.attempts > 0 && l.exec != execRunning {
		return fmt.Errorf("lifecycle: %d attempts in exec state %d", l.attempts, l.exec)
	}
	if l.completion != completionOpen && l.attempts > 0 {
		return fmt.Errorf("lifecycle: completion %d with %d attempts", l.completion, l.attempts)
	}
	switch l.completion {
	case completionAdmissionClosed:
		switch l.exec {
		case execParked, execIdle, execParkedEnding, execEnded:
		default:
			return fmt.Errorf("lifecycle: admission closed in exec state %d", l.exec)
		}
	case completionPublished:
		if l.exec != execEnded {
			return fmt.Errorf("lifecycle: published in exec state %d", l.exec)
		}
	}
	if l.exec == execReserved && (l.completion != completionOpen || l.attempts != 0) {
		return fmt.Errorf("lifecycle: reserved with completion %d, attempts %d", l.completion, l.attempts)
	}
	if l.class == directAttemptClass && l.attempts > 1 {
		return fmt.Errorf("lifecycle: direct runtime with %d attempts", l.attempts)
	}
	if l.pendingProbe != probeNone {
		if l.exec != execStarting && l.exec != execIdle {
			return fmt.Errorf("lifecycle: pending probe %d in exec state %d", l.pendingProbe, l.exec)
		}
		if l.completion != completionOpen {
			return fmt.Errorf("lifecycle: pending probe %d after completion %d", l.pendingProbe, l.completion)
		}
	}
	return nil
}

// requireOpen rejects signals that model attempt activity, admission or
// continuation. It rejects them once completion has left open or the run has
// reached a terminal exec state (cross-dimension contract).
func requireOpen(l lifecycleState, sig signalKind) error {
	if l.completion != completionOpen {
		return fmt.Errorf("lifecycle: signal %d after completion %d", sig, l.completion)
	}
	if l.exec == execEnded || l.exec == execParkedEnding {
		return fmt.Errorf("lifecycle: signal %d in exec state %d", sig, l.exec)
	}
	return nil
}

// validateLifecycleSignal rejects unknown signal, attempt-class, run-kind,
// probe-kind and boundary values, plus the per-signal payload rules.
func validateLifecycleSignal(sig lifecycleSignal) error {
	if sig.kind < signalPublished || sig.kind > signalFault {
		return fmt.Errorf("lifecycle: unknown signal kind %d", sig.kind)
	}
	if !sig.class.valid() {
		return fmt.Errorf("lifecycle: signal %d with invalid attempt class %d", sig.kind, sig.class)
	}
	switch sig.kind {
	case signalAttemptOutcome:
		if !sig.runKind.valid() {
			return fmt.Errorf("lifecycle: attemptOutcome with invalid run kind %d", sig.runKind)
		}
	case signalPromptContinuation:
		if !sig.probeKind.valid() {
			return fmt.Errorf("lifecycle: promptContinuation with invalid probe kind %d", sig.probeKind)
		}
	case signalSuccessPresented:
		if sig.exitCode != 0 {
			return fmt.Errorf("lifecycle: successPresented with exit code %d", sig.exitCode)
		}
	case signalParkRequested:
		if !sig.runKind.valid() || sig.runKind != runKindOrdinaryDirect {
			return fmt.Errorf("lifecycle: parkRequested with run kind %d", sig.runKind)
		}
	case signalWriterCloseFinished:
		if !sig.closeCalled {
			return errors.New("lifecycle: writerCloseFinished without a Close call on a nonnil sink")
		}
	case signalRunEnded:
		// runEnded emits publishCompletion at the teardown boundary; a wrong
		// attestation is rejected exactly like completionPublished's.
		if sig.boundary != afterRunTeardown {
			return fmt.Errorf("lifecycle: runEnded with boundary %d, want afterRunTeardown", sig.boundary)
		}
	case signalSpawnRolledBack:
		// Rollback publishes at the afterSpawnRollback boundary only.
		if sig.boundary != afterSpawnRollback {
			return fmt.Errorf("lifecycle: spawnRolledBack with boundary %d, want afterSpawnRollback", sig.boundary)
		}
	case signalCompletionPublished:
		if !sig.boundary.valid() {
			return fmt.Errorf("lifecycle: completionPublished with invalid boundary %d", sig.boundary)
		}
	}
	return nil
}

// next validates the input state and signal, applies the signal matrix, and
// validates the output state. It is pure: it never reads a child, clock,
// context, provider, channel or lock. An invalid input returns the unchanged
// input state, no continuation, no effects, and an error — without
// reconstruction or silent recovery.
func next(st lifecycleState, sig lifecycleSignal) (lifecycleState, continuation, []lifecycleEffect, error) {
	if err := validateLifecycleState(st); err != nil {
		return st, continuation{contNone}, nil, fmt.Errorf("lifecycle: signal %d on invalid state: %w", sig.kind, err)
	}
	if err := validateLifecycleSignal(sig); err != nil {
		return st, continuation{contNone}, nil, err
	}
	// The attempt class is immutable: a signal carrying a class that differs
	// from the state's class is rejected; every signal carries the class.
	if sig.class != st.class {
		return st, continuation{contNone}, nil, fmt.Errorf(
			"lifecycle: signal %d with attempt class %d on state class %d", sig.kind, sig.class, st.class)
	}
	// While a continuation probe is pending, the two-step protocol must
	// complete before another attempt lifecycle begins. The machine rejects
	// the signals that prepare, start or park a new attempt, and the
	// idle-context end. The probe sequence is synchronous inside runChild, so
	// no idle end can legitimately interleave. attemptStartFailed is not
	// blocked: it only aborts an already-prepared attempt, and preparing is
	// rejected while a probe is pending. It therefore cannot legitimately
	// occur mid-protocol.
	if st.pendingProbe != probeNone {
		switch sig.kind {
		case signalAttemptPreparing, signalAttemptStarted, signalParkRequested, signalIdleContextEnded:
			return st, continuation{contNone}, nil, fmt.Errorf(
				"lifecycle: signal %d while probe %d is pending", sig.kind, st.pendingProbe)
		}
	}

	out := st
	var eff []lifecycleEffect
	cont := continuation{contNone}

	switch sig.kind {
	case signalPublished:
		// Source V/C0/attempts 0; duplicate (non-reserved source) rejected.
		if st.exec != execReserved {
			return st, cont, nil, fmt.Errorf("lifecycle: published from exec state %d", st.exec)
		}
		out.exec = execStarting

	case signalAttemptPreparing:
		// Sources S/I, plus R for the workflow class (parallel preparation);
		// repeat allowed; clears phase+label even while another team attempt runs.
		if err := requireOpen(st, sig.kind); err != nil {
			return st, cont, nil, err
		}
		switch st.exec {
		case execStarting, execIdle:
		case execRunning:
			if sig.class != workflowAttemptClass {
				return st, cont, nil, fmt.Errorf("lifecycle: preparing in running state with class %d", sig.class)
			}
		default:
			return st, cont, nil, fmt.Errorf("lifecycle: preparing from exec state %d", st.exec)
		}
		eff = append(eff, lifecycleEffect{kind: effectClearPresentation})

	case signalAttemptStarted:
		// Sources S/I, plus R for the workflow class; attempts++ and exec R.
		// A direct parallel start is rejected; not idempotent. A requested
		// shutdown and a closing writer are both allowed (provider admission
		// is a separate gate).
		if err := requireOpen(st, sig.kind); err != nil {
			return st, cont, nil, err
		}
		switch st.exec {
		case execStarting, execIdle:
		case execRunning:
			if sig.class != workflowAttemptClass {
				return st, cont, nil, fmt.Errorf("lifecycle: direct parallel attempt start rejected")
			}
		default:
			return st, cont, nil, fmt.Errorf("lifecycle: attempt started from exec state %d", st.exec)
		}
		out.attempts++
		out.exec = execRunning

	case signalAttemptStartFailed:
		// Sources S/I, plus R for the workflow class; S→I, otherwise unchanged;
		// no count change; repeat allowed; no settle. Only for a prepared
		// attempt that never incremented activity.
		if err := requireOpen(st, sig.kind); err != nil {
			return st, cont, nil, err
		}
		switch st.exec {
		case execStarting, execIdle:
		case execRunning:
			if sig.class != workflowAttemptClass {
				return st, cont, nil, fmt.Errorf("lifecycle: start failed in running state with class %d", sig.class)
			}
		default:
			return st, cont, nil, fmt.Errorf("lifecycle: start failed from exec state %d", st.exec)
		}
		if st.exec == execStarting {
			out.exec = execIdle
		}

	case signalAttemptSettled:
		// Source R/C0/attempts>0; direct requires count 1. Extra settles are
		// rejected. Phase+label clear and waiter settle happen only on →0.
		if err := requireOpen(st, sig.kind); err != nil {
			return st, cont, nil, err
		}
		if st.exec != execRunning || st.attempts <= 0 {
			return st, cont, nil, fmt.Errorf("lifecycle: settled from exec %d with %d attempts", st.exec, st.attempts)
		}
		if sig.class == directAttemptClass && st.attempts != 1 {
			return st, cont, nil, fmt.Errorf("lifecycle: direct settle with %d attempts", st.attempts)
		}
		out.attempts--
		if out.attempts == 0 {
			out.exec = execIdle
			eff = append(eff,
				lifecycleEffect{kind: effectClearPresentation},
				lifecycleEffect{kind: effectSettleTurnWaiters},
			)
		}

	case signalWorkflowFinalized:
		// Source S/I/R, C0, workflow class; repeat allowed; settles waiters
		// unconditionally and never clears presentation.
		if err := requireOpen(st, sig.kind); err != nil {
			return st, cont, nil, err
		}
		if sig.class != workflowAttemptClass {
			return st, cont, nil, fmt.Errorf("lifecycle: workflow finalized with class %d", sig.class)
		}
		switch st.exec {
		case execStarting, execIdle, execRunning:
		default:
			return st, cont, nil, fmt.Errorf("lifecycle: finalized from exec state %d", st.exec)
		}
		out.attempts = 0
		out.exec = execIdle
		eff = append(eff, lifecycleEffect{kind: effectSettleTurnWaiters})
		// The caller writes child.exitCode after finalization returns; the
		// machine never records a workflowFinalized exit code.

	case signalAttemptReturned:
		// Source S/I, C0, direct class; no dimension change; releases the tree
		// token at the old post-attempt boundary. Repeated release is safe but
		// the caller emits the signal once per returned attempt.
		if err := requireOpen(st, sig.kind); err != nil {
			return st, cont, nil, err
		}
		if sig.class != directAttemptClass {
			return st, cont, nil, fmt.Errorf("lifecycle: attempt returned with class %d", sig.class)
		}
		if st.exec != execStarting && st.exec != execIdle {
			return st, cont, nil, fmt.Errorf("lifecycle: attempt returned from exec state %d", st.exec)
		}
		eff = append(eff, lifecycleEffect{kind: effectReleaseTreeToken})

	case signalAttemptOutcome:
		// Source S/I, C0, direct class, no probe pending; result policy. Never
		// changes presentation or ends execution by itself.
		if err := requireOpen(st, sig.kind); err != nil {
			return st, cont, nil, err
		}
		if sig.class != directAttemptClass {
			return st, cont, nil, fmt.Errorf("lifecycle: attempt outcome with class %d", sig.class)
		}
		if st.exec != execStarting && st.exec != execIdle {
			return st, cont, nil, fmt.Errorf("lifecycle: attempt outcome from exec state %d", st.exec)
		}
		if st.pendingProbe != probeNone {
			return st, cont, nil, fmt.Errorf("lifecycle: attempt outcome with probe %d still pending", st.pendingProbe)
		}
		record := lifecycleEffect{kind: effectRecordOutcomeExit, exitCode: sig.exitCode}
		sentinel := lifecycleEffect{
			kind:      effectWriteSentinel,
			exitCode:  sig.exitCode,
			reason:    sig.sentinelReason,
			sessionID: sig.resultSessionID,
		}
		switch {
		case !sig.retryable || !sig.retriesRemaining:
			// Nonretryable or retries exhausted.
			if sig.exitCode == 0 {
				// Success: record exit, sentinel; the separate successPresented
				// signal fires at the old post-sentinel site. Ordinary direct
				// parks; workflow-direct and decider end. No prompt probe.
				eff = append(eff, record, sentinel)
				if sig.runKind == runKindOrdinaryDirect {
					cont = continuation{contPark}
				} else {
					cont = continuation{contEnd}
				}
			} else if sig.runKind == runKindDecider || sig.sessionIDConflict {
				// A decider's failed turn is terminal; a session-ID conflict is
				// not resumable. Record exit, write sentinel, return.
				eff = append(eff, record, sentinel)
				cont = continuation{contEnd}
			} else {
				// Terminal probe selected: record exit first; the terminal
				// probe's sentinel is emitted by promptContinuation.
				eff = append(eff, record)
				cont = continuation{contProbeTerminalPrompt}
				out.pendingProbe = probeTerminalPrompt
			}
		case sig.runKind == runKindDecider:
			// Retryable with retries remaining, decider: record exit, write
			// sentinel with stop-reason fallback, return. No resumeID update,
			// dequeue or backoff.
			eff = append(eff, record, sentinel)
			cont = continuation{contEnd}
		default:
			// Retryable with retries remaining, not decider: retry probe. The
			// caller first updates resumeID only if result.sessionID is
			// nonempty, then dequeues before checking context.
			cont = continuation{contProbeRetryPrompt}
			out.pendingProbe = probeRetryPrompt
		}

	case signalPromptContinuation:
		// Source S/I, C0, direct class; final continuation of the two-step
		// protocol. The signal's probe kind must match the probe attemptOutcome
		// left pending, and some probe must be pending at all.
		if err := requireOpen(st, sig.kind); err != nil {
			return st, cont, nil, err
		}
		if sig.class != directAttemptClass {
			return st, cont, nil, fmt.Errorf("lifecycle: prompt continuation with class %d", sig.class)
		}
		if st.exec != execStarting && st.exec != execIdle {
			return st, cont, nil, fmt.Errorf("lifecycle: prompt continuation from exec state %d", st.exec)
		}
		if st.pendingProbe == probeNone {
			return st, cont, nil, errors.New("lifecycle: prompt continuation without a pending probe")
		}
		if sig.probeKind != st.pendingProbe {
			return st, cont, nil, fmt.Errorf(
				"lifecycle: prompt continuation with probe %d while probe %d is pending", sig.probeKind, st.pendingProbe)
		}
		sentinel := lifecycleEffect{
			kind:      effectWriteSentinel,
			exitCode:  sig.exitCode,
			reason:    sig.sentinelReason,
			sessionID: sig.resultSessionID,
		}
		switch sig.probeKind {
		case probeTerminalPrompt:
			// The terminal probe emits the result sentinel regardless of
			// consumed status. Resume only when the context was live and the
			// dequeue succeeded.
			eff = append(eff, sentinel)
			if sig.terminalContextErr == nil && sig.promptConsumed {
				cont = continuation{contResumeQueued}
			} else {
				cont = continuation{contEnd}
			}
		case probeRetryPrompt:
			// The retry probe dequeued before checking context. If a prompt
			// was consumed and the context is live, resume (no sentinel). If
			// resuming is off and the context is dead — even with a consumed
			// and discarded prompt — the idle-context end runs immediately.
			switch {
			case sig.promptConsumed && sig.resumeContextErr == nil:
				cont = continuation{contRetry}
			case sig.promptConsumed && sig.resumeContextErr != nil:
				cont = continuation{contIdleContextEnd}
			case sig.idleContextErr != nil:
				cont = continuation{contIdleContextEnd}
			default:
				cont = continuation{contBackoff}
			}
		}
		// The matching continuation consumed the pending probe.
		out.pendingProbe = probeNone

	case signalSuccessPresented:
		// Source S/I, C0, exit 0, after the sentinel; repeat sets phase done
		// and never clears the label.
		if err := requireOpen(st, sig.kind); err != nil {
			return st, cont, nil, err
		}
		if st.exec != execStarting && st.exec != execIdle {
			return st, cont, nil, fmt.Errorf("lifecycle: success presented from exec state %d", st.exec)
		}
		eff = append(eff, lifecycleEffect{kind: effectSetSuccessPresentation})

	case signalParkRequested:
		// Source S/I, C0, ordinary direct success; duplicate rejected. Only
		// runChild parks (its contPark continuation); loop/team children never park,
		// so a workflow-class park signal is rejected.
		if sig.class != directAttemptClass {
			return st, cont, nil, fmt.Errorf("lifecycle: park requested with class %d", sig.class)
		}
		if err := requireOpen(st, sig.kind); err != nil {
			return st, cont, nil, err
		}
		if st.exec != execStarting && st.exec != execIdle {
			return st, cont, nil, fmt.Errorf("lifecycle: park requested from exec state %d", st.exec)
		}
		out.exec = execParked
		eff = append(eff, lifecycleEffect{kind: effectSignalCapacity})

	case signalPromptDequeued:
		// Source P, C0; observational repeat with no activity or admission
		// effect (continuation probes use their own signals).
		if err := requireOpen(st, sig.kind); err != nil {
			return st, cont, nil, err
		}
		if st.exec != execParked {
			return st, cont, nil, fmt.Errorf("lifecycle: prompt dequeued from exec state %d", st.exec)
		}

	case signalLocalSlotAcquired:
		// Source P, C0; duplicate rejected; no active attempt yet.
		if err := requireOpen(st, sig.kind); err != nil {
			return st, cont, nil, err
		}
		if st.exec != execParked {
			return st, cont, nil, fmt.Errorf("lifecycle: local slot acquired from exec state %d", st.exec)
		}
		out.exec = execIdle

	case signalParkTimedOut:
		// Source P, C0, queue empty observed under the same mu; duplicate
		// rejected; no waiter settle, no done close.
		if err := requireOpen(st, sig.kind); err != nil {
			return st, cont, nil, err
		}
		if st.exec != execParked {
			return st, cont, nil, fmt.Errorf("lifecycle: park timed out from exec state %d", st.exec)
		}
		out.completion = completionAdmissionClosed

	case signalParkWaitEnded:
		// Source P, C0 or CA; only the not-ok wait path, never the local-slot
		// failure. Duplicate rejected.
		if st.completion == completionPublished {
			return st, cont, nil, fmt.Errorf("lifecycle: park wait ended after completion %d", st.completion)
		}
		if st.exec != execParked {
			return st, cont, nil, fmt.Errorf("lifecycle: park wait ended from exec state %d", st.exec)
		}
		out.exec = execIdle

	case signalIdleContextEnded:
		// Source S/I/P, C0 or CA; repeat overwrites exit/phase/reason, emits
		// sentinel then session.end; never clears parked or settles waiters.
		if st.completion == completionPublished {
			return st, cont, nil, fmt.Errorf("lifecycle: idle context ended after completion %d", st.completion)
		}
		switch st.exec {
		case execStarting, execIdle, execParked:
		default:
			return st, cont, nil, fmt.Errorf("lifecycle: idle context ended from exec state %d", st.exec)
		}
		eff = append(eff,
			lifecycleEffect{kind: effectSetIdleEndData, exitCode: sig.idleExitCode, reason: sig.idleStopReason},
			lifecycleEffect{kind: effectWriteSentinel, exitCode: sig.idleExitCode, reason: sig.idleStopReason, sessionID: sig.resultSessionID},
			lifecycleEffect{kind: effectEmitSessionEnd, exitCode: sig.idleExitCode, reason: sig.idleStopReason},
		)

	case signalRunEnded:
		// Source S/I/P, C0 or CA, attempts 0, at the end of teardown. Every
		// transition emits the publication cue at the afterRunTeardown
		// boundary; the interpreter's doneOnce and the completionPublished
		// repeat no-op keep the publication idempotent. Panic recovery runs
		// through this boundary too.
		//
		// runEnded deliberately does not read the writer dimension. Unlike
		// completionPublished, publication readiness is attested at the
		// completionPublished transaction, not here. A rollback child publishes
		// with the writer still open.
		if st.completion != completionOpen && st.completion != completionAdmissionClosed {
			return st, cont, nil, fmt.Errorf("lifecycle: run ended after completion %d", st.completion)
		}
		if st.attempts != 0 {
			return st, cont, nil, fmt.Errorf("lifecycle: run ended with %d attempts", st.attempts)
		}
		switch st.exec {
		case execStarting, execIdle:
			out.exec = execEnded
			eff = append(eff, lifecycleEffect{kind: effectPublishCompletion, boundary: afterRunTeardown})
		case execParked:
			out.exec = execParkedEnding
			eff = append(eff, lifecycleEffect{kind: effectPublishCompletion, boundary: afterRunTeardown})
		case execEnded, execParkedEnding:
			// Repeat: the publication cue re-emits. A fault recovery ends the
			// run before teardown, so the repeat is the normal post-fault
			// publication path. Without the cue an effect-driven teardown would
			// never publish, and external waiters would block on done forever.
			eff = append(eff, lifecycleEffect{kind: effectPublishCompletion, boundary: afterRunTeardown})
		default:
			return st, cont, nil, fmt.Errorf("lifecycle: run ended from exec state %d", st.exec)
		}

	case signalSpawnRolledBack:
		// Source V/S, C0, attempts 0, after rollback cleanup. The signal emits
		// the publication cue but leaves the child in V/S. A rollback child is
		// only ever reserved or starting with completion open when complete
		// runs: the releaseReservation rollback in spawnReserved is the only
		// source. The publication itself ends the child.
		// Repeats re-emit the cue only while the child is still unpublished
		// (V/S with completion open, i.e. before the publication ran). Once the
		// publication has moved the child to completionPublished, the
		// completion guard below rejects any further spawnRolledBack: a
		// post-publication repeat is not a state-preserving no-op.
		if st.completion != completionOpen {
			return st, cont, nil, fmt.Errorf("lifecycle: spawn rolled back after completion %d", st.completion)
		}
		switch st.exec {
		case execReserved, execStarting:
			eff = append(eff, lifecycleEffect{kind: effectPublishCompletion, boundary: afterSpawnRollback})
		default:
			return st, cont, nil, fmt.Errorf("lifecycle: spawn rolled back from exec state %d", st.exec)
		}

	case signalShutdownRequested:
		// Any otherwise-valid state. Repeats allowed; every call emits the
		// provider-cancel consideration; the context cancel is first-only.
		if st.shutdown == shutdownNone {
			out.shutdown = shutdownRequested
		}
		eff = append(eff, shutdownRequestedEffects(st.shutdown == shutdownRequested)...)

	case signalWriterCloseBegun:
		// Any otherwise-valid state; open→closing, closing/closed unchanged.
		// Repeat allowed even after closed; the interpreter still observes the
		// existing drain/idempotence protocol.
		if st.writer == writerOpen {
			out.writer = writerClosing
		}

	case signalWriterCloseFinished:
		// Source writerClosing with Close actually called on a nonnil sink; a
		// repeated finished signal is rejected (the caller's closed guard
		// avoids it). No signal when the sink is nil; a Close error still
		// counts as called.
		if st.writer != writerClosing {
			return st, cont, nil, fmt.Errorf("lifecycle: writer close finished from writer stage %d", st.writer)
		}
		out.writer = writerClosed

	case signalCompletionPublished:
		// CP repeat is a no-op; doneOnce prevents duplicate publication effects.
		if st.completion == completionPublished {
			break
		}
		if st.attempts != 0 {
			return st, cont, nil, fmt.Errorf("lifecycle: completion published with %d attempts", st.attempts)
		}
		if sig.boundary == afterSpawnRollback {
			// Rollback publishes while the child is still reserved or starting
			// with completion open (the releaseReservation rollback path in
			// spawnReserved). The publication
			// itself is what ends a rollback child, with writer open and no
			// shutdown. Parked-ending, ended and admission-closed sources are
			// rejected.
			if st.completion != completionOpen {
				return st, cont, nil, fmt.Errorf("lifecycle: rollback publication after completion %d", st.completion)
			}
			switch st.exec {
			case execReserved, execStarting:
			default:
				return st, cont, nil, fmt.Errorf("lifecycle: rollback publication from exec state %d", st.exec)
			}
			out.exec = execEnded
			out.completion = completionPublished
			eff = append(eff,
				lifecycleEffect{kind: effectSettleTurnWaiters},
				lifecycleEffect{kind: effectCloseDone},
			)
			break
		}
		// afterRunTeardown: source E/PE, C0/CA. It requires a requested
		// shutdown and a closing (or closed) writer. Writer closing is
		// sufficient when the sink is nil; writerClosed is never fabricated.
		// These preconditions are intentionally stricter than complete()
		// itself. All three teardown defers (runChild, runLoopChild,
		// runTeamChild) order beginChildShutdown and closeChildEventWriter
		// before complete. Every afterRunTeardown publication therefore
		// satisfies them. Do not loosen
		// them toward complete(); the afterSpawnRollback branch above is the
		// only writer-open/shutdown-none exception.
		switch st.exec {
		case execEnded, execParkedEnding:
		default:
			return st, cont, nil, fmt.Errorf("lifecycle: completion published from exec state %d", st.exec)
		}
		if st.completion != completionOpen && st.completion != completionAdmissionClosed {
			return st, cont, nil, fmt.Errorf("lifecycle: completion published after completion %d", st.completion)
		}
		if !st.isShuttingDown() {
			return st, cont, nil, errors.New("lifecycle: afterRunTeardown publication without a requested shutdown")
		}
		if !st.isWriterClosing() {
			return st, cont, nil, errors.New("lifecycle: afterRunTeardown publication without a closing writer")
		}
		out.completion = completionPublished
		if out.exec == execParkedEnding {
			out.exec = execEnded
		}
		eff = append(eff,
			lifecycleEffect{kind: effectSettleTurnWaiters},
			lifecycleEffect{kind: effectCloseDone},
		)

	case signalAgentStatusObserved:
		// Any valid state. The presentation write is emitted only when the
		// phase is nonempty; the caller's guard already deduplicates a done
		// presentation. Nonempty labels replace, empty labels retain.
		if sig.phase != "" {
			eff = append(eff, lifecycleEffect{kind: effectSetAgentPresentation, phase: sig.phase, label: sig.label})
		}

	case signalSessionEndObserved:
		// Any valid state; sets phase done and a nonempty stop reason; never
		// settles an attempt. The reason rides in the payload only when
		// nonempty, matching the adapter's effectSetSessionEndPresentation
		// write: an empty stop reason leaves the stored reason untouched.
		end := lifecycleEffect{kind: effectSetSessionEndPresentation}
		if sig.stopReason != "" {
			end.reason = sig.stopReason
		}
		eff = append(eff, end)

	case signalFault:
		// Fail-safe recovery after an illegal transition elsewhere (a machine
		// bug). The fault is legal from every state except a published
		// completion, so it can never strand a run. It moves exec to the ended
		// form the teardown accepts: execEnded, or execParkedEnding when the
		// child is parked, preserving the parked teardown path. It clears a
		// pending continuation probe, zeroes the attempt count and settles the
		// turn waiters the failed path left behind. The fault replays the
		// terminal writes the caller carries. recordExit selects the recorded
		// outcome exit. idleEnd selects the idle-end presentation: exit code,
		// done phase and stop reason. Completion, shutdown and
		// writer dimensions are untouched: the teardown's runEnded repeat and
		// completionPublished still validate. The fault selects only contEnd
		// and never resets a counted error.
		if st.completion == completionPublished {
			return st, cont, nil, fmt.Errorf("lifecycle: fault after completion %d", st.completion)
		}
		out.pendingProbe = probeNone
		out.attempts = 0
		switch st.exec {
		case execParked:
			out.exec = execParkedEnding
		case execParkedEnding:
			// Already in the parked teardown form; the fault is a repeat.
		default:
			out.exec = execEnded
		}
		if sig.recordExit {
			eff = append(eff, lifecycleEffect{kind: effectRecordOutcomeExit, exitCode: sig.exitCode})
		}
		if sig.idleEnd {
			eff = append(eff, lifecycleEffect{kind: effectSetIdleEndData, exitCode: sig.idleExitCode, reason: sig.idleStopReason})
		}
		eff = append(eff, lifecycleEffect{kind: effectSettleTurnWaiters})
		cont = continuation{contEnd}
	}

	if err := validateLifecycleState(out); err != nil {
		return st, continuation{contNone}, nil, fmt.Errorf("lifecycle: signal %d produces invalid state: %w", sig.kind, err)
	}
	return out, cont, eff, nil
}
