package stable

// Imperative adapter for the childRuntime lifecycle machine.
//
// The pure machine in runtime_lifecycle.go only predicts. This file commits
// the prediction field by field (exec, attempts, completion, pending probe,
// shutdown and writer stages). It executes the owned locked effects:
// presentation, the settled/finalized waiter release and the recorded
// outcome exit code. applyLifecycle requires the caller
// to hold child.mu and acquires no lock. applyLifecycleWriteLocked requires
// the caller to hold writeMu -> child.mu; it is the only path that commits
// the shutdown and writer dimensions. A failed transition commits the
// dimension's stage through the shutdown/writer fail-safes below. Errors are
// returned to the caller and must be reported with reportLifecycleError
// outside every lock.
//
// The adapter's effect switches partition effectKind: applyOwnedLifecycleEffects
// executes the locked kinds under the transaction's locks, and
// executeDeferredLifecycleEffects executes the rest after every lock is
// released. Each switch skips the other's kinds and reports a kind in neither
// set as a lifecycle error, never ignored.

import (
	"errors"
	"fmt"

	"github.com/sdougbrown/avenor/internal/cli"
)

// childAttemptClass selects the immutable attempt class for a spawn: workflow
// class when either LoopFile or TeamFile is nonempty, otherwise direct.
// Workflow-direct runs still use the direct class.
func childAttemptClass(params SpawnParams) attemptClass {
	if params.LoopFile != "" || params.TeamFile != "" {
		return workflowAttemptClass
	}
	return directAttemptClass
}

// signalKindName names a signal kind for diagnostics only.
func signalKindName(k signalKind) string {
	switch k {
	case signalPublished:
		return "published"
	case signalAttemptPreparing:
		return "attemptPreparing"
	case signalAttemptStarted:
		return "attemptStarted"
	case signalAttemptStartFailed:
		return "attemptStartFailed"
	case signalAttemptSettled:
		return "attemptSettled"
	case signalWorkflowFinalized:
		return "workflowFinalized"
	case signalAttemptReturned:
		return "attemptReturned"
	case signalAttemptOutcome:
		return "attemptOutcome"
	case signalPromptContinuation:
		return "promptContinuation"
	case signalSuccessPresented:
		return "successPresented"
	case signalParkRequested:
		return "parkRequested"
	case signalPromptDequeued:
		return "promptDequeued"
	case signalLocalSlotAcquired:
		return "localSlotAcquired"
	case signalParkTimedOut:
		return "parkTimedOut"
	case signalParkWaitEnded:
		return "parkWaitEnded"
	case signalIdleContextEnded:
		return "idleContextEnded"
	case signalRunEnded:
		return "runEnded"
	case signalSpawnRolledBack:
		return "spawnRolledBack"
	case signalShutdownRequested:
		return "shutdownRequested"
	case signalWriterCloseBegun:
		return "writerCloseBegun"
	case signalWriterCloseFinished:
		return "writerCloseFinished"
	case signalCompletionPublished:
		return "completionPublished"
	case signalAgentStatusObserved:
		return "agentStatusObserved"
	case signalSessionEndObserved:
		return "sessionEndObserved"
	case signalFault:
		return "fault"
	}
	return fmt.Sprintf("signal(%d)", int(k))
}

// applyLifecycle applies one lifecycle signal while the caller holds child.mu.
// It acquires no lock and never reports diagnostics itself; the caller reports
// the returned error through reportLifecycleError after releasing every lock.
func (s *Supervisor) applyLifecycle(child *childRuntime, sig lifecycleSignal) (continuation, []lifecycleEffect, error) {
	return s.transactLifecycle(child, sig, false)
}

// applyLifecycleWriteLocked applies one lifecycle signal while the caller
// holds writeMu -> child.mu. Only the dedicated shutdown/writer signal paths
// may use it: it is the only writer of lc.shutdown and lc.writer.
func (s *Supervisor) applyLifecycleWriteLocked(child *childRuntime, sig lifecycleSignal) (continuation, []lifecycleEffect, error) {
	return s.transactLifecycle(child, sig, true)
}

// errLifecycleFaultRecovery marks a slot-wait failure whose fault fail-safe
// already ended the run. The caller must not layer an idle-context end on top
// of the ended state.
var errLifecycleFaultRecovery = errors.New("lifecycle fault recovery ended the run")

// transactLifecycle is the shared transaction core: predict, then field-commit
// and execute the owned locked effects.
func (s *Supervisor) transactLifecycle(child *childRuntime, sig lifecycleSignal, dualLocked bool) (continuation, []lifecycleEffect, error) {
	if !dualLocked {
		switch sig.kind {
		case signalShutdownRequested, signalWriterCloseBegun, signalWriterCloseFinished:
			return continuation{contNone}, nil, fmt.Errorf(
				"lifecycle: signal %d requires the dual-locked wrapper", sig.kind)
		}
	}
	// Capture the full pre-state under the caller's locks. Every state writer
	// holds these locks, so the copy is consistent.
	pre := child.lc

	predicted, cont, eff, err := next(pre, sig)
	if err == nil && s.testHooks.lifecycleTransitionFailure != nil && s.testHooks.lifecycleTransitionFailure(sig.kind) {
		// Test-injected failure: the transition is rejected exactly like an
		// illegal one, with state unchanged, no effects and no continuation.
		// A test can therefore drive the fault fail-safe at a chosen site.
		err = fmt.Errorf("lifecycle: injected transition failure for signal %d (%s)", sig.kind, signalKindName(sig.kind))
		cont = continuation{contNone}
	}
	if !dualLocked && err == nil && (predicted.shutdown != pre.shutdown || predicted.writer != pre.writer) {
		// Defensive: a non-dedicated signal must never move the dual-locked
		// dimensions. next cannot do this today; the guard keeps the contract
		// enforced if the machine ever grows one.
		return continuation{contNone}, nil, fmt.Errorf(
			"lifecycle: signal %d changed a dual-locked dimension outside the wrapper", sig.kind)
	}
	if err != nil {
		// Invalid prediction: leave lc unchanged; the caller reports the error
		// and runs the fail-safe its site prescribes.
		return cont, nil, err
	}
	// Field-commit the valid prediction. Ordinary fields are committed by
	// both wrappers; shutdown/writer only by the dual-locked wrapper. Never
	// store the whole predicted struct.
	child.lc.exec = predicted.exec
	child.lc.attempts = predicted.attempts
	child.lc.completion = predicted.completion
	child.lc.pendingProbe = predicted.pendingProbe
	if dualLocked {
		child.lc.shutdown = predicted.shutdown
		child.lc.writer = predicted.writer
	}
	if err := s.applyOwnedLifecycleEffects(child, eff); err != nil {
		return cont, eff, err
	}
	return cont, eff, nil
}

// ownedLifecycleEffect and deferredLifecycleEffect partition effectKind
// between the adapter's two effect switches. The locked kinds execute under
// the transaction's locks; the rest after every lock is released. A kind in
// neither set is a machine bug that both switches report.
func ownedLifecycleEffect(k effectKind) bool {
	switch k {
	case effectClearPresentation, effectSetAgentPresentation, effectSetSuccessPresentation,
		effectSetSessionEndPresentation, effectSetIdleEndData, effectRecordOutcomeExit,
		effectSettleTurnWaiters:
		return true
	}
	return false
}

func deferredLifecycleEffect(k effectKind) bool {
	switch k {
	case effectWriteSentinel, effectSignalCapacity, effectReleaseTreeToken,
		effectEmitSessionEnd, effectConsiderProviderCancel, effectCancelContext,
		effectPublishCompletion, effectCloseDone:
		return true
	}
	return false
}

// applyOwnedLifecycleEffects executes the locked lifecycle effects this
// adapter owns: presentation (phase, phaseLabel), the idle-end data
// (exitCode, lastStopReason) and the recorded outcome exit code. It also
// executes the waiter release of a settled or finalized attempt, of a fault
// recovery and of the final publication. An unknown effect kind is a machine
// bug; it returns an error the caller reports outside every lock.
func (s *Supervisor) applyOwnedLifecycleEffects(child *childRuntime, eff []lifecycleEffect) error {
	var unhandled error
	for _, e := range eff {
		switch e.kind {
		case effectClearPresentation:
			child.phase, child.phaseLabel = "", ""
		case effectSetAgentPresentation:
			child.phase = e.phase
			if e.label != "" {
				child.phaseLabel = e.label
			}
		case effectSetSuccessPresentation:
			child.phase = "done"
		case effectSetSessionEndPresentation:
			child.phase = "done"
			if e.reason != "" {
				child.lastStopReason = e.reason
			}
		case effectSetIdleEndData:
			child.exitCode = e.exitCode
			child.phase = "done"
			child.lastStopReason = e.reason
		case effectRecordOutcomeExit:
			child.exitCode = e.exitCode
		case effectSettleTurnWaiters:
			child.settleTurnLocked()
		default:
			// Deferred kinds are the deferred executor's job; only a kind in
			// neither set is a machine bug.
			if !deferredLifecycleEffect(e.kind) {
				if unhandled == nil {
					unhandled = fmt.Errorf("lifecycle: unhandled effect kind %d", e.kind)
				}
			}
		}
	}
	return unhandled
}

// completePublicationFailSafe applies the publication's unconditional writes
// when the completionPublished transition itself failed. It commits the
// published completion with its implied ended exec, empty attempts and no
// pending probe, so the forced state satisfies the machine's invariants. It
// settles the turn waiters, so capacity accounting and registered waiters
// still observe the ended runtime. The caller must hold child.mu and report
// the transition error outside every lock.
func (s *Supervisor) completePublicationFailSafe(child *childRuntime) {
	child.lc.exec = execEnded
	child.lc.attempts = 0
	child.lc.pendingProbe = probeNone
	child.lc.completion = completionPublished
	child.settleTurnLocked()
}

// parkTimeoutFailSafe applies the park-timeout writes when the parkTimedOut
// transition itself failed. It sets completion to admission-closed when it is
// still open, so admission readers reject prompts as the timeout did. The
// caller must hold child.mu and report the transition error outside every
// lock.
func (s *Supervisor) parkTimeoutFailSafe(child *childRuntime) {
	if child.lc.completion == completionOpen {
		child.lc.completion = completionAdmissionClosed
	}
}

// shutdownFailSafe applies the shutdown write when the shutdownRequested
// transition itself failed. It commits the requested stage so admission gates
// reject new work and the parent cancel stays first-only. The caller must
// hold writeMu -> child.mu and report the transition error outside every lock.
func (s *Supervisor) shutdownFailSafe(child *childRuntime) {
	child.lc.shutdown = shutdownRequested
}

// writerCloseBegunFailSafe applies the writer write when the writerCloseBegun
// transition itself failed. It commits the closing stage (never regressing a
// closed writer), so admission gates reject new writes while the drain runs.
// The caller must hold writeMu -> child.mu and report the transition error
// outside every lock.
func (s *Supervisor) writerCloseBegunFailSafe(child *childRuntime) {
	if child.lc.writer == writerOpen {
		child.lc.writer = writerClosing
	}
}

// writerCloseFinishedFailSafe applies the writer write when the
// writerCloseFinished transition itself failed. The sink Close already ran,
// so the closed stage commits and a repeat close never closes the sink again.
// The caller must hold writeMu -> child.mu and report the transition error
// outside every lock.
func (s *Supervisor) writerCloseFinishedFailSafe(child *childRuntime) {
	child.lc.writer = writerClosed
}

// executeShutdownDeferredEffects executes the deferred effects a shutdown
// transaction returned, binding the data captured under the caller's locks.
// The consideration requests provider cancellation only for an eligible
// captured turn. The parent cancel fires on the captured callback. It must
// be called after every lock has been released.
func (s *Supervisor) executeShutdownDeferredEffects(child *childRuntime, eff []lifecycleEffect, turn *cli.ProviderTurn, sessionID string, cancelProvider bool, cancel func()) {
	for _, e := range eff {
		switch e.kind {
		case effectConsiderProviderCancel:
			if cancelProvider && turn != nil {
				turn.RequestCancel(sessionID, directPermissionJoinTimeout)
			}
		case effectCancelContext:
			if cancel != nil {
				cancel()
			}
		default:
			// Owned kinds already ran under the caller's locks; only a kind in
			// neither set is a machine bug.
			if !ownedLifecycleEffect(e.kind) {
				s.reportLifecycleError(child, fmt.Errorf("lifecycle: unhandled effect kind %d", e.kind))
			}
		}
	}
}

// reportLifecycleError counts every illegal transition and logs it. It must
// be called outside all locks.
func (s *Supervisor) reportLifecycleError(child *childRuntime, err error) {
	s.lifecycleErrors.Add(1)
	id := ""
	if child != nil {
		id = child.id
	}
	fmt.Fprintf(s.stderrWriter(), "avenor stable: lifecycle error (runtime %s): %v\n", id, err)
}

// failSafeRunChildTransition is the fixed fail-safe for an illegal transition
// in runChild's owned paths and the idle-context end. The failed prediction
// leaves external waiters: a sentinel consumer, a held tree token, a spawned
// peer waiting for capacity, an unsettled turn waiter. It also leaves a
// machine state teardown would reject. The fail-safe therefore applies
// signalFault under child.mu. The fault moves exec to the ended form teardown
// accepts, clears a pending probe and zeroes the attempts. It settles the turn
// waiters the failed path left behind. Through the machine's locked effects it
// records the terminal exit code and, on the idle-context path, the terminal
// phase and stop reason. The termination classification then sees the real
// outcome. Outside the lock it then releases the tree token. It writes
// the terminal sentinel with the caller's payload when this outcome has not
// written one. It emits the session.end on the idle-context path. It signals
// capacity when the failure happened on the park path. It must be called
// outside child.mu, where the original deferred effects would have run. The
// caller reports the transition error before this helper runs; a fault that
// itself fails is reported here.
func (s *Supervisor) failSafeRunChildTransition(child *childRuntime, fault lifecycleFault) {
	child.mu.Lock()
	sig := lifecycleSignal{kind: signalFault, class: child.lc.class}
	if fault.recordExit {
		sig.recordExit = true
		sig.exitCode = fault.exitCode
	}
	if fault.idleEnd {
		sig.idleEnd = true
		sig.idleExitCode = fault.exitCode
		sig.idleStopReason = fault.reason
	}
	_, _, err := s.applyLifecycle(child, sig)
	child.mu.Unlock()
	if err != nil {
		s.reportLifecycleError(child, err)
	}
	eff := []lifecycleEffect{{kind: effectReleaseTreeToken}}
	if !fault.sentinelWritten {
		eff = append(eff, lifecycleEffect{
			kind: effectWriteSentinel, exitCode: fault.exitCode, reason: fault.reason, sessionID: fault.sessionID,
		})
	}
	if fault.idleEnd {
		eff = append(eff, lifecycleEffect{kind: effectEmitSessionEnd, exitCode: fault.exitCode, reason: fault.reason})
	}
	if fault.parkPath {
		eff = append(eff, lifecycleEffect{kind: effectSignalCapacity})
	}
	s.executeDeferredLifecycleEffects(child, eff)
}

// lifecycleFault carries the terminal payload a fault fail-safe applies: the
// sentinel/session data. recordExit and idleEnd state which terminal writes
// the signalFault transaction must commit (the recorded outcome exit and the
// idle-end presentation); the bare fault sets neither. sentinelWritten states
// whether this outcome already wrote its sentinel. They also state whether the
// failure happened on the park path (capacity signal) or the idle-context path
// (session.end). An idle-end fault records the same exit code through both
// effects, so idleEnd callers set recordExit with the same exitCode
// (idleEndFault does both).
type lifecycleFault struct {
	exitCode        int
	reason          string
	sessionID       string
	recordExit      bool
	sentinelWritten bool
	parkPath        bool
	idleEnd         bool
}

// executeDeferredLifecycleEffects executes the deferred effects a valid
// prediction returned, at the caller's site after every lock has been
// released. Caller-bound skips: a sentinel writes only when the child has a
// sentinel file. effectPublishCompletion runs the completion boundary the cue
// names (the doneOnce-protected complete), and effectCloseDone closes the
// done channel — it must only run inside complete's doneOnce. Effects the
// adapter already executed under the caller's locks (activity, presentation,
// recorded exit, waiter release) are not deferred; they are skipped here. An
// unknown kind is counted and reported as a lifecycle error.
func (s *Supervisor) executeDeferredLifecycleEffects(child *childRuntime, eff []lifecycleEffect) {
	for _, e := range eff {
		switch e.kind {
		case effectWriteSentinel:
			if child.sentinelFile != "" {
				cli.WriteSentinel(child.sentinelFile, e.exitCode, e.sessionID, e.reason, s.runID, s.stderrWriter())
			}
		case effectSignalCapacity:
			s.signalCapacityChange()
		case effectReleaseTreeToken:
			s.releaseChildTreeToken(child)
		case effectEmitSessionEnd:
			s.emitSessionEnd(child, e.exitCode, e.reason)
		case effectPublishCompletion:
			child.complete(s, e.boundary)
		case effectCloseDone:
			close(child.done)
		default:
			// Owned kinds flow through call sites that pass the whole effect
			// list back; only a kind in neither set is a machine bug.
			if !ownedLifecycleEffect(e.kind) {
				s.reportLifecycleError(child, fmt.Errorf("lifecycle: unhandled effect kind %d", e.kind))
			}
		}
	}
}
