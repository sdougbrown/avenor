package stable

// Imperative adapter for the childRuntime lifecycle machine.
//
// The pure machine in runtime_lifecycle.go only predicts. This file commits
// the prediction field by field, executes the owned locked effects — activity
// and attempt count with their mirrors, presentation, the settled/finalized
// waiter release, the recorded outcome exit code, the parked and completed
// mirrors and the shutdown/writer mirrors. applyLifecycle requires the caller
// to hold child.mu and acquires no lock. applyLifecycleWriteLocked requires
// the caller to hold writeMu -> child.mu and is the only path that commits
// the shutdown and writer dimensions; a failed transition commits the legacy
// stage through the shutdown/writer fail-safes below. Errors are returned to
// the caller and must be reported with reportLifecycleError outside every lock.

import (
	"errors"
	"fmt"

	"github.com/sdougbrown/avenor/internal/cli"
)

// childAttemptClass selects the immutable attempt class for a spawn: workflow
// class when either LoopFile or TeamFile is nonempty, otherwise direct
// (plan decision 5). Workflow-direct runs still use the direct class.
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

// lifecycleTransitionFailed reports whether err is an illegal-transition
// error.
func lifecycleTransitionFailed(err error) bool {
	return err != nil
}

// errLifecycleFaultRecovery marks a slot-wait failure whose fault fail-safe
// already ended the run, so the caller must not layer an idle-context end on
// top of the ended state.
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
	// holds these locks, so the copy is consistent (plan decision 7).
	pre := child.lc

	predicted, cont, eff, err := next(pre, sig)
	if err == nil && s.testHooks.lifecycleTransitionFailure != nil && s.testHooks.lifecycleTransitionFailure(sig.kind) {
		// Test-injected failure: the transition is rejected exactly like an
		// illegal one — state unchanged, no effects, no continuation — so a
		// test can drive the fault fail-safe at a chosen site.
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
	// store the whole predicted struct (plan decision 7).
	child.lc.exec = predicted.exec
	child.lc.attempts = predicted.attempts
	child.lc.completion = predicted.completion
	child.lc.pendingProbe = predicted.pendingProbe
	if dualLocked {
		child.lc.shutdown = predicted.shutdown
		child.lc.writer = predicted.writer
	}
	s.applyOwnedLifecycleEffects(child, predicted, sig.kind, eff)
	return cont, eff, nil
}

// applyOwnedLifecycleEffects executes the locked lifecycle effects this
// adapter owns and writes the mirrors derived from them: the activity mirrors
// (active, activeAttempts, kept synchronized to the machine count for both
// classes), presentation (phase, phaseLabel), the idle-end data (exitCode,
// lastStopReason), the recorded outcome exit code and the waiter release of a
// settled or finalized attempt, of a fault recovery and of the final
// publication. The parked mirror is written only by the signals the legacy
// code parked with: parkRequested, localSlotAcquired and parkWaitEnded; every
// other signal leaves the mirror alone, and the final publication deliberately
// retains the obsolete parked value until the mirror is deleted. The completed
// mirror is written only by the two signals the legacy code completed with:
// parkTimedOut (admission closed before done) and completionPublished. The
// shutdown/writer mirrors are written only by the dual-locked signals:
// shutdownRequested (shuttingDown), writerCloseBegun (writerClosing, staying
// true after writerClosed as the legacy flags did) and writerCloseFinished
// (writerClosed).
func (s *Supervisor) applyOwnedLifecycleEffects(child *childRuntime, predicted lifecycleState, site signalKind, eff []lifecycleEffect) {
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
			if site == signalAttemptSettled || site == signalWorkflowFinalized || site == signalFault || site == signalCompletionPublished {
				child.settleTurnLocked()
			}
		}
	}
	child.activeAttempts = predicted.attempts
	child.active = predicted.attempts > 0
	switch site {
	case signalParkRequested, signalLocalSlotAcquired, signalParkWaitEnded:
		child.parked = predicted.isParked()
	case signalParkTimedOut, signalCompletionPublished:
		child.completed = predicted.completion != completionOpen
	case signalShutdownRequested:
		child.shuttingDown = predicted.isShuttingDown()
	case signalWriterCloseBegun:
		child.writerClosing = predicted.isWriterClosing()
	case signalWriterCloseFinished:
		// writerClosing stays true after writerClosed, as the legacy flags did.
		child.writerClosing = predicted.isWriterClosing()
		child.writerClosed = predicted.isWriterClosed()
	}
}

// completePublicationFailSafe applies the legacy publication's unconditional
// writes when the completionPublished transition itself failed: it commits the
// published completion, settles the turn waiters and sets the completed mirror
// so capacity accounting and registered waiters still observe the ended
// runtime. The caller must hold child.mu and report the transition error
// outside every lock.
func (s *Supervisor) completePublicationFailSafe(child *childRuntime) {
	// A published completion implies an ended exec with no attempts and no
	// pending probe; commit all of them so the forced state still satisfies
	// the machine's invariants.
	child.lc.exec = execEnded
	child.lc.attempts = 0
	child.lc.pendingProbe = probeNone
	child.lc.completion = completionPublished
	child.settleTurnLocked()
	child.completed = true
}

// parkTimeoutFailSafe applies the legacy park-timeout writes when the
// parkTimedOut transition itself failed: it sets completion to
// admission-closed when it is still open and sets the completed mirror so
// admission readers reject prompts as the legacy timeout did. The caller must
// hold child.mu and report the transition error outside every lock.
func (s *Supervisor) parkTimeoutFailSafe(child *childRuntime) {
	if child.lc.completion == completionOpen {
		child.lc.completion = completionAdmissionClosed
	}
	child.completed = true
}

// shutdownFailSafe applies the legacy shutdown write when the
// shutdownRequested transition itself failed: it commits the requested stage
// and sets its mirror so admission gates reject new work and the parent cancel
// stays first-only, exactly as the legacy flag write did. The caller must
// hold writeMu -> child.mu and report the transition error outside every lock.
func (s *Supervisor) shutdownFailSafe(child *childRuntime) {
	child.lc.shutdown = shutdownRequested
	child.shuttingDown = true
}

// writerCloseBegunFailSafe applies the legacy writer write when the
// writerCloseBegun transition itself failed: it commits the closing stage
// (never regressing a closed writer) and sets its mirror so admission gates
// reject new writes while the drain runs. The caller must hold writeMu ->
// child.mu and report the transition error outside every lock.
func (s *Supervisor) writerCloseBegunFailSafe(child *childRuntime) {
	if child.lc.writer == writerOpen {
		child.lc.writer = writerClosing
	}
	child.writerClosing = true
}

// writerCloseFinishedFailSafe applies the legacy writer writes when the
// writerCloseFinished transition itself failed: the sink Close already ran,
// so the closed stage commits and a repeat close never closes the sink again;
// the closing mirror stays true, as the legacy flags did. The caller must hold
// writeMu -> child.mu and report the transition error outside every lock.
func (s *Supervisor) writerCloseFinishedFailSafe(child *childRuntime) {
	child.lc.writer = writerClosed
	child.writerClosing = true
	child.writerClosed = true
}

// executeShutdownDeferredEffects executes the deferred effects a shutdown
// transaction returned, binding the data captured under the caller's locks:
// the consideration requests provider cancellation only for an eligible
// captured turn, and the parent cancel fires on the captured callback. It must
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
// leaves the run with external waiters (a sentinel consumer, a held tree
// token, a spawned peer waiting for capacity, an unsettled turn waiter) and a
// machine state teardown would reject, so the fail-safe first applies
// signalFault under child.mu: the fault moves exec to the ended form teardown
// accepts, clears a pending probe, zeroes the attempts and settles the turn
// waiters. In that same critical section it records the terminal exit code
// (and, on the idle-context path, the terminal phase and stop reason) so the
// termination classification sees the real outcome. Outside the lock it then
// releases the tree token, writes the terminal sentinel with the caller's
// payload when this outcome has not written one, emits the session.end on the
// idle-context path, and signals capacity when the failure happened on the
// park path. It must be called outside child.mu, where the original deferred
// effects would have run; the transition error is reported by the caller
// before this helper runs, and a fault that itself fails is reported here.
func (s *Supervisor) failSafeRunChildTransition(child *childRuntime, fault lifecycleFault) {
	child.mu.Lock()
	_, _, err := s.applyLifecycle(child, lifecycleSignal{kind: signalFault, class: child.lc.class})
	if err == nil {
		if fault.recordExit {
			child.exitCode = fault.exitCode
		}
		if fault.idleEnd {
			child.phase = "done"
			child.lastStopReason = fault.reason
		}
	}
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
// sentinel/session data, whether the exit code and idle-end presentation must
// be recorded under the lock, whether this outcome already wrote its
// sentinel, and whether the failure happened on the park path (capacity
// signal) or the idle-context path (session.end).
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
// recorded exit, waiter release) are not deferred and are ignored here.
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
		}
	}
}
