package stable

// Stage 2 imperative adapter for the childRuntime lifecycle machine
// (issue #316, plan decision 8).
//
// The pure machine in runtime_lifecycle.go only predicts. This file owns the
// shadow-mode transaction: capture the pre-state under the caller's locks,
// predict with next, execute the still-legacy-owned mutations through the
// caller-supplied closure, field-commit the prediction, and independently
// compare the committed state, presentation and legacy effect witnesses.
// Legacy writes stay authoritative; the machine owns nothing yet (plan
// decision 9).
//
// applyLifecycle requires the caller to hold child.mu and acquires no lock.
// applyLifecycleWriteLocked requires the caller to hold writeMu -> child.mu
// and is the only path that commits the shutdown and writer dimensions
// (plan decision 7). Both take the temporary legacy closure that performs the
// still-legacy-owned locked mutations; it is deleted in Stage 7. Errors are
// returned to the caller and must be reported with reportLifecycleError
// outside every lock.

import (
	"fmt"
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
	}
	return fmt.Sprintf("signal(%d)", int(k))
}

// legacyLifecycleWitness records the legacy effects the closure actually
// performed under the caller's locks, in execution order. Deferred legacy
// effects (sentinels, capacity signals, session ends, cancels, publication)
// are witnessed at their outer execution sites and compared with
// compareDeferredLifecycleEffects.
type legacyLifecycleWitness struct {
	locked []lifecycleEffect
}

// applyLifecycle applies one lifecycle signal while the caller holds child.mu.
// It acquires no lock and never reports diagnostics itself; the caller reports
// the returned error through reportLifecycleError after releasing every lock.
func (s *Supervisor) applyLifecycle(child *childRuntime, sig lifecycleSignal, legacy func() legacyLifecycleWitness) (continuation, []lifecycleEffect, error) {
	return s.transactLifecycle(child, sig, legacy, false)
}

// applyLifecycleWriteLocked applies one lifecycle signal while the caller
// holds writeMu -> child.mu. Only the dedicated shutdown/writer signal paths
// may use it: it is the only writer of lc.shutdown and lc.writer.
func (s *Supervisor) applyLifecycleWriteLocked(child *childRuntime, sig lifecycleSignal, legacy func() legacyLifecycleWitness) (continuation, []lifecycleEffect, error) {
	return s.transactLifecycle(child, sig, legacy, true)
}

// transactLifecycle is the shared shadow transaction core.
func (s *Supervisor) transactLifecycle(child *childRuntime, sig lifecycleSignal, legacy func() legacyLifecycleWitness, dualLocked bool) (continuation, []lifecycleEffect, error) {
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
	prePhase, preLabel := child.phase, child.phaseLabel
	preExit, preReason := child.exitCode, child.lastStopReason

	predicted, cont, eff, err := next(pre, sig)
	if !dualLocked && err == nil && (predicted.shutdown != pre.shutdown || predicted.writer != pre.writer) {
		// Defensive: a non-dedicated signal must never move the dual-locked
		// dimensions. next cannot do this today; the guard keeps the contract
		// enforced if the machine ever grows one.
		return continuation{contNone}, nil, fmt.Errorf(
			"lifecycle: signal %d changed a dual-locked dimension outside the wrapper", sig.kind)
	}
	witness := legacyLifecycleWitness{}
	if legacy != nil {
		witness = legacy()
	}
	if err != nil {
		// Invalid prediction: leave lc unchanged, the legacy operation has
		// finished in shadow mode; the caller reports and the gate fails.
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
	s.compareShadowTransaction(child, pre, prePhase, preLabel, preExit, preReason, predicted, eff, witness, sig.kind)
	return cont, eff, nil
}

// reportLifecycleError counts every illegal transition and logs it. It must
// be called outside all locks (plan decision 7).
func (s *Supervisor) reportLifecycleError(child *childRuntime, err error) {
	s.lifecycleErrors.Add(1)
	id := ""
	if child != nil {
		id = child.id
	}
	fmt.Fprintf(s.stderrWriter(), "avenor stable: lifecycle error (runtime %s): %v\n", id, err)
}

// compareShadowTransaction independently compares the committed prediction
// against the legacy mirrors, presentation and locked-effect witness. Every
// failed comparison increments lifecycleShadowMismatches.
func (s *Supervisor) compareShadowTransaction(child *childRuntime, pre lifecycleState, prePhase, preLabel string, preExit int, preReason string, predicted lifecycleState, eff []lifecycleEffect, witness legacyLifecycleWitness, site signalKind) {
	// Attempt count: workflow compares against activeAttempts; direct compares
	// against the 0/1 legacy active boolean (its activeAttempts is unused).
	switch predicted.class {
	case workflowAttemptClass:
		if child.activeAttempts != predicted.attempts {
			s.countShadowMismatch(child, site, fmt.Sprintf("attempts: lc=%d legacy activeAttempts=%d", predicted.attempts, child.activeAttempts))
		}
	case directAttemptClass:
		want := 0
		if child.active {
			want = 1
		}
		if want != predicted.attempts {
			s.countShadowMismatch(child, site, fmt.Sprintf("attempts: lc=%d legacy active=%v", predicted.attempts, child.active))
		}
	}
	// Parked: compared exactly until final publication (plan decision 4);
	// once completion is published the legacy parked value may be obsolete.
	if predicted.completion != completionPublished && child.parked != predicted.isParked() {
		s.countShadowMismatch(child, site, fmt.Sprintf("parked: lc=%v legacy=%v", predicted.isParked(), child.parked))
	}
	// Completion mirror: legacy completed covers both admission-closed and
	// published.
	if child.completed != (predicted.completion != completionOpen) {
		s.countShadowMismatch(child, site, fmt.Sprintf("completed: legacy=%v lc completion=%d", child.completed, predicted.completion))
	}
	// Shutdown mirror.
	if child.shuttingDown != predicted.isShuttingDown() {
		s.countShadowMismatch(child, site, fmt.Sprintf("shuttingDown: legacy=%v lc=%v", child.shuttingDown, predicted.isShuttingDown()))
	}
	// Writer mirror: legacy writerClosing stays true after writerClosed.
	switch predicted.writer {
	case writerOpen:
		if child.writerClosing || child.writerClosed {
			s.countShadowMismatch(child, site, fmt.Sprintf("writer open: legacy closing=%v closed=%v", child.writerClosing, child.writerClosed))
		}
	case writerClosing:
		if !child.writerClosing || child.writerClosed {
			s.countShadowMismatch(child, site, fmt.Sprintf("writer closing: legacy closing=%v closed=%v", child.writerClosing, child.writerClosed))
		}
	case writerClosed:
		if !child.writerClosed {
			s.countShadowMismatch(child, site, "writer closed: legacy writerClosed=false")
		}
	}
	// Effective capacity exclusion (completed || parked) at all times.
	if (child.completed || child.parked) != (predicted.isCompleted() || predicted.isParked()) {
		s.countShadowMismatch(child, site, fmt.Sprintf(
			"capacity exclusion: legacy=(completed=%v,parked=%v) lc=(completed=%v,parked=%v)",
			child.completed, child.parked, predicted.isCompleted(), predicted.isParked()))
	}
	// Presentation: apply the predicted presentation effects to the pre
	// snapshot and compare with the actual legacy post-values.
	wantPhase, wantLabel := applyLifecyclePresentation(prePhase, preLabel, eff)
	if child.phase != wantPhase || child.phaseLabel != wantLabel {
		s.countShadowMismatch(child, site, fmt.Sprintf("presentation: legacy=(%q,%q) predicted=(%q,%q)", child.phase, child.phaseLabel, wantPhase, wantLabel))
	}
	// Terminal data written by predicted locked effects.
	for _, e := range eff {
		switch e.kind {
		case effectRecordOutcomeExit:
			if child.exitCode != e.exitCode {
				s.countShadowMismatch(child, site, fmt.Sprintf("exitCode: legacy=%d predicted=%d", child.exitCode, e.exitCode))
			}
		case effectSetIdleEndData:
			if child.exitCode != e.exitCode {
				s.countShadowMismatch(child, site, fmt.Sprintf("idle exitCode: legacy=%d predicted=%d", child.exitCode, e.exitCode))
			}
			if child.lastStopReason != e.reason {
				s.countShadowMismatch(child, site, fmt.Sprintf("idle stop reason: legacy=%q predicted=%q", child.lastStopReason, e.reason))
			}
		case effectSetSessionEndPresentation:
			if e.reason != "" && child.lastStopReason != e.reason {
				s.countShadowMismatch(child, site, fmt.Sprintf("session end stop reason: legacy=%q predicted=%q", child.lastStopReason, e.reason))
			}
		}
	}
	// Locked-effect witness.
	s.compareLifecycleEffects(lifecycleLockedEffects(eff), witness.locked, child, site, "locked")
}

// applyLifecyclePresentation applies predicted presentation effects to a
// pre-image and returns the expected post-image. Label retention: only a
// nonempty label replaces the stored one; clear resets both.
func applyLifecyclePresentation(phase, label string, eff []lifecycleEffect) (string, string) {
	for _, e := range eff {
		switch e.kind {
		case effectClearPresentation:
			phase, label = "", ""
		case effectSetAgentPresentation:
			phase = e.phase
			if e.label != "" {
				label = e.label
			}
		case effectSetSuccessPresentation, effectSetSessionEndPresentation, effectSetIdleEndData:
			phase = "done"
		}
	}
	return phase, label
}

// compareLifecycleEffects compares two ordered effect sequences. It is used
// for locked witnesses (inside the transaction) and, via
// compareDeferredLifecycleEffects, for deferred witnesses at their outer
// execution sites.
func (s *Supervisor) compareLifecycleEffects(predicted, witnessed []lifecycleEffect, child *childRuntime, site signalKind, class string) {
	if len(predicted) != len(witnessed) {
		s.countShadowMismatch(child, site, fmt.Sprintf("%s effects: predicted %d %v, witnessed %d %v", class, len(predicted), predicted, len(witnessed), witnessed))
		return
	}
	for i := range predicted {
		p, w := predicted[i], witnessed[i]
		if p.kind != w.kind {
			s.countShadowMismatch(child, site, fmt.Sprintf("%s effect %d: predicted %d, witnessed %d", class, i, p.kind, w.kind))
			continue
		}
		switch p.kind {
		case effectRecordOutcomeExit, effectSetIdleEndData, effectEmitSessionEnd:
			if p.exitCode != w.exitCode || p.reason != w.reason {
				s.countShadowMismatch(child, site, fmt.Sprintf("%s effect %d payload: predicted (%d,%q), witnessed (%d,%q)", class, i, p.exitCode, p.reason, w.exitCode, w.reason))
			}
		case effectWriteSentinel:
			// The session ID is caller-resolved (the machine carries only what
			// the result snapshot had); an empty predicted value means the
			// caller resolves the session outside the transaction.
			if p.exitCode != w.exitCode || p.reason != w.reason || (p.sessionID != "" && p.sessionID != w.sessionID) {
				s.countShadowMismatch(child, site, fmt.Sprintf("%s sentinel payload: predicted (%d,%q,%q), witnessed (%d,%q,%q)", class, p.exitCode, p.reason, p.sessionID, w.exitCode, w.reason, w.sessionID))
			}
		case effectPublishCompletion:
			if p.boundary != w.boundary {
				s.countShadowMismatch(child, site, fmt.Sprintf("%s boundary: predicted %d, witnessed %d", class, p.boundary, w.boundary))
			}
		}
	}
}

// compareDeferredLifecycleEffects compares the predicted deferred effects of
// a transaction with the witnesses recorded at the legacy effect call sites.
// Two kinds are caller-bound and may legitimately be skipped by legacy:
//   - writeSentinel when the child has no sentinel file;
//   - cancelContext when the child has no cancel callback.
//
// Every other predicted kind must be witnessed, in order.
func (s *Supervisor) compareDeferredLifecycleEffects(child *childRuntime, predicted, witnessed []lifecycleEffect, site signalKind) {
	deferred := lifecycleDeferredEffects(predicted)
	filtered := make([]lifecycleEffect, 0, len(deferred))
	for _, e := range deferred {
		if e.kind == effectWriteSentinel && child.sentinelFile == "" {
			continue
		}
		if e.kind == effectCancelContext && child.cancelFn == nil {
			continue
		}
		filtered = append(filtered, e)
	}
	s.compareLifecycleEffects(filtered, witnessed, child, site, "deferred")
}

// compareLifecycleContinuation compares the continuation predicted for a
// signal with the branch the legacy code actually took. The legacy branch
// outcome is recorded at the branch, never derived from the prediction.
func (s *Supervisor) compareLifecycleContinuation(child *childRuntime, predicted, witnessed continuationKind, site signalKind) {
	if predicted != witnessed {
		s.countShadowMismatch(child, site, fmt.Sprintf("continuation: predicted %d, legacy branch %d", predicted, witnessed))
	}
}

// countShadowMismatch increments the mismatch counter and logs the detail.
func (s *Supervisor) countShadowMismatch(child *childRuntime, site signalKind, detail string) {
	s.lifecycleShadowMismatches.Add(1)
	id := ""
	if child != nil {
		id = child.id
	}
	fmt.Fprintf(s.stderrWriter(), "avenor stable: lifecycle shadow mismatch (%s, runtime %s): %s\n", signalKindName(site), id, detail)
}

// lifecycleLockedEffects filters the locked effects (executed under the
// caller's locks) out of a predicted effect list.
func lifecycleLockedEffects(eff []lifecycleEffect) []lifecycleEffect {
	locked := make([]lifecycleEffect, 0, len(eff))
	for _, e := range eff {
		switch e.kind {
		case effectClearPresentation, effectSetAgentPresentation, effectSetSuccessPresentation,
			effectSetSessionEndPresentation, effectSetIdleEndData, effectRecordOutcomeExit, effectSettleTurnWaiters:
			locked = append(locked, e)
		}
	}
	return locked
}

// lifecycleDeferredEffects filters the deferred effects out of a predicted
// effect list.
func lifecycleDeferredEffects(eff []lifecycleEffect) []lifecycleEffect {
	deferred := make([]lifecycleEffect, 0, len(eff))
	for _, e := range eff {
		switch e.kind {
		case effectClearPresentation, effectSetAgentPresentation, effectSetSuccessPresentation,
			effectSetSessionEndPresentation, effectSetIdleEndData, effectRecordOutcomeExit, effectSettleTurnWaiters:
		default:
			deferred = append(deferred, e)
		}
	}
	return deferred
}
