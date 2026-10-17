package stable

// Imperative adapter for the childRuntime lifecycle machine.
//
// The pure machine in runtime_lifecycle.go only predicts. This file commits
// the prediction field by field, executes the owned locked effects — activity
// and attempt count with their mirrors, presentation, the settled/finalized
// waiter release, the recorded outcome exit code and the parked mirror — and
// compares the remaining still-legacy-owned mirrors (completion, shutdown,
// writer) against the prediction as shadow evidence. The legacy closure
// performs only the still-legacy-owned locked mutations; it is deleted once
// every effect is owned. applyLifecycle requires the caller to hold child.mu
// and acquires no lock. applyLifecycleWriteLocked requires the caller to hold
// writeMu -> child.mu and is the only path that commits the shutdown and
// writer dimensions. Errors are returned to the caller and must be reported
// with reportLifecycleError outside every lock.

import (
	"errors"
	"fmt"
	"strings"

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

// legacyLifecycleWitness records the still-legacy-owned locked effects the
// closure actually performed under the caller's locks, in execution order.
// Owned effects (activity/count, presentation, the settled/finalized waiter
// release) are executed by the adapter and never witnessed; deferred legacy
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

// errLifecycleShadowDiag marks an error that carries only shadow-mismatch
// detail lines collected inside the caller's critical section. The outer lock
// owner emits those lines after every enclosing lock is released (decision 7);
// the mismatch counter was already incremented at detection, so this is not
// an illegal transition.
var errLifecycleShadowDiag = errors.New("lifecycle shadow mismatch")

// lifecycleShadowDiagError bundles the mismatch detail lines of one shadow
// transaction so they are emitted by the caller's existing post-unlock
// reportLifecycleError call instead of being written under the caller's
// locks.
type lifecycleShadowDiagError struct {
	details []string
}

func (e *lifecycleShadowDiagError) Error() string { return strings.Join(e.details, "; ") }
func (e *lifecycleShadowDiagError) Unwrap() error { return errLifecycleShadowDiag }

// lifecycleTransitionFailed reports whether err is an illegal-transition
// error rather than collected shadow-mismatch diagnostics. A mismatch never
// aborts the legacy operation or the later deferred comparisons.
func lifecycleTransitionFailed(err error) bool {
	return err != nil && !errors.Is(err, errLifecycleShadowDiag)
}

// errLifecycleFaultRecovery marks a slot-wait failure whose fault fail-safe
// already ended the run, so the caller must not layer an idle-context end on
// top of the ended state.
var errLifecycleFaultRecovery = errors.New("lifecycle fault recovery ended the run")

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
	var mismatchDetails []string
	s.applyOwnedLifecycleEffects(child, predicted, sig.kind, eff)
	s.compareShadowTransaction(child, pre, prePhase, preLabel, preExit, preReason, predicted, eff, witness, sig.kind, &mismatchDetails)
	if len(mismatchDetails) > 0 {
		// The mismatch counter was already incremented; bundle the detail lines
		// so the caller's post-unlock report emits them outside every lock.
		return cont, eff, &lifecycleShadowDiagError{details: mismatchDetails}
	}
	return cont, eff, nil
}

// applyOwnedLifecycleEffects executes the locked lifecycle effects this
// adapter owns and writes the mirrors derived from them: the activity mirrors
// (active, activeAttempts, kept synchronized to the machine count for both
// classes), presentation (phase, phaseLabel), the idle-end data (exitCode,
// lastStopReason), the recorded outcome exit code and the waiter release of a
// settled or finalized attempt. The parked mirror is written only by the
// signals the legacy code parked with: parkRequested, localSlotAcquired and
// parkWaitEnded; every other signal leaves the mirror alone, and the final
// publication deliberately retains the obsolete parked value until the mirror
// is deleted.
// The completion publication's waiter release is not owned here; complete()
// keeps its legacy settle until completion ownership moves.
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
			if site == signalAttemptSettled || site == signalWorkflowFinalized || site == signalFault {
				child.settleTurnLocked()
			}
		}
	}
	child.activeAttempts = predicted.attempts
	child.active = predicted.attempts > 0
	switch site {
	case signalParkRequested, signalLocalSlotAcquired, signalParkWaitEnded:
		child.parked = predicted.isParked()
	}
}

// reportLifecycleError counts every illegal transition and logs it. It must
// be called outside all locks. An error that carries only
// collected shadow-mismatch details is emitted here line by line; its
// mismatches were counted at detection, so the transition counter is not
// incremented for it.
func (s *Supervisor) reportLifecycleError(child *childRuntime, err error) {
	var diag *lifecycleShadowDiagError
	if errors.As(err, &diag) {
		id := ""
		if child != nil {
			id = child.id
		}
		for _, detail := range diag.details {
			fmt.Fprintf(s.stderrWriter(), "avenor stable: lifecycle shadow mismatch (runtime %s): %s\n", id, detail)
		}
		return
	}
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
func (s *Supervisor) compareShadowTransaction(child *childRuntime, pre lifecycleState, prePhase, preLabel string, preExit int, preReason string, predicted lifecycleState, eff []lifecycleEffect, witness legacyLifecycleWitness, site signalKind, sink *[]string) {
	// Adapter consistency: the mirrors this adapter owns must match the state
	// it just committed. These are not shadow comparisons — the adapter writes
	// the mirrors itself — but a failed write or stale read still indicates a
	// bug, so the shared mismatch counter records it.
	if child.activeAttempts != predicted.attempts {
		s.countMirrorMismatch(child, site, fmt.Sprintf("attempts mirror: lc=%d activeAttempts=%d", predicted.attempts, child.activeAttempts), sink)
	}
	if child.active != (predicted.attempts > 0) {
		s.countMirrorMismatch(child, site, fmt.Sprintf("active mirror: attempts=%d active=%v", predicted.attempts, child.active), sink)
	}
	// Presentation mirror: apply the predicted presentation effects to the pre
	// snapshot and compare with the post-values the adapter wrote.
	wantPhase, wantLabel := applyLifecyclePresentation(prePhase, preLabel, eff)
	if child.phase != wantPhase || child.phaseLabel != wantLabel {
		s.countMirrorMismatch(child, site, fmt.Sprintf("presentation mirror: post=(%q,%q) predicted=(%q,%q)", child.phase, child.phaseLabel, wantPhase, wantLabel), sink)
	}
	// Parked mirror: the adapter writes it for the park, slot-acquired and
	// wait-ended signals; a divergence here means the mirror write failed or
	// a rogue writer touched the field. Compared exactly until final
	// publication, where the legacy parked value may be obsolete and is
	// retained until the mirror is deleted.
	if predicted.completion != completionPublished && child.parked != predicted.isParked() {
		s.countMirrorMismatch(child, site, fmt.Sprintf("parked: lc=%v mirror=%v", predicted.isParked(), child.parked), sink)
	}
	// Still-legacy-owned mirrors, compared as shadow evidence against the
	// prediction.
	// Completion mirror: legacy completed covers both admission-closed and
	// published.
	if child.completed != (predicted.completion != completionOpen) {
		s.countShadowMismatch(child, site, fmt.Sprintf("completed: legacy=%v lc completion=%d", child.completed, predicted.completion), sink)
	}
	// Shutdown mirror.
	if child.shuttingDown != predicted.isShuttingDown() {
		s.countShadowMismatch(child, site, fmt.Sprintf("shuttingDown: legacy=%v lc=%v", child.shuttingDown, predicted.isShuttingDown()), sink)
	}
	// Writer mirror: legacy writerClosing stays true after writerClosed.
	switch predicted.writer {
	case writerOpen:
		if child.writerClosing || child.writerClosed {
			s.countShadowMismatch(child, site, fmt.Sprintf("writer open: legacy closing=%v closed=%v", child.writerClosing, child.writerClosed), sink)
		}
	case writerClosing:
		if !child.writerClosing || child.writerClosed {
			s.countShadowMismatch(child, site, fmt.Sprintf("writer closing: legacy closing=%v closed=%v", child.writerClosing, child.writerClosed), sink)
		}
	case writerClosed:
		if !child.writerClosed {
			s.countShadowMismatch(child, site, "writer closed: legacy writerClosed=false", sink)
		}
	}
	// Effective capacity exclusion (completed || parked) at all times.
	if (child.completed || child.parked) != (predicted.isCompleted() || predicted.isParked()) {
		s.countShadowMismatch(child, site, fmt.Sprintf(
			"capacity exclusion: legacy=(completed=%v,parked=%v) lc=(completed=%v,parked=%v)",
			child.completed, child.parked, predicted.isCompleted(), predicted.isParked()), sink)
	}
	// Terminal data written by predicted locked effects. The recorded outcome
	// exit code is an adapter-owned mirror checked for consistency against
	// its write; the idle-end and session-end data are adapter-owned mirrors
	// too.
	for _, e := range eff {
		switch e.kind {
		case effectRecordOutcomeExit:
			if child.exitCode != e.exitCode {
				s.countMirrorMismatch(child, site, fmt.Sprintf("exitCode mirror: post=%d predicted=%d", child.exitCode, e.exitCode), sink)
			}
		case effectSetIdleEndData:
			if child.exitCode != e.exitCode {
				s.countMirrorMismatch(child, site, fmt.Sprintf("idle exitCode mirror: post=%d predicted=%d", child.exitCode, e.exitCode), sink)
			}
			if child.lastStopReason != e.reason {
				s.countMirrorMismatch(child, site, fmt.Sprintf("idle stop reason mirror: post=%q predicted=%q", child.lastStopReason, e.reason), sink)
			}
		case effectSetSessionEndPresentation:
			if e.reason != "" && child.lastStopReason != e.reason {
				s.countMirrorMismatch(child, site, fmt.Sprintf("session end stop reason mirror: post=%q predicted=%q", child.lastStopReason, e.reason), sink)
			}
		}
	}
	// Locked-effect witness. Only the still-legacy-owned locked effects are
	// witnessed by the legacy closure; owned effects are executed by the
	// adapter. The sink collects detail lines so nothing is printed while the
	// caller's locks are held.
	s.compareLifecycleEffects(lifecycleLegacyLockedEffects(eff, site), witness.locked, child, site, "locked", sink)
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
func (s *Supervisor) compareLifecycleEffects(predicted, witnessed []lifecycleEffect, child *childRuntime, site signalKind, class string, sink *[]string) {
	if len(predicted) != len(witnessed) {
		s.countShadowMismatch(child, site, fmt.Sprintf("%s effects: predicted %d %v, witnessed %d %v", class, len(predicted), predicted, len(witnessed), witnessed), sink)
		return
	}
	for i := range predicted {
		p, w := predicted[i], witnessed[i]
		if p.kind != w.kind {
			s.countShadowMismatch(child, site, fmt.Sprintf("%s effect %d: predicted %d, witnessed %d", class, i, p.kind, w.kind), sink)
			continue
		}
		switch p.kind {
		case effectRecordOutcomeExit, effectRecordWorkflowExit, effectSetIdleEndData, effectEmitSessionEnd:
			if p.exitCode != w.exitCode || p.reason != w.reason {
				s.countShadowMismatch(child, site, fmt.Sprintf("%s effect %d payload: predicted (%d,%q), witnessed (%d,%q)", class, i, p.exitCode, p.reason, w.exitCode, w.reason), sink)
			}
		case effectWriteSentinel:
			// The session ID is caller-resolved (the machine carries only what
			// the result snapshot had); an empty predicted value means the
			// caller resolves the session outside the transaction.
			if p.exitCode != w.exitCode || p.reason != w.reason || (p.sessionID != "" && p.sessionID != w.sessionID) {
				s.countShadowMismatch(child, site, fmt.Sprintf("%s sentinel payload: predicted (%d,%q,%q), witnessed (%d,%q,%q)", class, p.exitCode, p.reason, p.sessionID, w.exitCode, w.reason, w.sessionID), sink)
			}
		case effectPublishCompletion:
			if p.boundary != w.boundary {
				s.countShadowMismatch(child, site, fmt.Sprintf("%s boundary: predicted %d, witnessed %d", class, p.boundary, w.boundary), sink)
			}
		}
	}
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
	_, _, err := s.applyLifecycle(child, lifecycleSignal{kind: signalFault, class: child.lc.class}, nil)
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
// sentinel file. Effects the adapter already executed under the caller's
// locks (activity, presentation, recorded exit, waiter release) are not
// deferred and are ignored here.
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
	s.compareLifecycleEffects(filtered, witnessed, child, site, "deferred", nil)
}

// countShadowMismatch increments the mismatch counter and records the detail
// of a failed comparison against a still-legacy-owned mirror or witnessed
// effect. A nonnil sink collects the formatted line for emission after the
// caller's locks are released; a nil sink (out-of-lock comparators, tests)
// emits the line immediately.
func (s *Supervisor) countShadowMismatch(child *childRuntime, site signalKind, detail string, sink *[]string) {
	s.countLifecycleComparisonMismatch(child, site, "lifecycle shadow mismatch", detail, sink)
}

// countMirrorMismatch records a failed adapter consistency check: an owned
// mirror does not match the state the adapter just committed. These are not
// shadow comparisons — the adapter writes the mirror itself — but they still
// indicate a bug and share the mismatch counter.
func (s *Supervisor) countMirrorMismatch(child *childRuntime, site signalKind, detail string, sink *[]string) {
	s.countLifecycleComparisonMismatch(child, site, "lifecycle mirror mismatch", detail, sink)
}

func (s *Supervisor) countLifecycleComparisonMismatch(child *childRuntime, site signalKind, class, detail string, sink *[]string) {
	s.lifecycleShadowMismatches.Add(1)
	id := ""
	if child != nil {
		id = child.id
	}
	line := fmt.Sprintf("%s (%s, runtime %s): %s", class, signalKindName(site), id, detail)
	if sink != nil {
		*sink = append(*sink, line)
		return
	}
	fmt.Fprintf(s.stderrWriter(), "avenor stable: %s\n", line)
}

// lifecycleLegacyLockedEffects filters the locked effects that are still
// legacy-owned out of a predicted effect list: the completion publication's
// waiter release (until completion ownership). Owned effects are executed by
// the adapter and carry no witness.
func lifecycleLegacyLockedEffects(eff []lifecycleEffect, site signalKind) []lifecycleEffect {
	locked := make([]lifecycleEffect, 0, len(eff))
	for _, e := range eff {
		if e.kind == effectSettleTurnWaiters && site == signalCompletionPublished {
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
