package claudecore

import (
	"context"
	"strings"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"
)

// Values of agent.prompt_submitted's ready_signal: what told avenor the input
// box was taking input.
const (
	ReadySignalProgStatus = "osc7501"
	ReadySignalPane       = "pane"
)

// progStatusSignal tags agent.status events derived from OSC 7501 reports.
const progStatusSignal = "osc7501"

// maxStatusLabel bounds the report text copied into an agent.status label.
const maxStatusLabel = 200

var (
	// progReadyPoll is how often readiness re-reads the root record.
	progReadyPoll = 50 * time.Millisecond
	// progQueryWindow is how long after the pane is ready readiness waits for
	// Claude's detection query. Claude asks within about a second of starting.
	progQueryWindow = 2 * time.Second
	// progIdleGrace is how long after the pane is ready readiness waits for the
	// startup idle report once the query was answered.
	progIdleGrace = 3 * time.Second
	// progFollowUpInterval paces the follow-ups a report starts.
	progFollowUpInterval = 100 * time.Millisecond
	// progPermissionChase bounds how long a blocked permission report keeps
	// re-reading the screen for the dialog, which paints just after the report.
	progPermissionChase = 2 * time.Second
	// progTranscriptBurst is how long the transcript is re-read at the
	// follow-up interval after the turn's root done or idle.
	progTranscriptBurst = 3 * time.Second
	// progFinishSettle is how long a held end_turn waits for a later one
	// before the turn finishes. After a background subagent the final reply's
	// end_turn lands about 130ms after root done, behind an earlier end_turn.
	progFinishSettle = 750 * time.Millisecond
	// progHoldCeiling bounds how long an end_turn waits for the root to stop
	// working; it then settles and finishes like any other held end_turn.
	progHoldCeiling = 10 * time.Minute
)

// progTurn is the session's view of the root OSC 7501 record. Guarded by
// Session.Mu.
type progTurn struct {
	// active is set by the first root report other than clear applied after
	// the detection query was answered. Until then every rule in this file is
	// off and the session behaves as without OSC.
	active bool
	root   terminal.ProgStatus

	// armed and promptSeq mark the last report applied before the prompt was
	// written; only later reports belong to the turn.
	armed              bool
	promptSeq          uint64
	workingSincePrompt bool
	doneSincePrompt    bool

	// endTurnHeld is a transcript end_turn waiting to finish the turn. A zero
	// finishAt holds it until the root leaves working (done, idle, clear or
	// error), reports were lost, or progHoldCeiling passes since heldAt. A
	// root working report before any done holds it again.
	endTurnHeld bool
	finishAt    time.Time
	// heldAt is when the end_turn was first held.
	heldAt time.Time

	burstUntil time.Time

	// chaseUntil bounds the permission chase. chaseLast holds the options of
	// the previous capture of a permission dialog, from the chase or the pane
	// scan, which the next capture must repeat before the dialog is brokered.
	chaseUntil time.Time
	chaseLast  string
	// permBrokered is set once the dialog of the current blocked report was
	// brokered. Answering it does not change the report, so it stops the
	// stale dialog from being brokered again until the root leaves blocked
	// or reports a different dialog.
	permBrokered bool
}

// AttachProgStatus makes the session follow term's OSC 7501 reports, when term
// has the capability and answers the detection query. Reports from a program
// that was never answered are not trusted.
func (s *Session) AttachProgStatus(term terminal.Session) {
	if ps, ok := term.(terminal.ProgStatusSession); ok && ps.ProgStatusAnswering() {
		s.progStatus = ps
	}
}

// ProgStatusChanged returns the attached source's change notification, or nil
// (which never fires) when none is attached.
func (s *Session) ProgStatusChanged() <-chan struct{} {
	if s.progStatus == nil {
		return nil
	}
	return s.progStatus.ProgStatusChanged()
}

// WaitForProgStatusReady runs after WaitForPaneReady. When the terminal answers
// detection queries it waits for Claude's startup idle report, since the input
// box paints before Claude can take a prompt. It gives up and trusts the pane
// when no query arrives within progQueryWindow, or no idle within
// progIdleGrace. It returns the ready_signal value.
func (s *Session) WaitForProgStatusReady(ctx context.Context) string {
	ps := s.progStatus
	if ps == nil || !ps.ProgStatusAnswering() {
		return ReadySignalPane
	}
	paneReady := time.Now()
	ticker := time.NewTicker(progReadyPoll)
	defer ticker.Stop()
	for {
		if root, ok := ps.ProgStatusRoot(); ok && root.State == terminal.ProgIdle && ps.ProgStatusAnswered() {
			return ReadySignalProgStatus
		}
		waited := time.Since(paneReady)
		if waited >= progIdleGrace || (!ps.ProgStatusAnswered() && waited >= progQueryWindow) {
			return ReadySignalPane
		}
		select {
		case <-ctx.Done():
			return ReadySignalPane
		case <-ticker.C:
		}
	}
}

// ArmProgStatusPrompt marks the moment before a prompt is written. Reports
// applied so far belong to the time before the turn.
func (s *Session) ArmProgStatusPrompt() {
	if s.progStatus == nil {
		return
	}
	s.progDrainMu.Lock()
	defer s.progDrainMu.Unlock()
	s.drainProgStatusLocked()
	s.Mu.Lock()
	root, active := s.osc.root, s.osc.active
	s.osc = progTurn{active: active, root: root, armed: true, promptSeq: s.progNext}
	s.Mu.Unlock()
}

// ProgStatusTurnStarted reports whether a root working report arrived after the
// prompt was armed: Claude took the prompt.
func (s *Session) ProgStatusTurnStarted() bool {
	s.drainProgStatus()
	s.Mu.Lock()
	defer s.Mu.Unlock()
	return s.osc.workingSincePrompt
}

// ProgStatusInterrupted reports whether the root record is idle after the turn
// had started working, which is how Claude reports a cancelled or denied turn.
// The startup idle never counts.
func (s *Session) ProgStatusInterrupted() bool {
	s.drainProgStatus()
	s.Mu.Lock()
	defer s.Mu.Unlock()
	return s.osc.workingSincePrompt && s.osc.root.State == terminal.ProgIdle
}

// ProgStatusTick applies new reports and runs the follow-ups they started:
// re-reading the screen for a permission dialog, re-reading the transcript
// after root done or idle, and finishing a held end_turn once it settles. It
// returns how soon to call it again, or 0 when nothing is pending.
func (s *Session) ProgStatusTick() time.Duration {
	if s.progStatus == nil {
		return 0
	}
	s.drainProgStatus()

	now := time.Now()
	s.Mu.Lock()
	if s.osc.endTurnHeld && s.osc.finishAt.IsZero() && !s.osc.heldAt.IsZero() && now.Sub(s.osc.heldAt) >= progHoldCeiling {
		s.releaseHeldEndTurnLocked(now)
	}
	chase := s.permissionChaseDue(now)
	scan := s.Prompted && !s.Finished && (now.Before(s.osc.burstUntil) || s.osc.endTurnHeld && !s.osc.finishAt.IsZero())
	s.Mu.Unlock()

	if chase {
		if out, err := s.Term.Capture(s.Ctx); err == nil && ClassifyPane(out) == PaneStatePermission {
			s.Mu.Lock()
			stable := s.permissionCaptureStableLocked(out)
			s.Mu.Unlock()
			if stable {
				s.brokerTerminalPermission(out)
			}
		}
	}
	if scan {
		s.ScanTranscriptTick()
	}

	now = time.Now()
	s.Mu.Lock()
	finish := !s.Finished && s.osc.endTurnHeld && !s.osc.finishAt.IsZero() && !now.Before(s.osc.finishAt)
	s.Mu.Unlock()
	if finish {
		s.finishEndTurn()
	}

	s.Mu.Lock()
	defer s.Mu.Unlock()
	if s.Finished {
		return 0
	}
	if s.permissionChaseDue(now) || now.Before(s.osc.burstUntil) || s.osc.endTurnHeld && !s.osc.finishAt.IsZero() {
		return progFollowUpInterval
	}
	return 0
}

// permissionChaseDue reports whether a blocked permission report is still
// waiting for its dialog to be brokered. Mu is held.
func (s *Session) permissionChaseDue(now time.Time) bool {
	return s.Prompted && !s.Finished && s.PendingTerminalPerm == nil && !s.osc.permBrokered &&
		s.osc.root.State == terminal.ProgBlocked && s.osc.root.Kind == terminal.ProgKindPermission &&
		now.Before(s.osc.chaseUntil)
}

// permissionCaptureStableLocked records the options of the permission dialog
// in out and reports whether they equal the previous capture's: the same
// options twice in a row mean the dialog has finished painting. Only the
// options are compared, so live output elsewhere on screen cannot keep the
// dialog from ever settling. Mu is held.
func (s *Session) permissionCaptureStableLocked(out string) bool {
	var key strings.Builder
	if perm := ParseTerminalPermission(out); perm != nil {
		for _, o := range perm.Options {
			key.WriteString(o.ID + ". " + o.Label + "\n")
		}
	}
	stable := key.Len() > 0 && key.String() == s.osc.chaseLast
	s.osc.chaseLast = key.String()
	return stable
}

// permissionBrokeredLocked reports whether the dialog of the current blocked
// report was already brokered. Mu is held.
func (s *Session) permissionBrokeredLocked() bool {
	return s.osc.permBrokered && s.osc.root.State == terminal.ProgBlocked
}

// notePermissionBrokeredLocked records that the dialog of the current blocked
// report was brokered and ends its chase. The next dialog needs two captures
// of its own, even when it shows the same options. Mu is held.
func (s *Session) notePermissionBrokeredLocked() {
	s.osc.chaseLast = ""
	if s.osc.active && s.osc.root.State == terminal.ProgBlocked {
		s.osc.permBrokered = true
		s.osc.chaseUntil = time.Time{}
	}
}

// releaseHeldEndTurnLocked lets a held end_turn finish once it settles. Once
// progHoldCeiling has passed, a released end_turn keeps its finish time, so
// repeated reports cannot keep moving it out. Mu is held.
func (s *Session) releaseHeldEndTurnLocked(now time.Time) {
	if !s.osc.endTurnHeld {
		return
	}
	if !s.osc.finishAt.IsZero() && !s.osc.heldAt.IsZero() && now.Sub(s.osc.heldAt) >= progHoldCeiling {
		return
	}
	s.osc.finishAt = now.Add(progFinishSettle)
}

// holdEndTurn decides what a transcript end_turn does while OSC is active.
// It returns false when OSC is inactive and the caller finishes as usual.
// Otherwise the end_turn is held: until the root leaves working when it is
// still working with no done since the prompt (a background subagent ends an
// inner turn first), or else for progFinishSettle, so a later end_turn's
// content is emitted before the turn ends.
func (s *Session) holdEndTurn() bool {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if !s.osc.active {
		return false
	}
	s.osc.endTurnHeld = true
	if s.osc.heldAt.IsZero() {
		s.osc.heldAt = time.Now()
	}
	if s.osc.root.State == terminal.ProgWorking && !s.osc.doneSincePrompt {
		s.osc.finishAt = time.Time{}
	} else {
		s.osc.finishAt = time.Now().Add(progFinishSettle)
	}
	return true
}

func (s *Session) finishEndTurn() {
	if !s.MarkFinished() {
		return
	}
	s.Emit(events.Event{
		Event:     "session.end",
		SessionID: s.SessionID,
		Fields: map[string]any{
			"status":      "done",
			"stop_reason": "end_turn",
		},
	})
}

func (s *Session) drainProgStatus() {
	if s.progStatus == nil {
		return
	}
	s.progDrainMu.Lock()
	defer s.progDrainMu.Unlock()
	s.drainProgStatusLocked()
}

// drainProgStatusLocked applies reports in order. progDrainMu is held, so
// events derived from reports keep report order whichever goroutine drains.
// Reports that arrive before the detection query was answered are skipped:
// Claude reports only after the answer, so they did not come from it.
func (s *Session) drainProgStatusLocked() {
	reports, lost := s.progStatus.ProgStatusSince(s.progNext)
	if !s.progStatus.ProgStatusAnswered() {
		if n := len(reports); n > 0 {
			s.progNext = reports[n-1].Seq
		}
		return
	}
	for _, r := range reports {
		s.progNext = r.Seq
		s.applyProgReport(r)
	}
	if lost {
		// Reports were dropped before they were applied; the retained tail is
		// applied above, and the current root record is the final word. A
		// dropped report may have been the one a held end_turn waits for.
		s.Mu.Lock()
		s.releaseHeldEndTurnLocked(time.Now())
		s.Mu.Unlock()
		if root, ok := s.progStatus.ProgStatusRoot(); ok {
			s.applyProgReport(terminal.ProgReport{Seq: s.progNext, Status: root})
		}
	}
}

// applyProgReport applies one report's lifecycle effect. Only root state
// transitions count: child records and repeats of the current state change
// nothing beyond the recorded root, except that a blocked report with a new
// message is a new dialog. Report text is otherwise display-only.
func (s *Session) applyProgReport(r terminal.ProgReport) {
	st := r.Status
	if st.ID != "" {
		return
	}
	now := time.Now()
	var emit []events.Event

	s.Mu.Lock()
	o := &s.osc
	prev := o.root
	if st.State != terminal.ProgBlocked {
		o.permBrokered = false
		// The dialog is gone; a later one must not match its captures.
		if o.root.State == terminal.ProgBlocked {
			o.chaseLast = ""
		}
	}
	if st.State == terminal.ProgClear {
		o.root = terminal.ProgStatus{}
		s.releaseHeldEndTurnLocked(now)
		s.Mu.Unlock()
		return
	}
	o.active = true
	o.root = st
	newDialog := st.State == terminal.ProgBlocked && prev.State == terminal.ProgBlocked && prev.Msg != st.Msg
	if prev.State == st.State && prev.Kind == st.Kind && !newDialog {
		s.Mu.Unlock()
		return
	}
	turn := o.armed && r.Seq > o.promptSeq
	announce := turn && !s.Finished
	if newDialog {
		o.permBrokered = false
	}

	if prev.State == terminal.ProgBlocked {
		switch st.State {
		case terminal.ProgWorking, terminal.ProgIdle, terminal.ProgDone:
			s.PendingTerminalPerm = nil
		}
	}
	switch st.State {
	case terminal.ProgWorking:
		if turn {
			o.workingSincePrompt = true
			// A clear or error released the held end_turn, but the root is
			// working again with no done yet: hold it again, until
			// progHoldCeiling has passed since heldAt.
			if o.endTurnHeld && !o.doneSincePrompt && now.Sub(o.heldAt) < progHoldCeiling {
				o.finishAt = time.Time{}
			}
		}
		if announce {
			emit = append(emit, s.progStatusEvent("working", "", ""))
		}
	case terminal.ProgBlocked:
		if st.Kind == terminal.ProgKindPermission {
			if turn {
				o.chaseUntil = now.Add(progPermissionChase)
				o.chaseLast = ""
			}
			// A request already pending on entering blocked is this
			// report's dialog, brokered by the pane scan.
			if !newDialog && s.PendingTerminalPerm != nil {
				o.permBrokered = true
			}
		} else if announce {
			emit = append(emit, s.progStatusEvent("waiting", string(st.Kind), st.Msg))
		}
	case terminal.ProgDone, terminal.ProgIdle:
		if turn {
			if st.State == terminal.ProgDone {
				o.doneSincePrompt = true
			}
			o.burstUntil = now.Add(progTranscriptBurst)
			s.releaseHeldEndTurnLocked(now)
		}
		// A late startup idle after the prompt is not a turn waiting for input.
		if announce && st.State == terminal.ProgIdle && o.workingSincePrompt {
			emit = append(emit, s.progStatusEvent("waiting", "idle", ""))
		}
	case terminal.ProgError:
		s.releaseHeldEndTurnLocked(now)
	}
	s.Mu.Unlock()

	for _, ev := range emit {
		s.Emit(ev)
	}
}

func (s *Session) progStatusEvent(phase, reason, label string) events.Event {
	fields := map[string]any{
		"phase":  phase,
		"source": s.Term.Kind(),
		"signal": progStatusSignal,
	}
	if reason != "" {
		fields["reason"] = reason
	}
	if label != "" {
		fields["label"] = truncateRunes(label, maxStatusLabel)
	}
	return events.Event{Event: "agent.status", SessionID: s.SessionID, Fields: fields}
}
