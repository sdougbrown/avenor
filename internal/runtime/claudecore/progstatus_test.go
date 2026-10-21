package claudecore

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"
)

const permissionPane = "Bash command\n  echo hi\nDo you want to proceed?\n❯ 1. Yes\n  2. No\nEsc to cancel\n"

func shortenProgStatusTimings(t *testing.T) {
	t.Helper()
	saved := []time.Duration{progReadyPoll, progQueryWindow, progIdleGrace, progFinishSettle, progFollowUpInterval, progTranscriptBurst}
	progReadyPoll, progQueryWindow, progIdleGrace = 5*time.Millisecond, 100*time.Millisecond, 200*time.Millisecond
	progFinishSettle, progFollowUpInterval, progTranscriptBurst = 50*time.Millisecond, 5*time.Millisecond, 150*time.Millisecond
	t.Cleanup(func() {
		progReadyPoll, progQueryWindow, progIdleGrace = saved[0], saved[1], saved[2]
		progFinishSettle, progFollowUpInterval, progTranscriptBurst = saved[3], saved[4], saved[5]
	})
}

// newProgStatusSession returns a prompted session on a fake PTY that answers
// detection and already reported its startup idle, with a transcript file the
// test appends records to.
func newProgStatusSession(t *testing.T, captures ...string) (*Session, *terminal.FakeSession, string) {
	t.Helper()
	s, term, path := newAttachedSession(t, true, true, captures...)
	term.ReportProgStatus(rootReport(terminal.ProgIdle, ""))
	s.ArmProgStatusPrompt()
	s.Prompted = true
	return s, term, path
}

// newAttachedSession returns a session attached to a fake PTY that has not
// reported anything yet and has no prompt armed.
func newAttachedSession(t *testing.T, answering, answered bool, captures ...string) (*Session, *terminal.FakeSession, string) {
	t.Helper()
	shortenProgStatusTimings(t)
	if len(captures) == 0 {
		captures = []string{workingPane}
	}
	term := terminal.NewFakeSessionQueue("osc", 1, captures)
	term.SetKind("pty")
	term.SetProgStatusAnswering(answering)
	if answered {
		term.AnswerProgStatusQuery()
	}
	path := filepath.Join(t.TempDir(), "session.jsonl")
	s := NewSession(context.Background(), SessionOptions{SessionID: "ses-osc", Transcript: NewTranscriptReader(path), EventsBuf: 64})
	t.Cleanup(s.CancelFn)
	s.Term = term
	s.AttachProgStatus(term)
	return s, term, path
}

func rootReport(state terminal.ProgState, kind terminal.ProgKind) terminal.ProgStatus {
	return terminal.ProgStatus{State: state, Kind: kind, Progress: -1, App: "claude-code"}
}

func appendTranscript(t *testing.T, path string, lines ...string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for _, l := range lines {
		if _, err := f.WriteString(l + "\n"); err != nil {
			t.Fatal(err)
		}
	}
}

func assistantText(text, stopReason string) string {
	sr := ""
	if stopReason != "" {
		sr = `,"stop_reason":"` + stopReason + `"`
	}
	return `{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"` + text + `"}]` + sr + `}}`
}

func eventNames(evs []events.Event) []string {
	out := make([]string, len(evs))
	for i, ev := range evs {
		out[i] = ev.Event
		if ev.Event == "agent.message_chunk" {
			out[i] += ":" + ev.Fields["delta"].(string)
		}
	}
	return out
}

func countEvents(evs []events.Event, name string) int {
	n := 0
	for _, ev := range evs {
		if ev.Event == name {
			n++
		}
	}
	return n
}

// tickUntilFinished runs ProgStatusTick the way runSession's follow-up timer
// does until the session finishes or nothing is pending.
func tickUntilFinished(t *testing.T, s *Session) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		d := s.ProgStatusTick()
		s.Mu.Lock()
		finished := s.Finished
		s.Mu.Unlock()
		if finished || d == 0 {
			return
		}
		time.Sleep(d)
	}
	t.Fatal("follow-ups still pending after 2s")
}

func TestWaitForProgStatusReady(t *testing.T) {
	shortenProgStatusTimings(t)
	cases := []struct {
		name      string
		answering bool
		answered  bool
		idleAfter time.Duration // < 0: never
		want      string
		maxWait   time.Duration
	}{
		{"waits for startup idle", true, true, 50 * time.Millisecond, ReadySignalProgStatus, 150 * time.Millisecond},
		{"answered but no idle", true, true, -1, ReadySignalPane, 400 * time.Millisecond},
		{"query never seen", true, false, -1, ReadySignalPane, 180 * time.Millisecond},
		{"not answering", false, false, -1, ReadySignalPane, 20 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			term := terminal.NewFakeSession("ready", 1, readyPane)
			term.SetProgStatusAnswering(tc.answering)
			if tc.answered {
				term.AnswerProgStatusQuery()
			}
			s := NewSession(context.Background(), SessionOptions{SessionID: "ses-ready", EventsBuf: 8})
			defer s.CancelFn()
			s.Term = term
			s.AttachProgStatus(term)
			if tc.idleAfter >= 0 {
				time.AfterFunc(tc.idleAfter, func() { term.ReportProgStatus(rootReport(terminal.ProgIdle, "")) })
			}
			start := time.Now()
			got := s.WaitForProgStatusReady(context.Background())
			if got != tc.want {
				t.Fatalf("ready signal = %q, want %q", got, tc.want)
			}
			if el := time.Since(start); el > tc.maxWait {
				t.Fatalf("readiness took %v, want <= %v", el, tc.maxWait)
			}
		})
	}
}

func TestProgStatusStartupIdleIsNeitherCompletionNorCancel(t *testing.T) {
	s, _, _ := newProgStatusSession(t)
	if s.ProgStatusInterrupted() {
		t.Fatal("startup idle counted as a cancelled turn")
	}
	if d := s.ProgStatusTick(); d != 0 {
		t.Fatalf("startup idle left follow-ups pending (%v)", d)
	}
	if got := drainSessionEvents(s.Events); len(got) != 0 {
		t.Fatalf("startup idle emitted %v", eventNames(got))
	}
}

func TestProgStatusWorkingStartsTheTurnOnce(t *testing.T) {
	s, term, _ := newProgStatusSession(t)
	if s.ProgStatusTurnStarted() {
		t.Fatal("turn started before any working report")
	}
	working := rootReport(terminal.ProgWorking, "")
	term.ReportProgStatus(working)
	working.Progress = 40
	term.ReportProgStatus(working)
	if !s.ProgStatusTurnStarted() {
		t.Fatal("working after the prompt did not stop the Enter resend")
	}
	got := drainSessionEvents(s.Events)
	if len(got) != 1 || got[0].Fields["phase"] != "working" || got[0].Fields["signal"] != "osc7501" || got[0].Fields["source"] != "pty" {
		t.Fatalf("events = %+v, want one working agent.status from osc7501", got)
	}

	term.ReportProgStatus(rootReport(terminal.ProgIdle, ""))
	if !s.ProgStatusInterrupted() {
		t.Fatal("idle after working not reported as an interrupted turn")
	}
}

func TestProgStatusWorkingBeforeThePromptDoesNotCount(t *testing.T) {
	s, term, _ := newProgStatusSession(t)
	term.ReportProgStatus(rootReport(terminal.ProgWorking, ""))
	term.ReportProgStatus(rootReport(terminal.ProgIdle, ""))
	s.ArmProgStatusPrompt()
	if s.ProgStatusTurnStarted() || s.ProgStatusInterrupted() {
		t.Fatal("reports applied before the prompt was armed counted for the turn")
	}
}

func TestProgStatusBlockedPermissionBrokersAndSurvivesPaneIdle(t *testing.T) {
	// The dialog paints just after the report, so the first capture misses it.
	s, term, _ := newProgStatusSession(t, workingPane, permissionPane)
	term.ReportProgStatus(rootReport(terminal.ProgWorking, ""))
	term.ReportProgStatus(rootReport(terminal.ProgBlocked, terminal.ProgKindPermission))
	if d := s.ProgStatusTick(); d == 0 {
		t.Fatal("blocked permission without a dialog yet scheduled no retry")
	}
	s.ProgStatusTick()
	s.ProgStatusTick()
	got := drainSessionEvents(s.Events)
	if countEvents(got, "permission.request") != 1 || countEvents(got, "agent.status") != 1 {
		t.Fatalf("events = %v, want working then one permission.request", eventNames(got))
	}
	s.Mu.Lock()
	perm := s.PendingTerminalPerm
	s.Mu.Unlock()
	if perm == nil || len(perm.Options) != 2 {
		t.Fatalf("pending permission = %+v", perm)
	}

	term.QueueCapture(readyPane)
	s.ScanTerminalTick()
	s.Mu.Lock()
	kept := s.PendingTerminalPerm
	s.Mu.Unlock()
	if kept != perm {
		t.Fatal("pane idle cleared the permission while Claude still reported blocked")
	}
	if got := drainSessionEvents(s.Events); len(got) != 0 {
		t.Fatalf("pane idle emitted %v with OSC active", eventNames(got))
	}

	term.ReportProgStatus(rootReport(terminal.ProgWorking, ""))
	s.ProgStatusTick()
	s.Mu.Lock()
	cleared := s.PendingTerminalPerm
	s.Mu.Unlock()
	if cleared != nil {
		t.Fatal("blocked to working left the permission pending")
	}
}

func TestProgStatusAnsweredPermissionIsNotBrokeredAgain(t *testing.T) {
	s, term, _ := newProgStatusSession(t, permissionPane)
	term.ReportProgStatus(rootReport(terminal.ProgWorking, ""))
	blocked := rootReport(terminal.ProgBlocked, terminal.ProgKindPermission)
	blocked.Msg = "approve Bash: echo hi"
	term.ReportProgStatus(blocked)
	s.ProgStatusTick()
	s.ProgStatusTick()
	if n := countEvents(drainSessionEvents(s.Events), "permission.request"); n != 1 {
		t.Fatalf("permission.request count = %d, want 1", n)
	}

	// AnswerPermission clears the request; Claude stays blocked and the
	// answered dialog stays on screen until it repaints.
	s.Mu.Lock()
	s.PendingTerminalPerm = nil
	s.Mu.Unlock()
	if d := s.ProgStatusTick(); d != 0 {
		t.Fatalf("chase still pending (%v) after the dialog was brokered", d)
	}
	s.ProgStatusTick()
	s.ScanTerminalTick()
	if got := drainSessionEvents(s.Events); countEvents(got, "permission.request") != 0 {
		t.Fatalf("answered dialog brokered again: %v", eventNames(got))
	}

	next := blocked
	next.Msg = "approve Bash: echo again"
	term.ReportProgStatus(next)
	s.ProgStatusTick()
	s.ProgStatusTick()
	if n := countEvents(drainSessionEvents(s.Events), "permission.request"); n != 1 {
		t.Fatalf("new dialog while blocked: permission.request count = %d, want 1", n)
	}
}

func TestProgStatusChaseWaitsForTheDialogToFinishPainting(t *testing.T) {
	half := "Bash command\n  echo hi\nDo you want to proceed?\n❯ 1. Yes\n"
	s, term, _ := newProgStatusSession(t, half, permissionPane)
	term.ReportProgStatus(rootReport(terminal.ProgWorking, ""))
	term.ReportProgStatus(rootReport(terminal.ProgBlocked, terminal.ProgKindPermission))
	for i := 0; i < 3; i++ {
		s.ProgStatusTick()
	}
	if n := countEvents(drainSessionEvents(s.Events), "permission.request"); n != 1 {
		t.Fatalf("permission.request count = %d, want 1", n)
	}
	s.Mu.Lock()
	perm := s.PendingTerminalPerm
	s.Mu.Unlock()
	if perm == nil || len(perm.Options) != 2 {
		t.Fatalf("brokered permission = %+v, want the fully painted dialog", perm)
	}
}

func TestProgStatusPaneScanWaitsForTheDialogToFinishPainting(t *testing.T) {
	half := "Bash command\n  echo hi\nDo you want to proceed?\n❯ 1. Yes\n"
	s, term, _ := newProgStatusSession(t, half, half, permissionPane)
	term.ReportProgStatus(rootReport(terminal.ProgWorking, ""))
	term.ReportProgStatus(rootReport(terminal.ProgBlocked, terminal.ProgKindPermission))
	s.ScanTerminalTick()
	for i := 0; i < 3; i++ {
		s.ProgStatusTick()
	}
	if n := countEvents(drainSessionEvents(s.Events), "permission.request"); n != 1 {
		t.Fatalf("permission.request count = %d, want 1", n)
	}
	s.Mu.Lock()
	perm := s.PendingTerminalPerm
	s.Mu.Unlock()
	if perm == nil || len(perm.Options) != 2 {
		t.Fatalf("brokered permission = %+v, want the fully painted dialog", perm)
	}
}

func TestProgStatusPaneScanBrokersAStableDialog(t *testing.T) {
	// No blocked report, so no chase: the pane scan is the only broker. Each
	// scan captures twice (classify, then parse).
	half := "Bash command\n  echo hi\nDo you want to proceed?\n❯ 1. Yes\n"
	s, term, _ := newProgStatusSession(t, half, half, permissionPane)
	term.ReportProgStatus(rootReport(terminal.ProgWorking, ""))
	s.ProgStatusTick()
	drainSessionEvents(s.Events)

	s.ScanTerminalTick()
	s.ScanTerminalTick()
	if n := countEvents(drainSessionEvents(s.Events), "permission.request"); n != 0 {
		t.Fatalf("permission.request count = %d before two matching captures, want 0", n)
	}
	s.ScanTerminalTick()
	s.ScanTerminalTick()
	if n := countEvents(drainSessionEvents(s.Events), "permission.request"); n != 1 {
		t.Fatalf("permission.request count = %d, want 1", n)
	}
	s.Mu.Lock()
	perm := s.PendingTerminalPerm
	s.Mu.Unlock()
	if perm == nil || len(perm.Options) != 2 {
		t.Fatalf("brokered permission = %+v, want the fully painted dialog", perm)
	}
}

func TestProgStatusTranscriptRecordsDoNotEndAWait(t *testing.T) {
	s, term, path := newProgStatusSession(t)
	term.ReportProgStatus(rootReport(terminal.ProgWorking, ""))
	q := rootReport(terminal.ProgBlocked, terminal.ProgKindQuestion)
	q.Msg = "answer: red or blue?"
	term.ReportProgStatus(q)
	s.ProgStatusTick()
	drainSessionEvents(s.Events)

	// The record written just before the blocked report is read after it.
	appendTranscript(t, path, assistantText("asking", ""))
	s.ScanTranscriptTick()
	got := drainSessionEvents(s.Events)
	if len(got) != 1 || got[0].Event != "agent.message_chunk" {
		t.Fatalf("events = %v, want only the chunk while Claude reports blocked", eventNames(got))
	}
}

func TestProgStatusQuestionWaitsWithoutPermissionRequest(t *testing.T) {
	s, term, _ := newProgStatusSession(t, permissionPane)
	term.ReportProgStatus(rootReport(terminal.ProgWorking, ""))
	q := rootReport(terminal.ProgBlocked, terminal.ProgKindQuestion)
	q.Msg = "answer: red or blue?"
	term.ReportProgStatus(q)
	term.ReportProgStatus(q)
	s.ProgStatusTick()
	got := drainSessionEvents(s.Events)
	if len(got) != 2 {
		t.Fatalf("events = %v, want working then one waiting", eventNames(got))
	}
	w := got[1].Fields
	if w["phase"] != "waiting" || w["reason"] != "question" || w["label"] != "answer: red or blue?" || w["signal"] != "osc7501" {
		t.Fatalf("waiting event fields = %+v", w)
	}
	if countEvents(got, "permission.request") != 0 {
		t.Fatal("question produced a permission.request")
	}
}

func TestProgStatusHoldsEndTurnWhileRootWorking(t *testing.T) {
	s, term, path := newProgStatusSession(t)
	term.ReportProgStatus(rootReport(terminal.ProgWorking, ""))
	// A background subagent: an inner end_turn while the root keeps working.
	appendTranscript(t, path, assistantText("started a subagent", "end_turn"))
	s.ScanTranscriptTick()
	term.ReportProgStatus(terminal.ProgStatus{ID: "a1", State: terminal.ProgDone, Progress: -1})
	term.ReportProgStatus(terminal.ProgStatus{ID: "a1", State: terminal.ProgClear, Progress: -1})
	appendTranscript(t, path, assistantText("subagent said OK", ""))
	s.ScanTranscriptTick()
	tickUntilFinished(t, s)
	s.Mu.Lock()
	finished := s.Finished
	s.Mu.Unlock()
	if finished {
		t.Fatalf("turn finished while root was still working: %v", eventNames(drainSessionEvents(s.Events)))
	}

	term.ReportProgStatus(rootReport(terminal.ProgDone, ""))
	s.ProgStatusTick()
	appendTranscript(t, path, assistantText("DONE", "end_turn"))
	tickUntilFinished(t, s)

	got := eventNames(drainSessionEvents(s.Events))
	want := []string{"agent.status", "agent.message_chunk:started a subagent", "agent.message_chunk:subagent said OK", "agent.message_chunk:DONE", "session.end"}
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events = %v, want %v", got, want)
		}
	}
}

func TestProgStatusHeldEndTurnFinishesWhenRootStopsWorking(t *testing.T) {
	cases := map[string]func(*terminal.FakeSession){
		// A terminal reset reaches the log as a root clear too.
		"root clear": func(term *terminal.FakeSession) { term.ReportProgStatus(rootReport(terminal.ProgClear, "")) },
		"root error": func(term *terminal.FakeSession) { term.ReportProgStatus(rootReport(terminal.ProgError, "")) },
		"lost reports": func(term *terminal.FakeSession) {
			for i := 0; i < 300; i++ {
				term.ReportProgStatus(terminal.ProgStatus{ID: "a1", State: terminal.ProgWorking, Progress: -1})
			}
		},
	}
	for name, release := range cases {
		t.Run(name, func(t *testing.T) {
			s, term, path := newProgStatusSession(t)
			term.ReportProgStatus(rootReport(terminal.ProgWorking, ""))
			appendTranscript(t, path, assistantText("OK", "end_turn"))
			s.ScanTranscriptTick()
			release(term)
			tickUntilFinished(t, s)
			if n := countEvents(drainSessionEvents(s.Events), "session.end"); n != 1 {
				t.Fatalf("session.end count = %d, want 1", n)
			}
		})
	}
}

func TestProgStatusHeldEndTurnHeldAgainWhenRootResumesWorking(t *testing.T) {
	for name, state := range map[string]terminal.ProgState{"after clear": terminal.ProgClear, "after error": terminal.ProgError} {
		t.Run(name, func(t *testing.T) {
			s, term, path := newProgStatusSession(t)
			term.ReportProgStatus(rootReport(terminal.ProgWorking, ""))
			appendTranscript(t, path, assistantText("started a subagent", "end_turn"))
			s.ScanTranscriptTick()
			term.ReportProgStatus(rootReport(state, ""))
			term.ReportProgStatus(rootReport(terminal.ProgWorking, ""))
			s.ProgStatusTick()
			time.Sleep(2 * progFinishSettle)
			tickUntilFinished(t, s)
			if n := countEvents(drainSessionEvents(s.Events), "session.end"); n != 0 {
				t.Fatal("inner end_turn finished the turn after the root resumed working")
			}

			term.ReportProgStatus(rootReport(terminal.ProgDone, ""))
			appendTranscript(t, path, assistantText("DONE", "end_turn"))
			tickUntilFinished(t, s)
			if n := countEvents(drainSessionEvents(s.Events), "session.end"); n != 1 {
				t.Fatalf("session.end count = %d, want 1 after root done", n)
			}
		})
	}
}

func TestProgStatusHeldEndTurnReleasedAtCeiling(t *testing.T) {
	s, term, path := newProgStatusSession(t)
	savedCeiling := progHoldCeiling
	progHoldCeiling = 50 * time.Millisecond
	t.Cleanup(func() { progHoldCeiling = savedCeiling })
	term.ReportProgStatus(rootReport(terminal.ProgWorking, ""))
	appendTranscript(t, path, assistantText("OK", "end_turn"))
	s.ScanTranscriptTick()
	time.Sleep(60 * time.Millisecond)
	s.ProgStatusTick()
	// Reports that keep returning to working, faster than the settle, do not
	// hold it again.
	deadline := time.Now().Add(4 * progFinishSettle)
	for time.Now().Before(deadline) {
		term.ReportProgStatus(rootReport(terminal.ProgBlocked, terminal.ProgKindQuestion))
		term.ReportProgStatus(rootReport(terminal.ProgWorking, ""))
		s.ProgStatusTick()
		time.Sleep(progFinishSettle / 5)
	}
	if n := countEvents(drainSessionEvents(s.Events), "session.end"); n != 1 {
		t.Fatalf("session.end count = %d, want 1 once the ceiling passed", n)
	}
}

func TestProgStatusEndTurnAfterDoneFinishesOnce(t *testing.T) {
	s, term, path := newProgStatusSession(t)
	term.ReportProgStatus(rootReport(terminal.ProgWorking, ""))
	term.ReportProgStatus(rootReport(terminal.ProgDone, ""))
	if d := s.ProgStatusTick(); d == 0 {
		t.Fatal("root done started no transcript follow-up")
	}
	appendTranscript(t, path, assistantText("OK", "end_turn"))
	tickUntilFinished(t, s)
	got := drainSessionEvents(s.Events)
	if countEvents(got, "session.end") != 1 || got[len(got)-1].Event != "session.end" || got[len(got)-1].Fields["stop_reason"] != "end_turn" {
		t.Fatalf("events = %v, want content then one end_turn session.end", eventNames(got))
	}
}

func TestProgStatusDoneAloneNeverFinishes(t *testing.T) {
	s, term, _ := newProgStatusSession(t)
	term.ReportProgStatus(rootReport(terminal.ProgWorking, ""))
	term.ReportProgStatus(rootReport(terminal.ProgDone, ""))
	tickUntilFinished(t, s)
	if countEvents(drainSessionEvents(s.Events), "session.end") != 0 {
		t.Fatal("root done without a transcript end_turn finished the turn")
	}
}

func TestProgStatusReportsIgnoredUnlessQueryAnswered(t *testing.T) {
	for name, answering := range map[string]bool{"not answering": false, "query not answered": true} {
		t.Run(name, func(t *testing.T) {
			s, term, path := newAttachedSession(t, answering, false)
			term.ReportProgStatus(rootReport(terminal.ProgIdle, ""))
			s.ArmProgStatusPrompt()
			s.Prompted = true
			term.ReportProgStatus(rootReport(terminal.ProgWorking, ""))
			appendTranscript(t, path, assistantText("OK", "end_turn"))
			s.ScanTranscriptTick()
			got := eventNames(drainSessionEvents(s.Events))
			if len(got) != 2 || got[0] != "agent.message_chunk:OK" || got[1] != "session.end" {
				t.Fatalf("events = %v, want the chunk then session.end with OSC off", got)
			}
		})
	}
}

func TestProgStatusLateStartupIdleIsNotAWait(t *testing.T) {
	s, term, _ := newAttachedSession(t, true, true)
	s.ArmProgStatusPrompt()
	s.Prompted = true
	term.ReportProgStatus(rootReport(terminal.ProgIdle, ""))
	s.ProgStatusTick()
	if got := drainSessionEvents(s.Events); len(got) != 0 {
		t.Fatalf("startup idle after the prompt emitted %v", eventNames(got))
	}
}

func TestProgStatusSilentSessionFinishesAfterEndTurnSettle(t *testing.T) {
	// A session that can observe reports but never received one behaves as
	// without OSC: an end_turn ends the turn after the transcript settle, with
	// the content written during the settle emitted first.
	term := terminal.NewFakeSession("silent", 1, workingPane)
	path := filepath.Join(t.TempDir(), "session.jsonl")
	s := NewSession(context.Background(), SessionOptions{SessionID: "ses-silent", Transcript: NewTranscriptReader(path), EventsBuf: 16})
	defer s.CancelFn()
	s.Term = term
	s.AttachProgStatus(term)
	s.ArmProgStatusPrompt()
	s.Prompted = true
	appendTranscript(t, path, assistantText("first", "end_turn"))
	stubEndTurnSettle(t, func() { appendTranscript(t, path, assistantText("second", "end_turn")) })
	s.ScanTranscriptTick()
	got := eventNames(drainSessionEvents(s.Events))
	if len(got) != 3 || got[0] != "agent.message_chunk:first" || got[1] != "agent.message_chunk:second" || got[2] != "session.end" {
		t.Fatalf("events = %v, want both chunks then session.end", got)
	}
}
