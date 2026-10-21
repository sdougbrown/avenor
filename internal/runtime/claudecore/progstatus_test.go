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
	shortenProgStatusTimings(t)
	if len(captures) == 0 {
		captures = []string{workingPane}
	}
	term := terminal.NewFakeSessionQueue("osc", 1, captures)
	term.SetKind("pty")
	term.SetProgStatusAnswering(true)
	term.AnswerProgStatusQuery()
	path := filepath.Join(t.TempDir(), "session.jsonl")
	s := NewSession(context.Background(), SessionOptions{SessionID: "ses-osc", Transcript: NewTranscriptReader(path), EventsBuf: 64})
	t.Cleanup(s.CancelFn)
	s.Term = term
	s.AttachProgStatus(term)
	term.ReportProgStatus(rootReport(terminal.ProgIdle, ""))
	s.ArmProgStatusPrompt()
	s.Prompted = true
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
	want := []string{"agent.status", "agent.message_chunk:started a subagent", "agent.message_chunk:subagent said OK", "agent.status", "agent.message_chunk:DONE", "session.end"}
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events = %v, want %v", got, want)
		}
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

func TestProgStatusSilentSessionFinishesOnFirstEndTurn(t *testing.T) {
	// A session that can observe reports but never received one behaves as
	// without OSC: the first end_turn ends the turn on the spot.
	term := terminal.NewFakeSession("silent", 1, workingPane)
	path := filepath.Join(t.TempDir(), "session.jsonl")
	s := NewSession(context.Background(), SessionOptions{SessionID: "ses-silent", Transcript: NewTranscriptReader(path), EventsBuf: 16})
	defer s.CancelFn()
	s.Term = term
	s.AttachProgStatus(term)
	s.ArmProgStatusPrompt()
	s.Prompted = true
	appendTranscript(t, path, assistantText("first", "end_turn"), assistantText("second", "end_turn"))
	s.ScanTranscriptTick()
	got := eventNames(drainSessionEvents(s.Events))
	if len(got) != 2 || got[0] != "agent.message_chunk:first" || got[1] != "session.end" {
		t.Fatalf("events = %v, want first chunk then session.end", got)
	}
}
