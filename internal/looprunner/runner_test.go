package looprunner

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
)

func TestRunResumeFromPreviousUsesImmediatePriorPhase(t *testing.T) {
	cfg := &LoopConfig{
		MaxIterations: 2,
		Loop: []Phase{
			{Name: "review", Prompt: "review"},
			{Name: "verify", Prompt: "verify", ResumeFromPrevious: true},
		},
	}

	var verifyPrevSessions []string
	result, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "run_1",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase Phase, attemptNum int, iteration int, prevSessionID string) (PhaseAttemptResult, error) {
			if phase.Name == "verify" {
				verifyPrevSessions = append(verifyPrevSessions, prevSessionID)
			}
			return PhaseAttemptResult{
				ExitCode:  0,
				SessionID: fmt.Sprintf("%s-session-%d", phase.Name, iteration),
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("ExitCode = %d, want 0", result.ExitCode)
	}
	want := []string{"review-session-1", "review-session-2"}
	if len(verifyPrevSessions) != len(want) {
		t.Fatalf("verify prev sessions = %#v, want %#v", verifyPrevSessions, want)
	}
	for i := range want {
		if verifyPrevSessions[i] != want[i] {
			t.Fatalf("verify prev sessions = %#v, want %#v", verifyPrevSessions, want)
		}
	}
}

func TestRunPreResumeFromPreviousThreadsSessionID(t *testing.T) {
	cfg := &LoopConfig{
		MaxIterations: 1,
		Pre: []Phase{
			{Name: "review", Prompt: "review"},
			{Name: "orient", Prompt: "orient", ResumeFromPrevious: true},
		},
	}

	prevSessions := map[string]string{}
	_, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "run_1",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase Phase, attemptNum int, iteration int, prevSessionID string) (PhaseAttemptResult, error) {
			prevSessions[phase.Name] = prevSessionID
			return PhaseAttemptResult{ExitCode: 0, SessionID: phase.Name + "-session"}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if prevSessions["review"] != "" {
		t.Fatalf("review prevSessionID = %q, want empty", prevSessions["review"])
	}
	if prevSessions["orient"] != "review-session" {
		t.Fatalf("orient prevSessionID = %q, want %q", prevSessions["orient"], "review-session")
	}
}

func TestRunPhaseWithRetryOnlyRetriesTransientFailure(t *testing.T) {
	attempts := 0
	result, err := runPhaseWithRetry(context.Background(), func(ctx context.Context) (PhaseAttemptResult, error) {
		attempts++
		return PhaseAttemptResult{ExitCode: 5, SessionID: "ses_blocked"}, nil
	}, 3)
	if err != nil {
		t.Fatalf("runPhaseWithRetry() error = %v", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
	if result.ExitCode != 5 || result.SessionID != "ses_blocked" {
		t.Fatalf("result = %+v, want blocked result", result)
	}
}

func TestRunEmitsRetryEventBeforeRetry(t *testing.T) {
	oldRetryAfter := phaseRetryAfter
	phaseRetryAfter = func(time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		ch <- time.Now()
		return ch
	}
	t.Cleanup(func() { phaseRetryAfter = oldRetryAfter })

	sink := &recordingEventWriter{}
	attempts := 0
	result, err := Run(context.Background(), RunOptions{
		WorkDir:    t.TempDir(),
		RunID:      "run_1",
		EventSink:  sink,
		Config:     &LoopConfig{Pre: []Phase{{Name: "build", Prompt: "build"}}, MaxIterations: 1},
		MaxRetries: 1,
		PhaseAttempt: func(ctx context.Context, phase Phase, attemptNum int, iteration int, prevSessionID string) (PhaseAttemptResult, error) {
			attempts++
			if attempts == 1 {
				return PhaseAttemptResult{ExitCode: 1, SessionID: "ses_1"}, nil
			}
			return PhaseAttemptResult{ExitCode: 0, SessionID: "ses_2"}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("ExitCode = %d, want 0", result.ExitCode)
	}
	retries := 0
	for _, ev := range sink.events {
		if ev.Event == "avenor.retry" {
			retries++
			if ev.Fields["attempt"] != 1 {
				t.Fatalf("retry attempt = %v, want 1", ev.Fields["attempt"])
			}
		}
	}
	if retries != 1 {
		t.Fatalf("retry events = %d, want 1; events=%+v", retries, sink.events)
	}
}

func TestRunPhaseEndPreservesExplicitStopReason(t *testing.T) {
	sink := &recordingEventWriter{}
	result, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "run_1",
		EventSink: sink,
		Config:    &LoopConfig{Pre: []Phase{{Name: "build", Prompt: "build"}}, MaxIterations: 1},
		PhaseAttempt: func(ctx context.Context, phase Phase, attemptNum int, iteration int, prevSessionID string) (PhaseAttemptResult, error) {
			return PhaseAttemptResult{
				ExitCode:   124,
				SessionID:  "ses_1",
				StopReason: "progress_timeout",
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.StopReason != "progress_timeout" {
		t.Fatalf("RunResult StopReason = %q, want progress_timeout", result.StopReason)
	}

	var got any
	for _, ev := range sink.events {
		if ev.Event == "avenor.phase.end" {
			got = ev.Fields["stop_reason"]
			break
		}
	}
	if got != "progress_timeout" {
		t.Fatalf("phase.end stop_reason = %v, want progress_timeout; events=%+v", got, sink.events)
	}
}

type discardEventWriter struct{}

func (discardEventWriter) Write(events.Event) error { return nil }

type recordingEventWriter struct {
	events []events.Event
}

func (w *recordingEventWriter) Write(ev events.Event) error {
	w.events = append(w.events, ev)
	return nil
}
