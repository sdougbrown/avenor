package looprunner

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/phaseconfig"
)

func TestRunResumeFromPreviousUsesImmediatePriorPhase(t *testing.T) {
	cfg := &LoopConfig{
		MaxIterations: 2,
		Loop: []phaseconfig.Phase{
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
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, iteration int, prevSessionID string) (PhaseAttemptResult, error) {
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
		Pre: []phaseconfig.Phase{
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
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, iteration int, prevSessionID string) (PhaseAttemptResult, error) {
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

func TestRunPostPhasesRunAfterLoop(t *testing.T) {
	cfg := &LoopConfig{
		MaxIterations: 2,
		Loop:          []phaseconfig.Phase{{Name: "work", Prompt: "work"}},
		Post:          []phaseconfig.Phase{{Name: "report", Prompt: "report"}},
	}

	var order []string
	_, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "run_1",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, iteration int, prevSessionID string) (PhaseAttemptResult, error) {
			order = append(order, fmt.Sprintf("%s-%d", phase.Name, iteration))
			return PhaseAttemptResult{ExitCode: 0, SessionID: phase.Name + "-session"}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	want := []string{"work-1", "work-2", "report-0"}
	if len(order) != len(want) {
		t.Fatalf("phase order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("phase order = %v, want %v", order, want)
		}
	}
}

func TestRunPostPhasesRunAfterExitMarker(t *testing.T) {
	cfg := &LoopConfig{
		MaxIterations: 10,
		Loop:          []phaseconfig.Phase{{Name: "work", Prompt: "work"}},
		Post:          []phaseconfig.Phase{{Name: "report", Prompt: "report"}},
	}

	var order []string
	_, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "run_1",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, iteration int, prevSessionID string) (PhaseAttemptResult, error) {
			order = append(order, fmt.Sprintf("%s-%d", phase.Name, iteration))
			r := PhaseAttemptResult{ExitCode: 0, SessionID: phase.Name + "-session"}
			if phase.Name == "work" {
				r.LoopDirective = "exit"
			}
			return r, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	want := []string{"work-1", "report-0"}
	if len(order) != len(want) {
		t.Fatalf("phase order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("phase order = %v, want %v", order, want)
		}
	}
}

func TestRunPostPhasesSkippedOnAbort(t *testing.T) {
	cfg := &LoopConfig{
		MaxIterations: 10,
		Loop:          []phaseconfig.Phase{{Name: "work", Prompt: "work"}},
		Post:          []phaseconfig.Phase{{Name: "report", Prompt: "report"}},
	}

	var phases []string
	_, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "run_1",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, iteration int, prevSessionID string) (PhaseAttemptResult, error) {
			phases = append(phases, phase.Name)
			return PhaseAttemptResult{ExitCode: 5, LoopDirective: "abort", SessionID: phase.Name + "-session"}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	for _, p := range phases {
		if p == "report" {
			t.Fatal("post phase ran after abort, expected it to be skipped")
		}
	}
}

func TestRunPostPhaseResumeFromPreviousThreadsSessionID(t *testing.T) {
	cfg := &LoopConfig{
		MaxIterations: 1,
		Loop:          []phaseconfig.Phase{{Name: "work", Prompt: "work"}},
		Post: []phaseconfig.Phase{
			{Name: "summarize", Prompt: "summarize"},
			{Name: "notify", Prompt: "notify", ResumeFromPrevious: true},
		},
	}

	prevSessions := map[string]string{}
	_, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "run_1",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, iteration int, prevSessionID string) (PhaseAttemptResult, error) {
			prevSessions[phase.Name] = prevSessionID
			return PhaseAttemptResult{ExitCode: 0, SessionID: phase.Name + "-session"}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if prevSessions["summarize"] != "" {
		t.Fatalf("summarize prevSessionID = %q, want empty", prevSessions["summarize"])
	}
	if prevSessions["notify"] != "summarize-session" {
		t.Fatalf("notify prevSessionID = %q, want %q", prevSessions["notify"], "summarize-session")
	}
}

func TestRunPostPhasesEmitCorrectKind(t *testing.T) {
	cfg := &LoopConfig{
		MaxIterations: 1,
		Loop:          []phaseconfig.Phase{{Name: "work", Prompt: "work"}},
		Post:          []phaseconfig.Phase{{Name: "report", Prompt: "report"}},
	}

	sink := &recordingEventWriter{}
	_, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "run_1",
		EventSink: sink,
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, iteration int, prevSessionID string) (PhaseAttemptResult, error) {
			return PhaseAttemptResult{ExitCode: 0, SessionID: phase.Name + "-session"}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	kinds := map[string]string{}
	for _, ev := range sink.events {
		if ev.Event == "avenor.phase.start" {
			kinds[ev.Fields["phase"].(string)] = ev.Fields["kind"].(string)
		}
	}
	if kinds["work"] != "loop" {
		t.Fatalf("work kind = %q, want loop", kinds["work"])
	}
	if kinds["report"] != "post" {
		t.Fatalf("report kind = %q, want post", kinds["report"])
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
		Config:     &LoopConfig{Pre: []phaseconfig.Phase{{Name: "build", Prompt: "build"}}, MaxIterations: 1},
		MaxRetries: 1,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, iteration int, prevSessionID string) (PhaseAttemptResult, error) {
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
		Config:    &LoopConfig{Pre: []phaseconfig.Phase{{Name: "build", Prompt: "build"}}, MaxIterations: 1},
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, iteration int, prevSessionID string) (PhaseAttemptResult, error) {
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

func TestRunNestedAbortPropagatesDirectiveAndReason(t *testing.T) {
	cfg := &LoopConfig{
		MaxIterations: 1,
		Loop:          []phaseconfig.Phase{{Name: "nested", TeamFile: "team.json"}},
	}

	result, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "run_1",
		EventSink: discardEventWriter{},
		Config:    cfg,
		ConfigDir: "/tmp/configs",
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, iteration int, prevSessionID string) (PhaseAttemptResult, error) {
			t.Fatal("PhaseAttempt should not run for nested phase")
			return PhaseAttemptResult{}, nil
		},
		NestedRun: func(ctx context.Context, configPath string, runType string) (NestedResult, error) {
			if configPath != "/tmp/configs/team.json" {
				t.Fatalf("configPath = %q, want /tmp/configs/team.json", configPath)
			}
			if runType != "team" {
				t.Fatalf("runType = %q, want team", runType)
			}
			return NestedResult{ExitCode: 5, StopReason: "blocked", SessionID: "child-s", Reason: "blocked downstream"}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 5 || result.StopReason != "blocked" || result.Reason != "blocked downstream" || result.SessionID != "child-s" {
		t.Fatalf("result = %+v, want blocked child result", result)
	}
}

func TestRunNestedPhaseRequiresNestedRun(t *testing.T) {
	cfg := &LoopConfig{
		MaxIterations: 1,
		Loop:          []phaseconfig.Phase{{Name: "nested", LoopFile: "child.json"}},
	}

	_, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "run_1",
		EventSink: discardEventWriter{},
		Config:    cfg,
	})
	if err == nil || err.Error() != `loop config: phase "nested" has loop_file or team_file but NestedRun is not configured` {
		t.Fatalf("err = %v, want NestedRun configuration error", err)
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
