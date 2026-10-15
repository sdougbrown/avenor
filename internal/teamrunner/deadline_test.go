package teamrunner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/phaseconfig"
)

// blockingAttemptResult simulates what cli.WaitForSession produces when a
// waiter is bounded by the shared run deadline: the attempt stops when its
// context (carrying the deadline) is done.
func blockingAttemptResult(ctx context.Context) PhaseAttemptResult {
	<-ctx.Done()
	if ctx.Err() == context.DeadlineExceeded {
		return PhaseAttemptResult{ExitCode: 124, StopReason: "timeout"}
	}
	return PhaseAttemptResult{ExitCode: 130, StopReason: "cancelled"}
}

// TestRunTeamDeadlineStopsEveryContinuouslyActiveMember proves the shared run
// deadline reaches every team member: each continuously active member stops
// with the timeout classification, and no member's failure silently cancels or
// bypasses the wall-clock limit for the others.
func TestRunTeamDeadlineStopsEveryContinuouslyActiveMember(t *testing.T) {
	cfg := makeConfig(nil, []phaseconfig.Phase{
		simplePhase("member-one", "work"),
		simplePhase("member-two", "work"),
	}, nil)

	var started atomic.Int32
	result, err := Run(func() context.Context {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(100*time.Millisecond))
		t.Cleanup(cancel)
		return ctx
	}(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			started.Add(1)
			return blockingAttemptResult(ctx), nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if started.Load() != 2 {
		t.Fatalf("members started = %d, want 2", started.Load())
	}
	if result.ExitCode != 124 || result.StopReason != "timeout" {
		t.Fatalf("Run() = {%d %q}, want {124 timeout}", result.ExitCode, result.StopReason)
	}
}

// TestRunRetryBackoffStopsAtRunDeadline proves a retry backoff cannot escape
// the shared run deadline: when the deadline expires during the backoff, the
// phase is classified as a timeout, not a generic retryable failure.
func TestRunRetryBackoffStopsAtRunDeadline(t *testing.T) {
	cfg := makeConfig(nil, []phaseconfig.Phase{simplePhase("flaky", "work")}, nil)

	var events eventsRecorder
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(50*time.Millisecond))
	defer cancel()

	result, err := Run(ctx, RunOptions{
		WorkDir:    t.TempDir(),
		RunID:      "test-run",
		EventSink:  &events,
		MaxRetries: 3,
		Config:     cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			return PhaseAttemptResult{ExitCode: 1}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 124 || result.StopReason != "timeout" {
		t.Fatalf("Run() = {%d %q}, want {124 timeout}", result.ExitCode, result.StopReason)
	}
	if !events.has("avenor.team.end", "timeout") {
		t.Fatalf("team end event = %+v, want exit_reason timeout", events.snapshot())
	}
}

// TestRunNudgeStopsAtRunDeadline proves an incomplete-output nudge cannot
// escape the shared run deadline: once the deadline has expired, no nudge is
// dispatched and the phase reports the timeout.
func TestRunNudgeStopsAtRunDeadline(t *testing.T) {
	dir := t.TempDir()
	cfg := makeConfig(nil, []phaseconfig.Phase{{
		Name:   "review",
		Prompt: "review",
		Requires: phaseconfig.PhaseRequirements{
			NonEmptyFiles: []string{"REVIEW_CODE.md"},
		},
	}}, nil)

	var attempts atomic.Int32
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	result, err := Run(ctx, RunOptions{
		WorkDir:   dir,
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			attempts.Add(1)
			return PhaseAttemptResult{ExitCode: 0, StopReason: "end_turn", SessionID: fmt.Sprintf("s%d", attempts.Load())}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("attempts = %d, want 1 (no nudge after the deadline)", attempts.Load())
	}
	if result.ExitCode != 124 || result.StopReason != "timeout" {
		t.Fatalf("Run() = {%d %q}, want {124 timeout}", result.ExitCode, result.StopReason)
	}
}

// TestRunNudgeCancelledParentReportsCancelled proves a parent cancellation
// (supervisor shutdown, workflow cancellation) during the requirement nudge
// loop is reported as a cancelled run, not a generic phase-failure error.
func TestRunNudgeCancelledParentReportsCancelled(t *testing.T) {
	dir := t.TempDir()
	cfg := makeConfig(nil, []phaseconfig.Phase{{
		Name:   "review",
		Prompt: "review",
		Requires: phaseconfig.PhaseRequirements{
			NonEmptyFiles: []string{"REVIEW_CODE.md"},
		},
	}}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attempts atomic.Int32
	result, err := Run(ctx, RunOptions{
		WorkDir:   dir,
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			attempts.Add(1)
			// The parent context is cancelled between the attempt and the nudge.
			cancel()
			return PhaseAttemptResult{ExitCode: 0, StopReason: "end_turn", SessionID: fmt.Sprintf("s%d", attempts.Load())}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("attempts = %d, want 1 (no nudge after cancellation)", attempts.Load())
	}
	if result.ExitCode != 130 || result.StopReason != "cancelled" {
		t.Fatalf("Run() = {%d %q}, want {130 cancelled}", result.ExitCode, result.StopReason)
	}
}

type eventsRecorder struct {
	mu     sync.Mutex
	events []events.Event
}

func (r *eventsRecorder) Write(ev events.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
	return nil
}

func (r *eventsRecorder) snapshot() []events.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]events.Event(nil), r.events...)
}

func (r *eventsRecorder) has(name, exitReason string) bool {
	for _, ev := range r.snapshot() {
		if ev.Event != name {
			continue
		}
		if reason, _ := ev.Fields["exit_reason"].(string); reason == exitReason {
			return true
		}
	}
	return false
}

// TestRunMemberErrorAfterDeadlineIsTimeout keeps the internal-error fallback
// for ordinary failures while making sure a member whose execution is cut by
// the run deadline is not mislabeled as an internal phase error.
func TestRunMemberErrorAfterDeadlineIsTimeout(t *testing.T) {
	cfg := makeConfig(nil, []phaseconfig.Phase{simplePhase("member", "work")}, nil)

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	result, err := Run(ctx, RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			return PhaseAttemptResult{}, fmt.Errorf("synthetic internal failure after deadline")
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 124 || result.StopReason != "timeout" {
		t.Fatalf("Run() = {%d %q}, want {124 timeout}", result.ExitCode, result.StopReason)
	}
}

// TestRunPrePhaseDeadlineReportsTimeout pins the pre-phase timeout
// classification: a pre-phase stopped by the run deadline (its per-waiter
// timer reports timeout) ends the team with the timeout exit reason, not
// pre_failure.
func TestRunPrePhaseDeadlineReportsTimeout(t *testing.T) {
	cfg := makeConfig(
		[]phaseconfig.Phase{simplePhase("setup", "do setup")},
		[]phaseconfig.Phase{simplePhase("review", "review")},
		nil,
	)

	var events eventsRecorder
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
	defer cancel()

	result, err := Run(ctx, RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "test-run",
		EventSink: &events,
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			// The attempt's per-waiter timer fired before the run deadline,
			// so the classification under test decides the exit reason.
			return PhaseAttemptResult{ExitCode: 124, StopReason: "timeout"}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 124 || result.StopReason != "timeout" {
		t.Fatalf("Run() = {%d %q}, want {124 timeout}", result.ExitCode, result.StopReason)
	}
	if !events.has("avenor.team.end", "timeout") {
		t.Fatalf("team end event = %+v, want exit_reason timeout", events.snapshot())
	}
}

// TestRunPrePhaseNudgeAttemptErrorPropagates pins the nudge attempt's error
// path: an error from the incomplete-output nudge must reach Run instead of
// being dropped, which would misclassify an internal failure as a clean or
// incomplete stop.
func TestRunPrePhaseNudgeAttemptErrorPropagates(t *testing.T) {
	cfg := makeConfig(
		[]phaseconfig.Phase{{
			Name:   "setup",
			Prompt: "do setup",
			Requires: phaseconfig.PhaseRequirements{
				NonEmptyFiles: []string{"SETUP.md"},
			},
		}},
		[]phaseconfig.Phase{simplePhase("review", "review")},
		nil,
	)

	boom := errors.New("nudge attempt exploded")
	var attempts atomic.Int32
	_, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			if attempts.Add(1) == 1 {
				return PhaseAttemptResult{ExitCode: 0, StopReason: "end_turn", SessionID: "s1"}, nil
			}
			return PhaseAttemptResult{}, boom
		},
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Run() error = %v, want the nudge attempt error", err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("attempts = %d, want 2 (initial attempt + nudge)", attempts.Load())
	}
}
