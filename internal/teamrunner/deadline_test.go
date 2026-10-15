package teamrunner

import (
	"context"
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
