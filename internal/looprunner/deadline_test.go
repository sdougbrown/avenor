package looprunner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/phaseconfig"
)

// TestLoopRequiresEnforcedWithNudge mirrors the team runner's requirement
// enforcement: a loop phase that ends end_turn without its required file is
// nudged, and a nudge that produces the file completes the iteration.
func TestLoopRequiresEnforcedWithNudge(t *testing.T) {
	dir := t.TempDir()
	cfg := &LoopConfig{
		MaxIterations: 1,
		Loop: []phaseconfig.Phase{{
			Name:   "review",
			Prompt: "review",
			Requires: phaseconfig.PhaseRequirements{
				Files: []string{"REVIEW_CODE.md"},
			},
		}},
	}

	var attempts int
	var nudgePrompt string
	var nudgeIteration int
	result, err := Run(context.Background(), RunOptions{
		WorkDir:   dir,
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, iteration int, prevSessionID string) (PhaseAttemptResult, error) {
			attempts++
			if attempts == 2 {
				nudgePrompt = phase.Prompt
				nudgeIteration = iteration
				if err := os.WriteFile(filepath.Join(dir, "REVIEW_CODE.md"), []byte("done"), 0o600); err != nil {
					return PhaseAttemptResult{}, err
				}
			}
			return PhaseAttemptResult{ExitCode: 0, StopReason: "end_turn", SessionID: fmt.Sprintf("s%d", attempts)}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("ExitCode = %d, want 0", result.ExitCode)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want initial + nudge", attempts)
	}
	if nudgeIteration != 1 {
		t.Fatalf("nudge iteration = %d, want 1", nudgeIteration)
	}
	if nudgePrompt == "" {
		t.Fatal("expected a nudge prompt")
	}
}

// TestLoopMissingRequiredFileFailsPhase proves a missing deliverable cannot
// count as successful loop completion: after the bounded nudges are exhausted,
// the phase is classified incomplete_output with a non-zero exit.
func TestLoopMissingRequiredFileFailsPhase(t *testing.T) {
	dir := t.TempDir()
	cfg := &LoopConfig{
		MaxIterations: 1,
		Loop: []phaseconfig.Phase{{
			Name:   "review",
			Prompt: "review",
			Requires: phaseconfig.PhaseRequirements{
				NonEmptyFiles: []string{"REVIEW_CODE.md"},
			},
			OnIncomplete: phaseconfig.PhaseOnIncomplete{MaxNudges: 1},
		}},
	}

	var attempts atomic.Int32
	result, err := Run(context.Background(), RunOptions{
		WorkDir:   dir,
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, iteration int, prevSessionID string) (PhaseAttemptResult, error) {
			attempts.Add(1)
			return PhaseAttemptResult{ExitCode: 0, StopReason: "end_turn", SessionID: fmt.Sprintf("s%d", attempts.Load())}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 1 || result.StopReason != "incomplete_output" {
		t.Fatalf("Run() = {%d %q}, want {1 incomplete_output}", result.ExitCode, result.StopReason)
	}
	if attempts.Load() != 2 {
		t.Fatalf("attempts = %d, want initial + 1 nudge", attempts.Load())
	}
}

// TestLoopRetryBackoffStopsAtRunDeadline proves a loop retry backoff cannot
// escape the shared run deadline.
func TestLoopRetryBackoffStopsAtRunDeadline(t *testing.T) {
	cfg := &LoopConfig{
		MaxIterations: 1,
		Loop:          []phaseconfig.Phase{{Name: "flaky", Prompt: "work"}},
	}

	var events eventRecorder
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(50*time.Millisecond))
	defer cancel()

	result, err := Run(ctx, RunOptions{
		WorkDir:    t.TempDir(),
		RunID:      "test-run",
		EventSink:  &events,
		MaxRetries: 3,
		Config:     cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, iteration int, prevSessionID string) (PhaseAttemptResult, error) {
			return PhaseAttemptResult{ExitCode: 1}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 124 || result.StopReason != "timeout" {
		t.Fatalf("Run() = {%d %q}, want {124 timeout}", result.ExitCode, result.StopReason)
	}
	if !events.has("avenor.loop.end", "timeout") {
		t.Fatalf("loop end event = %+v, want exit_reason timeout", events.snapshot())
	}
}

// TestLoopNudgeCancelledParentReportsCancelled proves a parent cancellation
// (supervisor shutdown, workflow cancellation) during the requirement nudge
// loop is reported as a cancelled run, not a generic phase-failure error.
func TestLoopNudgeCancelledParentReportsCancelled(t *testing.T) {
	dir := t.TempDir()
	cfg := &LoopConfig{
		MaxIterations: 1,
		Loop: []phaseconfig.Phase{{
			Name:   "review",
			Prompt: "review",
			Requires: phaseconfig.PhaseRequirements{
				NonEmptyFiles: []string{"REVIEW_CODE.md"},
			},
		}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attempts atomic.Int32
	result, err := Run(ctx, RunOptions{
		WorkDir:   dir,
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, iteration int, prevSessionID string) (PhaseAttemptResult, error) {
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

// TestLoopNudgeStopsAtRunDeadline proves an incomplete-output nudge cannot
// escape the shared run deadline: once the deadline has expired, no nudge is
// dispatched and the phase reports the timeout instead of an
// incomplete-output failure.
func TestLoopNudgeStopsAtRunDeadline(t *testing.T) {
	dir := t.TempDir()
	cfg := &LoopConfig{
		MaxIterations: 1,
		Loop: []phaseconfig.Phase{{
			Name:   "review",
			Prompt: "review",
			Requires: phaseconfig.PhaseRequirements{
				NonEmptyFiles: []string{"REVIEW_CODE.md"},
			},
		}},
	}

	var attempts atomic.Int32
	var events eventRecorder
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(50*time.Millisecond))
	defer cancel()

	result, err := Run(ctx, RunOptions{
		WorkDir:   dir,
		RunID:     "test-run",
		EventSink: &events,
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, iteration int, prevSessionID string) (PhaseAttemptResult, error) {
			attempts.Add(1)
			// The run deadline expires during the attempt, so the requirement
			// check runs against an already-expired context.
			<-ctx.Done()
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
	if !events.has("avenor.loop.end", "timeout") {
		t.Fatalf("loop end event = %+v, want exit_reason timeout", events.snapshot())
	}
}

type eventRecorder struct {
	mu     sync.Mutex
	events []events.Event
}

func (r *eventRecorder) Write(ev events.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
	return nil
}

func (r *eventRecorder) snapshot() []events.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]events.Event(nil), r.events...)
}

func (r *eventRecorder) has(name, exitReason string) bool {
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

// TestLoopPrePhaseDeadlineReportsTimeout pins the sequential-phase timeout
// classification: a pre-phase stopped by the run deadline (its per-waiter
// timer reports timeout) ends the loop with the timeout exit reason, not
// phase_failure.
func TestLoopPrePhaseDeadlineReportsTimeout(t *testing.T) {
	dir := t.TempDir()
	cfg := &LoopConfig{
		MaxIterations: 1,
		Pre:           []phaseconfig.Phase{{Name: "orient", Prompt: "orient"}},
		Loop:          []phaseconfig.Phase{{Name: "review", Prompt: "review"}},
	}

	var events eventRecorder
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
	defer cancel()

	result, err := Run(ctx, RunOptions{
		WorkDir:   dir,
		RunID:     "test-run",
		EventSink: &events,
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, iteration int, prevSessionID string) (PhaseAttemptResult, error) {
			// The attempt's per-waiter timer fired before the run deadline:
			// the run deadline is still in the future, so the classification
			// under test — not the post-attempt deadline check — decides the
			// exit reason.
			return PhaseAttemptResult{ExitCode: 124, StopReason: "timeout"}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 124 || result.StopReason != "timeout" {
		t.Fatalf("Run() = {%d %q}, want {124 timeout}", result.ExitCode, result.StopReason)
	}
	if !events.has("avenor.loop.end", "timeout") {
		t.Fatalf("loop end event = %+v, want exit_reason timeout", events.snapshot())
	}
}

// TestLoopNudgeAttemptErrorPropagates pins the nudge attempt's error path:
// an error from the incomplete-output nudge must reach Run instead of being
// dropped, which would misclassify an internal failure as a clean or
// incomplete stop.
func TestLoopNudgeAttemptErrorPropagates(t *testing.T) {
	dir := t.TempDir()
	cfg := &LoopConfig{
		MaxIterations: 1,
		Loop: []phaseconfig.Phase{{
			Name:   "review",
			Prompt: "review",
			Requires: phaseconfig.PhaseRequirements{
				NonEmptyFiles: []string{"REVIEW_CODE.md"},
			},
		}},
	}

	boom := errors.New("nudge attempt exploded")
	var attempts atomic.Int32
	_, err := Run(context.Background(), RunOptions{
		WorkDir:   dir,
		RunID:     "test-run",
		EventSink: &eventRecorder{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, iteration int, prevSessionID string) (PhaseAttemptResult, error) {
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
