package looprunner

import (
	"context"
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
