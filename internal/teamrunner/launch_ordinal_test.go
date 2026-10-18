package teamrunner

import (
	"context"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/phaseconfig"
)

// TestPhaseAttemptLaunchOrdinalIsSharedByRetriesAndNudges pins one launch
// ordinal per member invocation: retries and requirement nudges consume it in
// sequence instead of restarting an independent nudge counter.
func TestPhaseAttemptLaunchOrdinalIsSharedByRetriesAndNudges(t *testing.T) {
	original := phaseRetryAfter
	phaseRetryAfter = func(time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		ch <- time.Now()
		return ch
	}
	t.Cleanup(func() { phaseRetryAfter = original })

	cfg := makeConfig(nil, []phaseconfig.Phase{{
		Name:         "reviewer",
		Prompt:       "review",
		Requires:     phaseconfig.PhaseRequirements{Files: []string{"NEVER_WRITTEN.md"}},
		OnIncomplete: phaseconfig.PhaseOnIncomplete{MaxNudges: 2},
	}}, nil)

	var launches []int
	result, err := Run(context.Background(), RunOptions{
		WorkDir:    t.TempDir(),
		RunID:      "run_ordinal",
		EventSink:  discardEventWriter{},
		Config:     cfg,
		MaxRetries: 2,
		PhaseAttempt: func(_ context.Context, phase phaseconfig.Phase, attemptNum int, _ string) (PhaseAttemptResult, error) {
			if phase.Name != "reviewer" {
				t.Errorf("phase %q, want reviewer", phase.Name)
			}
			launches = append(launches, attemptNum)
			if attemptNum < 2 {
				return PhaseAttemptResult{ExitCode: 1}, nil
			}
			return PhaseAttemptResult{ExitCode: 0, StopReason: "end_turn", SessionID: "s1"}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	want := []int{0, 1, 2, 3, 4}
	if len(launches) != len(want) {
		t.Fatalf("launch ordinals = %v, want %v", launches, want)
	}
	for i := range want {
		if launches[i] != want[i] {
			t.Fatalf("launch ordinals = %v, want %v", launches, want)
		}
	}
	if result.StopReason != "incomplete_output" {
		t.Fatalf("StopReason = %q, want incomplete_output", result.StopReason)
	}
}
