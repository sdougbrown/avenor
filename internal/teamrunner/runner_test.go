package teamrunner

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/phaseconfig"
	"github.com/sdougbrown/avenor/internal/runtime"
)

// ---- Test helpers ----

type discardEventWriter struct{}

func (discardEventWriter) Write(events.Event) error { return nil }

type recordingEventWriter struct {
	mu     sync.Mutex
	events []events.Event
}

func (w *recordingEventWriter) Write(ev events.Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.events = append(w.events, ev)
	return nil
}

func makeConfig(pre, team, post []phaseconfig.Phase) *TeamConfig {
	return &TeamConfig{Pre: pre, Team: team, Post: post}
}

func simplePhase(name, prompt string) phaseconfig.Phase {
	return phaseconfig.Phase{Name: name, Prompt: prompt}
}

func conditionalPhase(name, prompt string) phaseconfig.Phase {
	return phaseconfig.Phase{Name: name, Prompt: prompt, Conditional: true}
}

// ---- Test cases ----

func TestRunHappyPath(t *testing.T) {
	cfg := makeConfig(
		[]phaseconfig.Phase{simplePhase("setup", "do setup")},
		[]phaseconfig.Phase{simplePhase("security", "review security"), simplePhase("style", "review style")},
		[]phaseconfig.Phase{simplePhase("report", "write report")},
	)

	var order []string
	var orderMu sync.Mutex
	sink := &recordingEventWriter{}
	result, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "test-run",
		EventSink: sink,
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			orderMu.Lock()
			order = append(order, phase.Name)
			orderMu.Unlock()
			return PhaseAttemptResult{ExitCode: 0, SessionID: phase.Name + "-session"}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("ExitCode = %d, want 0", result.ExitCode)
	}
	if result.StopReason != "end_turn" {
		t.Fatalf("StopReason = %q, want end_turn", result.StopReason)
	}

	// Setup runs before all team members, report runs after
	// Order: setup, then security+style in parallel (either order), then report
	if len(order) != 4 {
		t.Fatalf("expected 4 phases, got %d: %v", len(order), order)
	}
	if order[0] != "setup" {
		t.Fatalf("first phase = %q, want setup", order[0])
	}
	if order[3] != "report" {
		t.Fatalf("last phase = %q, want report", order[3])
	}
	names := map[string]bool{order[1]: true, order[2]: true}
	if !names["security"] || !names["style"] {
		t.Fatalf("middle phases = %v, want security and style", order[1:3])
	}
}

func TestRunTeamOnly(t *testing.T) {
	cfg := makeConfig(nil, []phaseconfig.Phase{simplePhase("review", "review code")}, nil)

	result, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			return PhaseAttemptResult{ExitCode: 0, SessionID: "s1"}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("ExitCode = %d, want 0", result.ExitCode)
	}
}

func TestRunPreOnly(t *testing.T) {
	cfg := makeConfig([]phaseconfig.Phase{simplePhase("setup", "setup env")}, nil, nil)

	result, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			return PhaseAttemptResult{ExitCode: 0, SessionID: "s1"}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("ExitCode = %d, want 0", result.ExitCode)
	}
}

func TestRunAbortInTeamMember(t *testing.T) {
	cfg := makeConfig(nil, []phaseconfig.Phase{simplePhase("security", "review"), simplePhase("style", "review")}, nil)

	var executed []string
	var executedMu sync.Mutex
	sink := &recordingEventWriter{}
	result, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "test-run",
		EventSink: sink,
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			executedMu.Lock()
			executed = append(executed, phase.Name)
			executedMu.Unlock()
			if phase.Name == "security" {
				return PhaseAttemptResult{
					ExitCode:      5,
					StopReason:    "blocked",
					SessionID:     "sec-session",
					LoopDirective: "abort",
					LoopLabel:     "CVE found",
				}, nil
			}
			return PhaseAttemptResult{ExitCode: 0, SessionID: "style-session"}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 5 {
		t.Fatalf("ExitCode = %d, want 5", result.ExitCode)
	}
	if result.StopReason != "blocked" {
		t.Fatalf("StopReason = %q, want blocked", result.StopReason)
	}
	if result.Reason != "CVE found" {
		t.Fatalf("Reason = %q, want 'CVE found'", result.Reason)
	}

	// Check team.end event has abort exit_reason
	var teamEndFound bool
	for _, ev := range sink.events {
		if ev.Event == "avenor.team.end" {
			teamEndFound = true
			if ev.Fields["exit_reason"] != "abort" {
				t.Fatalf("team.end exit_reason = %v, want abort", ev.Fields["exit_reason"])
			}
		}
	}
	if !teamEndFound {
		t.Fatal("expected avenor.team.end event")
	}
}

func TestRunMemberFailureDoesNotCancelPeers(t *testing.T) {
	cfg := makeConfig(nil, []phaseconfig.Phase{simplePhase("security", "review"), simplePhase("style", "review")}, nil)

	var executed []string
	var executedMu sync.Mutex
	result, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			executedMu.Lock()
			executed = append(executed, phase.Name)
			executedMu.Unlock()
			if phase.Name == "security" {
				return PhaseAttemptResult{ExitCode: 2, StopReason: "refusal", SessionID: "sec-s"}, nil
			}
			return PhaseAttemptResult{ExitCode: 0, SessionID: "style-s"}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 2 {
		t.Fatalf("ExitCode = %d, want 2", result.ExitCode)
	}
	// Both members should have been allowed to complete
	if len(executed) != 2 {
		t.Fatalf("expected 2 members executed, got %d: %v", len(executed), executed)
	}
}

func TestRunPrePhaseFailure(t *testing.T) {
	cfg := makeConfig(
		[]phaseconfig.Phase{simplePhase("setup", "setup")},
		[]phaseconfig.Phase{simplePhase("review", "review")},
		nil,
	)

	result, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			return PhaseAttemptResult{ExitCode: 1, StopReason: "error", SessionID: "s1"}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 1 {
		t.Fatalf("ExitCode = %d, want 1", result.ExitCode)
	}
}

func TestRunPostPhaseFailure(t *testing.T) {
	cfg := makeConfig(
		nil,
		[]phaseconfig.Phase{simplePhase("review", "review")},
		[]phaseconfig.Phase{simplePhase("report", "write report")},
	)

	result, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			if phase.Name == "report" {
				return PhaseAttemptResult{ExitCode: 1, StopReason: "error", SessionID: "r-s"}, nil
			}
			return PhaseAttemptResult{ExitCode: 0, SessionID: phase.Name + "-s"}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 1 {
		t.Fatalf("ExitCode = %d, want 1", result.ExitCode)
	}
}

func TestRunConditionalMemberSkipping(t *testing.T) {
	cfg := makeConfig(
		[]phaseconfig.Phase{simplePhase("decide", "decide which reviewers")},
		[]phaseconfig.Phase{
			conditionalPhase("security", "review security"),
			simplePhase("style", "review style"),
		},
		nil,
	)

	var executed []string
	result, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			executed = append(executed, phase.Name)
			if phase.Name == "decide" {
				return PhaseAttemptResult{
					ExitCode:  0,
					SessionID: "decide-s",
					Output:    "[team: skip | security]\nOK here's my analysis",
				}, nil
			}
			return PhaseAttemptResult{ExitCode: 0, SessionID: phase.Name + "-s"}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("ExitCode = %d, want 0", result.ExitCode)
	}
	// "security" should not be in the executed list (it was skipped)
	for _, name := range executed {
		if name == "security" {
			t.Fatal("security should have been skipped but was executed")
		}
	}
	// "decide" and "style" should both have executed
	seen := map[string]bool{}
	for _, name := range executed {
		seen[name] = true
	}
	if !seen["decide"] {
		t.Fatal("decide phase should have executed")
	}
	if !seen["style"] {
		t.Fatal("style phase should have executed")
	}
}

func TestRunConditionalInjectionInPrePrompt(t *testing.T) {
	cfg := makeConfig(
		[]phaseconfig.Phase{simplePhase("decide", "decide which reviewers")},
		[]phaseconfig.Phase{
			conditionalPhase("security", "review security"),
			conditionalPhase("api-compat", "review API compat"),
		},
		nil,
	)

	result, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			// Verify the decide phase's prompt includes the conditional skip syntax
			if phase.Name == "decide" {
				if !strings.Contains(phase.Prompt, "[team: skip | <name>]") {
					t.Fatalf("decide prompt should contain conditional skip syntax, got: %q", phase.Prompt)
				}
				if !strings.Contains(phase.Prompt, "Conditional members: security, api-compat") {
					t.Fatalf("decide prompt should list conditional members, got: %q", phase.Prompt)
				}
			}
			return PhaseAttemptResult{ExitCode: 0, SessionID: "s1"}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("ExitCode = %d, want 0", result.ExitCode)
	}
}

func TestRunContextCancellation(t *testing.T) {
	cfg := makeConfig(nil, []phaseconfig.Phase{simplePhase("slow", "work slowly")}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel immediately so the phase attempt sees cancellation
	cancel()

	result, err := Run(ctx, RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			return PhaseAttemptResult{ExitCode: 130, SessionID: "s1"}, nil
		},
	})
	if err != nil {
		// context.Canceled is acceptable
		if ctx.Err() == nil {
			t.Fatalf("Run() unexpected error = %v", err)
		}
	}
	// Should get exit code 130 (cancelled) or 124 (timeout)
	if result.ExitCode != 130 && result.ExitCode != 0 {
		t.Fatalf("ExitCode = %d, want 130 or 0", result.ExitCode)
	}
}

func TestRunEventPayloads(t *testing.T) {
	cfg := makeConfig(
		[]phaseconfig.Phase{simplePhase("setup", "setup")},
		[]phaseconfig.Phase{simplePhase("review", "review")},
		[]phaseconfig.Phase{simplePhase("report", "report")},
	)

	sink := &recordingEventWriter{}
	_, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "test-run",
		EventSink: sink,
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			return PhaseAttemptResult{ExitCode: 0, SessionID: phase.Name + "-s"}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	// Check avenor.team.start
	var startFound bool
	for _, ev := range sink.events {
		if ev.Event == "avenor.team.start" {
			startFound = true
			if ev.Fields["pre_phase_count"] != 1 {
				t.Fatalf("pre_phase_count = %v, want 1", ev.Fields["pre_phase_count"])
			}
			if ev.Fields["team_member_count"] != 1 {
				t.Fatalf("team_member_count = %v, want 1", ev.Fields["team_member_count"])
			}
			if ev.Fields["post_phase_count"] != 1 {
				t.Fatalf("post_phase_count = %v, want 1", ev.Fields["post_phase_count"])
			}
		}
	}
	if !startFound {
		t.Fatal("expected avenor.team.start event")
	}

	// Check phase start events have correct kind values
	kinds := map[string]string{}
	for _, ev := range sink.events {
		if ev.Event == "avenor.phase.start" {
			kinds[ev.Fields["phase"].(string)] = ev.Fields["kind"].(string)
		}
	}
	if kinds["setup"] != "pre" {
		t.Fatalf("setup kind = %q, want pre", kinds["setup"])
	}
	if kinds["review"] != "team" {
		t.Fatalf("review kind = %q, want team", kinds["review"])
	}
	if kinds["report"] != "post" {
		t.Fatalf("report kind = %q, want post", kinds["report"])
	}

	// Check avenor.team.end with end_turn
	var endFound bool
	for _, ev := range sink.events {
		if ev.Event == "avenor.team.end" {
			endFound = true
			if ev.Fields["exit_reason"] != "end_turn" {
				t.Fatalf("team.end exit_reason = %v, want end_turn", ev.Fields["exit_reason"])
			}
		}
	}
	if !endFound {
		t.Fatal("expected avenor.team.end event")
	}
}

func TestRunRetryOnTransientFailure(t *testing.T) {
	oldRetryAfter := phaseRetryAfter
	phaseRetryAfter = func(d time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		ch <- time.Now()
		return ch
	}
	t.Cleanup(func() { phaseRetryAfter = oldRetryAfter })

	cfg := makeConfig(nil, []phaseconfig.Phase{simplePhase("review", "review")}, nil)

	attempts := 0
	result, err := Run(context.Background(), RunOptions{
		WorkDir:    t.TempDir(),
		RunID:      "test-run",
		EventSink:  discardEventWriter{},
		Config:     cfg,
		MaxRetries: 2,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			attempts++
			if attempts <= 2 {
				return PhaseAttemptResult{ExitCode: 1, SessionID: fmt.Sprintf("attempt-%d", attempts)}, nil
			}
			return PhaseAttemptResult{ExitCode: 0, SessionID: "final-s"}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("ExitCode = %d, want 0", result.ExitCode)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestExtractTeamSkipMarkers(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "single skip marker",
			input: "Analysis\n[team: skip | security]\nDone",
			want:  []string{"security"},
		},
		{
			name:  "multiple skip markers",
			input: "[team: skip | security]\n[team: skip | api-compat]\nDone",
			want:  []string{"security", "api-compat"},
		},
		{
			name:  "no skip markers",
			input: "Just analysis, no skips",
			want:  nil,
		},
		{
			name:  "case insensitive",
			input: "[TEAM: SKIP | Security]",
			want:  []string{"Security"},
		},
		{
			name:  "whitespace tolerant",
			input: "  [team:  skip  |  api-compat  ]  ",
			want:  []string{"api-compat"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractTeamSkipMarkers(tt.input)
			if len(got) != len(tt.want) {
				t.Fatalf("extractTeamSkipMarkers(%q) = %v, want %v", tt.input, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("extractTeamSkipMarkers(%q)[%d] = %q, want %q", tt.input, i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestBuildMemberListFiltersSkipped(t *testing.T) {
	team := []phaseconfig.Phase{
		{Name: "security", Prompt: "review security", Conditional: true},
		{Name: "style", Prompt: "review style"},
		{Name: "api-compat", Prompt: "review API", Conditional: true},
	}

	filtered := buildMemberList(team, "[team: skip | security]\n[team: skip | api-compat]")
	if len(filtered) != 1 {
		t.Fatalf("expected 1 member after filtering, got %d", len(filtered))
	}
	if filtered[0].Name != "style" {
		t.Fatalf("expected style to remain, got %s", filtered[0].Name)
	}
}

func TestBuildMemberListNoSkips(t *testing.T) {
	team := []phaseconfig.Phase{
		{Name: "security", Prompt: "review security"},
		{Name: "style", Prompt: "review style"},
	}

	filtered := buildMemberList(team, "")
	if len(filtered) != 2 {
		t.Fatalf("expected 2 members, got %d", len(filtered))
	}
}

func TestRunAbortInPreSkipsTeam(t *testing.T) {
	cfg := makeConfig(
		[]phaseconfig.Phase{simplePhase("setup", "setup")},
		[]phaseconfig.Phase{simplePhase("review", "review")},
		[]phaseconfig.Phase{simplePhase("report", "report")},
	)

	var executed []string
	result, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			executed = append(executed, phase.Name)
			return PhaseAttemptResult{
				ExitCode:      5,
				StopReason:    "blocked",
				SessionID:     "s1",
				LoopDirective: "abort",
				LoopLabel:     "critical issue",
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 5 {
		t.Fatalf("ExitCode = %d, want 5", result.ExitCode)
	}
	if result.StopReason != "blocked" {
		t.Fatalf("StopReason = %q, want blocked", result.StopReason)
	}
	// Only setup should have executed (it aborted)
	for _, name := range executed {
		if name != "setup" {
			t.Fatalf("only setup should have executed, also saw: %s", name)
		}
	}
}

func TestRunPostSkippedOnTeamAbort(t *testing.T) {
	cfg := makeConfig(
		nil,
		[]phaseconfig.Phase{simplePhase("review", "review")},
		[]phaseconfig.Phase{simplePhase("report", "report")},
	)

	var executed []string
	result, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			executed = append(executed, phase.Name)
			if phase.Name == "review" {
				return PhaseAttemptResult{
					ExitCode:      5,
					StopReason:    "blocked",
					SessionID:     "s1",
					LoopDirective: "abort",
					LoopLabel:     "blocker",
				}, nil
			}
			return PhaseAttemptResult{ExitCode: 0, SessionID: "s1"}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 5 {
		t.Fatalf("ExitCode = %d, want 5", result.ExitCode)
	}
	for _, name := range executed {
		if name == "report" {
			t.Fatal("post phase should not run after abort")
		}
	}
}

func TestRunAbortCancelsOtherMembers(t *testing.T) {
	cfg := makeConfig(nil, []phaseconfig.Phase{
		simplePhase("fast-abort", "abort quickly"),
		simplePhase("slow", "takes time"),
	}, nil)

	var slowStarted atomic.Bool
	var slowCancelled atomic.Bool
	slowStartedCh := make(chan struct{}, 1)

	result, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			if phase.Name == "fast-abort" {
				return PhaseAttemptResult{
					ExitCode:      5,
					LoopDirective: "abort",
					LoopLabel:     "found blocker",
					SessionID:     "abort-s",
				}, nil
			}
			slowStarted.Store(true)
			slowStartedCh <- struct{}{}
			// Simulate a slow member — but context may get cancelled
			select {
			case <-ctx.Done():
				slowCancelled.Store(true)
				return PhaseAttemptResult{ExitCode: 130, SessionID: "slow-s"}, nil
			case <-time.After(100 * time.Millisecond):
				return PhaseAttemptResult{ExitCode: 0, SessionID: "slow-s"}, nil
			}
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 5 {
		t.Fatalf("ExitCode = %d, want 5", result.ExitCode)
	}
	if result.StopReason != "blocked" {
		t.Fatalf("StopReason = %q, want blocked", result.StopReason)
	}
	if result.SessionID != "abort-s" {
		t.Fatalf("SessionID = %q, want abort-s", result.SessionID)
	}
	if !slowStarted.Load() {
		t.Fatal("slow member never started")
	}
	select {
	case <-slowStartedCh:
	default:
		t.Fatal("slow member start signal was not observed")
	}
	if !slowCancelled.Load() {
		t.Fatal("slow member did not observe ctx.Done()")
	}
}

func TestRunAgentModelOverride(t *testing.T) {
	cfg := makeConfig(nil, []phaseconfig.Phase{
		{Name: "review", Prompt: "review", Agent: "special-agent", Model: "gpt-5"},
	}, nil)

	var capturedAgent, capturedModel string
	_, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			capturedAgent = phase.Agent
			capturedModel = phase.Model
			return PhaseAttemptResult{ExitCode: 0, SessionID: "s1"}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if capturedAgent != "special-agent" {
		t.Fatalf("Agent = %q, want special-agent", capturedAgent)
	}
	if capturedModel != "gpt-5" {
		t.Fatalf("Model = %q, want gpt-5", capturedModel)
	}
}

func TestRunNudgesIncompletePhaseUntilRequiredFileExists(t *testing.T) {
	dir := t.TempDir()
	cfg := makeConfig(nil, []phaseconfig.Phase{{
		Name:   "review",
		Prompt: "review",
		Requires: phaseconfig.PhaseRequirements{
			Files: []string{"REVIEW_CODE.md"},
		},
	}}, nil)

	var attempts int
	var nudgePrompt string
	var nudgePrevSession string
	result, err := Run(context.Background(), RunOptions{
		WorkDir:   dir,
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			attempts++
			if attempts == 2 {
				nudgePrompt = phase.Prompt
				nudgePrevSession = prevSessionID
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
	if nudgePrevSession != "s1" {
		t.Fatalf("nudge prevSessionID = %q, want s1", nudgePrevSession)
	}
	if !strings.Contains(nudgePrompt, "REVIEW_CODE.md") || !strings.Contains(nudgePrompt, "Do not ask for permission") {
		t.Fatalf("nudge prompt = %q, want default missing-file nudge", nudgePrompt)
	}
}

func TestRunIncompleteRequiredFileFailsPhase(t *testing.T) {
	dir := t.TempDir()
	cfg := makeConfig(nil, []phaseconfig.Phase{{
		Name:   "review",
		Prompt: "review",
		Requires: phaseconfig.PhaseRequirements{
			Files: []string{"REVIEW_CODE.md"},
		},
		OnIncomplete: phaseconfig.PhaseOnIncomplete{MaxNudges: 1},
	}}, nil)

	result, err := Run(context.Background(), RunOptions{
		WorkDir:   dir,
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			return PhaseAttemptResult{ExitCode: 0, StopReason: "end_turn", SessionID: "s1"}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 1 {
		t.Fatalf("ExitCode = %d, want 1", result.ExitCode)
	}
	if result.StopReason != "incomplete_output" {
		t.Fatalf("StopReason = %q, want incomplete_output", result.StopReason)
	}
}

func TestRunDefaultNudgeUsesCurrentMissingFiles(t *testing.T) {
	dir := t.TempDir()
	cfg := makeConfig(nil, []phaseconfig.Phase{{
		Name:   "review",
		Prompt: "review",
		Requires: phaseconfig.PhaseRequirements{
			Files: []string{"REVIEW_CODE.md", "REVIEW_TESTS.md"},
		},
		OnIncomplete: phaseconfig.PhaseOnIncomplete{MaxNudges: 2},
	}}, nil)

	var nudgePrompts []string
	_, err := Run(context.Background(), RunOptions{
		WorkDir:   dir,
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			if attemptNum > 0 {
				nudgePrompts = append(nudgePrompts, phase.Prompt)
			}
			if attemptNum == 1 {
				if err := os.WriteFile(filepath.Join(dir, "REVIEW_CODE.md"), []byte("done"), 0o600); err != nil {
					return PhaseAttemptResult{}, err
				}
			}
			if attemptNum == 2 {
				if err := os.WriteFile(filepath.Join(dir, "REVIEW_TESTS.md"), []byte("done"), 0o600); err != nil {
					return PhaseAttemptResult{}, err
				}
			}
			return PhaseAttemptResult{ExitCode: 0, StopReason: "end_turn", SessionID: fmt.Sprintf("s%d", attemptNum)}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(nudgePrompts) != 2 {
		t.Fatalf("nudge prompts = %d, want 2", len(nudgePrompts))
	}
	if !strings.Contains(nudgePrompts[0], "REVIEW_CODE.md") || !strings.Contains(nudgePrompts[0], "REVIEW_TESTS.md") {
		t.Fatalf("first nudge prompt = %q, want both missing files", nudgePrompts[0])
	}
	if strings.Contains(nudgePrompts[1], "REVIEW_CODE.md") || !strings.Contains(nudgePrompts[1], "REVIEW_TESTS.md") {
		t.Fatalf("second nudge prompt = %q, want only REVIEW_TESTS.md", nudgePrompts[1])
	}
}

func TestRequiredFilePathRejectsPathsOutsideWorkDir(t *testing.T) {
	dir := t.TempDir()
	for _, path := range []string{"/etc/passwd", "../outside", "nested/../../outside"} {
		if _, ok := requiredFilePath(dir, path); ok {
			t.Fatalf("requiredFilePath(%q) allowed path outside workdir", path)
		}
	}
	fullPath, ok := requiredFilePath(dir, "nested/file.txt")
	if !ok {
		t.Fatal("expected relative path under workdir to be allowed")
	}
	if fullPath != filepath.Join(dir, "nested", "file.txt") {
		t.Fatalf("fullPath = %q, want nested file under workdir", fullPath)
	}
}

func TestRunAllTeamPhasesAllowAgentModelOverride(t *testing.T) {
	cfg := makeConfig(
		[]phaseconfig.Phase{{Name: "setup", Prompt: "setup", Agent: "pre-agent", Model: "pre-model"}},
		[]phaseconfig.Phase{{Name: "review", Prompt: "review", Agent: "team-agent", Model: "team-model"}},
		[]phaseconfig.Phase{{Name: "report", Prompt: "report", Agent: "post-agent", Model: "post-model"}},
	)

	got := map[string][2]string{}
	_, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			got[phase.Name] = [2]string{phase.Agent, phase.Model}
			return PhaseAttemptResult{ExitCode: 0, SessionID: phase.Name + "-s"}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got["setup"] != [2]string{"pre-agent", "pre-model"} {
		t.Fatalf("pre phase overrides = %+v, want pre-agent/pre-model", got["setup"])
	}
	if got["review"] != [2]string{"team-agent", "team-model"} {
		t.Fatalf("team phase overrides = %+v, want team-agent/team-model", got["review"])
	}
	if got["report"] != [2]string{"post-agent", "post-model"} {
		t.Fatalf("post phase overrides = %+v, want post-agent/post-model", got["report"])
	}
}

func TestBuildMemberListSkipsConditionalCaseInsensitive(t *testing.T) {
	team := []phaseconfig.Phase{
		{Name: "security", Prompt: "review security", Conditional: true},
		{Name: "style", Prompt: "review style"},
	}

	filtered := buildMemberList(team, "[TEAM: SKIP | Security]")
	if len(filtered) != 1 || filtered[0].Name != "style" {
		t.Fatalf("filtered = %+v, want only style", filtered)
	}
}

func TestBuildMemberListDoesNotSkipNonConditionalMembers(t *testing.T) {
	team := []phaseconfig.Phase{
		{Name: "security", Prompt: "review security"},
		{Name: "style", Prompt: "review style", Conditional: true},
	}

	filtered := buildMemberList(team, "[team: skip | security]\n[team: skip | style]")
	if len(filtered) != 1 || filtered[0].Name != "security" {
		t.Fatalf("filtered = %+v, want only non-conditional security", filtered)
	}
}

func TestRunNestedAbortPropagatesDirectiveAndReason(t *testing.T) {
	cfg := makeConfig(nil, []phaseconfig.Phase{{Name: "nested", LoopFile: "child.json"}}, nil)

	result, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		ConfigDir: "/tmp/configs",
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			t.Fatal("PhaseAttempt should not run for nested phase")
			return PhaseAttemptResult{}, nil
		},
		NestedRun: func(ctx context.Context, configPath string, runType string) (NestedResult, error) {
			if configPath != "/tmp/configs/child.json" {
				t.Fatalf("configPath = %q, want /tmp/configs/child.json", configPath)
			}
			if runType != "loop" {
				t.Fatalf("runType = %q, want loop", runType)
			}
			return NestedResult{ExitCode: 5, StopReason: "blocked", SessionID: "child-s", Reason: "needs review"}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 5 || result.StopReason != "blocked" || result.Reason != "needs review" || result.SessionID != "child-s" {
		t.Fatalf("result = %+v, want blocked child result", result)
	}
}

func TestRunNestedPhaseRequiresNestedRun(t *testing.T) {
	cfg := makeConfig(nil, []phaseconfig.Phase{{Name: "nested", TeamFile: "child.json"}}, nil)

	result, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 1 || result.StopReason != "phase_error" {
		t.Fatalf("result = %+v, want phase_error result", result)
	}
}

func TestRunPostPhaseReceivesTeamMemberOutput(t *testing.T) {
	cfg := makeConfig(
		nil,
		[]phaseconfig.Phase{
			simplePhase("worker-a", "do work a"),
			simplePhase("worker-b", "do work b"),
		},
		[]phaseconfig.Phase{
			{Name: "report", Prompt: "summarize: {{.TeamOutput}}"},
		},
	)

	var capturedPostPrompt string
	_, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			switch phase.Name {
			case "worker-a":
				return PhaseAttemptResult{ExitCode: 0, Output: "output from worker-a"}, nil
			case "worker-b":
				return PhaseAttemptResult{ExitCode: 0, Output: "output from worker-b"}, nil
			case "report":
				capturedPostPrompt = phase.Prompt
			}
			return PhaseAttemptResult{ExitCode: 0}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !strings.Contains(capturedPostPrompt, "output from worker-a") {
		t.Errorf("post phase prompt missing worker-a output; got: %q", capturedPostPrompt)
	}
	if !strings.Contains(capturedPostPrompt, "output from worker-b") {
		t.Errorf("post phase prompt missing worker-b output; got: %q", capturedPostPrompt)
	}
}

func TestRunTeamFinalOutputTemplate(t *testing.T) {
	cfg := makeConfig(
		nil,
		[]phaseconfig.Phase{
			simplePhase("worker-a", "do work a"),
			simplePhase("worker-b", "do work b"),
		},
		[]phaseconfig.Phase{
			{Name: "report", Prompt: "final outputs:\n{{.TeamOutput}}\n---\nfinal replies:\n{{.TeamFinalOutput}}"},
		},
	)

	var capturedPostPrompt string
	_, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "test-run",
		EventSink: discardEventWriter{},
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			switch phase.Name {
			case "worker-a":
				return PhaseAttemptResult{ExitCode: 0, Output: "thinking a\nfinal a", FinalReply: "final a"}, nil
			case "worker-b":
				return PhaseAttemptResult{ExitCode: 0, Output: "thinking b\nfinal b", FinalReply: "final b"}, nil
			case "report":
				capturedPostPrompt = phase.Prompt
			}
			return PhaseAttemptResult{ExitCode: 0}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	want := "final outputs:\nthinking a\nfinal a\nthinking b\nfinal b\n---\nfinal replies:\nfinal a\nfinal b"
	if capturedPostPrompt != want {
		t.Errorf("post phase prompt mismatch.\n got: %q\nwant: %q", capturedPostPrompt, want)
	}
}

func TestRunPhaseEndPreservesExplicitStopReason(t *testing.T) {
	sink := &recordingEventWriter{}
	result, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "test-run",
		EventSink: sink,
		Config:    makeConfig(nil, []phaseconfig.Phase{simplePhase("review", "review")}, nil),
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			return PhaseAttemptResult{
				ExitCode:   124,
				SessionID:  "s1",
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

// TestRunTeamPreservesCompletedMembersOnDegenerateStream verifies the
// cross-member behaviour the user asked for: when one team member
// aborts with degenerate_reasoning_stream, the team result must
// clearly report the failed member while still preserving the
// outputs of completed members. This is the user-visible contract
// that umpire-bot's Run() relies on when it falls back to partial
// reviewer output (see review/runner.go: reviewRawHasFindings +
// "Pony team produced no raw findings").
func TestRunTeamPreservesCompletedMembersOnDegenerateStream(t *testing.T) {
	cfg := makeConfig(
		nil,
		[]phaseconfig.Phase{
			simplePhase("reviewer", "review code"),
			simplePhase("test-reviewer", "review tests"),
		},
		nil,
	)

	sink := &recordingEventWriter{}
	result, err := Run(context.Background(), RunOptions{
		WorkDir:   t.TempDir(),
		RunID:     "test-run",
		EventSink: sink,
		Config:    cfg,
		PhaseAttempt: func(ctx context.Context, phase phaseconfig.Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error) {
			switch phase.Name {
			case "reviewer":
				// Healthy member: returns a review with output and final reply.
				return PhaseAttemptResult{
					ExitCode:   0,
					SessionID:  "reviewer-s",
					StopReason: "end_turn",
					Output:     "reviewer raw output\n## Findings\nnone",
					FinalReply: "Looks good.",
				}, nil
			case "test-reviewer":
				// Failed member: pony's reasoning stream guard tripped.
				// The runner returns ExitCode 1 (mapped from
				// unknown stop reason) and the explicit stop_reason
				// of degenerate_reasoning_stream.
				return PhaseAttemptResult{
					ExitCode:   runtime.ExitCodeForStopReason("degenerate_reasoning_stream"),
					SessionID:  "test-reviewer-s",
					StopReason: "degenerate_reasoning_stream",
				}, nil
			}
			return PhaseAttemptResult{}, nil
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	// Team run is reported as a phase failure: exit_code=1, stop_reason
	// surfaces the failed member's reason.
	if result.StopReason != "degenerate_reasoning_stream" {
		t.Errorf("RunResult.StopReason = %q, want degenerate_reasoning_stream", result.StopReason)
	}
	if result.ExitCode == 0 {
		t.Errorf("RunResult.ExitCode = %d, want non-zero for failed member", result.ExitCode)
	}

	// Verify the team.end event reports the failure with the members counts.
	// Note: the team runner counts every member that returned a result as
	// "completed" (only directive=abort counts as aborted). The failure
	// is signalled by exit_reason=phase_failure together with the
	// RunResult's stop_reason. This mirrors the actual production
	// behaviour seen in /tmp/pony/failed-run-18.ndjson where
	// members_completed=2, members_aborted=0, exit_reason=phase_failure.
	var teamEnd *events.Event
	for i := range sink.events {
		if sink.events[i].Event == "avenor.team.end" {
			teamEnd = &sink.events[i]
			break
		}
	}
	if teamEnd == nil {
		t.Fatal("expected avenor.team.end event in stream")
	}
	if got := teamEnd.Fields["exit_reason"]; got != "phase_failure" {
		t.Errorf("avenor.team.end exit_reason = %v, want phase_failure", got)
	}
	if got := teamEnd.Fields["members_aborted"]; got != 0 {
		t.Errorf("members_aborted = %v, want 0 (no abort directive was issued)", got)
	}

	// Per-member phase.end events: test-reviewer should carry the
	// degenerate_reasoning_stream stop_reason, reviewer should be end_turn.
	phaseEndByName := map[string]string{}
	for _, ev := range sink.events {
		if ev.Event != "avenor.phase.end" {
			continue
		}
		name, _ := ev.Fields["phase"].(string)
		reason, _ := ev.Fields["stop_reason"].(string)
		phaseEndByName[name] = reason
	}
	if phaseEndByName["test-reviewer"] != "degenerate_reasoning_stream" {
		t.Errorf("test-reviewer phase.end stop_reason = %q, want degenerate_reasoning_stream",
			phaseEndByName["test-reviewer"])
	}
	if phaseEndByName["reviewer"] != "end_turn" {
		t.Errorf("reviewer phase.end stop_reason = %q, want end_turn", phaseEndByName["reviewer"])
	}
}
