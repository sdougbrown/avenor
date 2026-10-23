package cli

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

func TestBackoffDelay(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
		{5, 30 * time.Second}, // capped (would be 32s)
		{6, 30 * time.Second}, // capped (would be 64s)
	}
	for _, tt := range tests {
		if got := backoffDelay(tt.attempt); got != tt.want {
			t.Errorf("backoffDelay(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestResumeSessionGatesTheStartOnlyDrop(t *testing.T) {
	fresh := &plainThinkingProvider{}
	if _, err := ResumeSession(context.Background(), fresh, "claude", runtime.StartOptions{Thinking: "high", ClaudeMCPWait: time.Second}, ""); err != nil {
		t.Fatalf("fresh ResumeSession: %v", err)
	}
	if fresh.startCalls != 1 || fresh.resumeCalls != 0 {
		t.Fatalf("fresh calls = start:%d resume:%d, want one start", fresh.startCalls, fresh.resumeCalls)
	}
	if fresh.lastStartOpts.Thinking != "high" || fresh.lastStartOpts.ClaudeMCPWait != time.Second {
		t.Fatalf("fresh start lost the start-only options: %+v", fresh.lastStartOpts)
	}

	resumed := &plainThinkingProvider{}
	if _, err := ResumeSession(context.Background(), resumed, "claude", runtime.StartOptions{Thinking: "high", ClaudeMCPWait: time.Second}, "ses"); err != nil {
		t.Fatalf("resume ResumeSession: %v", err)
	}
	if resumed.resumeCalls != 1 || resumed.startCalls != 0 {
		t.Fatalf("resume calls = start:%d resume:%d, want one plain resume", resumed.startCalls, resumed.resumeCalls)
	}
}

func TestStartSessionClassifiesResumeThinkingByPolicy(t *testing.T) {
	p := &plainThinkingProvider{}
	_, err := StartSession(context.Background(), p, "claude", runtime.StartOptions{Thinking: "off"}, "ses")
	if err == nil || !strings.Contains(err.Error(), `"off"`) {
		t.Fatalf("claude/off resume error = %v, want the policy value error", err)
	}
	for _, level := range []string{"low", "high", "max"} {
		p := &plainThinkingProvider{}
		_, err := StartSession(context.Background(), p, "claude", runtime.StartOptions{Thinking: level}, "ses")
		if err == nil || !strings.Contains(err.Error(), "only when starting a session") || !strings.Contains(err.Error(), `value "`+level+`"`) {
			t.Fatalf("claude/%s resume error = %v, want the start-only error", level, err)
		}
	}
}

// runSingleAttempt tests drive the real attempt path (Start, the mid-loop
// resume sites, and the retry adoption) with a claude backend carrying
// start-only options, so a regression to a raw StartSession aborts the test
// with the start-only error instead of resuming.

type nopEventSink struct{}

func (nopEventSink) Write(events.Event) error { return nil }
func (nopEventSink) Close() error             { return nil }

func TestRunRetryResumesWithStartOnlyOptionsDropped(t *testing.T) {
	provider := newScriptedProvider()
	provider.turns = []scriptedTurn{
		{stopReason: runtime.RateLimitedStopReason},
		{stopReason: "end_turn"},
	}
	oldNewProvider := newProvider
	newProvider = func(runtime.StartOptions, string) (runtime.Provider, error) { return provider, nil }
	t.Cleanup(func() { newProvider = oldNewProvider })
	oldRetryAfter := retryAfter
	retryAfter = func(time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		ch <- time.Now()
		return ch
	}
	t.Cleanup(func() { retryAfter = oldRetryAfter })

	var stderr strings.Builder
	if got := run([]string{
		"--dir", t.TempDir(),
		"--backend", "claude",
		"--thinking", "high",
		"--claude-mcp-wait", "7s",
		"--prompt", "hi",
		"--max-retries", "1",
	}, func(string) string { return "" }, &stderr); got != 0 {
		t.Fatalf("run() = %d, want 0; stderr=%s", got, stderr.String())
	}
	if provider.resumeCalls != 1 {
		t.Fatalf("resume calls = %d, want 1 (the retry resumes the adopted session)", provider.resumeCalls)
	}
}

func runSingleAttemptWithControl(t *testing.T, setup func(provider *scriptedProvider, cs *control.ControlServer)) (*scriptedProvider, attemptResult) {
	t.Helper()
	provider := newScriptedProvider()
	provider.turns = []scriptedTurn{
		{stopReason: "end_turn"},
		{stopReason: "end_turn"},
	}
	oldNewProvider := newProvider
	newProvider = func(runtime.StartOptions, string) (runtime.Provider, error) { return provider, nil }
	t.Cleanup(func() { newProvider = oldNewProvider })

	owner, err := control.GenerateOwnerCapability()
	if err != nil {
		t.Fatalf("generate owner capability: %v", err)
	}
	cs := control.NewServer(control.NewState("run_1", "label", 1), owner)
	setup(provider, cs)

	cfg := attemptConfig{
		startOptions:    runtime.StartOptions{Thinking: "high", ClaudeMCPWait: 7 * time.Second},
		backend:         "claude",
		sessionBackends: newSessionBackendMap(),
		initialPrompt:   "hi",
		runID:           "run_1",
		runLabel:        "label",
	}
	deps := attemptDeps{writer: nopEventSink{}, controlServer: cs, stderr: io.Discard}
	result := runSingleAttempt(context.Background(), cfg, deps)
	return provider, result
}

func TestRunSingleAttemptResumesForInterruptQueuedBeforeStart(t *testing.T) {
	provider, result := runSingleAttemptWithControl(t, func(_ *scriptedProvider, cs *control.ControlServer) {
		cs.InterruptPrompt("interrupted!", false)
	})
	if result.exitCode != 0 {
		t.Fatalf("exit code = %d, want 0; stopReason=%q", result.exitCode, result.stopReason)
	}
	if provider.resumeCalls != 1 {
		t.Fatalf("resume calls = %d, want 1 (the interrupt resumes the session)", provider.resumeCalls)
	}
}

func TestRunSingleAttemptResumesForMidWaitInterrupt(t *testing.T) {
	provider, result := runSingleAttemptWithControl(t, func(p *scriptedProvider, cs *control.ControlServer) {
		started := make(chan struct{})
		p.promptStarted = started
		go func() {
			<-started
			cs.InterruptPrompt("interrupted!", false)
		}()
	})
	if result.exitCode != 0 {
		t.Fatalf("exit code = %d, want 0; stopReason=%q", result.exitCode, result.stopReason)
	}
	if provider.resumeCalls != 1 {
		t.Fatalf("resume calls = %d, want 1 (the interrupt resumes the session)", provider.resumeCalls)
	}
}

func TestRunSingleAttemptResumesForQueuedPromptAfterEndTurn(t *testing.T) {
	provider, result := runSingleAttemptWithControl(t, func(_ *scriptedProvider, cs *control.ControlServer) {
		cs.QueuePrompt("next")
	})
	if result.exitCode != 0 {
		t.Fatalf("exit code = %d, want 0; stopReason=%q", result.exitCode, result.stopReason)
	}
	if provider.resumeCalls != 1 {
		t.Fatalf("resume calls = %d, want 1 (the queued prompt resumes the session)", provider.resumeCalls)
	}
}
