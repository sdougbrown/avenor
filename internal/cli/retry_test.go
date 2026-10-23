package cli

import (
	"context"
	"strings"
	"testing"
	"time"

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

func TestRunRetryDropsStartOnlyOptionsOnAdoptedResume(t *testing.T) {
	for _, tc := range []struct {
		name            string
		backend         string
		thinking        string
		wantSecondThink string
	}{
		// claude resumes take no per-call options, so carrying start-only
		// options into the adopted resume would abort the retry.
		{"plain resume backend", "claude", "high", ""},
		{"plain resume backend, channel", "claude-channel", "medium", ""},
		// codex resumes with options, so thinking survives the retry.
		{"resume-with-options backend", "codex-app-server", "max", "max"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oldRunAttempt := runAttempt
			oldRetryAfter := retryAfter
			t.Cleanup(func() { runAttempt, retryAfter = oldRunAttempt, oldRetryAfter })

			type attempt struct {
				opts runtime.StartOptions
			}
			var attempts []attempt
			runAttempt = func(ctx context.Context, cfg attemptConfig, deps attemptDeps) attemptResult {
				attempts = append(attempts, attempt{opts: cfg.startOptions})
				if len(attempts) == 1 {
					return attemptResult{exitCode: 7, sessionID: "ses_1", stopReason: runtime.RateLimitedStopReason, retryAfter: 25}
				}
				return attemptResult{exitCode: 0, sessionID: "ses_2"}
			}
			retryAfter = func(time.Duration) <-chan time.Time {
				ch := make(chan time.Time, 1)
				ch <- time.Now()
				return ch
			}

			var stderr strings.Builder
			args := []string{
				"--backend", tc.backend,
				"--thinking", tc.thinking,
				"--prompt", "hi",
				"--max-retries", "1",
			}
			if tc.wantSecondThink == "" {
				args = append(args, "--claude-mcp-wait", "7s")
			}
			if got := run(args, func(string) string { return "" }, &stderr); got != 0 {
				t.Fatalf("run() = %d, want 0; stderr=%s", got, stderr.String())
			}
			if len(attempts) != 2 {
				t.Fatalf("attempts = %d, want 2", len(attempts))
			}
			if attempts[0].opts.Thinking != tc.thinking {
				t.Fatalf("first attempt lost thinking: %+v", attempts[0].opts)
			}
			if tc.wantSecondThink == "" && attempts[0].opts.ClaudeMCPWait != 7*time.Second {
				t.Fatalf("first attempt lost the start options: %+v", attempts[0].opts)
			}
			if attempts[1].opts.Thinking != tc.wantSecondThink {
				t.Fatalf("second attempt Thinking = %q, want %q", attempts[1].opts.Thinking, tc.wantSecondThink)
			}
			if attempts[1].opts.ClaudeMCPWait != 0 {
				t.Fatalf("second attempt ClaudeMCPWait = %v, want 0", attempts[1].opts.ClaudeMCPWait)
			}
		})
	}
}

func TestResumeSessionDropsStartOnlyOptionsForPlainResume(t *testing.T) {
	p := &plainThinkingProvider{}
	if _, err := resumeSession(context.Background(), p, "claude", runtime.StartOptions{Thinking: "high", ClaudeMCPWait: 7 * time.Second}, "ses"); err != nil {
		t.Fatalf("resumeSession: %v", err)
	}
	if p.resumeCalls != 1 || p.startCalls != 0 {
		t.Fatalf("resume calls = %d, start calls = %d, want one plain resume", p.resumeCalls, p.startCalls)
	}
}

func TestDropStartOnlyOptionsKeysOnResumeCapability(t *testing.T) {
	for _, backend := range []string{"claude", "claude-channel"} {
		plain := dropStartOnlyOptions(backend, runtime.StartOptions{Thinking: "high", ClaudeMCPWait: 7 * time.Second})
		if plain.Thinking != "" || plain.ClaudeMCPWait != 0 {
			t.Fatalf("%s start-only options not dropped: %+v", backend, plain)
		}
	}
	withOptions := dropStartOnlyOptions("codex-app-server", runtime.StartOptions{Thinking: "max", ClaudeMCPWait: time.Second})
	if withOptions.Thinking != "max" || withOptions.ClaudeMCPWait != time.Second {
		t.Fatalf("resume-with-options backend lost options: %+v", withOptions)
	}
}
