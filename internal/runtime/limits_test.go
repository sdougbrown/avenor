package runtime

import (
	"testing"
	"time"
)

// TestClassifyProviderLimit covers the error shapes each backend forwards, so a
// new provider wording that should classify does not silently regress to a
// generic failure.
func TestClassifyProviderLimit(t *testing.T) {
	tests := []struct {
		name       string
		stopReason string
		message    string
		wantReason string
		wantRetry  int64
	}{
		{
			// pi forwards the gateway's raw body; this is the OpenAI-style
			// quota error LiteLLM returns for an exhausted key.
			name:       "pi gateway quota body",
			stopReason: "error",
			message:    `400: {"type":"insufficient_quota","message":"You exceeded your current quota"}`,
			wantReason: QuotaExhaustedStopReason,
		},
		{
			// codex app-server turn/completed failure text.
			name:       "codex usage limit",
			stopReason: "error",
			message:    "Codex error: The usage limit has been reached",
			wantReason: QuotaExhaustedStopReason,
		},
		{
			// Claude subscription limit printed by the CLI.
			name:       "claude usage limit",
			stopReason: "error",
			message:    "Claude AI usage limit reached. Your limit will reset at 3pm",
			wantReason: QuotaExhaustedStopReason,
		},
		{
			// ACP agents surface the provider error as a JSON-RPC message.
			name:       "acp rate limit",
			stopReason: "error",
			message:    "rate limit exceeded, try again in 1.5s",
			wantReason: RateLimitedStopReason,
			wantRetry:  2,
		},
		{
			// opencode-http reports the provider error name as the stop reason.
			name:       "opencode error name",
			stopReason: "RateLimitError",
			wantReason: RateLimitedStopReason,
		},
		{
			name:       "gateway 429 with retry hint",
			stopReason: "error",
			message:    "429: rate limit reached for requests; retry-after: 30",
			wantReason: RateLimitedStopReason,
			wantRetry:  30,
		},
		{
			// A throttling body that also says "quota" is transient, so the
			// retry hint wins over the non-retryable quota class.
			name:       "quota wording with retry hint is throttling",
			stopReason: "error",
			message:    "You exceeded your current quota; try again in 2.5s",
			wantReason: RateLimitedStopReason,
			wantRetry:  3,
		},
		{
			name:       "quota wording with 429 is throttling",
			stopReason: "error",
			message:    "quota exceeded: 429 Too Many Requests",
			wantReason: RateLimitedStopReason,
		},
		{
			// A source location is not an HTTP status.
			name:       "line number is not a 429",
			stopReason: "error",
			message:    "error at /src/foo.go:429: invalid request",
		},
		{
			name:       "grpc resource exhausted",
			stopReason: "error",
			message:    "grpc: RESOURCE_EXHAUSTED",
			wantReason: RateLimitedStopReason,
		},
		{
			name:       "no limit signal",
			stopReason: "error",
			message:    "400: invalid model name passed in model=definitely-not-a-model",
		},
		{
			name:       "explicit max_tokens wins",
			stopReason: "max_tokens",
			message:    "rate limit reached",
		},
		{
			name:       "avenor verdict wins",
			stopReason: DegenerateReasoningStreamStopReason,
			message:    "rate limit reached",
		},
		{
			name:       "session conflict wins",
			stopReason: SessionIDConflictStopReason,
			message:    "quota exceeded",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason, retryAfter := ClassifyProviderLimit(tt.stopReason, tt.message)
			if reason != tt.wantReason || retryAfter != tt.wantRetry {
				t.Fatalf("ClassifyProviderLimit(%q, %q) = (%q, %d), want (%q, %d)",
					tt.stopReason, tt.message, reason, retryAfter, tt.wantReason, tt.wantRetry)
			}
		})
	}
}

func TestRetryAfterSeconds(t *testing.T) {
	tests := map[string]int64{
		"try again in 20s":          20,
		"retry after 30":            30,
		"retry-after: 1500ms":       2,
		"resets in 2 minutes":       120,
		"try again in 6.5s":         7,
		"resets in 2 hours":         7200,
		"try again in 6h":           21600,
		"try again in 1 hour":       3600,
		"try again in 2 days":       0,
		"try again in 2 fortnights": 0,
		"try again in 2 parsecs":    0,
		"no hint here":              0,
		"retry after 0":             0,
		"retry after 1000000000s":   0,
	}
	for text, want := range tests {
		if got := retryAfterSeconds(text); got != want {
			t.Errorf("retryAfterSeconds(%q) = %d, want %d", text, got, want)
		}
	}
}

func TestHasProviderLimitSignal(t *testing.T) {
	tests := map[string]bool{
		"429 Too Many Requests":              true,
		"Error: rate limit exceeded":         true,
		"quota exhausted":                    true,
		"returned 429 Too Many Requests":     false,
		"the logs showed a rate limit error": false,
		"task completed":                     false,
	}
	for text, want := range tests {
		if got := HasProviderLimitSignal(text); got != want {
			t.Errorf("HasProviderLimitSignal(%q) = %v, want %v", text, got, want)
		}
	}
}

func TestRetryDelay(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name       string
		backoff    time.Duration
		retryAfter int64
		deadline   time.Time
		want       time.Duration
		wantFits   bool
		clamped    bool
	}{
		{name: "backoff only", backoff: 2 * time.Second, want: 2 * time.Second, wantFits: true},
		{name: "hint exceeds backoff", backoff: 2 * time.Second, retryAfter: 20, want: 20 * time.Second, wantFits: true},
		{name: "backoff exceeds hint", backoff: 8 * time.Second, retryAfter: 3, want: 8 * time.Second, wantFits: true},
		{name: "clamped to deadline", backoff: 8 * time.Second, retryAfter: 30, deadline: now.Add(5 * time.Second), want: 5 * time.Second, clamped: true},
		{name: "deadline already passed", backoff: 2 * time.Second, deadline: now.Add(-time.Second), want: 0, wantFits: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delay, fits := RetryDelay(tt.backoff, tt.retryAfter, tt.deadline)
			if fits != tt.wantFits {
				t.Fatalf("RetryDelay fits = %v, want %v", fits, tt.wantFits)
			}
			if tt.clamped {
				// The clamped value depends on the clock, so only its bounds are fixed.
				if delay <= 0 || delay > tt.want {
					t.Fatalf("RetryDelay = %v, want it clamped to at most %v", delay, tt.want)
				}
				return
			}
			if delay != tt.want {
				t.Fatalf("RetryDelay = %v, want %v", delay, tt.want)
			}
		})
	}
}

// TestRetryWait pins the shared retry wait every scheduler uses: the provider's
// hint is honored, and a wait that cannot fit before the run deadline yields no
// timer so the caller's context decides the outcome.
func TestRetryWait(t *testing.T) {
	var waited time.Duration
	after := func(delay time.Duration) <-chan time.Time {
		waited = delay
		return make(chan time.Time)
	}

	if RetryWait(after, 2*time.Second, 20, time.Time{}) == nil || waited != 20*time.Second {
		t.Fatalf("RetryWait = timer after %v, want a timer after the 20s hint", waited)
	}
	if wait := RetryWait(after, 8*time.Second, 0, time.Now().Add(time.Second)); wait != nil {
		t.Fatalf("RetryWait = %v, want no timer for a deadline-clamped wait", wait)
	}
}
