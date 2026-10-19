package runtime

import (
	"context"
	"math"
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
		// The incident text: a billing block forwarded verbatim by pi.
		{name: "pi billing block 402", stopReason: "error", message: `turn failed for session 01a151cb: 402: {"message":"Insufficient Balance (request_id: 4e2d2a94-3469-4e99-9750-2ce9e58722b1)","type":"unknown_error","param":null,"code":"invalid_request_error"}`, wantReason: QuotaExhaustedStopReason},
		{name: "codex usage limit", stopReason: "error", message: "Codex error: The usage limit has been reached", wantReason: QuotaExhaustedStopReason},
		{name: "acp rate limit", stopReason: "error", message: "rate limit exceeded, try again in 1.5s", wantReason: RateLimitedStopReason, wantRetry: 2},
		{name: "opencode error name", stopReason: "RateLimitError", wantReason: RateLimitedStopReason},
		{name: "gateway 429 with retry hint", stopReason: "error", message: "429: rate limit reached for requests; retry-after: 30", wantReason: RateLimitedStopReason, wantRetry: 30},
		// A reset hint describes a hard-capped account: the class wins.
		{name: "quota wording with reset hint stays exhausted", stopReason: "error", message: "You exceeded your current quota; try again in 2.5s", wantReason: QuotaExhaustedStopReason, wantRetry: 3},
		{name: "quota wording with 429 is throttling", stopReason: "error", message: "quota exceeded: 429 Too Many Requests", wantReason: RateLimitedStopReason},
		{name: "line number is not a 429", stopReason: "error", message: "error at /src/foo.go:429: invalid request"},
		{name: "line number is not a 402", stopReason: "error", message: "error at /src/foo.go:402: invalid request"},
		{name: "no limit signal", stopReason: "error", message: "400: invalid model name passed in model=definitely-not-a-model"},
		{name: "explicit max_tokens wins", stopReason: "max_tokens", message: "rate limit reached"},
		{name: "avenor verdict wins", stopReason: DegenerateReasoningStreamStopReason, message: "rate limit reached"},
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
		"try again in 2 days":       0, // beyond a day, so no caller can schedule it
		"try again in 2 fortnights": 0, // unrecognized unit
		"no hint here":              0,
		"retry after 1000000000s":   0,
	}
	for text, want := range tests {
		if got := retryAfterSeconds(text); got != want {
			t.Errorf("retryAfterSeconds(%q) = %d, want %d", text, got, want)
		}
	}
}

func TestRetryWait(t *testing.T) {
	// The ceiling is compared in whole seconds: the boundary and one above it.
	tests := []struct {
		name       string
		backoff    time.Duration
		retryAfter int64
		deadline   time.Duration
		wantDelay  time.Duration // zero: no timer
		wantRetry  bool
	}{
		{name: "hint exceeds backoff", backoff: 2 * time.Second, retryAfter: 20, wantDelay: 20 * time.Second, wantRetry: true},
		{name: "backoff exceeds hint", backoff: 8 * time.Second, retryAfter: 3, wantDelay: 8 * time.Second, wantRetry: true},
		{name: "hint at the ceiling", retryAfter: 300, wantDelay: retryWaitCeiling, wantRetry: true},
		{name: "hint above the ceiling", retryAfter: 301},
		{name: "hint past the deadline", backoff: 8 * time.Second, retryAfter: 30, deadline: 5 * time.Second},
		{name: "hint past an expired deadline", retryAfter: 20, deadline: -time.Second},
		{name: "absurd hint", retryAfter: math.MaxInt64}, // must not wrap the conversion
		{name: "backoff past the deadline", backoff: 8 * time.Second, deadline: 5 * time.Second, wantRetry: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.deadline != 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, time.Now().Add(tt.deadline))
				defer cancel()
			}
			var waited time.Duration
			called := false
			after := func(delay time.Duration) <-chan time.Time {
				called = true
				waited = delay
				return make(chan time.Time)
			}
			wait, retry := RetryWait(ctx, after, tt.backoff, tt.retryAfter)
			if retry != tt.wantRetry {
				t.Fatalf("RetryWait retry = %v, want %v", retry, tt.wantRetry)
			}
			if tt.wantDelay == 0 {
				if wait != nil || called {
					t.Fatalf("RetryWait = %v after %v, want no timer", wait, waited)
				}
				return
			}
			if wait == nil || waited != tt.wantDelay {
				t.Fatalf("RetryWait = %v after %v, want a timer after %v", wait, waited, tt.wantDelay)
			}
		})
	}
}
