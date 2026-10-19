package runtime

import "testing"

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
			stopReason: degenerateReasoningStreamStopReason,
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
		"try again in 20s":        20,
		"retry after 30":          30,
		"retry-after: 1500ms":     2,
		"resets in 2 minutes":     120,
		"try again in 6.5s":       7,
		"try again in 2 days":     0,
		"no hint here":            0,
		"retry after 0":           0,
		"retry after 1000000000s": 0,
	}
	for text, want := range tests {
		if got := retryAfterSeconds(text); got != want {
			t.Errorf("retryAfterSeconds(%q) = %d, want %d", text, got, want)
		}
	}
}
