package runtime

import (
	"math"
	"regexp"
	"strconv"
	"strings"
)

// Provider limit stop reasons. They separate "the provider refused for
// capacity" from "the agent failed", so an orchestrator can retry later or
// move the work to another model without reading provider text.
const (
	QuotaExhaustedStopReason = "quota_exhausted"
	RateLimitedStopReason    = "rate_limited"
)

// quotaSignals and rateLimitSignals are matched against the lowercased stop
// reason and terminal failure text. Quota is checked first because provider
// usage-limit wording often also mentions a limit.
var quotaSignals = []string{
	"quota",
	"usage limit",
	"usage_limit",
	"credit balance",
	"billing hard limit",
	"spend limit",
	"hit your limit",
}

var rateLimitSignals = []string{
	"rate limit",
	"rate_limit",
	"rate-limit",
	"ratelimit",
	"too many requests",
	"resource exhausted",
	"resource_exhausted",
	"overloaded",
}

// retryAfterHint captures the interval providers print next to a limit error,
// for example "try again in 20s", "retry after 30" or "resets in 2 minutes".
var retryAfterHint = regexp.MustCompile(`(?i)(?:retry[- ]?after|try again in|retry in|resets? in)\D{0,16}?(\d+(?:\.\d+)?)\s*(ms|milliseconds?|s|secs?|seconds?|m|mins?|minutes?|d|days?)?\b`)

// status429 matches an HTTP status token, so an unrelated number that happens
// to contain "429" is not read as a rate-limit status.
var status429 = regexp.MustCompile(`(^|[\s:(])429($|[\s:),])`)

// ClassifyProviderLimit maps a provider's usage-limit or rate-limit failure
// onto a stop reason, and returns the provider's retry-after hint in whole
// seconds when the text carries one. An empty stop reason means the text has
// no limit signal.
//
// An explicit stop reason wins: only generic failures (no dedicated exit code)
// are reclassified. Session identity conflicts and degenerate reasoning
// streams are Avenor's own verdicts, not provider capacity.
func ClassifyProviderLimit(stopReason, message string) (string, int64) {
	switch stopReason {
	case SessionIDConflictStopReason, ToolDeniedStopReason, degenerateReasoningStreamStopReason:
		return "", 0
	}
	if ExitCodeForStopReason(stopReason) != 1 {
		return "", 0
	}
	text := strings.ToLower(stopReason + " " + message)
	for _, signal := range quotaSignals {
		if strings.Contains(text, signal) {
			return QuotaExhaustedStopReason, retryAfterSeconds(message)
		}
	}
	if status429.MatchString(text) {
		return RateLimitedStopReason, retryAfterSeconds(message)
	}
	for _, signal := range rateLimitSignals {
		if strings.Contains(text, signal) {
			return RateLimitedStopReason, retryAfterSeconds(message)
		}
	}
	return "", 0
}

// degenerateReasoningStreamStopReason mirrors the loop runner's abort reason.
// It shares the generic exit-1 code, so classification must not adopt it.
const degenerateReasoningStreamStopReason = "degenerate_reasoning_stream"

// retryAfterSeconds parses the retry-after hint out of provider text. Values
// beyond a day are treated as absent: no caller can schedule against them.
func retryAfterSeconds(text string) int64 {
	match := retryAfterHint.FindStringSubmatch(text)
	if match == nil {
		return 0
	}
	value, err := strconv.ParseFloat(match[1], 64)
	if err != nil || value <= 0 || math.IsInf(value, 0) {
		return 0
	}
	switch unit := strings.ToLower(match[2]); {
	case strings.HasPrefix(unit, "ms") || strings.HasPrefix(unit, "milli"):
		value /= 1000
	case strings.HasPrefix(unit, "m"):
		value *= 60
	case strings.HasPrefix(unit, "d"):
		value *= 86400
	}
	seconds := int64(math.Ceil(value))
	if seconds <= 0 {
		seconds = 1
	}
	if seconds > 86400 {
		return 0
	}
	return seconds
}
