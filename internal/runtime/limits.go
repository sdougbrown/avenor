package runtime

import (
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Provider limit stop reasons. They separate "the provider refused for
// capacity" from "the agent failed", so an orchestrator can retry later or
// move the work to another model without reading provider text.
const (
	QuotaExhaustedStopReason = "quota_exhausted"
	RateLimitedStopReason    = "rate_limited"
)

// quotaSignals and rateLimitSignals are matched against the lowercased stop
// reason and terminal failure text.
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
// for example "try again in 20s", "retry after 30" or "resets in 2 hours".
var retryAfterHint = regexp.MustCompile(`(?i)(?:retry[- ]?after|try again in|retry in|resets? in)\D{0,16}?(\d+(?:\.\d+)?)\s*(milliseconds?|ms|hours?|hrs?|h|minutes?|mins?|m|secs?|seconds?|s|days?|d)?\b`)

// status429 matches an HTTP status token. The left boundary excludes ':' so a
// source location such as "foo.go:429:" is not read as an HTTP status.
var status429 = regexp.MustCompile(`(^|\s)429($|[\s:),.])`)

// providerSummarySignal matches a provider limit only in the shape a provider
// reports it: the message opens with the status code or the limit wording.
// A model-authored summary that merely mentions a 429 does not describe the
// provider's own refusal.
var providerSummarySignal = regexp.MustCompile(`(?i)^\s*(?:(?:error|failed|failure)[:=]?\s*)?(?:(?:http\s*)?[45]\d\d\b|quota|usage[ _]limit|rate[ _-]?limit|too many requests|resource[ _]exhausted|overloaded|credit balance|spend limit|billing hard limit|hit your limit)`)

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
	case SessionIDConflictStopReason, ToolDeniedStopReason, DegenerateReasoningStreamStopReason:
		return "", 0
	}
	if ExitCodeForStopReason(stopReason) != 1 {
		return "", 0
	}
	text := strings.ToLower(stopReason + " " + message)
	retryAfter := retryAfterSeconds(message)
	quota := containsSignal(text, quotaSignals)
	// A retry hint or a 429 alongside usage-limit wording describes throttling,
	// not an exhausted account: prefer the retryable class so --max-retries is
	// not silently disabled for a transient condition.
	if containsSignal(text, rateLimitSignals) || status429.MatchString(text) || (quota && retryAfter > 0) {
		return RateLimitedStopReason, retryAfter
	}
	if quota {
		return QuotaExhaustedStopReason, retryAfter
	}
	return "", 0
}

// HasProviderLimitSignal reports whether text opens with a provider limit
// signal. A model-authored summary is accepted as provider text only when it
// reports the refusal rather than mentioning one.
func HasProviderLimitSignal(text string) bool {
	return providerSummarySignal.MatchString(text)
}

// RetryDelay returns the sleep before the next attempt: the caller's backoff,
// extended to honor the provider's retry-after hint. A wait that does not fit
// before the run deadline is clamped to the time remaining and fits is false:
// waiting it out ends with the deadline, so the caller's deadline path owns
// the classification instead of scheduling an attempt that cannot start.
func RetryDelay(backoff time.Duration, retryAfter int64, deadline time.Time) (delay time.Duration, fits bool) {
	delay = backoff
	if hint := time.Duration(retryAfter) * time.Second; hint > delay {
		delay = hint
	}
	if deadline.IsZero() {
		return delay, true
	}
	remaining := time.Until(deadline)
	if remaining < delay {
		if remaining < 0 {
			remaining = 0
		}
		return remaining, false
	}
	return delay, true
}

// RetryWait returns the channel to wait on before the next attempt, or nil
// when the wait does not fit before the run deadline. A nil channel leaves
// only the caller's context able to wake the select, so its deadline path
// classifies the run instead of an attempt starting after the deadline.
func RetryWait(after func(time.Duration) <-chan time.Time, backoff time.Duration, retryAfter int64, deadline time.Time) <-chan time.Time {
	delay, fits := RetryDelay(backoff, retryAfter, deadline)
	if !fits {
		return nil
	}
	return after(delay)
}

func containsSignal(text string, signals []string) bool {
	for _, signal := range signals {
		if strings.Contains(text, signal) {
			return true
		}
	}
	return false
}

// retryAfterSeconds parses the retry-after hint out of provider text. Values
// beyond a day are treated as absent: no caller can schedule against them.
func retryAfterSeconds(text string) int64 {
	idx := retryAfterHint.FindStringSubmatchIndex(text)
	if idx == nil {
		return 0
	}
	value, err := strconv.ParseFloat(text[idx[2]:idx[3]], 64)
	if err != nil || value <= 0 || math.IsInf(value, 0) {
		return 0
	}
	unit := ""
	if idx[4] >= 0 {
		unit = strings.ToLower(text[idx[4]:idx[5]])
	}
	// A trailing word that is not a recognized unit ("2 fortnights") states no
	// interval Avenor can schedule against.
	if unit == "" && startsWithLetter(strings.TrimLeft(text[idx[1]:], " \t")) {
		return 0
	}
	switch {
	case strings.HasPrefix(unit, "ms") || strings.HasPrefix(unit, "milli"):
		value /= 1000
	case strings.HasPrefix(unit, "h"):
		value *= 3600
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

func startsWithLetter(text string) bool {
	if text == "" {
		return false
	}
	c := text[0]
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}
