package runtime

import (
	"context"
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

// retryWaitCeiling bounds a provider's retry hint: a longer wait is not a retry
// this run can usefully make. It follows workflowcontroller.ClampRetryDelay.
const retryWaitCeiling = 5 * time.Minute

// quotaSignals and rateLimitSignals are matched against the lowercased stop
// reason and terminal failure text. They are the only copy of the vocabulary.
var quotaSignals = []string{
	"quota",
	"usage limit",
	"usage_limit",
	"credit balance",
	"insufficient balance",
	"insufficient credits",
	"payment required",
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

// status429 and status402 match an HTTP status a provider reports. The left
// boundary accepts a space, '(' or '=' so "API error (429)" and "status=429"
// read as statuses, while a source location such as "foo.go:429:" does not.
var (
	status429 = regexp.MustCompile(`(?:^|[\s(=])429(?:$|[\s:),.])`)
	status402 = regexp.MustCompile(`(?:^|[\s(=])402(?:$|[\s:),.])`)
)

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
	// Only an explicit throttle signal makes a limit retryable. Quota wording
	// with a reset hint describes a hard-capped account: retrying spends the
	// budget on the same refusal, while quota_exhausted tells the caller to
	// switch models.
	if containsSignal(text, rateLimitSignals) || status429.MatchString(text) {
		return RateLimitedStopReason, retryAfterSeconds(message)
	}
	if containsSignal(text, quotaSignals) || status402.MatchString(text) {
		return QuotaExhaustedStopReason, retryAfterSeconds(message)
	}
	return "", 0
}

// RetryWait returns the timer to wait on before the next attempt, and whether
// the provider's hint leaves an attempt worth making.
//
// The wait is the caller's backoff, extended to the provider's retry-after
// hint. A hint above retryWaitCeiling, or a wait that cannot fit before the run
// deadline, yields no timer and false: the run cannot honor what the provider
// asked for, so the caller stops retrying and reports the provider's own class
// instead of parking the run. With no hint, a backoff that does not fit before
// the deadline also yields no timer, but true: the caller's context case
// classifies the run.
func RetryWait(ctx context.Context, after func(time.Duration) <-chan time.Time, backoff time.Duration, retryAfter int64) (<-chan time.Time, bool) {
	delay := backoff
	hinted := retryAfter > 0
	if hinted {
		// Compare in seconds so an absurd hint cannot wrap the conversion.
		if retryAfter > int64(retryWaitCeiling/time.Second) {
			return nil, false
		}
		if hint := time.Duration(retryAfter) * time.Second; hint > delay {
			delay = hint
		}
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < delay {
		return nil, !hinted
	}
	return after(delay), true
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
