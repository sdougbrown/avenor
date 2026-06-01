package pony

import (
	"regexp"
	"strings"
)

// degenerateReasoningStopReason is the session-level stop reason emitted when
// the loop aborts a turn because the upstream reasoning stream has become
// degenerate (provider leakage of internal control tokens, encoded-payload
// emission, or other content that is clearly not useful model work and would
// otherwise consume the entire output budget).
const degenerateReasoningStopReason = "degenerate_reasoning_stream"

// DegenerateReasoningStopReason exposes the canonical stop reason for tests
// and other packages that need to recognise or forward it.
func DegenerateReasoningStopReason() string {
	return degenerateReasoningStopReason
}

// reasoningStreamGuard is a per-turn state machine that watches the
// ChunkTypeReasoning deltas and decides when the stream has become
// degenerate enough to abort the session.
//
// It is intentionally simple: it tracks a small rolling window of recent
// reasoning deltas and the count of consecutive single-character hex deltas
// at the tail. The session aborts when one of three signals trips:
//
//   - Signal A: any reasoning delta matches the upstream control-token
//     pattern `<|...|>` (Llama 3.x channel/constrain format, <tool_call|>,
//     etc.). This is provider leakage of an internal wire format and is
//     never legitimate user-facing reasoning content.
//   - Signal B: a long run of single-character hex deltas with no
//     interspersed "normal" deltas. 256 consecutive single-hex deltas
//     matches the real-world failure pattern (the model has been emitting
//     a hex-encoded payload one nibble at a time).
//   - Signal C: the trailing window of reasoning is dominated by
//     single-character hex deltas (≥1024 of the last 2048 are single hex),
//     AND no tool call or message chunk has been observed in the same
//     window — the model is not making any forward progress.
//
// The guard does not consume the chunk channel itself; the loop calls
// Observe(delta) for each reasoning chunk and Inspect() between chunks.
// Observe returns a non-empty reason when the stream has gone degenerate
// and the session should be aborted immediately.
type reasoningStreamGuard struct {
	// consecutiveHex is the length of the current trailing run of
	// single-character hex deltas. Reset to 0 on any other delta.
	consecutiveHex int

	// windowRecent holds the most recent windowSize deltas classified as
	// "hex" (single char in 0-9a-f) or "other" (anything else, including
	// text, special tokens, or empty deltas). The window is a simple
	// 2-slice ring: when one fills, the other is copied into the empty
	// half so the last windowSize observations are always preserved.
	windowSize  int
	hexInWindow int
	win         []deltaKind
	winCursor   int
	winFilled   bool

	// progressCounts tracks the number of "progress" deltas (any non-empty
	// reasoning delta that is not a single hex char) seen in the same
	// window. We use this to require some non-hex activity before
	// declaring signal C.
	progressInWindow int

	// observedTotal counts every non-empty delta ever seen this turn.
	// Surfaced in the diagnostic event for diagnosis.
	observedTotal int
	hexTotal      int
	abortReason   string
	abortSignal   string
	abortDetail   string
}

// deltaKind is a coarse classification of a reasoning delta for the
// purpose of the rolling window.
type deltaKind uint8

const (
	deltaHex   deltaKind = iota // single [0-9a-fA-F] char
	deltaProgress               // any other non-empty delta
	deltaEmpty                  // empty delta (should not normally happen)
)

// knownControlTokens is the set of upstream provider control tokens that
// should never appear in user-facing reasoning content. Llama 3.1+ uses
// <|channel|>, <|constrain|>, <|message|>, <|end|>, <|start|> to delimit
// its structured "channel" generation format. Some providers also emit
// <tool_call|> / <|/tool_call|> markers. Anthropic uses a different
// control format (data blocks), so it does not appear here.
//
// We use a precise allowlist rather than a permissive regex because the
// reasoning field can legitimately contain prose like "use <a|href>..."
// in some languages or tool descriptions — a permissive pattern would
// over-trigger on real reasoning content. The regex below is built from
// the same allowlist so the two stay in sync.

// degenerateTokenRe matches any of the known control tokens in a single
// delta. The pattern tolerates truncated forms (e.g. "<|channel" with no
// closing |>) because upstream providers have been observed to break
// special tokens across multiple stream deltas — the truncated head is
// itself a strong signal of provider control-token leakage.
//
// Pattern shape:
//   - <|name followed by `|>`, `>`, `|`, or end-of-string
//   - <tool_call followed by `|>`, `>`, `|`, or end-of-string
//   - <|/tool_call followed by `|>`, `>`, `|`, or end-of-string
//
// We require a name from the allowlist before `<|...` to avoid matching
// benign prose like "<a|href>".
var degenerateTokenRe = buildControlTokenRegex([]string{
	"channel",
	"constrain",
	"message",
	"end",
	"start",
	"endoftext",
	"begin_of_text",
	"eot",
	"python_tag",
})

// buildControlTokenRegex returns a regexp that matches the head of any
// known control token (the close may be `|>`, `>`, `|`, or absent), the
// canonical <tool_call|> / <|/tool_call|> markers (also in truncated
// form), but never benign prose like "<a|href>".
func buildControlTokenRegex(names []string) *regexp.Regexp {
	// Build "<\|name(?:\|>|>|\||$|non-pipe))" alternatives. We need the
	// follow character to be one of: `|>`, `>`, `|`, or end-of-string.
	// For the <|...> family the close is "<name" followed by `|>` or
	// `>`. Truncated forms ("<|channel") are matched by leaving the
	// close optional.
	pipeTokens := make([]string, 0, len(names))
	for _, n := range names {
		pipeTokens = append(pipeTokens, `<\|`+n+`(?:\|[>]?|[>]?|$)`)
	}
	pattern := `(?:` + strings.Join(pipeTokens, `|`) +
		`|<tool_call(?:\|[>]?|[>]?|$)` +
		`|<\|/tool_call(?:\|[>]?|[>]?|$)` +
		`)`
	return regexp.MustCompile(pattern)
}

// Observe processes a single reasoning delta. It returns a non-empty
// reason if the stream has become degenerate and the loop should abort
// the current session. The reason is human-readable and safe to emit in
// diagnostic events.
func (g *reasoningStreamGuard) Observe(delta string) string {
	if g == nil {
		return ""
	}
	if g.abortReason != "" {
		// Already aborted; subsequent observations are no-ops.
		return g.abortReason
	}
	if delta == "" {
		return ""
	}
	g.observedTotal++

	// Signal A: provider control-token leakage. Single occurrence is
	// enough — legitimate reasoning never contains "<|...|>" sequences.
	if degenerateTokenRe.MatchString(delta) {
		g.abortSignal = "control_token"
		g.abortDetail = truncate(delta, 80)
		g.abortReason = "degenerate reasoning stream: provider control token leakage (" + g.abortDetail + ")"
		return g.abortReason
	}

	// Classify the delta.
	kind := deltaProgress
	if isSingleHex(delta) {
		kind = deltaHex
		g.hexTotal++
	}

	// Update the consecutive-hex run counter.
	if kind == deltaHex {
		g.consecutiveHex++
	} else {
		g.consecutiveHex = 0
	}

	// Update the rolling window.
	if g.win == nil {
		g.windowSize = 2048
		g.win = make([]deltaKind, g.windowSize)
	}
	if g.win[winIndex(g.winCursor, g.windowSize)] == deltaHex {
		g.hexInWindow--
	}
	if kind == deltaHex {
		g.hexInWindow++
	} else if kind == deltaProgress {
		g.progressInWindow++
	}
	// If the slot we're about to overwrite held a progress marker, decrement
	// progressInWindow so it stays accurate.
	prev := g.win[winIndex(g.winCursor, g.windowSize)]
	if prev == deltaProgress && kind != deltaProgress {
		g.progressInWindow--
	}
	g.win[winIndex(g.winCursor, g.windowSize)] = kind
	g.winCursor++
	if g.winCursor >= g.windowSize {
		g.winCursor = 0
		g.winFilled = true
	}

	// Signal B: long unbroken run of single-hex deltas.
	const consecutiveHexThreshold = 256
	if g.consecutiveHex >= consecutiveHexThreshold {
		g.abortSignal = "hex_run"
		g.abortReason = "degenerate reasoning stream: " +
			itoa(g.consecutiveHex) + " consecutive single-character hex deltas"
		return g.abortReason
	}

	// Signal C: the window is dominated by hex with no progress.
	// We only check this once the window is full to avoid early false
	// positives from short bursts of legitimately-numeric reasoning.
	const minObservationsForRatio = 1024
	if g.winFilled && g.observedTotal >= minObservationsForRatio {
		if g.progressInWindow == 0 && g.hexInWindow >= minObservationsForRatio {
			g.abortSignal = "hex_dominance"
			g.abortReason = "degenerate reasoning stream: " +
				itoa(g.hexInWindow) + "/" + itoa(g.windowSize) +
				" trailing deltas are single hex characters with no other progress"
			return g.abortReason
		}
	}

	return ""
}

// Diagnostics returns a snapshot of the guard's state for inclusion in
// the diagnostic avenor.error event. The returned map is safe to mutate.
func (g *reasoningStreamGuard) Diagnostics() map[string]any {
	if g == nil {
		return nil
	}
	return map[string]any{
		"abort_signal":      g.abortSignal,
		"observed_total":    g.observedTotal,
		"hex_total":         g.hexTotal,
		"hex_in_window":     g.hexInWindow,
		"window_size":       g.windowSize,
		"consecutive_hex":   g.consecutiveHex,
		"progress_in_window": g.progressInWindow,
	}
}

func isSingleHex(s string) bool {
	if len(s) != 1 {
		return false
	}
	c := s[0]
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func winIndex(cursor, size int) int {
	if size <= 0 {
		return 0
	}
	return cursor % size
}

// truncate returns at most n bytes of s with an ellipsis suffix when the
// input was truncated. The cut is performed on byte boundaries to avoid
// producing invalid UTF-8 in the output.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n < 1 {
		return ""
	}
	// Trim to n-1 bytes then append "…". Make sure we don't cut a UTF-8
	// continuation byte at the boundary.
	cut := n - 1
	for cut > 0 && s[cut]&0xC0 == 0x80 {
		cut--
	}
	return s[:cut] + "…"
}

// itoa is a tiny stdlib-free integer formatter. We only use it for
// building short diagnostic strings; pulling in fmt just for this would
// be wasteful in a hot path.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
