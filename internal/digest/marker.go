package digest

import (
	"regexp"
	"strings"
)

// statusMarkerRe matches [status: phase] or [status: phase | label] anywhere
// in text, case-insensitively. Group 1 is the phase word; group 2 is the
// optional label (may be empty string when the pipe is absent).
var statusMarkerRe = regexp.MustCompile(`(?i)\[status:\s*(\w+)(?:\s*\|\s*([^\]]*))?\]`)

// statusAngleRe matches <|status: phase|> or <|status: phase | label|> on its
// own line, case-insensitively. Group 1 is the phase word; group 2 is the
// optional label.
var statusAngleRe = regexp.MustCompile(`(?i)^\s*<\|status:\s*(\w+)\s*(?:\|\s*(.*))?\|>\s*$`)

// ExtractStatusMarker scans text for the first [status: <phase>] or
// [status: <phase> | <label>] marker (inline, legacy). Returns the normalised
// phase, trimmed label, and ok=true when a known phase is found. Unknown phase
// words are ignored (ok=false) to avoid spurious events from unrelated bracket
// text.
//
// Known phases: thinking, working, waiting, done.
func ExtractStatusMarker(text string) (phase, label string, ok bool) {
	m := statusMarkerRe.FindStringSubmatch(text)
	if m == nil {
		return "", "", false
	}
	phase = strings.ToLower(m[1])
	switch phase {
	case "thinking", "working", "waiting", "done":
	default:
		return "", "", false
	}
	return phase, strings.TrimSpace(m[2]), true
}

// ExtractStatusAngle scans text for a full-line <|status: phase|> or
// <|status: phase | label|> angle-token. Returns the normalised phase, trimmed
// label, and ok=true when a known phase is found. Unlike the legacy bracket
// syntax, angle tokens must occupy their own line.
func ExtractStatusAngle(text string) (phase, label string, ok bool) {
	for _, line := range strings.Split(text, "\n") {
		m := statusAngleRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		phase = strings.ToLower(m[1])
		switch phase {
		case "thinking", "working", "waiting", "done":
		default:
			continue
		}
		return phase, strings.TrimSpace(m[2]), true
	}
	return "", "", false
}
