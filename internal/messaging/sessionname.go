package messaging

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// disallowedIdentifierRune reports whether r is a rune no identifier may
// hold: control, format, line- or paragraph-separator characters, so a stored
// value cannot render as something it is not.
func disallowedIdentifierRune(r rune) bool {
	return unicode.In(r, unicode.Cc, unicode.Cf, unicode.Zl, unicode.Zp)
}

// disallowedIdentifierRunes reports whether the string carries a rune no
// identifier may hold: control, format, line- or paragraph-separator
// characters, so a stored value cannot render as something it is not.
func disallowedIdentifierRunes(s string) bool {
	for _, r := range s {
		if disallowedIdentifierRune(r) {
			return true
		}
	}
	return false
}

// StripDisallowedIdentifierRunes returns s without the runes
// disallowedIdentifierRunes rejects, so a derived label can be sanitized
// toward ValidSessionName under the one rune rule.
func StripDisallowedIdentifierRunes(s string) string {
	return strings.Map(func(r rune) rune {
		if disallowedIdentifierRune(r) {
			return -1
		}
		return r
	}, s)
}

// MaxSessionNameRunes bounds a session display name's rune length.
const MaxSessionNameRunes = 64

// ReservedSelfAlias is the reference a caller's own session is addressed by.
// Resolution substitutes it before any label is consulted, so no session can
// be reached by a label of the same spelling.
const ReservedSelfAlias = "self"

// ValidSessionName applies the one session display-name rule: 1-64 runes with
// no control, format, line- or paragraph-separator characters, and not a
// reserved reference — the empty reference, the self alias, or the supervisor
// run id — because resolution answers those before it ever reads a label. The
// rename authority and the broker's /rename and /session_name writers share
// it, so no path can store a name the others reject.
func ValidSessionName(name string) bool {
	runes := []rune(name)
	if len(runes) == 0 || len(runes) > MaxSessionNameRunes {
		return false
	}
	if name == ReservedSelfAlias || name == SupervisorRunID {
		return false
	}
	return !disallowedIdentifierRunes(name)
}

// SessionNameRuleMessage renders the one refusal text for a name the rule
// rejects, naming every part of the rule so a caller can tell which one the
// name broke.
func SessionNameRuleMessage(name string) string {
	return fmt.Sprintf("name must be 1-64 characters with no control or formatting characters, and must not be a reserved reference (%s or %s): %q", ReservedSelfAlias, SupervisorRunID, name)
}

// MaxSessionIDBytes bounds a native session id's byte size. The id names a
// bound conversation's record directory, so the bound keeps it a safe
// filesystem path component with headroom under the 255-byte name limit.
const MaxSessionIDBytes = 200

// ValidSessionID applies the one native session id rule: 1-200 bytes of valid
// UTF-8 with no control, format, line- or paragraph-separator runes, forming a
// single filesystem path component: no path separators and no dot segments.
func ValidSessionID(id string) bool {
	if id == "" || len(id) > MaxSessionIDBytes || !utf8.ValidString(id) {
		return false
	}
	if id == "." || id == ".." || strings.ContainsAny(id, "/\\") {
		return false
	}
	return !disallowedIdentifierRunes(id)
}

// runLabelTailRunes is how many trailing characters of a run id tell its label
// from another run's.
const runLabelTailRunes = 6

// RunLabelTail is the last six letters and digits of a run id.
func RunLabelTail(runID string) string {
	var kept []rune
	for _, r := range runID {
		if r < 0x80 && (r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z') {
			kept = append(kept, r)
		}
	}
	if len(kept) > runLabelTailRunes {
		kept = kept[len(kept)-runLabelTailRunes:]
	}
	return string(kept)
}

// LabelWithRunTail follows label with a dash and the run's tail, cutting the
// label to leave room within the name limit. It returns the label unchanged
// when the run id carries no tail, and "" when nothing of the label is left.
func LabelWithRunTail(label, runID string) string {
	tail := RunLabelTail(runID)
	if tail == "" {
		return label
	}
	stem := []rune(label)
	if limit := MaxSessionNameRunes - 1 - len([]rune(tail)); len(stem) > limit {
		stem = stem[:limit]
	}
	trimmed := strings.TrimRight(string(stem), "-")
	if trimmed == "" {
		return ""
	}
	return trimmed + "-" + tail
}
