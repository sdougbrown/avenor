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

// ValidSessionName applies the one session display-name rule: 1-64 runes with
// no control, format, line- or paragraph-separator characters. The rename
// authority and the broker-only /rename fallback share it so neither can store
// a name the other rejects.
func ValidSessionName(name string) bool {
	runes := []rune(name)
	if len(runes) == 0 || len(runes) > MaxSessionNameRunes {
		return false
	}
	return !disallowedIdentifierRunes(name)
}

// SessionNameRuleMessage renders the one refusal text for a name the rule
// rejects.
func SessionNameRuleMessage(name string) string {
	return fmt.Sprintf("name must be 1-64 characters with no control or formatting characters: %q", name)
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
