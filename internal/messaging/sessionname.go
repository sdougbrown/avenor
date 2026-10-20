package messaging

import (
	"fmt"
	"unicode"
)

// ValidSessionName applies the one session display-name rule: 1-64 runes with
// no control, format, line- or paragraph-separator characters. The rename
// authority and the broker-only /rename fallback share it so neither can store
// a name the other rejects.
func ValidSessionName(name string) bool {
	runes := []rune(name)
	if len(runes) == 0 || len(runes) > 64 {
		return false
	}
	for _, r := range runes {
		if unicode.In(r, unicode.Cc, unicode.Cf, unicode.Zl, unicode.Zp) {
			return false
		}
	}
	return true
}

// SessionNameRuleMessage renders the one refusal text for a name the rule
// rejects.
func SessionNameRuleMessage(name string) string {
	return fmt.Sprintf("name must be 1-64 characters with no control or formatting characters: %q", name)
}
