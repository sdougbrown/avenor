package terminal

import (
	"strings"
	"unicode/utf8"
)

// SanitizePrompt returns text without the characters a terminal reads as
// control input rather than typed text: ESC and every other C0 control except
// tab, newline and carriage return, DEL, and the C1 controls U+0080–U+009F.
// Without ESC nothing in a prompt can form an escape sequence, such as a
// bracketed-paste end marker, a key sequence or a CSI introducer. Bytes that
// are not valid UTF-8 are dropped too; a 0x9B continuation byte inside a
// valid multi-byte character is part of that character and is kept.
func SanitizePrompt(text string) string {
	var b strings.Builder
	b.Grow(len(text))
	for i := 0; i < len(text); {
		r, size := utf8.DecodeRuneInString(text[i:])
		i += size
		switch {
		case r == utf8.RuneError && size == 1:
		case r == '\t' || r == '\n' || r == '\r':
			b.WriteRune(r)
		case r < 0x20, r == 0x7f, r >= 0x80 && r <= 0x9f:
		default:
			b.WriteString(text[i-size : i])
		}
	}
	return b.String()
}
