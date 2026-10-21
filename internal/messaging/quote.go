package messaging

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf16"
)

// Quote renders value as one JSON string literal. Control, format, line- and
// paragraph-separator characters are escaped, so a label a producer controls
// cannot carry a bidi override or a zero-width character into tool text.
func Quote(value string) string {
	return `"` + escapeNonPrintable(value) + `"`
}

// Sanitize escapes the same characters as Quote without adding the surrounding
// literal quotes, for a fact a template renders bare.
func Sanitize(value string) string { return escapeNonPrintable(value) }

// QuoteList renders every value with Quote and joins them with ", ".
func QuoteList(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, Quote(value))
	}
	return strings.Join(quoted, ", ")
}

func escapeNonPrintable(value string) string {
	var out strings.Builder
	out.Grow(len(value))
	for _, r := range value {
		switch {
		case r == '"':
			out.WriteString(`\"`)
		case r == '\\':
			out.WriteString(`\\`)
		case r == '\b':
			out.WriteString(`\b`)
		case r == '\f':
			out.WriteString(`\f`)
		case r == '\n':
			out.WriteString(`\n`)
		case r == '\r':
			out.WriteString(`\r`)
		case r == '\t':
			out.WriteString(`\t`)
		case r < 0x20 || r == 0x7f || (r >= 0x80 && r <= 0x9f) || unicode.In(r, unicode.Cf, unicode.Zl, unicode.Zp):
			if r > 0xffff {
				high, low := utf16.EncodeRune(r)
				fmt.Fprintf(&out, `\u%04x\u%04x`, high, low)
			} else {
				fmt.Fprintf(&out, `\u%04x`, r)
			}
		default:
			out.WriteRune(r)
		}
	}
	return out.String()
}
