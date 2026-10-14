// Package shellquote provides POSIX shell word quoting shared by launchers
// that must join argv elements into a single shell command string.
package shellquote

import "strings"

// Quote produces one POSIX-shell word. Single quotes are represented by
// adjacent quoted fragments, so whitespace and shell metacharacters —
// including $(), backticks, and ; — stay literal data instead of being parsed
// as syntax. A double-quoted string would not do: the shell still expands $()
// and backticks inside double quotes.
func Quote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'"'"'`) + "'"
}
