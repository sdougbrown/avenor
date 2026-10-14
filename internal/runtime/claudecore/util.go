package claudecore

import (
	"encoding/json"
	"strings"
)

func MustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return json.RawMessage(b)
}

// ValidTmuxKey returns true if key is a safe single-character string for tmux send-keys.
func ValidTmuxKey(key string) bool {
	if len(key) == 0 || len(key) > 3 {
		return false
	}
	for _, r := range key {
		if r >= '0' && r <= '9' {
			continue
		}
		if r >= 'a' && r <= 'z' {
			continue
		}
		if r >= 'A' && r <= 'Z' {
			continue
		}
		return false
	}
	return true
}

// ShellQuote produces one POSIX-shell word. Single quotes are represented by
// adjacent quoted fragments, so whitespace and shell metacharacters — including
// $(), backticks, and ; — stay literal data instead of being parsed as syntax.
// A double-quoted string would not do: the shell still expands $() and
// backticks inside double quotes.
func ShellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'"'"'`) + "'"
}
