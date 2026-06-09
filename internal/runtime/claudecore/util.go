package claudecore

import (
	"encoding/json"
	"strconv"
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

// ShellQuote single-quotes a string for safe interpolation into a shell command.
func ShellQuote(s string) string {
	return strconv.Quote(s)
}
