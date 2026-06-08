package claudecore

import (
	"context"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"
)

type PaneState string

const (
	PaneStateUnknown    PaneState = "unknown"
	PaneStateActive     PaneState = "active"
	PaneStatePermission PaneState = "permission"
	PaneStateIdle       PaneState = "idle"
)

func ScanTerminal(ctx context.Context, term terminal.Session) (PaneState, error) {
	out, err := term.Capture(ctx)
	if err != nil {
		return PaneStateUnknown, err
	}
	return ClassifyPane(string(out)), nil
}

// ClassifyPane returns the most-specific pane state. After a turn is submitted
// the pane shows both an echo line that starts with `❯` (the prior user prompt)
// AND a spinner line below it; the input box `❯` reappears at the bottom. An
// activity line therefore must beat any `❯` line, otherwise we'd report idle
// while a turn is still running.
func ClassifyPane(text string) PaneState {
	if strings.Contains(text, "Do you want to proceed?") || strings.Contains(text, "Do you want to make this edit") || strings.Contains(text, "Yes, allow all edits") {
		return PaneStatePermission
	}
	sawInputPrompt := false
	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if looksLikeClaudeActivityLine(trimmed) {
			return PaneStateActive
		}
		if strings.HasPrefix(trimmed, "❯") {
			sawInputPrompt = true
		}
	}
	if sawInputPrompt {
		return PaneStateIdle
	}
	return PaneStateUnknown
}

// looksLikeClaudeActivityLine matches Claude's spinner line. The TUI cycles
// through many glyphs (✻ ✢ ● ○ ◯ ✽ ✳ · …), some of which are punctuation rather
// than symbols, so we look for "<single-rune-glyph> <Verb>ing… …" structure
// rather than a fixed glyph set.
func looksLikeClaudeActivityLine(line string) bool {
	r, sz := utf8.DecodeRuneInString(line)
	if sz == 0 || r == utf8.RuneError {
		return false
	}
	if unicode.IsLetter(r) || unicode.IsDigit(r) {
		return false
	}
	if r == '❯' {
		return false
	}
	// Box Drawing (U+2500–U+257F) and Block Elements (U+2580–U+259F) frame
	// the TUI's chrome but aren't spinner glyphs.
	if r >= 0x2500 && r <= 0x259F {
		return false
	}
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return false
	}
	if utf8.RuneCountInString(fields[0]) != 1 {
		return false
	}
	for _, field := range fields {
		word := strings.Trim(field, "…,.·:;()[]{}")
		if len(word) > 4 && strings.HasSuffix(word, "ing") {
			return true
		}
	}
	return strings.Contains(line, "tool use") || strings.Contains(line, "tokens")
}
