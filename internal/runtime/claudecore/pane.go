package claudecore

import (
	"context"
	"strings"

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

func ClassifyPane(text string) PaneState {
	if strings.Contains(text, "Do you want to proceed?") || strings.Contains(text, "Do you want to make this edit") || strings.Contains(text, "Yes, allow all edits") {
		return PaneStatePermission
	}
	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "❯") {
			return PaneStateIdle
		}
		if looksLikeClaudeActivityLine(trimmed) {
			return PaneStateActive
		}
	}
	return PaneStateUnknown
}

func looksLikeClaudeActivityLine(line string) bool {
	if !(strings.HasPrefix(line, "✻") || strings.HasPrefix(line, "✢") || strings.HasPrefix(line, "●") || strings.HasPrefix(line, "○") || strings.HasPrefix(line, "◯")) {
		return false
	}
	fields := strings.Fields(line)
	for _, field := range fields {
		word := strings.Trim(field, "…,.·:;()[]{}")
		if len(word) > 4 && strings.HasSuffix(word, "ing") {
			return true
		}
	}
	return strings.Contains(line, "tool use") || strings.Contains(line, "tokens")
}
