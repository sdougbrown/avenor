package claudecore

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

type TerminalPermission struct {
	RequestID string
	Prompt    string
	Options   []PermissionOption
	CreatedAt time.Time
}

type PermissionOption struct {
	ID    string
	Label string
	Kind  string
}

var tmuxOptionRE = regexp.MustCompile(`^\s*(?:❯\s*)?(\d+)\.\s+(.*)`)

// permissionOptionKind classifies a dialog option by its label. Claude Code
// starts approving labels with "Yes" and declining labels with "No". Any other
// label gets no kind, so the resolvers refuse to select it.
func permissionOptionKind(label string) string {
	switch {
	case labelStartsWith(label, "Yes"):
		return "allow"
	case labelStartsWith(label, "No"):
		return "reject"
	default:
		return ""
	}
}

func labelStartsWith(label, word string) bool {
	return label == word || strings.HasPrefix(label, word+",") || strings.HasPrefix(label, word+" ")
}

// ParseTerminalPermission extracts a TerminalPermission from pane text, or nil
// if the text does not represent a Claude permission dialog. The pane can show
// numbered lists above the dialog, so the options are the last list numbered
// from 1. The prompt is the text between that list and the numbered line
// before it.
func ParseTerminalPermission(text string) *TerminalPermission {
	text = normalizePane(text)
	lines := strings.Split(text, "\n")
	promptLines := make([]string, 0)
	options := make([]PermissionOption, 0)
	// pending holds the prompt candidates seen since the last numbered line.
	pending := make([]string, 0)

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if matches := tmuxOptionRE.FindStringSubmatch(trimmed); len(matches) == 3 {
			if matches[1] == "1" {
				options = options[:0]
				promptLines, pending = pending, make([]string, 0)
			}
			if matches[1] == strconv.Itoa(len(options)+1) {
				label := strings.TrimSpace(matches[2])
				options = append(options, PermissionOption{ID: matches[1], Label: label, Kind: permissionOptionKind(label)})
			}
			pending = pending[:0]
			continue
		}
		if !strings.HasPrefix(trimmed, "❯") && !strings.HasPrefix(trimmed, "●") && !strings.HasPrefix(trimmed, "✻") && !strings.HasPrefix(trimmed, "✢") {
			pending = append(pending, trimmed)
		}
	}

	if len(options) == 0 {
		return nil
	}

	prompt := strings.Join(promptLines, " ")
	if prompt == "" {
		// Try to use the permission markers as prompt fallback.
		if strings.Contains(text, "Do you want to proceed?") {
			prompt = "Claude is requesting permission to proceed"
		} else if strings.Contains(text, "Do you want to make this edit") {
			prompt = "Claude is requesting permission to edit"
		} else {
			prompt = "Claude permission request"
		}
	}

	return &TerminalPermission{
		Prompt:  prompt,
		Options: options,
	}
}
