package claudecore

import (
	"regexp"
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

// permissionOptionKind classifies a dialog option by its label: Claude Code
// labels the declining choices "No" or "No, ...".
func permissionOptionKind(label string) string {
	if label == "No" || strings.HasPrefix(label, "No,") || strings.HasPrefix(label, "No ") {
		return "reject"
	}
	return "allow"
}

// ParseTerminalPermission extracts a TerminalPermission from pane text, or nil
// if the text doesn't represent a Claude permission dialog.
func ParseTerminalPermission(text string) *TerminalPermission {
	text = normalizePane(text)
	lines := strings.Split(text, "\n")
	promptLines := make([]string, 0)
	options := make([]PermissionOption, 0)

	inOptions := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if tmuxOptionRE.MatchString(trimmed) {
			matches := tmuxOptionRE.FindStringSubmatch(trimmed)
			if len(matches) == 3 {
				label := strings.TrimSpace(matches[2])
				options = append(options, PermissionOption{ID: matches[1], Label: label, Kind: permissionOptionKind(label)})
				inOptions = true
				continue
			}
		}
		if inOptions {
			// After options, skip footer lines (Esc to cancel, etc.)
			if strings.Contains(trimmed, "Esc to cancel") || strings.HasPrefix(trimmed, "Esc ") {
				continue
			}
			break
		}
		if !strings.HasPrefix(trimmed, "❯") && !strings.HasPrefix(trimmed, "●") && !strings.HasPrefix(trimmed, "✻") && !strings.HasPrefix(trimmed, "✢") {
			promptLines = append(promptLines, trimmed)
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
