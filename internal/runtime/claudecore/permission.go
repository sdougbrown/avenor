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

// permissionDialogQuestion reports whether line is part of Claude's permission
// question, whatever tool it names: "Do you want to proceed?", "Do you want to
// make this edit to x?", "Do you want to create <file>?". The question
// soft-wraps at pane width, so its tail line ends with "?" while only its head
// line starts with "Do you want to ".
func permissionDialogQuestion(line string) bool {
	line = strings.TrimSpace(line)
	return strings.HasPrefix(line, "Do you want to ") || strings.HasSuffix(line, "?")
}

// permissionDialogFooter is the hint row Claude paints below a permission
// dialog's options. Every captured dialog carries "Esc to cancel"; the wording
// around it varies ("Tab to amend", "Enter to confirm").
const permissionDialogFooter = "Esc to cancel"

// isPermissionDialog reports whether text shows a permission dialog: a
// question line above an option list numbered from 1 whose first option offers
// to allow, and the dialog's hint footer below. Claude words the question per
// tool, so the dialog is recognised by this structure rather than by its
// wording; the footer separates it from a reply that merely asks a question
// above a numbered list of suggestions.
func isPermissionDialog(text string) bool {
	text = normalizePane(text)
	if !strings.Contains(text, permissionDialogFooter) {
		return false
	}
	promptLines, options := scanDialogOptions(text)
	if len(options) == 0 || options[0].Kind != "allow" {
		return false
	}
	for _, line := range promptLines {
		if permissionDialogQuestion(line) {
			return true
		}
	}
	return false
}

// scanDialogOptions splits pane text into the pane lines above the dialog's
// option list and the options themselves. The pane can show numbered lists
// above the dialog, so the options are the last list numbered from 1; numbered
// lines in a command or diff preview have no period after the number.
func scanDialogOptions(text string) (promptLines []string, options []PermissionOption) {
	promptLines = make([]string, 0)
	options = make([]PermissionOption, 0)
	// above holds every prompt candidate line seen so far.
	above := make([]string, 0)

	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if matches := tmuxOptionRE.FindStringSubmatch(trimmed); len(matches) == 3 {
			if matches[1] == "1" {
				options = options[:0]
				promptLines = append(promptLines[:0], above...)
			}
			if matches[1] == strconv.Itoa(len(options)+1) {
				label := strings.TrimSpace(matches[2])
				options = append(options, PermissionOption{ID: matches[1], Label: label, Kind: permissionOptionKind(label)})
			}
		}
		if !strings.HasPrefix(trimmed, "❯") && !strings.HasPrefix(trimmed, "●") && !strings.HasPrefix(trimmed, "✻") && !strings.HasPrefix(trimmed, "✢") {
			above = append(above, trimmed)
		}
	}
	return promptLines, options
}

// ParseTerminalPermission extracts a TerminalPermission from pane text, or nil
// if the text does not represent a Claude permission dialog (see
// scanDialogOptions for how the options are found). The prompt is all pane text
// above the option list, numbered lines included, so the dialog's header and
// the file path it names ("Create file" / "hello.txt") end up in the
// description; spinner and echoed-prompt lines are left out. A numbered line in
// a command preview therefore cannot hide the lines above it.
func ParseTerminalPermission(text string) *TerminalPermission {
	text = normalizePane(text)
	promptLines, options := scanDialogOptions(text)

	if len(options) == 0 {
		return nil
	}

	prompt := strings.Join(promptLines, " ")
	if prompt == "" {
		// Try to use the permission markers as prompt fallback.
		switch {
		case strings.Contains(text, "Do you want to proceed?"):
			prompt = "Claude is requesting permission to proceed"
		case strings.Contains(text, "Do you want to make this edit"):
			prompt = "Claude is requesting permission to edit"
		case strings.Contains(text, "Do you want to create"):
			prompt = "Claude is requesting permission to create a file"
		default:
			prompt = "Claude permission request"
		}
	}

	return &TerminalPermission{
		Prompt:  prompt,
		Options: options,
	}
}
