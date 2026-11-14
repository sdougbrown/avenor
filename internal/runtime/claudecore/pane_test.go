package claudecore

import (
	"strings"
	"testing"
)

// paneCaptureCreateFile is a tmux capture of Claude Code's new-file permission
// dialog, trimmed to the region from the Write tool call to the option list.
const paneCaptureCreateFile = `● Write(hello.txt)

──────────────────────────────────────────────────────────────────────────────────────────────────────────────
 Create file
 hello.txt
╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌
  1 hi
╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌
 Do you want to create hello.txt?
 ❯ 1. Yes
   2. Yes, and switch to accept edits (auto-approve file edits and common file commands) for this session
      (shift+tab)
   3. No

 Esc to cancel · Tab to amend`

// paneCaptureEditFile is the matching capture for an edit to an existing file.
// Its diff preview above the dialog is numbered without periods, so the option
// list still starts at the "❯ 1. Yes" line.
const paneCaptureEditFile = `● Update(hello.txt)

──────────────────────────────────────────────────────────────────────────────────────────────────────────────
 Edit file
 hello.txt
╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌
 1 -hi
 1   No newline at end of file
 2 +hello
 3   No newline at end of file
╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌
 Do you want to make this edit to hello.txt?
 ❯ 1. Yes
   2. Yes, and switch to accept edits (auto-approve file edits and common file commands) for this session
      (shift+tab)
   3. No

 Esc to cancel · Tab to amend`

// paneCaptureCreateFileWrapped is the create dialog with its question
// soft-wrapped at pane width: neither row carries both the question's opening
// words and its question mark.
var paneCaptureCreateFileWrapped = strings.Replace(
	paneCaptureCreateFile,
	" Do you want to create hello.txt?",
	" Do you want to create\n hello.txt?",
	1,
)

func TestClassifyPanePermissionDialogs(t *testing.T) {
	tests := []struct {
		name string
		pane string
	}{
		{name: "create file", pane: paneCaptureCreateFile},
		{name: "create file, wrapped question", pane: paneCaptureCreateFileWrapped},
		{name: "edit file", pane: paneCaptureEditFile},
		{name: "bash command", pane: permissionPane},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClassifyPane(tt.pane); got != PaneStatePermission {
				t.Fatalf("ClassifyPane = %q, want %q", got, PaneStatePermission)
			}
		})
	}
}

// A reply can quote the question wording and still be an ordinary answer: with
// no numbered option list offering to allow, the pane is not a dialog.
func TestClassifyPaneIgnoresPermissionWordingWithoutOptions(t *testing.T) {
	const reply = "● Here is what I would do:\n\n  Do you want to run the migration?\n\n" +
		"  1. Back up the database\n  2. Apply the schema\n  3. Restore on failure\n"
	if got := ClassifyPane(reply); got == PaneStatePermission {
		t.Fatalf("ClassifyPane = %q for a reply quoting the question", got)
	}
}

// A reply can also ask a question directly above a suggestion list that starts
// with a Yes. Only the dialog's hint footer separates the two, and answering a
// reply as if it were a dialog would type into a pane that has no dialog.
func TestClassifyPaneIgnoresProseQuestionOverSuggestionList(t *testing.T) {
	const reply = "● I can migrate or wait.\n\n  Do you want to run the migration?\n\n" +
		"❯ 1. Yes, and back up first\n  2. Yes, and skip the backup\n  3. No, wait\n"
	if got := ClassifyPane(reply); got == PaneStatePermission {
		t.Fatalf("ClassifyPane = %q for prose above a suggestion list", got)
	}
}

// The question paints before the option list, and the dialog wordings that
// identify themselves classify on their own in that frame.
func TestClassifyPanePermissionWordingBeforeOptions(t *testing.T) {
	const painted = "Bash command\n  rm -rf build\n Do you want to proceed?\n"
	if got := ClassifyPane(painted); got != PaneStatePermission {
		t.Fatalf("ClassifyPane = %q, want %q", got, PaneStatePermission)
	}
}
