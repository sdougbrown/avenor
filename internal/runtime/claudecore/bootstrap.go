package claudecore

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"
)

const (
	PromptInjectCheckInterval = 500 * time.Millisecond
	PromptInjectTimeout       = 30 * time.Second
)

// Trust-safety dialog Claude shows on first launch in an unfamiliar directory.
// We require both the header and the trust option to be present, so a stray
// match on either string in unrelated TUI content doesn't trigger the
// auto-dismiss. Claude Code 2.1.303 highlights "No, exit" by default, so one
// Down moves the selection to "Yes, I trust this folder" before Enter
// confirms it; older releases highlight the trust option and take Enter
// alone. avenor is invoked against a directory the user chose.
const (
	trustDialogHeader = "Quick safety check"
	trustDialogOption = "Yes, I trust this folder"
	trustDialogExit   = "No, exit"
)

// paneReadyMarkers are strings that appear once the input box accepts input.
// "Tips for getting started" is the right-column header of the welcome screen;
// the input placeholder `❯ Try` appears below the screen separator. The old
// "anything except the loading banner" check returned true on a still-empty
// pane and the paste landed before claude was reading stdin — or worse, while
// the trust dialog was rendering, where the paste then dismissed the dialog
// rather than submitting the prompt.
var paneReadyMarkers = []string{
	"Tips for getting started",
	"❯ Try",
}

// inputBoxRule is the horizontal rule Claude draws above and below the input
// box. Because Claude Code 2.1.x removed both paneReadyMarkers (the welcome
// column and the `❯ Try` placeholder), use this rule to identify the framed
// input box instead.
const inputBoxRule = "────────────────────"

// paneHasInputBox reports whether the framed, empty input box is on screen.
//
// Both conditions are load-bearing. A bare `strings.Contains(out, "❯")` also
// matches a selection dialog. Options render as `❯ 1. Yes` (see
// ParseTerminalPermission), and the echoed prompt renders as `❯ <text>`.
// Pasting into either dismisses the dialog instead of submitting the prompt.
// That is the failure the paneReadyMarkers comment warns about. So the chevron
// must be alone on its line, meaning an empty input box. The rule must also be
// present: Claude draws it only once the box is framed.
func paneHasInputBox(out string) bool {
	if !strings.Contains(out, inputBoxRule) {
		return false
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) == "❯" {
			return true
		}
	}
	return false
}

// Errors WaitForPaneReady returns when the pane never became ready. Only
// ErrPaneNotReady means claude is running and showing something else; the
// other failures say nothing about what claude is doing.
var (
	// ErrPaneNotReady means the deadline passed with claude still running and
	// no input box on screen.
	ErrPaneNotReady = errors.New("claude's input box did not appear before the deadline")
	// ErrTerminalGone means the terminal ended while avenor waited. The
	// session's exit watcher classifies the exit.
	ErrTerminalGone = errors.New("the terminal ended before claude's input box appeared")
)

// WaitForPaneReady waits for Claude's input box on the session's terminal,
// bounded by SessionOptions.PaneReadyTimeout.
func (s *Session) WaitForPaneReady(ctx context.Context) error {
	return WaitForPaneReady(ctx, s.Term, s.paneReadyTimeout)
}

// WaitForPaneReady blocks until Claude's input box is taking input, and
// returns nil then. It auto-dismisses the trust-safety dialog if present.
// timeout bounds the wait; zero or less means PromptInjectTimeout. Any error
// means "do not paste, the pane is not in a known-ready state": ctx's error on
// cancel, ErrTerminalGone, a capture or send error, or ErrPaneNotReady at the
// deadline.
func WaitForPaneReady(ctx context.Context, term terminal.Session, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = PromptInjectTimeout
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(PromptInjectCheckInterval)
	defer ticker.Stop()

	// downBase is the capture a Down was sent on, for the dialog currently on
	// screen; empty when no Down has been sent for it.
	var downBase string
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			if !term.Alive(ctx) {
				return ErrTerminalGone
			}
			return ErrPaneNotReady
		case <-ticker.C:
		}

		// A PTY keeps its last screen after claude exits, and a tmux capture
		// of a dead session fails, so neither capture says whether claude is
		// still there.
		if !term.Alive(ctx) {
			return ErrTerminalGone
		}
		out, err := term.Capture(ctx)
		if err != nil {
			return err
		}
		norm := normalizePane(out)
		if !strings.Contains(norm, trustDialogHeader) || !strings.Contains(norm, trustDialogOption) {
			downBase = ""
			if paneIsReady(out) {
				return nil
			}
			continue
		}
		if key, ok := trustDialogKey(norm, downBase); ok {
			if err := term.SendKeys(ctx, key); err != nil {
				return err
			}
			if key == terminal.KeyDown {
				downBase = norm
			}
		}
	}
}

// trustDialogKey picks the key for a trust dialog capture, or ok=false to send
// nothing. downBase is the capture the dialog's one Down was sent on, or empty.
//
// A dialog gets at most one Down: with two options a second one wraps the
// selection back to "No, exit". After the Down nothing is sent until the
// screen changes, since the capture it was based on can still be on screen;
// then Enter once a `❯` line shows the trust option. The repainted frame may
// keep the old marker, so any marked line counts there. Before a Down, Enter
// only when the trust option is marked and "No, exit" is not.
func trustDialogKey(norm, downBase string) (key terminal.Key, ok bool) {
	if downBase != "" {
		if norm == downBase || !trustOptionHighlighted(norm) {
			return "", false
		}
		return terminal.KeyEnter, true
	}
	if trustOptionHighlighted(norm) && !markedLineContains(norm, trustDialogExit) {
		return terminal.KeyEnter, true
	}
	return terminal.KeyDown, true
}

// trustOptionHighlighted reports whether a `❯`-marked line of the trust dialog
// is the trust option. The text must already be normalised. Any marked line
// counts, not just the first: the PTY screen emulator can keep the previous
// frame's marker on screen after Claude repaints the dialog with the selection
// moved, and the repainted marker can follow a stray rule character.
func trustOptionHighlighted(out string) bool {
	return markedLineContains(out, trustDialogOption)
}

// markedLineContains reports whether text follows a `❯` on any line of out.
func markedLineContains(out, text string) bool {
	for _, line := range strings.Split(out, "\n") {
		_, after, found := strings.Cut(line, "❯")
		if found && strings.Contains(after, text) {
			return true
		}
	}
	return false
}

// paneIsReady reports whether the captured pane shows an input box that takes
// input.
func paneIsReady(out string) bool {
	out = normalizePane(out)
	for _, marker := range paneReadyMarkers {
		if strings.Contains(out, marker) {
			return true
		}
	}
	return paneHasInputBox(out)
}

// normalizePane maps non-breaking spaces to ASCII spaces. Claude Code 2.1.303
// draws the input placeholder as `❯` + U+00A0 + `Try "..."`; literal-space
// matching and Go's ASCII-only `\s` would otherwise miss it.
func normalizePane(out string) string {
	return strings.ReplaceAll(out, "\u00a0", " ")
}
