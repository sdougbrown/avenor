package claudecore

import (
	"context"
	"strings"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"
)

const (
	PromptInjectCheckInterval = 500 * time.Millisecond
	PromptInjectTimeout       = 30 * time.Second
)

// Trust-safety dialog Claude shows on first launch in an unfamiliar directory.
// We require both the header and the highlighted option to be present, so a
// stray match on either string in unrelated TUI content doesn't trigger the
// auto-dismiss. The default-highlighted option is "Yes, I trust this folder",
// so a single Enter dismisses it — avenor is invoked against a directory the
// user chose.
const (
	trustDialogHeader = "Quick safety check"
	trustDialogOption = "Yes, I trust this folder"
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

// WaitForPaneReady blocks until Claude's input box is taking input. It
// auto-dismisses the trust-safety dialog if present. Returns false on context
// cancel, capture error, send error, or the PromptInjectTimeout deadline —
// callers must treat any false return as "do not paste, the pane is not in a
// known-ready state".
func WaitForPaneReady(ctx context.Context, term terminal.Session) bool {
	deadline := time.NewTimer(PromptInjectTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(PromptInjectCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-deadline.C:
			return false
		case <-ticker.C:
		}

		out, err := term.Capture(ctx)
		if err != nil {
			return false
		}
		if strings.Contains(out, trustDialogHeader) && strings.Contains(out, trustDialogOption) {
			if err := term.SendKeys(ctx, terminal.KeyEnter); err != nil {
				return false
			}
			continue
		}
		for _, marker := range paneReadyMarkers {
			if strings.Contains(out, marker) {
				return true
			}
		}
	}
}
