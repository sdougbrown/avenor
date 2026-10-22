package claudecore

import (
	"context"
	"strings"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"
)

const (
	// SubmitResendInterval is how often a still-unsent prompt gets another Enter.
	SubmitResendInterval = time.Second
	// SubmitResendWindow bounds how long ResendEnterUntilSubmitted keeps trying.
	SubmitResendWindow = 10 * time.Second
)

// pastedPlaceholder is what Claude Code 2.1.303 shows in the input box in place
// of a pasted prompt of roughly 1000 characters or more, or one with several
// lines: `[Pasted text #1]` or `[Pasted text #2 +6 lines]`.
const pastedPlaceholder = "[Pasted text #"

// promptNeedleLen caps how much of the prompt's first line is matched against
// the input box.
const promptNeedleLen = 40

// ResendEnterUntilSubmitted resends Enter about once a second, for about ten
// seconds, while the input box still holds the prompt and the pane is idle.
// Claude can drop an Enter that arrives while it is still starting up, which
// leaves the prompt sitting unsent.
//
// stop is polled before each attempt; a true result ends the loop early, for
// callers that learn from another signal that the turn has begun. It may be nil.
func ResendEnterUntilSubmitted(ctx context.Context, term terminal.Session, prompt string, stop func() bool) {
	resendEnterUntilSubmitted(ctx, term, prompt, stop, SubmitResendInterval, SubmitResendWindow)
}

func resendEnterUntilSubmitted(ctx context.Context, term terminal.Session, prompt string, stop func() bool, interval, window time.Duration) {
	deadline := time.NewTimer(window)
	defer deadline.Stop()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline.C:
			return
		case <-ticker.C:
		}
		if stop != nil && stop() {
			return
		}
		out, err := term.Capture(ctx)
		if err != nil {
			return
		}
		if ClassifyPane(out) != PaneStateIdle || !inputBoxHoldsPrompt(out, prompt) {
			return
		}
		if err := term.SendKeys(ctx, terminal.KeyEnter); err != nil {
			return
		}
	}
}

// inputBoxHoldsPrompt reports whether the last `❯` line, the live input box
// rather than an earlier echo of a submitted prompt, starts with the prompt or
// holds its pasted-text placeholder. A prefix match keeps a short prompt from
// matching inside an option line such as `❯ 1. Yes`.
func inputBoxHoldsPrompt(out, prompt string) bool {
	var box string
	for _, line := range strings.Split(normalizePane(out), "\n") {
		if trimmed := strings.TrimSpace(line); strings.HasPrefix(trimmed, "❯") {
			box = strings.TrimSpace(strings.TrimPrefix(trimmed, "❯"))
		}
	}
	if box == "" {
		return false
	}
	if strings.Contains(box, pastedPlaceholder) {
		return true
	}
	needle, _, _ := strings.Cut(strings.TrimSpace(normalizePane(prompt)), "\n")
	if len(needle) > promptNeedleLen {
		needle = needle[:promptNeedleLen]
	}
	return needle != "" && strings.HasPrefix(box, needle)
}
