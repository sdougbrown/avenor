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
			return true
		case <-ticker.C:
		}

		out, err := term.Capture(ctx)
		if err != nil {
			return false
		}
		if !strings.Contains(out, "Loading development channels") {
			return true
		}
	}
}
