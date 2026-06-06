package claudecore

import (
	"os"

	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"
)

func DefaultLauncher() terminal.Launcher {
	switch os.Getenv("AVENOR_CLAUDE_TERMINAL") {
	case "pty":
		return terminal.PTYLauncher{}
	case "", "tmux":
		return terminal.TmuxLauncher{}
	default:
		// Unknown value — still default to tmux but could log a warning.
		return terminal.TmuxLauncher{}
	}
}
