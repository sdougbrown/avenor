package runtime

import (
	"fmt"
	"time"
)

// MaxClaudeMCPWait caps ClaudeMCPWait. Claude Code gives each MCP server 30s
// to connect by default, so a wait past twice that no longer rides out a
// slow connect; it only stalls a run whose server is down.
const MaxClaudeMCPWait = 60 * time.Second

// ValidateClaudeMCPWait checks a startup MCP wait for a backend. Zero means
// no wait and is valid everywhere; only the claude backends hold the first
// prompt for MCP servers, so any other backend rejects a wait instead of
// ignoring it.
func ValidateClaudeMCPWait(backend string, wait time.Duration) error {
	if wait == 0 {
		return nil
	}
	if wait < 0 {
		return fmt.Errorf("--claude-mcp-wait must not be negative")
	}
	if wait > MaxClaudeMCPWait {
		return fmt.Errorf("--claude-mcp-wait %s exceeds the %s limit", wait, MaxClaudeMCPWait)
	}
	if backend != "claude" && backend != "claude-channel" {
		return fmt.Errorf("backend %q does not support parameter %q", backend, "claude-mcp-wait")
	}
	return nil
}

// NewStartOnlyClaudeMCPWaitError reports that the backend holds the first
// prompt for MCP servers only on a new session, not an explicit resume.
func NewStartOnlyClaudeMCPWaitError(backend string, wait time.Duration) error {
	return fmt.Errorf("backend %q supports parameter %q only when starting a session; cannot apply value %q on an explicit resume", backend, "claude-mcp-wait", wait)
}
