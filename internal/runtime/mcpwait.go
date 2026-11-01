package runtime

import (
	"fmt"
	"time"
)

// MaxClaudeMCPWait caps ClaudeMCPWait. Claude Code gives each MCP server 30s
// to connect by default, so a wait past twice that no longer rides out a
// slow connect; it only stalls a run whose server is down.
const MaxClaudeMCPWait = 60 * time.Second

// ClaudeMCPWaitStartOnly reports whether the backend treats the MCP wait as
// start-only on an explicit resume. The wait is supported only by the
// claude family, whose resume takes no per-call options, so every supported
// backend is start-only.
func ClaudeMCPWaitStartOnly(backend string) bool {
	return IsClaudeFamily(backend)
}

// ValidateClaudeMCPWait checks a startup MCP wait for a backend. Zero means
// no wait and is valid everywhere; only the claude backends hold the first
// prompt for MCP servers, so any other backend rejects a wait instead of
// ignoring it.
func ValidateClaudeMCPWait(backend string, wait time.Duration) error {
	if wait == 0 {
		return nil
	}
	if err := validateClaudeMCPWaitDuration(wait); err != nil {
		return err
	}
	if !IsClaudeFamily(backend) {
		return fmt.Errorf("backend %q does not support parameter %q", backend, "claude-mcp-wait")
	}
	return nil
}

// ValidateClaudeMCPWaitForChosenBackend applies the wait policy and decides what
// a rejection says. A duration error keeps its own message, because naming a
// backend cannot fix a duration. When nothing supplied a backend and the
// runtime default rejected the wait, the message names the omission too.
func ValidateClaudeMCPWaitForChosenBackend(backend string, wait time.Duration, backendSupplied bool) error {
	if err := validateClaudeMCPWaitDuration(wait); err != nil {
		return err
	}
	if err := ValidateClaudeMCPWait(backend, wait); err != nil {
		if backendSupplied {
			return err
		}
		return NewOmittedBackendError("claude-mcp-wait", backend, err)
	}
	return nil
}

// validateClaudeMCPWaitDuration checks the wait's own bounds, independently of
// the backend.
func validateClaudeMCPWaitDuration(wait time.Duration) error {
	if wait < 0 {
		return fmt.Errorf("--claude-mcp-wait must not be negative")
	}
	if wait > MaxClaudeMCPWait {
		return fmt.Errorf("--claude-mcp-wait %s exceeds the %s limit", wait, MaxClaudeMCPWait)
	}
	return nil
}

// NewStartOnlyClaudeMCPWaitError reports that the backend holds the first
// prompt for MCP servers only on a new session, not an explicit resume.
func NewStartOnlyClaudeMCPWaitError(backend string, wait time.Duration) error {
	return fmt.Errorf("backend %q supports parameter %q only when starting a session; cannot apply value %q on an explicit resume", backend, "claude-mcp-wait", wait)
}
