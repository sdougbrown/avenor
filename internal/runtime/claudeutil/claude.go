// Package claudeutil contains shared Claude Code startup helpers.
package claudeutil

import (
	"context"
	"os/exec"
	"strings"

	"github.com/sdougbrown/avenor/internal/runtime"
)

func claudeHelpOutput(ctx context.Context) ([]byte, error) {
	return exec.CommandContext(ctx, "claude", "--help").CombinedOutput()
}

// CheckEffortCapability verifies that the installed Claude CLI supports the
// native startup effort flag. An empty effort needs no capability probe.
func CheckEffortCapability(ctx context.Context, backend, effort string) error {
	return checkEffortCapability(ctx, backend, effort, claudeHelpOutput)
}

func checkEffortCapability(ctx context.Context, backend, effort string, helpOutput func(context.Context) ([]byte, error)) error {
	if effort == "" {
		return nil
	}
	out, err := helpOutput(ctx)
	if err != nil || !strings.Contains(string(out), "--effort <level>") {
		return runtime.NewUnsupportedThinkingError(backend)
	}
	return nil
}

// BuildArgs returns the Claude CLI arguments for a normal or channel-backed
// session. A non-empty serverName enables the Claude Channel startup flag.
func BuildArgs(sessionID, serverName string, opts runtime.StartOptions) []string {
	var args []string
	if serverName != "" {
		args = append(args, "--dangerously-load-development-channels", "server:"+serverName)
	}
	args = append(args, "--session-id", sessionID)
	if opts.Agent != "" {
		args = append(args, "--agent", opts.Agent)
	}
	if opts.Label != "" {
		args = append(args, "--name", opts.Label)
	}
	if opts.Model != "" {
		args = append(args, "--model", opts.Model)
	}
	if opts.Thinking != "" {
		args = append(args, "--effort", opts.Thinking)
	}
	return append(args, "--permission-mode", "default")
}
