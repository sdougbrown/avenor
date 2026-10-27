package main

import (
	"os"

	"github.com/sdougbrown/avenor/internal/stable"
)

// The launch-wrapper policy pickup chain reads the process environment and the
// operator's home directory. Both are indirected so tests can drive the chain
// without touching the ambient environment.
var (
	mcpLaunchWrapperGetenv  = os.Getenv
	mcpLaunchWrapperHomeDir = os.UserHomeDir
)

// resolveMCPLaunchWrapperPolicy is the single pickup point shared by
// `avenor mcp` (forwarded to a supervisor it autostarts) and `avenor stable`
// (loaded into its own Config). explicitPath is the --mcp-launch-wrapper-policy
// flag, which wins over the environment and the well-known file.
func resolveMCPLaunchWrapperPolicy(explicitPath string) (*stable.MCPLaunchWrapperPolicy, string, error) {
	return stable.ResolveMCPLaunchWrapperPolicy(explicitPath, mcpLaunchWrapperGetenv, mcpLaunchWrapperHomeDir)
}
