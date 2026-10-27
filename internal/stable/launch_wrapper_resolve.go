package stable

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// EnvMCPLaunchWrapperPolicy names the environment variable that points at a
// launch-wrapper policy file.
const EnvMCPLaunchWrapperPolicy = "AVENOR_MCP_LAUNCH_WRAPPER_POLICY"

// mcpLaunchWrapperPolicyFile is the well-known machine-local policy file,
// relative to the operator's home directory.
const mcpLaunchWrapperPolicyFile = ".avenor/launch-wrapper-policy.json"

// ResolveMCPLaunchWrapperPolicy applies the machine-local pickup chain for the
// MCP launch wrapper policy and returns the loaded policy together with the
// path it came from ("" when disabled):
//
//  1. explicitPath (an explicit --mcp-launch-wrapper-policy flag),
//  2. $AVENOR_MCP_LAUNCH_WRAPPER_POLICY,
//  3. $HOME/.avenor/launch-wrapper-policy.json,
//  4. none of the above: a nil policy, matching the disabled default.
//
// An explicitly named path must exist and load; a missing or invalid file is an
// error. The well-known file is optional, but a malformed one is also an error:
// the chain never falls back to a lower-precedence source. getenv and homeDir
// are injected so callers and tests never depend on the ambient process
// environment; an unresolvable home directory means no well-known file.
func ResolveMCPLaunchWrapperPolicy(explicitPath string, getenv func(string) string, homeDir func() (string, error)) (*MCPLaunchWrapperPolicy, string, error) {
	if explicitPath != "" {
		return loadResolvedMCPLaunchWrapperPolicy(explicitPath)
	}
	if envPath := strings.TrimSpace(getenv(EnvMCPLaunchWrapperPolicy)); envPath != "" {
		return loadResolvedMCPLaunchWrapperPolicy(envPath)
	}
	home, err := homeDir()
	if err != nil {
		return nil, "", nil
	}
	wellKnown := filepath.Join(home, mcpLaunchWrapperPolicyFile)
	if _, err := os.Stat(wellKnown); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, "", nil
		}
		return nil, "", err
	}
	return loadResolvedMCPLaunchWrapperPolicy(wellKnown)
}

func loadResolvedMCPLaunchWrapperPolicy(path string) (*MCPLaunchWrapperPolicy, string, error) {
	policy, err := LoadMCPLaunchWrapperPolicy(path)
	if err != nil {
		return nil, "", fmt.Errorf("%s: %w", path, err)
	}
	return policy, path, nil
}
