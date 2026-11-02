package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
	"unicode"

	"github.com/sdougbrown/avenor/internal/mcpserver"
	clicontract "github.com/sdougbrown/cli-contract"
)

type allowedHostList []string

// newMCPServerFn is the MCP server constructor used by runMCP. Tests patch it
// to capture Options without starting a server.
var newMCPServerFn = mcpserver.NewServer

func (a *allowedHostList) String() string { return strings.Join(*a, ",") }

func (a *allowedHostList) Set(value string) error {
	if value == "" {
		return errors.New("--allowed-host requires a non-empty hostname")
	}
	if strings.ContainsAny(value, "*/:<>") {
		return errors.New("--allowed-host must be an exact hostname")
	}
	for _, r := range value {
		if unicode.IsSpace(r) {
			return errors.New("--allowed-host must not contain whitespace")
		}
	}
	*a = append(*a, value)
	return nil
}

func mcpFlagError(transport string, allowedHosts []string) error {
	if transport != "stdio" && transport != "http" {
		return errors.New(`--transport only supports "stdio" or "http"`)
	}
	if transport == "stdio" && len(allowedHosts) > 0 {
		return errors.New("--allowed-host is only supported with --transport http")
	}
	return nil
}

// mcpMaxWaitError rejects a negative wait budget; the parse-level default is
// 25s and an explicit 0 disables the clamp on both transports.
func mcpMaxWaitError(value time.Duration) error {
	if value < 0 {
		return errors.New("--max-wait must not be negative")
	}
	return nil
}

// validateAllowedHost applies the per-entry rules the flag.Var binding used
// to enforce at parse time: non-empty, exact hostname, no whitespace.
func validateAllowedHost(value string) error {
	if value == "" {
		return errors.New("requires a non-empty hostname")
	}
	if strings.ContainsAny(value, "*/:<>") {
		return errors.New("must be an exact hostname")
	}
	for _, r := range value {
		if unicode.IsSpace(r) {
			return errors.New("must not contain whitespace")
		}
	}
	return nil
}

func runMCP(args []string) int {
	return runDeclared(append([]string{"mcp"}, args...), os.Stdout, os.Stderr)
}

// handleMCP serves the MCP interface until it exits; the stdio transport
// writes protocol traffic directly to os.Stdout.
func handleMCP(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	transport := orDefault(inv.Flags["transport"], "stdio")
	controlSocket := inv.Flags["control-socket"]
	supervisorSocket := inv.Flags["supervisor-socket"]
	noAutostart := inv.Bools["no-autostart"]
	idleTimeout := orDefault(inv.Durations["idle-timeout"], 30*time.Minute)
	addr := orDefault(inv.Flags["addr"], "127.0.0.1:3748")
	authToken := inv.Flags["auth-token"]
	authTokenFile := inv.Flags["auth-token-file"]
	maxWait := inv.Durations["max-wait"]
	allowedHosts := allowedHostList(inv.Events)
	for _, host := range allowedHosts {
		if err := validateAllowedHost(host); err != nil {
			return nil, usageErrorf("avenor mcp: --allowed-host: %v", err)
		}
	}
	mcpLaunchWrapperPolicy := inv.Flags["mcp-launch-wrapper-policy"]

	// Presence checks use the parsed values: an explicitly empty token or
	// file path is indistinguishable from an absent flag.
	tokenSet := authToken != ""
	fileSet := authTokenFile != ""

	if err := mcpFlagError(transport, allowedHosts); err != nil {
		return nil, usageErrorf("avenor mcp: %v", err)
	}
	if err := mcpMaxWaitError(maxWait); err != nil {
		return nil, usageErrorf("avenor mcp: %v", err)
	}
	if tokenSet && fileSet {
		return nil, usageErrorf("avenor mcp: --auth-token and --auth-token-file cannot both be set")
	}
	if noAutostart && supervisorSocket == "" {
		return nil, usageErrorf("avenor mcp: --no-autostart requires --supervisor-socket")
	}
	if transport == "http" {
		// Precedence: explicit --auth-token, then --auth-token-file, then the
		// MCP_AUTH_TOKEN environment variable.
		token, err := resolveAuthToken(tokenSet, fileSet, authToken, authTokenFile, os.Getenv("MCP_AUTH_TOKEN"))
		if err != nil {
			return nil, fmt.Errorf("avenor mcp: %w", err)
		}
		authToken = token
	}

	// Resolve and validate the policy here, then forward only its path: the
	// autostarted supervisor loads the same file again and owns the policy it
	// actually enforces. This eager load is validation-only, so a malformed
	// named file fails the command before any supervisor starts; do not "fix"
	// the double load by dropping one side without moving validation with it.
	_, mcpPolicyPath, err := resolveMCPLaunchWrapperPolicy(mcpLaunchWrapperPolicy)
	if err != nil {
		return nil, fmt.Errorf("avenor mcp: mcp launch wrapper policy: %w", err)
	}

	s, err := newMCPServerFn(mcpserver.Options{
		Transport:                  transport,
		ControlSocket:              controlSocket,
		SupervisorSocket:           supervisorSocket,
		NoAutostart:                noAutostart,
		IdleTimeout:                idleTimeout,
		Addr:                       addr,
		AuthToken:                  authToken,
		MaxWait:                    maxWait,
		AllowedHosts:               allowedHosts,
		ControlClient:              nil,
		MCPLaunchWrapperPolicyPath: mcpPolicyPath,
	})
	if err != nil {
		return nil, fmt.Errorf("avenor mcp: %w", err)
	}
	defer s.Close()

	if transport == "http" {
		if err := s.RunHTTP(addr); err != nil {
			return nil, fmt.Errorf("avenor mcp: %w", err)
		}
	} else {
		if err := s.Run(); err != nil {
			return nil, fmt.Errorf("avenor mcp: %w", err)
		}
	}
	return nil, nil
}

// resolveAuthToken applies the bearer-token precedence for HTTP transport:
// an explicit --auth-token wins over --auth-token-file, which wins over the
// MCP_AUTH_TOKEN environment variable. A set token file is always read; read
// or validation errors are returned, never swallowed in favor of the env.
func resolveAuthToken(tokenSet, fileSet bool, token, tokenFile, envToken string) (string, error) {
	switch {
	case tokenSet && strings.TrimSpace(token) != "":
		return token, nil
	case fileSet:
		return readAuthTokenFile(tokenFile)
	default:
		return envToken, nil
	}
}

// readAuthTokenFile reads a bearer token from path, rejecting files that are
// group- or world-readable and files whose trimmed content is empty.
func readAuthTokenFile(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if mode := info.Mode(); mode.Perm()&0o077 != 0 {
		return "", fmt.Errorf("auth token file must not be group- or world-readable: %s", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", fmt.Errorf("auth token file is empty: %s", path)
	}
	return token, nil
}
