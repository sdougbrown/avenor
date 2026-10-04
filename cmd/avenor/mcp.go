package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
	"unicode"

	"github.com/sdougbrown/avenor/internal/mcpserver"
)

type allowedHostList []string

func (a *allowedHostList) String() string { return strings.Join(*a, ",") }

func (a *allowedHostList) Set(value string) error {
	if value == "" {
		return errors.New("--allowed-host requires a non-empty hostname")
	}
	if strings.ContainsAny(value, "*/:") {
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

// effectiveMaxWait resolves the configured polling budget: an explicit
// --max-wait wins; otherwise HTTP gets the 25s default and stdio no clamp.
func effectiveMaxWait(transport string, explicit bool, value time.Duration) time.Duration {
	if explicit {
		return value
	}
	if transport == "http" {
		return 25 * time.Second
	}
	return 0
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

// mcpMaxWaitError reports whether an explicit --max-wait is valid for the
// transport. stdio never clamps, so an explicit nonzero value is rejected;
// an explicit 0 is a legal no-op.
func mcpMaxWaitError(transport string, explicit bool, value time.Duration) error {
	if transport == "stdio" && explicit && value != 0 {
		return errors.New("--max-wait is only supported with --transport http")
	}
	return nil
}

func runMCP(args []string) int {
	fs := flag.NewFlagSet("mcp", flag.ContinueOnError)
	transport := fs.String("transport", "stdio", "transport for MCP server (\"stdio\" or \"http\")")
	controlSocket := fs.String("control-socket", "", "unix socket path for the control plane")
	supervisorSocket := fs.String("supervisor-socket", "", "unix socket path for the supervisor")
	noAutostart := fs.Bool("no-autostart", false, "disable automatic supervisor start")
	idleTimeout := fs.Duration("idle-timeout", 30*time.Minute, "idle timeout before server exits")
	addr := fs.String("addr", "127.0.0.1:3748", "address to listen on for HTTP transport")
	authToken := fs.String("auth-token", "", "bearer token required for HTTP transport (defaults to MCP_AUTH_TOKEN)")
	maxWait := fs.Duration("max-wait", 0, "approximate polling budget for blocking tools (default 25s over HTTP; 0 disables)")
	var allowedHosts allowedHostList
	fs.Var(&allowedHosts, "allowed-host", "exact tailnet hostname accepted on the HTTP transport (repeatable)")

	if err := fs.Parse(args); err != nil {
		return 1
	}

	maxWaitExplicit := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "max-wait" {
			maxWaitExplicit = true
		}
	})

	if err := mcpFlagError(*transport, allowedHosts); err != nil {
		fmt.Fprintln(os.Stderr, "avenor mcp:", err)
		return 1
	}
	if err := mcpMaxWaitError(*transport, maxWaitExplicit, *maxWait); err != nil {
		fmt.Fprintln(os.Stderr, "avenor mcp:", err)
		return 1
	}
	if *maxWait < 0 {
		fmt.Fprintln(os.Stderr, "avenor mcp: --max-wait must not be negative")
		return 1
	}
	if *noAutostart && *supervisorSocket == "" {
		fmt.Fprintln(os.Stderr, "avenor mcp: --no-autostart requires --supervisor-socket")
		return 1
	}
	if *transport == "http" && *authToken == "" {
		*authToken = os.Getenv("MCP_AUTH_TOKEN")
	}

	s, err := mcpserver.NewServer(mcpserver.Options{
		Transport:        *transport,
		ControlSocket:    *controlSocket,
		SupervisorSocket: *supervisorSocket,
		NoAutostart:      *noAutostart,
		IdleTimeout:      *idleTimeout,
		Addr:             *addr,
		AuthToken:        *authToken,
		MaxWait:          effectiveMaxWait(*transport, maxWaitExplicit, *maxWait),
		AllowedHosts:     allowedHosts,
		ControlClient:    nil,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "avenor mcp: %v\n", err)
		return 1
	}
	defer s.Close()

	if *transport == "http" {
		if err := s.RunHTTP(*addr); err != nil {
			fmt.Fprintf(os.Stderr, "avenor mcp: %v\n", err)
			return 1
		}
	} else {
		if err := s.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "avenor mcp: %v\n", err)
			return 1
		}
	}
	return 0
}
