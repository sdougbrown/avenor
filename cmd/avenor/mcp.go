package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/sdougbrown/avenor/internal/mcpserver"
)
func runMCP(args []string) int {
	fs := flag.NewFlagSet("mcp", flag.ContinueOnError)
	transport := fs.String("transport", "stdio", "transport for MCP server (only \"stdio\" is supported)")
	controlSocket := fs.String("control-socket", "", "unix socket path for the control plane")
	supervisorSocket := fs.String("supervisor-socket", "", "unix socket path for the supervisor")
	noAutostart := fs.Bool("no-autostart", false, "disable automatic supervisor start")
	idleTimeout := fs.Duration("idle-timeout", 30*time.Minute, "idle timeout before server exits")

	if err := fs.Parse(args); err != nil {
		return 1
	}

	if *transport != "stdio" {
		fmt.Fprintln(os.Stderr, "avenor mcp: --transport only supports \"stdio\"")
		return 1
	}
	if *noAutostart && *supervisorSocket == "" {
		fmt.Fprintln(os.Stderr, "avenor mcp: --no-autostart requires --supervisor-socket")
		return 1
	}

	s, err := mcpserver.NewServer(mcpserver.Options{
		Transport:        *transport,
		ControlSocket:    *controlSocket,
		SupervisorSocket: *supervisorSocket,
		NoAutostart:      *noAutostart,
		IdleTimeout:      *idleTimeout,
		ControlClient:    nil,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "avenor mcp: %v\n", err)
		return 1
	}
	defer s.Close()

	if err := s.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "avenor mcp: %v\n", err)
		return 1
	}
	return 0
}
