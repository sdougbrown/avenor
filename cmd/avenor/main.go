package main

import (
	"os"

	"github.com/sdougbrown/avenor/internal/cli"
)

// Version is the release tag, injected at build time via -ldflags. Unreleased
// builds default to "dev" so they don't masquerade as a real version.
var Version = "dev"

func main() {
	if len(os.Args) > 1 {
		// The declared surface handles every named invocation: the commands,
		// help, version, and caller errors for unknown commands.
		os.Exit(runDeclared(os.Args[1:], os.Stdout, os.Stderr))
	}

	// A bare invocation stays on the pre-library path: the declared surface
	// does not yet cover every command, so bare help remains the pre-library
	// usage.
	os.Exit(cli.Run(os.Args[1:]))
}
