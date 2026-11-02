package main

import (
	"os"

	"github.com/sdougbrown/avenor/internal/cli"
	"github.com/sdougbrown/cli-contract"
)

// Version is the release tag, injected at build time via -ldflags. Unreleased
// builds default to "dev" so they don't masquerade as a real version.
var Version = "dev"

// legacyCommands dispatch through their pre-library paths until their
// migration to the declared surface lands.
var legacyCommands = map[string]func([]string) int{
	"answer":   runAnswer,
	"await":    runAwait,
	"watch":    runWatch,
	"control":  runControl,
	"workflow": runWorkflow,
	"mcp":      runMCP,
}

func main() {
	if len(os.Args) > 1 {
		if legacy, ok := legacyCommands[os.Args[1]]; ok {
			os.Exit(legacy(os.Args[2:]))
		}
		if os.Args[1] == "run" {
			os.Exit(cli.Run(os.Args[2:]))
		}
		// The declared surface handles everything else: migrated commands,
		// help, version, and caller errors for unknown commands.
		os.Exit(clicontract.Run(app(), avenorAdapter{}, os.Args[1:], os.Stdout, os.Stderr))
	}

	// A bare invocation stays on the legacy path: the declared surface does
	// not yet cover every command, so bare help remains the legacy usage.
	os.Exit(cli.Run(os.Args[1:]))
}
