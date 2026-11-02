package main

import (
	"os"
)

// Version is the release tag, injected at build time via -ldflags. Unreleased
// builds default to "dev" so they don't masquerade as a real version.
var Version = "dev"

func main() {
	// The declared surface handles every invocation: the commands, help,
	// version, and caller errors for unknown commands. A bare invocation is
	// equivalent to "run".
	args := os.Args[1:]
	if len(args) == 0 {
		args = []string{"run"}
	}
	os.Exit(runDeclared(args, os.Stdout, os.Stderr))
}
