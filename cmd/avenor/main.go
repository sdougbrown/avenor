package main

import (
	"os"
)

// Version is the release tag, injected at build time via -ldflags. Unreleased
// builds default to "dev" so they don't masquerade as a real version.
var Version = "dev"

func main() {
	os.Exit(runDeclared(os.Args[1:], os.Stdout, os.Stderr))
}
