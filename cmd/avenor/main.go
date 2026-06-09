package main

import (
	"fmt"
	"os"

	"github.com/sdougbrown/avenor/internal/cli"
)

// Version is the release tag, injected at build time via -ldflags. Unreleased
// builds default to "dev" so they don't masquerade as a real version.
var Version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "answer" {
		os.Exit(runAnswer(os.Args[2:]))
	}
	if len(os.Args) > 1 && os.Args[1] == "probe" {
		os.Exit(runProbe(os.Args[2:]))
	}
	if len(os.Args) > 1 && os.Args[1] == "watch" {
		os.Exit(runWatch(os.Args[2:]))
	}
	if len(os.Args) > 1 && os.Args[1] == "stable" {
		os.Exit(runStable(os.Args[2:]))
	}
	if len(os.Args) > 1 && os.Args[1] == "control" {
		os.Exit(runControl(os.Args[2:]))
	}
	if len(os.Args) > 1 && os.Args[1] == "mcp" {
		os.Exit(runMCP(os.Args[2:]))
	}
	if len(os.Args) > 1 && os.Args[1] == "claude-channel" {
		os.Exit(runClaudeChannel(os.Args[2:]))
	}
	if len(os.Args) > 1 && os.Args[1] == "claude-channel-cleanup" {
		os.Exit(runClaudeChannelCleanup(os.Args[2:]))
	}
	if len(os.Args) > 1 && os.Args[1] == "channel-tools" {
		os.Exit(runChannelTools(os.Args[2:]))
	}
	if len(os.Args) > 1 && os.Args[1] == "board" {
		os.Exit(runBoard(os.Args[2:]))
	}

	if len(os.Args) > 1 && (os.Args[1] == "--version" || os.Args[1] == "-v") {
		fmt.Printf("avenor v%s\n", Version)
		os.Exit(0)
	}

	if len(os.Args) > 1 && (os.Args[1] == "run") {
		os.Exit(cli.Run(os.Args[2:]))
	}

	os.Exit(cli.Run(os.Args[1:]))
}
