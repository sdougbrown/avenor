package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sdougbrown/avenor/internal/runtime/claudechannel"
)

func runClaudeChannelCleanup(args []string) int {
	fs := flag.NewFlagSet("claude-channel-cleanup", flag.ContinueOnError)
	dir := fs.String("dir", ".", "project directory containing .mcp.json")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	projectDir, err := filepath.Abs(*dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "avenor claude-channel-cleanup: resolve dir: %v\n", err)
		return 1
	}
	path := filepath.Join(projectDir, ".mcp.json")
	removed, err := claudechannel.CleanupProjectMCP(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "avenor claude-channel-cleanup: %v\n", err)
		return 1
	}
	fmt.Printf("removed %d avenor-channel entries from %s\n", removed, path)
	return 0
}
