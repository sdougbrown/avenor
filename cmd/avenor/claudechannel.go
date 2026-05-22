package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
)

func runClaudeChannel(args []string) int {
	fs := flag.NewFlagSet("claude-channel", flag.ContinueOnError)
	runID := fs.String("run-id", "", "Avenor run ID")
	token := fs.String("token", "", "sidecar bearer token")
	brokerURL := fs.String("broker-url", "", "broker HTTP base URL")

	if err := fs.Parse(args); err != nil {
		return 1
	}
	if *runID == "" || *token == "" || *brokerURL == "" {
		fmt.Fprintln(os.Stderr, "avenor claude-channel: --run-id, --token, and --broker-url are required")
		return 1
	}

	// Locate the sidecar script relative to the compiled binary.
	_, callerFile, _, ok := runtime.Caller(0)
	if !ok {
		fmt.Fprintln(os.Stderr, "avenor claude-channel: cannot determine script path")
		return 1
	}
	// Walk from cmd/avenor/claudechannel.go to repo root, then into internal/claudechannelsidecar/sidecar.ts
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(callerFile)))
	sidecarPath := filepath.Join(repoRoot, "internal", "claudechannelsidecar", "sidecar.ts")
	if _, err := os.Stat(sidecarPath); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "avenor claude-channel: sidecar not found at %s\n", sidecarPath)
		return 1
	}

	cmd := exec.Command("bun", "run", sidecarPath,
		"--run-id", *runID,
		"--token", *token,
		"--broker-url", *brokerURL,
	)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "avenor claude-channel: %v\n", err)
		return 1
	}
	return 0
}
