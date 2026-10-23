package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/sdougbrown/avenor/internal/claudechannelsidecar"
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

	if err := claudechannelsidecar.Run(context.Background(), claudechannelsidecar.Options{
		RunID:     *runID,
		Token:     *token,
		BrokerURL: *brokerURL,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "avenor claude-channel: %v\n", err)
		return 1
	}
	return 0
}
