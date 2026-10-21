package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/sdougbrown/avenor/internal/channeltools"
)

func runChannelTools(args []string) int {
	fs := flag.NewFlagSet("channel-tools", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	runID := fs.String("run-id", "", "broker run id")
	token := fs.String("token", "", "broker auth token")
	brokerURL := fs.String("broker-url", "", "broker HTTP base URL")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *runID == "" || *token == "" || *brokerURL == "" {
		fmt.Fprintln(os.Stderr, "avenor channel-tools: --run-id, --token, and --broker-url are required")
		return 2
	}
	if err := channeltools.Run(context.Background(), channeltools.Options{RunID: *runID, Token: *token, BrokerURL: *brokerURL}); err != nil {
		fmt.Fprintf(os.Stderr, "avenor channel-tools: %v\n", err)
		return 1
	}
	return 0
}
