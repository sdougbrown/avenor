package main

import (
	"context"
	"fmt"
	"io"

	"github.com/sdougbrown/avenor/internal/channeltools"
	"github.com/sdougbrown/cli-contract"
)

func runChannelTools(args []string) int {
	return runDeclared("channel-tools", args, handleChannelTools)
}

// handleChannelTools runs the channel tools loop; the run-id, token, and
// broker-url flags are required.
func handleChannelTools(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	runID := inv.Flags["run-id"]
	brokerToken := inv.Flags["token"]
	brokerURL := inv.Flags["broker-url"]
	if runID == "" || brokerToken == "" || brokerURL == "" {
		return nil, usageErrorf("avenor channel-tools: --run-id, --token, and --broker-url are required")
	}
	if err := channeltools.Run(ctx, channeltools.Options{RunID: runID, Token: brokerToken, BrokerURL: brokerURL}); err != nil {
		return nil, fmt.Errorf("avenor channel-tools: %w", err)
	}
	return nil, nil
}
