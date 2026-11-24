package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/sdougbrown/avenor/internal/channeltools"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/cli-contract"
)

// handleChannelTools runs the channel tools loop. The broker coordinates fall
// back to the peer environment, which is how a spawning backend hands over the
// run token without putting it on an argv readable through /proc/<pid>/cmdline.
func handleChannelTools(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	runID := orDefault(inv.Flags["run-id"], os.Getenv(runtime.EnvBrokerRunID))
	brokerToken := orDefault(inv.Flags["token"], os.Getenv(runtime.EnvBrokerToken))
	brokerURL := orDefault(inv.Flags["broker-url"], os.Getenv(runtime.EnvBrokerURL))
	if runID == "" || brokerToken == "" || brokerURL == "" {
		return nil, usageErrorf("avenor channel-tools: run id, token, and broker URL are required (--run-id/--token/--broker-url or $AVENOR_RUN_ID/$AVENOR_BROKER_TOKEN/$AVENOR_BROKER_URL)")
	}
	if err := channeltools.Run(ctx, channeltools.Options{RunID: runID, Token: brokerToken, BrokerURL: brokerURL}); err != nil {
		return nil, fmt.Errorf("avenor channel-tools: %w", err)
	}
	return nil, nil
}
