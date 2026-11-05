package main

import (
	"context"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/sdougbrown/avenor/internal/appdir"
	"github.com/sdougbrown/avenor/internal/feed"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/cli-contract"
)

// feedCommand declares the broker feed surface.
func feedCommand() *clicontract.Command {
	return &clicontract.Command{Name: "feed", Short: "run a session feed", Subcommands: []*clicontract.Command{{
		Name: "broker", Short: "forward broker messages to a poo session",
		Flags: []clicontract.Flag{
			{Name: "broker-url", Kind: clicontract.String, Usage: "broker URL (default: $AVENOR_BROKER_URL)"},
			{Name: "run-id", Kind: clicontract.String, Usage: "broker run id (default: $AVENOR_RUN_ID)"},
			{Name: "token", Kind: clicontract.String, Usage: "broker token (default: $AVENOR_BROKER_TOKEN; prefer the environment to argv)"},
			{Name: "limit", Kind: clicontract.Int, Usage: "maximum messages per poll", Default: 8},
			{Name: "request-timeout", Kind: clicontract.Duration, Usage: "maximum poll duration", Default: 15 * time.Second},
			{Name: "lock-dir", Kind: clicontract.String, Usage: "feed lock directory"},
			{Name: "asks-dir", Kind: clicontract.String, Usage: "inbound ask claim directory"},
		}, Handle: handleFeedBroker,
	}}}
}

// handleFeedBroker emits only feed records on stdout.
func handleFeedBroker(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	brokerURL := orDefault(inv.Flags["broker-url"], os.Getenv(runtime.EnvBrokerURL))
	runID := orDefault(inv.Flags["run-id"], os.Getenv(runtime.EnvBrokerRunID))
	token := orDefault(inv.Flags["token"], os.Getenv(runtime.EnvBrokerToken))
	if brokerURL == "" || runID == "" || token == "" {
		return nil, usageErrorf("avenor feed broker: broker URL, run id, and token are required")
	}
	lockDir := orDefault(inv.Flags["lock-dir"], filepath.Join(appdir.StateHome(), "feed-locks"))
	signal.Ignore(syscall.SIGPIPE)
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return exitCode((feed.Broker{RunID: runID, Token: token, BrokerURL: brokerURL, Limit: inv.Ints["limit"], RequestTimeout: inv.Durations["request-timeout"], LockDir: lockDir, AsksDir: inv.Flags["asks-dir"], Out: os.Stdout, Log: stderr}).Run(ctx)), nil
}
