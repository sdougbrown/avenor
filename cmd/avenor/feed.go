package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/appdir"
	"github.com/sdougbrown/avenor/internal/feed"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/cli-contract"
)

// feedCommand declares the broker feed surface.
func feedCommand() *clicontract.Command {
	return &clicontract.Command{Name: "feed", Short: "run a session feed", Subcommands: []*clicontract.Command{{
		Name: "broker", Short: "forward broker messages to a poo session",
		Flags: append(bindCoordinateFlags(),
			clicontract.Flag{Name: "broker-url", Kind: clicontract.String, Usage: "broker URL (default: $AVENOR_BROKER_URL)"},
			clicontract.Flag{Name: "run-id", Kind: clicontract.String, Usage: "broker run id (default: $AVENOR_RUN_ID)"},
			clicontract.Flag{Name: "token", Kind: clicontract.String, Usage: "broker token (default: $AVENOR_BROKER_TOKEN; prefer the environment to argv)"},
			clicontract.Flag{Name: "limit", Kind: clicontract.Int, Usage: "maximum messages per poll", Default: 8},
			clicontract.Flag{Name: "request-timeout", Kind: clicontract.Duration, Usage: "maximum poll duration", Default: 15 * time.Second},
			clicontract.Flag{Name: "lock-dir", Kind: clicontract.String, Usage: "feed lock directory"},
			clicontract.Flag{Name: "asks-dir", Kind: clicontract.String, Usage: "inbound ask claim directory"},
		), Handle: handleFeedBroker,
	}}}
}

// handleFeedBroker emits only feed records on stdout.
//
// The feed is the one attachment that binds. A session configured with the
// conversation's coordinates binds once, then serves the inbox with the
// credentials that bind returned. Every other consumer
// reads that binding instead of minting one, so the inbox and the registered
// commands always address the same run.
//
// A conversation that names no socket gets its own derived supervisor socket, and
// the feed is the one consumer allowed to start what it names: it starts the
// supervisor, serves the inbox through it, and stops it when the feed exits. A
// configured socket is attach-only for the same reason it always was — the
// supervisor behind it belongs to whoever configured it.
func handleFeedBroker(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	brokerURL := orDefault(inv.Flags["broker-url"], os.Getenv(runtime.EnvBrokerURL))
	runID := orDefault(inv.Flags["run-id"], os.Getenv(runtime.EnvBrokerRunID))
	token := orDefault(inv.Flags["token"], os.Getenv(runtime.EnvBrokerToken))
	coords, requested := boundCoordinatesFrom(inv)
	if requested {
		if err := coords.complete(); err != nil {
			return nil, fmt.Errorf("avenor feed broker: %w", err)
		}
		if inv.Supplied["broker-url"] || inv.Supplied["run-id"] || inv.Supplied["token"] {
			return nil, usageErrorf("avenor feed broker: --broker-url, --run-id and --token are mutually exclusive with --bind-host and --bind-session")
		}
		if coords.derived {
			lifecycle, err := startOwnedConversationSupervisor(coords)
			if err != nil {
				return nil, fmt.Errorf("avenor feed broker: start the conversation's supervisor at %s: %w", coords.socket, err)
			}
			// The supervisor serves this conversation for as long as the feed does.
			// Stopping it is the feed's last act on every exit route it can observe;
			// a lifecycle that attached to someone else's supervisor closes its
			// connection without shutting it down.
			defer func() {
				if closeErr := lifecycle.Close(); closeErr != nil {
					fmt.Fprintf(stderr, "avenor feed broker: stop the conversation's supervisor at %s: %v\n", coords.socket, closeErr)
				}
			}()
		}
		bound, err := bindConversation(ctx, coords)
		if err != nil {
			return nil, fmt.Errorf("avenor feed broker: %w", err)
		}
		brokerURL, runID, token = bound.BrokerURL, bound.RunID, bound.RunToken
	}
	if brokerURL == "" || runID == "" || token == "" {
		return nil, usageErrorf("avenor feed broker: broker URL, run id, and token are required")
	}
	lockDir := orDefault(inv.Flags["lock-dir"], feed.DefaultLockDir(appdir.StateHome()))
	signal.Ignore(syscall.SIGPIPE)
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return exitCode((feed.Broker{RunID: runID, Token: token, BrokerURL: brokerURL, Limit: inv.Ints["limit"], RequestTimeout: inv.Durations["request-timeout"], LockDir: lockDir, AsksDir: inv.Flags["asks-dir"], Out: os.Stdout, Log: stderr}).Run(ctx)), nil
}

// bindConversation binds the configured conversation on the supervisor the
// coordinates name. The bind is idempotent for a live conversation, so a feed
// restart rejoins the same inbox instead of starting a second identity. Only the
// conversation's own derived supervisor may be started for it, and only before
// this call: the bind itself never starts one.
func bindConversation(ctx context.Context, coords boundCoordinates) (messaging.MessageBindResult, error) {
	c, err := client.AttachOwner(coords.socket)
	if err != nil {
		return messaging.MessageBindResult{}, fmt.Errorf("attach %s: %w", coords.attachTarget(), err)
	}
	defer c.Close()
	return c.MessageBindContext(ctx, messaging.MessageBindRequest{Host: coords.host, SessionID: coords.session})
}
