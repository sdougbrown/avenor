package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/sdougbrown/avenor/internal/childmessaging"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
	"github.com/sdougbrown/avenor/internal/runtime"
	clicontract "github.com/sdougbrown/cli-contract"
)

// messageCommand declares the child-authenticated messaging commands.
func messageCommand() *clicontract.Command {
	text := clicontract.Flag{Name: "text", Kind: clicontract.String, Usage: "message text", Required: true}
	target := clicontract.Flag{Name: "target", Kind: clicontract.String, Usage: "recipient run id or label", Default: "supervisor"}
	return &clicontract.Command{
		Name: "message", Short: "send messages as the current child run",
		PersistentFlags: []clicontract.Flag{
			{Name: "broker-url", Kind: clicontract.String, Usage: "broker URL (default: $AVENOR_BROKER_URL)"},
			{Name: "run-id", Kind: clicontract.String, Usage: "caller run id (default: $AVENOR_RUN_ID)"},
			{Name: "token", Kind: clicontract.String, Usage: "broker token (default: $AVENOR_BROKER_TOKEN; prefer the environment)"},
		},
		Subcommands: []*clicontract.Command{
			{Name: "tell", Short: "send a message", Flags: []clicontract.Flag{target, text}, Handle: handleMessage("tell")},
			{Name: "ask", Short: "send a question and wait for a reply", Flags: []clicontract.Flag{target, text, {Name: "timeout-ms", Kind: clicontract.Int, Usage: "wait in milliseconds (0 uses the default; maximum 600000)"}}, Handle: handleMessage("ask")},
			{Name: "reply", Short: "answer a message", Flags: []clicontract.Flag{{Name: "reply-to", Kind: clicontract.String, Usage: "message id", Required: true}, {Name: "to", Kind: clicontract.String, Usage: "asker's broker run id"}, text}, Handle: handleMessage("reply")},
		},
	}
}

// messageClaim supplies the asker named by a reply invocation to the router.
type messageClaim struct {
	*childmessaging.MessageProvider
	messageID string
	asker     string
}

// ClaimAsk returns the asker for the message named on the command line.
func (p *messageClaim) ClaimAsk(messageID string) (string, bool) {
	return p.asker, messageID == p.messageID
}

// RestoreAsk leaves the caller-supplied claim available for a retry.
func (p *messageClaim) RestoreAsk(_, _ string) {}

// handleMessage routes one command through the shared child messaging router.
func handleMessage(action string) func(context.Context, clicontract.Invocation, io.Writer) (any, error) {
	return func(ctx context.Context, inv clicontract.Invocation, _ io.Writer) (any, error) {
		url := orDefault(inv.Flags["broker-url"], os.Getenv(runtime.EnvBrokerURL))
		token := orDefault(inv.Flags["token"], os.Getenv(runtime.EnvBrokerToken))
		runID := orDefault(inv.Flags["run-id"], os.Getenv(runtime.EnvBrokerRunID))
		if url == "" || token == "" || runID == "" {
			return nil, usageErrorf("avenor message: --broker-url, --token and --run-id (or their environment variables) are required")
		}
		if inv.Flags["text"] == "" || (action == "reply" && inv.Flags["reply-to"] == "") || (action != "reply" && inv.Flags["target"] == "") {
			return nil, usageErrorf("avenor message %s: text and recipient must not be empty", action)
		}
		timeout := inv.Ints["timeout-ms"]
		if timeout < 0 {
			return nil, usageErrorf("avenor message ask: --timeout-ms must not be negative")
		}
		if max := int(messaging.MaxWait.Milliseconds()); timeout > max {
			timeout = max
		}
		input := map[string]any{"action": action, "message": inv.Flags["text"]}
		if action == "reply" {
			input["reply_to"] = inv.Flags["reply-to"]
		} else {
			input["target"] = inv.Flags["target"]
		}
		if action == "ask" {
			input["timeout_ms"] = timeout
		}
		raw, err := json.Marshal(input)
		if err != nil {
			return nil, err
		}
		provider, err := childmessaging.NewMessageProvider(messaging.SurfaceFeed, childmessaging.Options{RunID: runID, Token: token, BrokerURL: url})
		if err != nil {
			return nil, fmt.Errorf("avenor message: %w", err)
		}
		defer provider.Close()
		var routed router.Provider = provider
		if action == "reply" && inv.Flags["to"] != "" {
			routed = &messageClaim{MessageProvider: provider, messageID: inv.Flags["reply-to"], asker: inv.Flags["to"]}
		}
		if err := provider.Prepare(ctx); err != nil {
			return nil, fmt.Errorf("avenor message: %w", err)
		}
		rt, err := router.NewRouter(routed)
		if err != nil {
			return nil, fmt.Errorf("avenor message: %w", err)
		}
		result, err := rt.Execute(ctx, raw)
		if err != nil {
			return nil, fmt.Errorf("avenor message: %w", err)
		}
		return result.Output(), nil
	}
}
