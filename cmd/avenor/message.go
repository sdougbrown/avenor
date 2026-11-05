package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/sdougbrown/avenor/internal/appdir"
	"github.com/sdougbrown/avenor/internal/askclaim"
	"github.com/sdougbrown/avenor/internal/childmessaging"
	"github.com/sdougbrown/avenor/internal/childsurface"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
	"github.com/sdougbrown/avenor/internal/runtime"
	clicontract "github.com/sdougbrown/cli-contract"
)

// messageCommand declares the child-authenticated messaging command.
func messageCommand() *clicontract.Command {
	return &clicontract.Command{
		Name: "message", Short: "send messages as the current child run",
		PersistentFlags: []clicontract.Flag{
			{Name: "broker-url", Kind: clicontract.String, Usage: "broker URL (default: $AVENOR_BROKER_URL)"},
			{Name: "run-id", Kind: clicontract.String, Usage: "caller run id (default: $AVENOR_RUN_ID)"},
			{Name: "token", Kind: clicontract.String, Usage: "broker token (default: $AVENOR_BROKER_TOKEN; prefer the environment)"},
		},
		Flags: []clicontract.Flag{
			{Name: "action", Kind: clicontract.String, Usage: "tell, ask, reply or withdraw", Required: true},
			{Name: "target", Kind: clicontract.String, Usage: "recipient run id or label"},
			{Name: "message", Kind: clicontract.String, Usage: "message text"},
			{Name: "reply-to", Kind: clicontract.String, Usage: "message id to reply to"},
			{Name: "message-id", Kind: clicontract.String, Usage: "message id to withdraw"},
			{Name: "timeout-ms", Kind: clicontract.Int, Usage: "wait in milliseconds (maximum 600000)"},
			{Name: "asks-dir", Kind: clicontract.String, Usage: "directory of feed ask records"},
		},
		Handle: handleMessage,
	}
}

// handleMessage routes one command through the shared child messaging router.
func handleMessage(ctx context.Context, inv clicontract.Invocation, _ io.Writer) (any, error) {
	if inv.Flags["action"] == "" {
		return nil, usageErrorf("avenor message: --action is required")
	}
	url := orDefault(inv.Flags["broker-url"], os.Getenv(runtime.EnvBrokerURL))
	token := orDefault(inv.Flags["token"], os.Getenv(runtime.EnvBrokerToken))
	runID := orDefault(inv.Flags["run-id"], os.Getenv(runtime.EnvBrokerRunID))
	if url == "" || token == "" || runID == "" {
		return nil, usageErrorf("avenor message: --broker-url, --token and --run-id (or their environment variables) are required")
	}
	input := map[string]any{"action": inv.Flags["action"]}
	for _, field := range []struct{ flag, key string }{{"target", "target"}, {"message", "message"}, {"reply-to", "reply_to"}, {"message-id", "message_id"}} {
		if inv.Supplied[field.flag] {
			input[field.key] = inv.Flags[field.flag]
		}
	}
	if inv.Supplied["timeout-ms"] {
		input["timeout_ms"] = inv.Ints["timeout-ms"]
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
	var routed childsurface.Provider = provider
	if inv.Flags["action"] == "reply" {
		dir := inv.Flags["asks-dir"]
		if dir == "" {
			dir, err = askclaim.DefaultDir(appdir.StateHome(), url, runID)
			if err != nil {
				return nil, fmt.Errorf("avenor message: %w", err)
			}
		}
		routed = &askclaim.Provider{MessageProvider: provider, Dir: dir}
	}
	rt, err := childsurface.Router(ctx, routed)
	if err != nil {
		return nil, fmt.Errorf("avenor message: %w", err)
	}
	result, err := rt.Execute(ctx, raw)
	if err != nil {
		if object, _, ok := router.ToolFailure(err); ok {
			return failureResult(object), nil
		}
		return nil, fmt.Errorf("avenor message: %w", err)
	}
	return result.Output(), nil
}
