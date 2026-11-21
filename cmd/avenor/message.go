package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"unicode/utf8"

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
		Flags: append(bindCoordinateFlags(),
			clicontract.Flag{Name: "broker-url", Kind: clicontract.String, Usage: "broker URL (default: $AVENOR_BROKER_URL)"},
			clicontract.Flag{Name: "run-id", Kind: clicontract.String, Usage: "caller run id (default: $AVENOR_RUN_ID)"},
			clicontract.Flag{Name: "token", Kind: clicontract.String, Usage: "broker token (default: $AVENOR_BROKER_TOKEN; prefer the environment)"},
			clicontract.Flag{Name: "action", Kind: clicontract.String, Usage: "tell, ask, reply or withdraw", Required: true},
			clicontract.Flag{Name: "target", Kind: clicontract.String, Usage: "recipient run id or label"},
			clicontract.Flag{Name: "message", Kind: clicontract.String, Usage: "message text"},
			clicontract.Flag{Name: "message-stdin", Kind: clicontract.Bool, Usage: "read the message text from stdin to EOF (mutually exclusive with --message)"},
			clicontract.Flag{Name: "reply-to", Kind: clicontract.String, Usage: "message id to reply to"},
			clicontract.Flag{Name: "message-id", Kind: clicontract.String, Usage: "message id to withdraw"},
			clicontract.Flag{Name: "timeout-ms", Kind: clicontract.Int, Usage: "wait in milliseconds (maximum 600000)"},
			clicontract.Flag{Name: "asks-dir", Kind: clicontract.String, Usage: "directory of feed ask records"},
		),
		InputRules: []*clicontract.Rule{
			clicontract.EnabledOn("message", clicontract.Not(clicontract.Truthy("message-stdin")), "--message and --message-stdin are mutually exclusive"),
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
	// A command configured with the attachment's coordinates reaches the
	// conversation through its live binding instead of a credential handed to
	// it. Absent credentials alone never select that path: only the configured
	// coordinates do, so an unconfigured call keeps failing as it always has.
	coords, requested := boundCoordinatesFrom(inv)
	if requested {
		if err := coords.complete(); err != nil {
			return nil, fmt.Errorf("avenor message: %w", err)
		}
		if url != "" || token != "" || runID != "" {
			return nil, usageErrorf("avenor message: broker credentials and --bind-host/--bind-session are mutually exclusive")
		}
		c, bound, err := attachBoundRead(ctx, coords)
		if err != nil {
			return nil, fmt.Errorf("avenor message: %w", err)
		}
		defer c.Close()
		url, token, runID = bound.BrokerURL, bound.RunToken, bound.RunID
	}
	if url == "" || token == "" || runID == "" {
		return nil, usageErrorf("avenor message: --broker-url, --token and --run-id (or their environment variables) are required")
	}
	input := map[string]any{"action": inv.Flags["action"]}
	for _, field := range []struct{ flag, key string }{{"target", "target"}, {"reply-to", "reply_to"}, {"message-id", "message_id"}} {
		if inv.Supplied[field.flag] {
			input[field.key] = inv.Flags[field.flag]
		}
	}
	if inv.Supplied["timeout-ms"] {
		input["timeout_ms"] = inv.Ints["timeout-ms"]
	}
	stdinEnabled := inv.Supplied["message-stdin"] && inv.Bools["message-stdin"]
	switch {
	case stdinEnabled:
		text, err := readStdinText("message stdin", os.Stdin)
		if err != nil {
			return nil, fmt.Errorf("avenor message: %w", err)
		}
		if err := validateText("message stdin", text); err != nil {
			return nil, fmt.Errorf("avenor message: %w", err)
		}
		input["message"] = text
	case inv.Supplied["message"]:
		text := inv.Flags["message"]
		if err := validateText("--message", text); err != nil {
			return nil, fmt.Errorf("avenor message: %w", err)
		}
		input["message"] = text
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	// The command is the reply path of the broker feed, so it routes as the feed
	// surface.
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

// messageStdinLimit bounds a --message-stdin read at 1 MiB of raw input for
// bounded memory; reading one byte past it detects oversized input without
// truncating it. The broker separately caps the entire JSON-encoded request
// at 1 MiB, so encoding and metadata can make the effective text limit
// smaller.
const messageStdinLimit = 1 << 20

// readStdinText reads a command's text input to EOF without truncating it. The
// bound is the message body's, which is the largest text a model-facing
// command takes.
func readStdinText(source string, r io.Reader) (string, error) {
	buf, err := io.ReadAll(io.LimitReader(r, messageStdinLimit+1))
	if err != nil {
		return "", err
	}
	if len(buf) > messageStdinLimit {
		return "", fmt.Errorf("%s exceeds 1 MiB", source)
	}
	return string(buf), nil
}

// validateText rejects invalid UTF-8 before the transport sees it. JSON
// encoding would otherwise replace those bytes with the Unicode replacement
// character.
func validateText(source, text string) error {
	if !utf8.ValidString(text) {
		return fmt.Errorf("%s is not valid UTF-8", source)
	}
	return nil
}
