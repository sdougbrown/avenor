package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/supervisor"
	clicontract "github.com/sdougbrown/cli-contract"
)

// The environment a configured host session names its Avenor attachment in.
// Every value is static configuration — an optional supervisor socket, the native
// host and the native session id — so a plain host session reaches its own
// supervisor without a credential, a wrapper or a broker of its own. Only the
// session id is dynamic, and the host resolves it from its own session, never
// from a model argument.
const (
	envControlSocket = "AVENOR_SOCKET"
	envBindHost      = "AVENOR_BIND_HOST"
	envBindSession   = "AVENOR_BIND_SESSION"
)

// conversationShutdownTimeout bounds a conversation supervisor's shutdown so it
// finishes inside the grace a host gives the feed it is stopping. The feed is a
// poo feed, and poo sends SIGTERM and then kills the process group two seconds
// later; a shutdown still running then would be killed and could leave the
// supervisor's children behind.
const conversationShutdownTimeout = time.Second

// startConversationSupervisor is the one place a conversation's supervisor is
// started. Tests replace it to observe the launch without a real binary.
var startConversationSupervisor = supervisor.Start

// startOwnedConversationSupervisor starts the supervisor a derived conversation
// socket names, or attaches to the one already running, and returns the
// lifecycle the feed holds until it stops serving.
func startOwnedConversationSupervisor(coords boundCoordinates) (*supervisor.Lifecycle, error) {
	return startConversationSupervisor(coords.socket, 0, "", supervisor.Options{
		ShutdownTimeout:      conversationShutdownTimeout,
		ShutdownOnParentExit: true,
	})
}

// bindCoordinateFlags are the three flags every bound command carries: which
// supervisor the call speaks to, and which native conversation it speaks for.
// They are configuration, so a registered manifest fixes them in argv and its
// env map, and no model-facing argument can choose them.
func bindCoordinateFlags() []clicontract.Flag {
	return []clicontract.Flag{
		{Name: "socket", Kind: clicontract.String, Usage: "control socket of the configured supervisor (default: $" + envControlSocket + "; derived from the bind coordinates when unset)"},
		{Name: "bind-host", Kind: clicontract.String, Usage: "native host of the configured conversation (default: $" + envBindHost + ")"},
		{Name: "bind-session", Kind: clicontract.String, Usage: "native session id of the configured conversation (default: $" + envBindSession + ")"},
	}
}

// boundCoordinates are the coordinates of one configured conversation binding.
type boundCoordinates struct {
	socket  string
	host    string
	session string
	// derived reports that socket is this conversation's own derived supervisor
	// socket, not an operator-configured one. Only the feed may start what a
	// derived socket names; a configured socket is attach-only.
	derived bool
}

// boundCoordinatesFrom reads the coordinates from a command's flags and their
// environment defaults. requested reports whether the caller asked for a binding
// at all: a command that names no coordinate keeps whatever path it had.
func boundCoordinatesFrom(inv clicontract.Invocation) (coords boundCoordinates, requested bool) {
	coords = boundCoordinates{
		socket:  orDefault(inv.Flags["socket"], os.Getenv(envControlSocket)),
		host:    orDefault(inv.Flags["bind-host"], os.Getenv(envBindHost)),
		session: orDefault(inv.Flags["bind-session"], os.Getenv(envBindSession)),
	}
	return coords, coords.socket != "" || coords.host != "" || coords.session != ""
}

// complete resolves the coordinates into one attachment. A configured socket is
// used as given. A conversation that names no socket gets its own derived one, so
// a session needs no supervisor configuration at all; half a conversation names
// no session to serve, and guessing the rest would attach the call to a session
// the operator did not configure.
func (c *boundCoordinates) complete() error {
	if c.host == "" || c.session == "" {
		return usageErrorf("--bind-host and --bind-session (or %s and %s) are required together; --socket (or %s) names a supervisor without a conversation", envBindHost, envBindSession, envControlSocket)
	}
	if c.socket != "" {
		return nil
	}
	socket, err := supervisor.ConversationSocket(c.host, c.session)
	if err != nil {
		return err
	}
	c.socket, c.derived = socket, true
	return nil
}

// attachBoundRead dials the configured supervisor as its owner and reads the
// conversation's live binding. The read never binds: a conversation whose feed
// has not attached yet, or whose bound run is gone, is refused here instead of
// minted behind the inbox that would have to read it.
func attachBoundRead(ctx context.Context, coords boundCoordinates) (*client.Client, messaging.MessageBindResult, error) {
	c, err := client.AttachOwner(coords.socket)
	if err != nil {
		return nil, messaging.MessageBindResult{}, fmt.Errorf("attach %s%s: %w", coords.attachTarget(), coords.attachHint(), err)
	}
	bound, err := c.MessageBindLookupContext(ctx, messaging.MessageBindRequest{Host: coords.host, SessionID: coords.session})
	if err != nil {
		c.Close()
		return nil, messaging.MessageBindResult{}, err
	}
	return c, bound, nil
}

// attachTarget names the supervisor a call wanted to reach: the conversation's
// own one for a derived socket, the operator's for a configured one.
func (c boundCoordinates) attachTarget() string {
	if c.derived {
		return fmt.Sprintf("the conversation's supervisor at %s", c.socket)
	}
	return fmt.Sprintf("the configured supervisor at %s", c.socket)
}

// attachHint tells a command's reader who starts the supervisor it could not
// reach. A derived socket belongs to the conversation's own supervisor, which only
// the conversation's feed starts: a command reports the missing attachment instead
// of starting one.
func (c boundCoordinates) attachHint() string {
	if c.derived {
		return " (its inbox feed starts it)"
	}
	return ""
}

// spawnCommand declares the model-facing spawn: a child on the configured
// supervisor whose broker parent is this conversation's live binding.
func spawnCommand() *clicontract.Command {
	return &clicontract.Command{
		Name:  "spawn",
		Short: "spawn a child run on this conversation's bound supervisor",
		Flags: append(bindCoordinateFlags(),
			clicontract.Flag{Name: "prompt", Kind: clicontract.String, Usage: "task text for the child"},
			clicontract.Flag{Name: "prompt-stdin", Kind: clicontract.Bool, Usage: "read the task text from stdin to EOF (mutually exclusive with --prompt)"},
			clicontract.Flag{Name: "dir", Kind: clicontract.String, Usage: "working directory for the child", Required: true},
			clicontract.Flag{Name: "agent", Kind: clicontract.String, Usage: "agent name the supervisor's roster resolves"},
			clicontract.Flag{Name: "label", Kind: clicontract.String, Usage: "free-form label for log correlation"},
		),
		InputRules: []*clicontract.Rule{
			clicontract.EnabledOn("prompt", clicontract.Not(clicontract.Truthy("prompt-stdin")), "--prompt and --prompt-stdin are mutually exclusive"),
			clicontract.EnabledOn("dir", clicontract.Truthy("dir"), "--dir must not be empty"),
		},
		Handle: handleBoundSpawn,
	}
}

// handleBoundSpawn reads the conversation's binding, then spawns through the
// gated spawn verb with that binding as the child's broker parent. The launch
// re-checks the parent's liveness, so a binding retired between the read and the
// launch is refused rather than adopted into a record no inbox reads.
func handleBoundSpawn(ctx context.Context, inv clicontract.Invocation, _ io.Writer) (any, error) {
	coords, err := boundCommandCoordinates(inv, "avenor spawn")
	if err != nil {
		return nil, err
	}
	prompt := inv.Flags["prompt"]
	switch {
	case inv.Supplied["prompt-stdin"] && inv.Bools["prompt-stdin"]:
		text, err := readStdinText("prompt stdin", os.Stdin)
		if err != nil {
			return nil, fmt.Errorf("avenor spawn: %w", err)
		}
		if err := validateText("prompt stdin", text); err != nil {
			return nil, fmt.Errorf("avenor spawn: %w", err)
		}
		prompt = text
	case prompt == "":
		return nil, usageErrorf("avenor spawn: --prompt or --prompt-stdin is required")
	default:
		if err := validateText("--prompt", prompt); err != nil {
			return nil, fmt.Errorf("avenor spawn: %w", err)
		}
	}
	c, bound, err := attachBoundRead(ctx, coords)
	if err != nil {
		return nil, fmt.Errorf("avenor spawn: %w", err)
	}
	defer c.Close()
	params := map[string]any{
		"prompt":        prompt,
		"dir":           inv.Flags["dir"],
		"parent_run_id": bound.RunID,
	}
	for _, field := range []struct{ flag, key string }{{"agent", "agent"}, {"label", "label"}} {
		if value := inv.Flags[field.flag]; value != "" {
			params[field.key] = value
		}
	}
	result, err := c.SpawnMCP(params)
	if err != nil {
		return nil, fmt.Errorf("avenor spawn: %w", err)
	}
	return result, nil
}

// statusCommand declares the model-facing status read: one runtime this
// conversation spawned, optionally waiting out its turn first.
func statusCommand() *clicontract.Command {
	return &clicontract.Command{
		Name:  "status",
		Short: "read one run's status, optionally waiting out its turn",
		Flags: append(bindCoordinateFlags(),
			clicontract.Flag{Name: "runtime", Kind: clicontract.String, Usage: "runtime id reported by spawn", Required: true},
			clicontract.Flag{Name: "wait", Kind: clicontract.String, Usage: "wait for turn_complete before reading the status"},
			clicontract.Flag{Name: "timeout-ms", Kind: clicontract.Int, Usage: "bound on the wait in milliseconds", Default: 600000},
		),
		InputRules: nonEmptyRequiredRules("runtime"),
		Handle:     handleBoundStatus,
	}
}

// handleBoundStatus reads the conversation's binding, then the runtime's status.
// A --wait turn_complete wait goes through the supervisor's own bounded wait, so
// the call returns once the child's turn settles instead of polling.
func handleBoundStatus(ctx context.Context, inv clicontract.Invocation, _ io.Writer) (any, error) {
	coords, err := boundCommandCoordinates(inv, "avenor status")
	if err != nil {
		return nil, err
	}
	if wait := inv.Flags["wait"]; wait != "" && wait != "turn_complete" {
		return nil, usageErrorf("avenor status: --wait takes turn_complete")
	}
	c, _, err := attachBoundRead(ctx, coords)
	if err != nil {
		return nil, fmt.Errorf("avenor status: %w", err)
	}
	defer c.Close()
	runtimeID := inv.Flags["runtime"]
	if inv.Flags["wait"] == "turn_complete" {
		timeout := time.Duration(inv.Ints["timeout-ms"]) * time.Millisecond
		if _, err := c.WaitTurn(runtimeID, timeout); err != nil {
			return nil, fmt.Errorf("avenor status: wait for turn_complete on %s: %w", runtimeID, err)
		}
	}
	status, err := c.Status(runtimeID)
	if err != nil {
		return nil, fmt.Errorf("avenor status: %w", err)
	}
	return status, nil
}

// resultCommand declares the model-facing result read: the complete terminal
// reply of one runtime this conversation spawned.
func resultCommand() *clicontract.Command {
	return &clicontract.Command{
		Name:  "result",
		Short: "read one run's complete final reply",
		Flags: append(bindCoordinateFlags(),
			clicontract.Flag{Name: "runtime", Kind: clicontract.String, Usage: "runtime id reported by spawn", Required: true},
		),
		InputRules: nonEmptyRequiredRules("runtime"),
		Handle:     handleBoundResult,
	}
}

// handleBoundResult reads the conversation's binding, then the runtime's result.
func handleBoundResult(ctx context.Context, inv clicontract.Invocation, _ io.Writer) (any, error) {
	coords, err := boundCommandCoordinates(inv, "avenor result")
	if err != nil {
		return nil, err
	}
	c, _, err := attachBoundRead(ctx, coords)
	if err != nil {
		return nil, fmt.Errorf("avenor result: %w", err)
	}
	defer c.Close()
	result, err := c.Result(inv.Flags["runtime"])
	if err != nil {
		return nil, fmt.Errorf("avenor result: %w", err)
	}
	return result, nil
}

// boundCommandCoordinates resolves the coordinates a bound command requires,
// naming the command in the refusals so a model reading stderr knows which call
// was misconfigured.
func boundCommandCoordinates(inv clicontract.Invocation, command string) (boundCoordinates, error) {
	coords, requested := boundCoordinatesFrom(inv)
	if !requested {
		return coords, usageErrorf("%s: --bind-host and --bind-session (or %s and %s) are required", command, envBindHost, envBindSession)
	}
	if err := coords.complete(); err != nil {
		return coords, fmt.Errorf("%s: %w", command, err)
	}
	return coords, nil
}
