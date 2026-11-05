package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/spawnselection"
	clicontract "github.com/sdougbrown/cli-contract"
)

// controlClient dials the control socket named by the persistent --socket
// flag; owner verbs attach the parent-owner credential.
func controlClient(inv clicontract.Invocation, owner bool) (*client.Client, error) {
	socket := inv.Flags["socket"]
	if socket == "" {
		return nil, usageErrorf("avenor control: --socket is required")
	}
	if owner {
		return client.AttachOwner(socket)
	}
	return client.Dial(socket)
}

// handleControlStatus prints one runtime's snapshot, or every runtime's.
func handleControlStatus(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	c, err := controlClient(inv, false)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	rtID := ""
	if len(inv.Args) > 0 {
		rtID = inv.Args[0]
	}
	snap, err := c.Status(rtID)
	if err != nil {
		return nil, fmt.Errorf("avenor control: status: %w", err)
	}
	return snap, nil
}

// handleControlList prints every runtime on the socket.
func handleControlList(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	c, err := controlClient(inv, false)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	runtimes, err := c.List()
	if err != nil {
		return nil, fmt.Errorf("avenor control: list: %w", err)
	}
	return runtimes, nil
}

// handleControlTail streams events until the server stops them.
func handleControlTail(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	c, err := controlClient(inv, false)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	if code := controlTail(c); code != 0 {
		return exitCode(code), nil
	}
	return nil, nil
}

// handleControlSpawn spawns one runtime through the owner socket.
func handleControlSpawn(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	c, err := controlClient(inv, true)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	params, err := spawnParamsFromInvocation(inv)
	if err != nil {
		return nil, err
	}
	result, err := c.Spawn(params)
	if err != nil {
		return nil, fmt.Errorf("avenor control: spawn: %w", err)
	}
	return result, nil
}

// handleControlCancel cancels one runtime (or the whole session).
func handleControlCancel(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	c, err := controlClient(inv, true)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	rtID := ""
	if len(inv.Args) > 0 {
		rtID = inv.Args[0]
	}
	if err := c.Cancel(rtID); err != nil {
		return nil, fmt.Errorf("avenor control: cancel: %w", err)
	}
	fmt.Println("cancelled")
	return nil, nil
}

// handleControlPrompt queues one prompt for one runtime.
func handleControlPrompt(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	c, err := controlClient(inv, true)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	rtID := ""
	if len(inv.Args) >= 2 {
		rtID = inv.Args[1]
	}
	if err := c.Prompt(rtID, inv.Args[0]); err != nil {
		return nil, fmt.Errorf("avenor control: prompt: %w", err)
	}
	fmt.Println("prompt queued")
	return nil, nil
}

// handleControlInterruptAndPrompt interrupts and queues one prompt.
func handleControlInterruptAndPrompt(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	c, err := controlClient(inv, true)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	rtID := ""
	if len(inv.Args) >= 2 {
		rtID = inv.Args[1]
	}
	if err := c.InterruptAndPrompt(rtID, inv.Args[0], false); err != nil {
		return nil, fmt.Errorf("avenor control: interrupt-and-prompt: %w", err)
	}
	fmt.Println("interrupt queued")
	return nil, nil
}

// handleControlAnswerPermission answers one pending permission request.
func handleControlAnswerPermission(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	c, err := controlClient(inv, true)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	rtID := ""
	if len(inv.Args) >= 3 {
		rtID = inv.Args[2]
	}
	if err := c.AnswerPermission(rtID, inv.Args[0], inv.Args[1]); err != nil {
		return nil, fmt.Errorf("avenor control: answer-permission: %w", err)
	}
	fmt.Println("permission answered")
	return nil, nil
}

// handleControlShutdown stops the supervisor: graceful by default.
func handleControlShutdown(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	c, err := controlClient(inv, true)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	mode := "graceful"
	if len(inv.Args) > 0 {
		mode = inv.Args[0]
	}
	if err := c.Shutdown(mode); err != nil {
		return nil, fmt.Errorf("avenor control: shutdown: %w", err)
	}
	fmt.Println("shutting down")
	return nil, nil
}

// spawnParamsFromInvocation validates the spawn flags and builds the spawn
// request: the prompt disjunction, thinking level, and selector rules run
// before any socket traffic.
func spawnParamsFromInvocation(inv clicontract.Invocation) (map[string]any, error) {
	prompt := inv.Flags["prompt"]
	promptFile := inv.Flags["prompt-file"]
	thinking := inv.Flags["thinking"]

	if prompt != "" && promptFile != "" {
		return nil, usageErrorf("avenor control: spawn: --prompt and --prompt-file are mutually exclusive")
	}
	if prompt == "" && promptFile == "" {
		return nil, usageErrorf("avenor control: spawn: --prompt or --prompt-file is required")
	}
	if err := runtime.ValidateThinking(thinking); err != nil {
		return nil, usageErrorf("avenor control: spawn: %v", err)
	}
	if err := spawnselection.Validate(spawnselection.Input{
		Agent:       inv.Flags["agent"],
		Model:       inv.Flags["model"],
		Backend:     inv.Flags["backend"],
		RosterFile:  inv.Flags["roster-file"],
		RosterEntry: inv.Flags["roster-entry"],
	}, false); err != nil {
		return nil, usageErrorf("avenor control: spawn: %v", err)
	}
	params := map[string]any{}
	addString := func(key, value string) {
		if value != "" {
			params[key] = value
		}
	}
	addString("prompt", prompt)
	addString("prompt_file", promptFile)
	addString("dir", inv.Flags["dir"])
	addString("agent", inv.Flags["agent"])
	addString("label", inv.Flags["label"])
	addString("model", inv.Flags["model"])
	addString("thinking", thinking)
	addString("backend", inv.Flags["backend"])
	addString("roster_file", inv.Flags["roster-file"])
	addString("roster_entry", inv.Flags["roster-entry"])
	addString("server_url", inv.Flags["server-url"])
	addString("on_event", inv.Flags["on-event"])
	addString("sentinel_file", inv.Flags["sentinel-file"])
	addString("permission_handler", inv.Flags["permission-handler"])
	if inv.Bools["auto-approve"] {
		params["auto_approve"] = true
	}
	if inv.Supplied["persist-sessions"] {
		params["persist_sessions"] = inv.Bools["persist-sessions"]
	}
	if inv.Supplied["messaging"] {
		params["messaging"] = inv.Bools["messaging"]
	}
	if launchWrapper := launchWrapperFlag(inv.Events); len(launchWrapper) > 0 {
		params["launch_wrapper"] = []string(launchWrapper)
	}
	if timeout := inv.Ints["timeout"]; timeout > 0 {
		params["timeout"] = timeout
	}
	if maxRetries := inv.Ints["max-retries"]; maxRetries > 0 {
		params["max_retries"] = maxRetries
	}
	return params, nil
}

// launchWrapperFlag collects the repeatable --launch-wrapper argv prefix.
type launchWrapperFlag []string

// controlTail is the test seam for handleControlTail: it tails an already
// dialed client and returns the exit-code style result the tests assert on.
func controlTail(c *client.Client) int {
	if err := c.Subscribe(nil); err != nil && !errors.Is(err, client.ErrControlStopped) {
		fmt.Fprintf(os.Stderr, "avenor control: subscribe: %v\n", err)
		return 1
	}
	for ev := range c.Events() {
		data, _ := json.Marshal(ev.Raw)
		fmt.Println(string(data))
	}
	return 0
}
