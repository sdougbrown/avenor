package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/spawnselection"
)

// controlVerb is one control subcommand and the connection it needs: the
// read-only verbs dial anonymously so they work against a live socket with no
// published owner record, and the gated verbs attach as the socket's owner.
type controlVerb struct {
	dial func(string) (*client.Client, error)
	run  func(*client.Client, []string) int
}

var controlVerbs = map[string]controlVerb{
	"status":               {client.Dial, cmdStatus},
	"list":                 {client.Dial, func(c *client.Client, _ []string) int { return cmdList(c) }},
	"tail":                 {client.Dial, func(c *client.Client, _ []string) int { return cmdTail(c) }},
	"spawn":                {client.AttachOwner, cmdSpawn},
	"cancel":               {client.AttachOwner, cmdCancel},
	"prompt":               {client.AttachOwner, cmdPrompt},
	"interrupt-and-prompt": {client.AttachOwner, cmdInterruptAndPrompt},
	"answer-permission":    {client.AttachOwner, cmdAnswerPermission},
	"shutdown":             {client.AttachOwner, cmdShutdown},
}

func runControl(args []string) int {
	fs := flag.NewFlagSet("control", flag.ContinueOnError)
	socketFile := fs.String("socket", "", "control socket path (required)")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if *socketFile == "" {
		fmt.Fprintln(os.Stderr, "avenor control: --socket is required")
		return 1
	}
	nonFlag := fs.Args()
	if len(nonFlag) == 0 {
		fmt.Fprintln(os.Stderr, "avenor control: command required (status, list, tail, spawn, cancel, prompt, interrupt-and-prompt, answer-permission, shutdown)")
		return 1
	}
	verb, ok := controlVerbs[nonFlag[0]]
	if !ok {
		fmt.Fprintf(os.Stderr, "avenor control: unknown command %q\n", nonFlag[0])
		return 1
	}

	c, err := verb.dial(*socketFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "avenor control: %v\n", err)
		return 1
	}
	defer c.Close()
	return verb.run(c, nonFlag[1:])
}

func cmdStatus(c *client.Client, args []string) int {
	rtID := ""
	if len(args) > 0 {
		rtID = args[0]
	}
	snap, err := c.Status(rtID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "avenor control: status: %v\n", err)
		return 1
	}
	data, _ := json.MarshalIndent(snap, "", "  ")
	fmt.Println(string(data))
	return 0
}

func cmdList(c *client.Client) int {
	runtimes, err := c.List()
	if err != nil {
		fmt.Fprintf(os.Stderr, "avenor control: list: %v\n", err)
		return 1
	}
	data, _ := json.MarshalIndent(runtimes, "", "  ")
	fmt.Println(string(data))
	return 0
}

func cmdTail(c *client.Client) int {
	// A subscribe the server refuses because it is shutting down ends the stream
	// the caller asked to tail, since no event can follow. Any other failure,
	// including a connection that drops before the answer, is reported.
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

func cmdSpawn(c *client.Client, args []string) int {
	params, code := spawnParamsFromArgs(args, os.Stderr)
	if code != 0 {
		return code
	}
	result, err := c.Spawn(params)
	if err != nil {
		fmt.Fprintf(os.Stderr, "avenor control: spawn: %v\n", err)
		return 1
	}
	data, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(data))
	return 0
}

func spawnParamsFromArgs(args []string, stderr io.Writer) (map[string]any, int) {
	fs := flag.NewFlagSet("spawn", flag.ContinueOnError)
	fs.SetOutput(stderr)
	prompt := fs.String("prompt", "", "inline prompt text")
	promptFile := fs.String("prompt-file", "", "path to prompt file")
	dir := fs.String("dir", "", "working directory for the runtime")
	agent := fs.String("agent", "", "agent name")
	label := fs.String("label", "", "free-form label for log correlation")
	model := fs.String("model", "", "backend-specific model id")
	thinking := fs.String("thinking", "", "thinking level (off, minimal, low, medium, high, xhigh, max)")
	backend := fs.String("backend", "", "runtime backend")
	rosterFile := fs.String("roster-file", "", "path to the roster map")
	rosterEntry := fs.String("roster-entry", "", "roster entry key")
	serverURL := fs.String("server-url", "", "long-lived ACP server endpoint")
	onEvent := fs.String("on-event", "", "path to write NDJSON events")
	sentinelFile := fs.String("sentinel-file", "", "path to write a completion sentinel")
	permissionHandler := fs.String("permission-handler", "", "permission handler, supports file:<path>")
	autoApprove := fs.Bool("auto-approve", false, "automatically approve all permission requests")
	persistSessions := fs.Bool("persist-sessions", false, "persist the backend session (pi: a session directory instead of --no-session)")
	timeout := fs.Int("timeout", 0, "overall session timeout in seconds")
	maxRetries := fs.Int("max-retries", 0, "maximum retry attempts on transient failure")
	var launchWrapper launchWrapperFlag
	fs.Var(&launchWrapper, "launch-wrapper", "opaque argv prefix in front of the backend command; repeatable, each use adds one element")
	if err := fs.Parse(args); err != nil {
		return nil, 1
	}
	if *prompt != "" && *promptFile != "" {
		fmt.Fprintln(stderr, "avenor control: spawn: --prompt and --prompt-file are mutually exclusive")
		return nil, 1
	}
	if *prompt == "" && *promptFile == "" {
		fmt.Fprintln(stderr, "avenor control: spawn: --prompt or --prompt-file is required")
		return nil, 1
	}
	if err := runtime.ValidateThinking(*thinking); err != nil {
		fmt.Fprintf(stderr, "avenor control: spawn: %v\n", err)
		return nil, 1
	}
	if err := spawnselection.Validate(spawnselection.Input{
		Agent:       *agent,
		Model:       *model,
		Backend:     *backend,
		RosterFile:  *rosterFile,
		RosterEntry: *rosterEntry,
	}, false); err != nil {
		fmt.Fprintf(stderr, "avenor control: spawn: %v\n", err)
		return nil, 1
	}
	params := map[string]any{}
	addString := func(key, value string) {
		if value != "" {
			params[key] = value
		}
	}
	addString("prompt", *prompt)
	addString("prompt_file", *promptFile)
	addString("dir", *dir)
	addString("agent", *agent)
	addString("label", *label)
	addString("model", *model)
	addString("thinking", *thinking)
	addString("backend", *backend)
	addString("roster_file", *rosterFile)
	addString("roster_entry", *rosterEntry)
	addString("server_url", *serverURL)
	addString("on_event", *onEvent)
	addString("sentinel_file", *sentinelFile)
	addString("permission_handler", *permissionHandler)
	if *autoApprove {
		params["auto_approve"] = true
	}
	if *persistSessions {
		params["persist_sessions"] = true
	}
	if len(launchWrapper) > 0 {
		params["launch_wrapper"] = []string(launchWrapper)
	}
	if *timeout > 0 {
		params["timeout"] = *timeout
	}
	if *maxRetries > 0 {
		params["max_retries"] = *maxRetries
	}
	return params, 0
}

type launchWrapperFlag []string

func (w *launchWrapperFlag) String() string { return strings.Join(*w, " ") }

func (w *launchWrapperFlag) Set(value string) error {
	*w = append(*w, value)
	return nil
}

func cmdCancel(c *client.Client, args []string) int {
	rtID := ""
	if len(args) > 0 {
		rtID = args[0]
	}
	if err := c.Cancel(rtID); err != nil {
		fmt.Fprintf(os.Stderr, "avenor control: cancel: %v\n", err)
		return 1
	}
	fmt.Println("cancelled")
	return 0
}

func cmdPrompt(c *client.Client, args []string) int {
	rtID := ""
	text := ""
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "avenor control: prompt <text> [runtime-id]")
		return 1
	}
	if len(args) >= 2 {
		rtID = args[1]
	}
	text = args[0]
	if err := c.Prompt(rtID, text); err != nil {
		fmt.Fprintf(os.Stderr, "avenor control: prompt: %v\n", err)
		return 1
	}
	fmt.Println("prompt queued")
	return 0
}

func cmdInterruptAndPrompt(c *client.Client, args []string) int {
	rtID := ""
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "avenor control: interrupt-and-prompt <text> [runtime-id]")
		return 1
	}
	if len(args) >= 2 {
		rtID = args[1]
	}
	if err := c.InterruptAndPrompt(rtID, args[0], false); err != nil {
		fmt.Fprintf(os.Stderr, "avenor control: interrupt-and-prompt: %v\n", err)
		return 1
	}
	fmt.Println("interrupt queued")
	return 0
}

func cmdAnswerPermission(c *client.Client, args []string) int {
	rtID := ""
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "avenor control: answer-permission <request-id> <option-id> [runtime-id]")
		return 1
	}
	if len(args) >= 3 {
		rtID = args[2]
	}
	if err := c.AnswerPermission(rtID, args[0], args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "avenor control: answer-permission: %v\n", err)
		return 1
	}
	fmt.Println("permission answered")
	return 0
}

func cmdShutdown(c *client.Client, args []string) int {
	mode := "graceful"
	if len(args) > 0 {
		mode = args[0]
	}
	if err := c.Shutdown(mode); err != nil {
		fmt.Fprintf(os.Stderr, "avenor control: shutdown: %v\n", err)
		return 1
	}
	fmt.Println("shutting down")
	return 0
}
