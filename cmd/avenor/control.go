package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/runtime"
)

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

	c, err := client.Dial(*socketFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "avenor control: %v\n", err)
		return 1
	}
	defer c.Close()

	switch nonFlag[0] {
	case "status":
		return cmdStatus(c, nonFlag[1:])
	case "list":
		return cmdList(c)
	case "tail":
		return cmdTail(c)
	case "spawn":
		return cmdSpawn(c, nonFlag[1:])
	case "cancel":
		return cmdCancel(c, nonFlag[1:])
	case "prompt":
		return cmdPrompt(c, nonFlag[1:])
	case "interrupt-and-prompt":
		return cmdInterruptAndPrompt(c, nonFlag[1:])
	case "answer-permission":
		return cmdAnswerPermission(c, nonFlag[1:])
	case "shutdown":
		return cmdShutdown(c, nonFlag[1:])
	default:
		fmt.Fprintf(os.Stderr, "avenor control: unknown command %q\n", nonFlag[0])
		return 1
	}
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
	if err := c.Call("subscribe", nil, nil); err != nil {
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
	serverURL := fs.String("server-url", "", "long-lived ACP server endpoint")
	onEvent := fs.String("on-event", "", "path to write NDJSON events")
	sentinelFile := fs.String("sentinel-file", "", "path to write a completion sentinel")
	permissionHandler := fs.String("permission-handler", "", "permission handler, supports file:<path>")
	autoApprove := fs.Bool("auto-approve", false, "automatically approve all permission requests")
	timeout := fs.Int("timeout", 0, "overall session timeout in seconds")
	maxRetries := fs.Int("max-retries", 0, "maximum retry attempts on transient failure")
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
	addString("server_url", *serverURL)
	addString("on_event", *onEvent)
	addString("sentinel_file", *sentinelFile)
	addString("permission_handler", *permissionHandler)
	if *autoApprove {
		params["auto_approve"] = true
	}
	if *timeout > 0 {
		params["timeout"] = *timeout
	}
	if *maxRetries > 0 {
		params["max_retries"] = *maxRetries
	}
	return params, 0
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
