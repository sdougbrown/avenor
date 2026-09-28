package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/sdougbrown/avenor/client"
)

// cmdWorkflowController dispatches the workflow-controller subcommands.
func cmdWorkflowController(c *client.Client, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "avenor workflow controller: subcommand required (create, enable, disable, status, list)")
		return 1
	}
	sub := args[0]
	subArgs := args[1:]
	switch sub {
	case "create":
		return cmdWorkflowControllerCreate(c, subArgs, stdout, stderr)
	case "enable":
		return cmdWorkflowControllerEnable(c, subArgs, stdout, stderr)
	case "disable":
		return cmdWorkflowControllerDisable(c, subArgs, stdout, stderr)
	case "status":
		return cmdWorkflowControllerStatus(c, subArgs, stdout, stderr)
	case "list":
		return cmdWorkflowControllerList(c, subArgs, stdout, stderr)
	default:
		fmt.Fprintln(stderr, "avenor workflow controller: unknown subcommand", sub)
		return 1
	}
}

func cmdWorkflowControllerCreate(c *client.Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("controller create", flag.ContinueOnError)
	fs.SetOutput(stderr)
	requestFile := fs.String("request-file", "", "path to controller JSON file (required)")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if *requestFile == "" {
		fmt.Fprintln(stderr, "avenor workflow controller create: --request-file is required")
		return 1
	}
	data, err := os.ReadFile(*requestFile)
	if err != nil {
		fmt.Fprintf(stderr, "avenor workflow controller create: %v\n", err)
		return 1
	}
	result, err := c.WorkflowControllerCreate(json.RawMessage(data))
	if err != nil {
		fmt.Fprintf(stderr, "avenor workflow controller create: %v\n", err)
		return 1
	}
	printWorkflowResult(stdout, result)
	return 0
}

func cmdWorkflowControllerEnable(c *client.Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("controller enable", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if err := fs.Parse(args); err != nil {
		return 1
	}
	id, code := workflowArgID("controller enable", fs, stderr)
	if code != 0 {
		return code
	}
	result, err := c.WorkflowControllerEnable(id)
	if err != nil {
		fmt.Fprintf(stderr, "avenor workflow controller enable: %v\n", err)
		return 1
	}
	printWorkflowResult(stdout, result)
	return 0
}

func cmdWorkflowControllerDisable(c *client.Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("controller disable", flag.ContinueOnError)
	fs.SetOutput(stderr)
	reason := fs.String("reason", "", "reason for disabling (required)")
	flagArgs, id, code := splitIDArgs("controller disable", args, []string{"reason"}, stderr)
	if code != 0 {
		return code
	}
	if err := fs.Parse(flagArgs); err != nil {
		return 1
	}
	if id == "" {
		fmt.Fprintln(stderr, "usage: avenor workflow controller disable --socket SOCKET <controller-id> --reason TEXT")
		return 1
	}
	if *reason == "" {
		fmt.Fprintln(stderr, "avenor workflow controller disable: --reason is required")
		return 1
	}
	result, err := c.WorkflowControllerDisable(id, *reason)
	if err != nil {
		fmt.Fprintf(stderr, "avenor workflow controller disable: %v\n", err)
		return 1
	}
	printWorkflowResult(stdout, result)
	return 0
}

func cmdWorkflowControllerStatus(c *client.Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("controller status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if err := fs.Parse(args); err != nil {
		return 1
	}
	id, code := workflowArgID("controller status", fs, stderr)
	if code != 0 {
		return code
	}
	result, err := c.WorkflowControllerStatus(id)
	if err != nil {
		fmt.Fprintf(stderr, "avenor workflow controller status: %v\n", err)
		return 1
	}
	printWorkflowResult(stdout, result)
	return 0
}

func cmdWorkflowControllerList(c *client.Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("controller list", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if err := fs.Parse(args); err != nil {
		return 1
	}
	result, err := c.WorkflowControllerList()
	if err != nil {
		fmt.Fprintf(stderr, "avenor workflow controller list: %v\n", err)
		return 1
	}
	printWorkflowResult(stdout, result)
	return 0
}

func cmdWorkflowReady(c *client.Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("ready", flag.ContinueOnError)
	fs.SetOutput(stderr)
	limit := fs.Int("limit", 0, "maximum number of ready items (0 for server default)")
	flagArgs, id, code := splitIDArgs("ready", args, []string{"limit"}, stderr)
	if code != 0 {
		return code
	}
	if err := fs.Parse(flagArgs); err != nil {
		return 1
	}
	if id == "" {
		fmt.Fprintln(stderr, "usage: avenor workflow ready --socket SOCKET <controller-id> [--limit N]")
		return 1
	}
	result, err := c.WorkflowReady(id, *limit)
	if err != nil {
		fmt.Fprintf(stderr, "avenor workflow ready: %v\n", err)
		return 1
	}
	printWorkflowResult(stdout, result)
	return 0
}
