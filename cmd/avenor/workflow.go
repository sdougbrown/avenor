package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/sdougbrown/avenor/client"
)

func runWorkflow(args []string) int {
	return runWorkflowTo(args, os.Stdout, os.Stderr)
}

// runWorkflowTo is the testable form of runWorkflow. It accepts --socket
// before or after the subcommand.
func runWorkflowTo(args []string, stdout, stderr io.Writer) int {
	socket := ""
	var rest []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--socket":
			if i+1 >= len(args) {
				fmt.Fprintln(stderr, "avenor workflow: --socket requires a value")
				return 1
			}
			socket = args[i+1]
			i++
		case len(arg) > len("--socket=") && arg[:len("--socket=")] == "--socket=":
			socket = arg[len("--socket="):]
		default:
			rest = append(rest, arg)
		}
	}
	if socket == "" {
		fmt.Fprintln(stderr, "avenor workflow: --socket is required")
		return 1
	}
	if len(rest) == 0 {
		fmt.Fprintln(stderr, "avenor workflow: command required (create, instantiate, status, wait, inspect, events)")
		return 1
	}

	c, err := client.Dial(socket)
	if err != nil {
		fmt.Fprintf(stderr, "avenor workflow: %v\n", err)
		return 1
	}
	defer c.Close()

	sub := rest[0]
	subArgs := rest[1:]
	switch sub {
	case "create":
		return cmdWorkflowCreate(c, subArgs, stdout, stderr)
	case "instantiate":
		return cmdWorkflowInstantiate(c, subArgs, stdout, stderr)
	case "status":
		return cmdWorkflowStatus(c, subArgs, stdout, stderr)
	case "wait":
		return cmdWorkflowWait(c, subArgs, stdout, stderr)
	case "inspect":
		return cmdWorkflowInspect(c, subArgs, stdout, stderr)
	case "events":
		return cmdWorkflowEvents(c, subArgs, stdout, stderr)
	default:
		fmt.Fprintln(stderr, "avenor workflow: unknown command", sub)
		return 1
	}
}

func printWorkflowResult(stdout io.Writer, result map[string]any) {
	data, _ := json.MarshalIndent(result, "", "  ")
	fmt.Fprintln(stdout, string(data))
}

// workflowArgID requires exactly one positional workflow ID after flag
// parsing and returns it.
func workflowArgID(name string, fs *flag.FlagSet, stderr io.Writer) (string, int) {
	args := fs.Args()
	if len(args) != 1 || args[0] == "" {
		fmt.Fprintf(stderr, "usage: avenor workflow %s --socket SOCKET <workflow-id>\n", name)
		return "", 1
	}
	return args[0], 0
}

// splitIDArgs separates a single positional workflow ID from the named flag
// arguments (each may appear as "--flag value" or "--flag=value"), so the ID
// may come before or after flags.
func splitIDArgs(name string, args []string, flagNames []string, stderr io.Writer) ([]string, string, int) {
	var flagArgs []string
	id := ""
	for i := 0; i < len(args); i++ {
		arg := args[i]
		matched := false
		for _, f := range flagNames {
			if arg == "--"+f {
				if i+1 >= len(args) {
					fmt.Fprintf(stderr, "avenor workflow: %s: --%s requires a value\n", name, f)
					return nil, "", 1
				}
				flagArgs = append(flagArgs, arg, args[i+1])
				i++
				matched = true
				break
			}
			if strings.HasPrefix(arg, "--"+f+"=") {
				flagArgs = append(flagArgs, arg)
				matched = true
				break
			}
		}
		if matched {
			continue
		}
		if strings.HasPrefix(arg, "--") {
			// Unknown flag: pass through for the FlagSet to reject.
			flagArgs = append(flagArgs, arg)
			continue
		}
		if id != "" {
			fmt.Fprintf(stderr, "avenor workflow: %s: expected exactly one <workflow-id>\n", name)
			return nil, "", 1
		}
		id = arg
	}
	return flagArgs, id, 0
}

func cmdWorkflowCreate(c *client.Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("create", flag.ContinueOnError)
	fs.SetOutput(stderr)
	requestFile := fs.String("request-file", "", "path to template JSON file (required)")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if *requestFile == "" {
		fmt.Fprintln(stderr, "avenor workflow: create: --request-file is required")
		return 1
	}
	data, err := os.ReadFile(*requestFile)
	if err != nil {
		fmt.Fprintf(stderr, "avenor workflow: create: %v\n", err)
		return 1
	}
	result, err := c.WorkflowCreate(json.RawMessage(data))
	if err != nil {
		fmt.Fprintf(stderr, "avenor workflow: create: %v\n", err)
		return 1
	}
	printWorkflowResult(stdout, result)
	return 0
}

func cmdWorkflowInstantiate(c *client.Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("instantiate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	templateID := fs.String("template-id", "", "template id (required)")
	templateVersion := fs.String("template-version", "", "template version (required)")
	requestFile := fs.String("request-file", "", "path to instance JSON file (required; {\"metadata\":{...}} or {})")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if *templateID == "" || *templateVersion == "" || *requestFile == "" {
		fmt.Fprintln(stderr, "avenor workflow: instantiate: --template-id, --template-version, and --request-file are required")
		return 1
	}
	var instance struct {
		Metadata map[string]any `json:"metadata,omitempty"`
	}
	if data, err := os.ReadFile(*requestFile); err == nil {
		_ = json.Unmarshal(data, &instance)
	}
	var metadata map[string]any
	if len(instance.Metadata) > 0 {
		metadata = instance.Metadata
	}
	result, err := c.WorkflowInstantiate(*templateID, *templateVersion, metadata)
	if err != nil {
		fmt.Fprintf(stderr, "avenor workflow: instantiate: %v\n", err)
		return 1
	}
	printWorkflowResult(stdout, result)
	return 0
}

func cmdWorkflowStatus(c *client.Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Parse(args)
	id, code := workflowArgID("status", fs, stderr)
	if code != 0 {
		return code
	}
	result, err := c.WorkflowStatus(id)
	if err != nil {
		fmt.Fprintf(stderr, "avenor workflow: status: %v\n", err)
		return 1
	}
	printWorkflowResult(stdout, result)
	return 0
}

func cmdWorkflowWait(c *client.Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("wait", flag.ContinueOnError)
	fs.SetOutput(stderr)
	timeout := fs.String("timeout", "30s", "wall-clock timeout for the wait")
	flagArgs, id, code := splitIDArgs("wait", args, []string{"timeout"}, stderr)
	if code != 0 {
		return code
	}
	if err := fs.Parse(flagArgs); err != nil {
		return 1
	}
	if id == "" {
		fmt.Fprintln(stderr, "usage: avenor workflow wait --socket SOCKET <workflow-id> [--timeout duration]")
		return 1
	}
	dur, err := time.ParseDuration(*timeout)
	if err != nil || dur < 0 {
		fmt.Fprintf(stderr, "avenor workflow: wait: invalid --timeout %q\n", *timeout)
		return 1
	}
	result, err := c.WorkflowWait(id, dur)
	if err != nil {
		fmt.Fprintf(stderr, "avenor workflow: wait: %v\n", err)
		return 1
	}
	printWorkflowResult(stdout, result)
	return 0
}

func cmdWorkflowInspect(c *client.Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("inspect", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Parse(args)
	id, code := workflowArgID("inspect", fs, stderr)
	if code != 0 {
		return code
	}
	result, err := c.WorkflowInspect(id)
	if err != nil {
		fmt.Fprintf(stderr, "avenor workflow: inspect: %v\n", err)
		return 1
	}
	printWorkflowResult(stdout, result)
	return 0
}

func cmdWorkflowEvents(c *client.Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("events", flag.ContinueOnError)
	fs.SetOutput(stderr)
	afterSeq := fs.Int64("after-seq", 0, "only events with sequence greater than this value")
	limit := fs.Int("limit", 0, "maximum number of events (0 for server default)")
	flagArgs, id, code := splitIDArgs("events", args, []string{"after-seq", "limit"}, stderr)
	if code != 0 {
		return code
	}
	if err := fs.Parse(flagArgs); err != nil {
		return 1
	}
	if id == "" {
		fmt.Fprintln(stderr, "usage: avenor workflow events --socket SOCKET <workflow-id> [--after-seq n] [--limit n]")
		return 1
	}
	result, err := c.WorkflowEvents(id, *afterSeq, *limit)
	if err != nil {
		fmt.Fprintf(stderr, "avenor workflow: events: %v\n", err)
		return 1
	}
	printWorkflowResult(stdout, result)
	return 0
}
