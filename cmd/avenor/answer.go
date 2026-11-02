package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/sdougbrown/avenor/internal/durablefile"
	clicontract "github.com/sdougbrown/cli-contract"
)

// answerOption mirrors the per-entry shape from the options array in a .req file.
type answerOption struct {
	OptionID string `json:"optionId"`
	Kind     string `json:"kind"`
}

// answerRequest is the subset of a .req file we care about for validation.
type answerRequest struct {
	Options []answerOption `json:"options"`
}

// answerResponse is written to <perm-base>.req.response.
type answerResponse struct {
	Outcome  string `json:"outcome"`
	OptionID string `json:"option_id"`
	Message  string `json:"message"`
}

// runAnswer implements the `avenor answer <perm-base>` subcommand.
// It returns 0 on success, 2 on usage/validation errors, 1 on I/O errors.
func runAnswer(args []string) int {
	return runDeclared(append([]string{"answer"}, args...), os.Stdout, os.Stderr)
}

// answerFlags holds the four answer flags.
type answerFlags struct {
	option  string
	message string
	outcome string
	force   bool
}

// handleAnswer writes the response file for one pending permission request;
// both parse passes' semantics collapse into cobra's native interspersed
// flag parsing (last flag wins; duplicate detection is dropped).
func handleAnswer(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	flags := answerFlagsFromInvocation(inv)
	code := answerWrite(inv.Args[0], flags, stderr)
	if code == 0 {
		return nil, nil
	}
	return exitCode(code), nil
}

// runAnswerTo is the testable form of runAnswer.
func runAnswerTo(args []string, errOut io.Writer) int {
	inv, err := parseInvocation(append([]string{"answer"}, args...))
	if err != nil {
		fmt.Fprintf(errOut, "%v\n", err)
		return 2
	}
	flags := answerFlagsFromInvocation(inv)
	if len(inv.Args) != 1 {
		fmt.Fprintln(errOut, "avenor answer: expected exactly one <perm-base>")
		return 2
	}
	return answerWrite(inv.Args[0], flags, errOut)
}

// answerFlagsFromInvocation builds the answer flags from the declared
// invocation: the single source both the production handler and the test
// wrapper read.
func answerFlagsFromInvocation(inv clicontract.Invocation) answerFlags {
	return answerFlags{
		option:  inv.Flags["option"],
		message: inv.Flags["message"],
		outcome: orDefault(inv.Flags["outcome"], "selected"),
		force:   inv.Bools["force"],
	}
}

// answerRun validates and writes the response file, returning the exit code.
func answerWrite(permBase string, flags answerFlags, errOut io.Writer) int {
	// Validate --outcome before anything else.
	if flags.outcome != "selected" && flags.outcome != "cancelled" {
		fmt.Fprintf(errOut, "avenor answer: --outcome must be \"selected\" or \"cancelled\", got %q\n", flags.outcome)
		return 2
	}

	// Validate --option is present.
	if flags.option == "" {
		fmt.Fprintln(errOut, "avenor answer: --option <id> is required")
		return 2
	}

	reqPath := permBase + ".req"
	responsePath := reqPath + ".response"

	// Read and parse the request file.
	reqData, err := os.ReadFile(reqPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// bad path = usage error
			fmt.Fprintf(errOut, "avenor answer: request file %s does not exist\n", reqPath)
			return 2
		}
		// anything else = I/O error
		fmt.Fprintf(errOut, "avenor answer: read %s: %v\n", reqPath, err)
		return 1
	}

	var req answerRequest
	if err := json.Unmarshal(reqData, &req); err != nil {
		fmt.Fprintf(errOut, "avenor answer: parse %s: %v\n", reqPath, err)
		return 2
	}

	// Validate --option against the offered set.
	// Filter out any options with empty optionId values.
	validIDs := make([]string, 0, len(req.Options))
	found := false
	for _, opt := range req.Options {
		if opt.OptionID == "" {
			continue
		}
		validIDs = append(validIDs, opt.OptionID)
		if opt.OptionID == flags.option {
			found = true
		}
	}
	if !found {
		if len(validIDs) == 0 {
			fmt.Fprintf(errOut, "avenor answer: option %q is not in the offered set; no options were offered\n", flags.option)
		} else {
			fmt.Fprintf(errOut, "avenor answer: option %q is not in the offered set; valid options: %s\n",
				flags.option, strings.Join(validIDs, ", "))
		}
		return 2
	}

	// Build response JSON with HTML escaping disabled.
	resp := answerResponse{
		Outcome:  flags.outcome,
		OptionID: flags.option,
		Message:  flags.message,
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(resp); err != nil {
		fmt.Fprintf(errOut, "avenor answer: marshal response: %v\n", err)
		return 1
	}
	// enc.Encode appends a trailing newline; keep it.
	data := buf.Bytes()

	// Write atomically through a uniquely-named temp file: linked into place so
	// an existing response is never replaced, or renamed over it under --force.
	write := durablefile.WriteFileAtomic
	if !flags.force {
		write = durablefile.WriteFileAtomicExclusive
	}
	if err := write(responsePath, durablefile.TempPattern(responsePath), data, 0o600); err != nil {
		var leftover *durablefile.TempNotRemovedError
		if errors.As(err, &leftover) {
			// The response is published; only the staged temp file survived it.
			fmt.Fprintf(errOut, "avenor answer: wrote %s, but could not remove the temp file %s: %v\n",
				responsePath, leftover.Path, leftover.Err)
			return 0
		}
		if !flags.force && errors.Is(err, os.ErrExist) {
			fmt.Fprintf(errOut, "avenor answer: response already exists at %s; pass --force to overwrite\n", responsePath)
			return 2
		}
		fmt.Fprintf(errOut, "avenor answer: write %s: %v\n", responsePath, err)
		return 1
	}
	return 0
}
