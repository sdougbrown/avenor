package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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
	return runAnswerTo(args, os.Stderr)
}

// runAnswerTo is the testable entry point that accepts an io.Writer for
// error output so tests can capture stderr without OS-level redirection.
func runAnswerTo(args []string, errOut io.Writer) int {
	fs := flag.NewFlagSet("avenor answer", flag.ContinueOnError)
	fs.SetOutput(errOut)

	optionFlag := fs.String("option", "", "option id to select (required)")
	messageFlag := fs.String("message", "", "free-text message to include in the response")
	outcomeFlag := fs.String("outcome", "selected", "outcome: selected or cancelled")
	forceFlag := fs.Bool("force", false, "overwrite an existing response file")

	// The flag package stops at the first positional argument, so flags that
	// follow <perm-base> would be left in fs.Args() unprocessed. Parse
	// repeatedly, lifting the positional out, until only flags remain.
	var permBase string
	rest := args
	for {
		if err := fs.Parse(rest); err != nil {
			return 2
		}
		if fs.NArg() == 0 {
			break
		}
		if permBase != "" {
			fmt.Fprintln(errOut, "avenor answer: expected exactly one <perm-base>")
			return 2
		}
		permBase = fs.Arg(0)
		rest = fs.Args()[1:]
	}

	// Validate --outcome before anything else.
	if *outcomeFlag != "selected" && *outcomeFlag != "cancelled" {
		fmt.Fprintf(errOut, "avenor answer: --outcome must be \"selected\" or \"cancelled\", got %q\n", *outcomeFlag)
		return 2
	}

	// Validate --option is present.
	if *optionFlag == "" {
		fmt.Fprintln(errOut, "avenor answer: --option <id> is required")
		return 2
	}

	if permBase == "" {
		fmt.Fprintln(errOut, "avenor answer: <perm-base> is required")
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
		if opt.OptionID == *optionFlag {
			found = true
		}
	}
	if !found {
		if len(validIDs) == 0 {
			fmt.Fprintf(errOut, "avenor answer: option %q is not in the offered set; no options were offered\n", *optionFlag)
		} else {
			fmt.Fprintf(errOut, "avenor answer: option %q is not in the offered set; valid options: %s\n",
				*optionFlag, strings.Join(validIDs, ", "))
		}
		return 2
	}

	// Build response JSON with HTML escaping disabled.
	resp := answerResponse{
		Outcome:  *outcomeFlag,
		OptionID: *optionFlag,
		Message:  *messageFlag,
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

	// Write atomically via a uniquely-named tmp file, then link/rename.
	dir := filepath.Dir(responsePath)
	base := filepath.Base(responsePath)
	tmpFile, err := os.CreateTemp(dir, base+".tmp.*")
	if err != nil {
		fmt.Fprintf(errOut, "avenor answer: create tmp: %v\n", err)
		return 1
	}
	tmpPath := tmpFile.Name()

	// Ensure tmp is removed on any failure path after this point.
	tmpCleaned := false
	defer func() {
		if !tmpCleaned {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		fmt.Fprintf(errOut, "avenor answer: write %s: %v\n", tmpPath, err)
		return 1
	}
	if err := tmpFile.Close(); err != nil {
		fmt.Fprintf(errOut, "avenor answer: close %s: %v\n", tmpPath, err)
		return 1
	}

	if *forceFlag {
		// Atomic overwrite.
		if err := os.Rename(tmpPath, responsePath); err != nil {
			fmt.Fprintf(errOut, "avenor answer: rename to %s: %v\n", responsePath, err)
			return 1
		}
	} else {
		// Atomic exclusive-create: Link fails if target already exists, eliminating TOCTOU.
		if err := os.Link(tmpPath, responsePath); err != nil {
			if os.IsExist(err) {
				fmt.Fprintf(errOut, "avenor answer: response already exists at %s; pass --force to overwrite\n", responsePath)
				return 2
			}
			fmt.Fprintf(errOut, "avenor answer: link to %s: %v\n", responsePath, err)
			return 1
		}
		_ = os.Remove(tmpPath)
	}

	tmpCleaned = true
	return 0
}
