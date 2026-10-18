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

// answerFlags holds the four answer flags. Values from separate parse passes
// are kept in separate answerFlags values and merged by the caller.
type answerFlags struct {
	option  string
	message string
	outcome string
	force   bool
}

// defineAnswerFlags registers the answer flags on fs, binding to the fields of
// v. Every FlagSet that parses answer arguments (the main parse, the second
// pass over tokens after the positional, and the "--" terminator probe) uses
// this helper so the flag list is declared exactly once.
func defineAnswerFlags(fs *flag.FlagSet, v *answerFlags) {
	fs.StringVar(&v.option, "option", "", "option id to select (required)")
	fs.StringVar(&v.message, "message", "", "free-text message to include in the response")
	fs.StringVar(&v.outcome, "outcome", "selected", "outcome: selected or cancelled")
	fs.BoolVar(&v.force, "force", false, "overwrite an existing response file")
}

// answerConsumedTerminator reports whether the "--" token at args[idx-1] acted
// as the end-of-flags terminator during the first parse pass rather than as a
// flag value. Re-parsing the tokens before it on a throwaway FlagSet fails
// exactly when a flag was left waiting for that "--" as its value.
func answerConsumedTerminator(args []string, idx int) bool {
	if idx == 0 || args[idx-1] != "--" {
		return false
	}
	probe := flag.NewFlagSet("avenor answer", flag.ContinueOnError)
	probe.SetOutput(io.Discard)
	defineAnswerFlags(probe, &answerFlags{})
	return probe.Parse(args[:idx-1]) == nil
}

// runAnswerTo is the testable entry point that accepts an io.Writer for
// error output so tests can capture stderr without OS-level redirection.
func runAnswerTo(args []string, errOut io.Writer) int {
	var passOne answerFlags
	fs := flag.NewFlagSet("avenor answer", flag.ContinueOnError)
	fs.SetOutput(errOut)
	defineAnswerFlags(fs, &passOne)

	// Parse once to locate the <perm-base> positional. The flag package stops
	// at the first positional argument, so any tokens after it stay in
	// fs.Args() and are handled below.
	if err := fs.Parse(args); err != nil {
		return 2
	}
	passOneSet := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) { passOneSet[f.Name] = true })

	// positionalSupplied records that a <perm-base> token was present, even if
	// that token was the empty string. extraPositional records that more than
	// one positional was supplied. Both are held until the validations below
	// run so that error priority matches a plain single-pass parse.
	positionalSupplied := fs.NArg() > 0
	permBase := ""
	extraPositional := false
	flags := passOne
	if positionalSupplied {
		permBase = fs.Arg(0)
		rest := fs.Args()[1:]
		if answerConsumedTerminator(args, len(args)-len(fs.Args())) {
			// The first pass ended at a "--" terminator, so every remaining
			// token is positional and must not be parsed as flags.
			extraPositional = len(rest) > 0
		} else {
			var passTwo answerFlags
			fs2 := flag.NewFlagSet("avenor answer", flag.ContinueOnError)
			fs2.SetOutput(errOut)
			defineAnswerFlags(fs2, &passTwo)
			if err := fs2.Parse(rest); err != nil {
				return 2
			}
			// A flag set in both passes is a duplicate; within a single pass
			// the flag package already applies last-wins.
			duplicateName := ""
			fs2.Visit(func(f *flag.Flag) {
				if passOneSet[f.Name] && duplicateName == "" {
					duplicateName = f.Name
				}
			})
			if duplicateName != "" {
				fmt.Fprintf(errOut, "avenor answer: flag -%s given more than once\n", duplicateName)
				return 2
			}
			// Pass-two values win; flags it did not set keep their pass-one
			// values.
			fs2.Visit(func(f *flag.Flag) {
				switch f.Name {
				case "option":
					flags.option = passTwo.option
				case "message":
					flags.message = passTwo.message
				case "outcome":
					flags.outcome = passTwo.outcome
				case "force":
					flags.force = passTwo.force
				}
			})
			extraPositional = fs2.NArg() > 0
		}
	}

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

	if !positionalSupplied {
		fmt.Fprintln(errOut, "avenor answer: <perm-base> is required")
		return 2
	}

	if extraPositional {
		fmt.Fprintln(errOut, "avenor answer: expected exactly one <perm-base>")
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

	if flags.force {
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
