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
	"sort"
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

// countedValue wraps a flag.Value and counts Set calls so a caller that parses
// the same FlagSet in multiple passes can detect that a flag set in an earlier
// pass was set again.
type countedValue struct {
	flag.Value
	sets int
}

func (c *countedValue) Set(s string) error {
	c.sets++
	return c.Value.Set(s)
}

// IsBoolFlag forwards the bool-flag marker so wrapped bool flags still parse
// without an explicit value.
func (c *countedValue) IsBoolFlag() bool {
	b, ok := c.Value.(interface{ IsBoolFlag() bool })
	return ok && b.IsBoolFlag()
}

// String is nil-safe because the flag package constructs a zero value of this
// type when rendering usage output.
func (c *countedValue) String() string {
	if c.Value == nil {
		return ""
	}
	return c.Value.String()
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
	probe.String("option", "", "")
	probe.String("message", "", "")
	probe.String("outcome", "", "")
	probe.Bool("force", false, "")
	return probe.Parse(args[:idx-1]) == nil
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

	// Wrap each value with a Set counter so a second parse pass can detect that
	// a flag set in the first pass was set again. Swapping the Value in place
	// preserves parsing behavior; the pointers above still receive the values.
	setCounts := make(map[string]*int, 4)
	for _, name := range []string{"option", "message", "outcome", "force"} {
		f := fs.Lookup(name)
		counted := &countedValue{Value: f.Value}
		setCounts[name] = &counted.sets
		f.Value = counted
	}

	// Parse once to locate the <perm-base> positional. The flag package stops
	// at the first positional argument, so any tokens after it stay in
	// fs.Args() and are handled below.
	if err := fs.Parse(args); err != nil {
		return 2
	}

	// extraPositional records that more than one positional was supplied. It is
	// held until the validations below run so that error priority matches a
	// plain single-pass parse.
	permBase := ""
	extraPositional := false
	if fs.NArg() > 0 {
		permBase = fs.Arg(0)
		rest := fs.Args()[1:]
		if answerConsumedTerminator(args, len(args)-len(fs.Args())) {
			// The first pass ended at a "--" terminator, so every remaining
			// token is positional and must not be parsed as flags.
			extraPositional = len(rest) > 0
		} else {
			// Remember how many times each flag was set in the first pass so a
			// set in the pass below counts as a duplicate.
			passOne := make(map[string]int, len(setCounts))
			for name, count := range setCounts {
				passOne[name] = *count
			}
			if err := fs.Parse(rest); err != nil {
				return 2
			}
			names := make([]string, 0, len(setCounts))
			for name := range setCounts {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				if passOne[name] > 0 && *setCounts[name] > passOne[name] {
					fmt.Fprintf(errOut, "avenor answer: flag -%s given more than once\n", name)
					return 2
				}
			}
			extraPositional = fs.NArg() > 0
		}
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
