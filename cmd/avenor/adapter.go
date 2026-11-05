package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/sdougbrown/cli-contract"
)

// exitCode carries a handler-decided process exit code through the adapter:
// probe maps its stop reason to Avenor's locked exit codes, and the
// long-running servers return their shutdown status.
type exitCode int

// failureResult carries a structured refusal with a nonzero exit status.
type failureResult map[string]any

// avenorAdapter routes the library's dispatch through Avenor's conventions:
// handlers that print their own output return nil or an exitCode, other
// results are written as one JSON value, and nothing lands on stdout for
// errors. Usage-class failures exit 2, operational failures 1, and a
// handler-decided exitCode wins.
type avenorAdapter struct{}

func (avenorAdapter) ErrorResult(command string, err error) any { return nil }

func (avenorAdapter) BeforeHandler(inv clicontract.Invocation, stderr io.Writer) {}

func (avenorAdapter) Deadline(ctx context.Context, inv clicontract.Invocation) (context.Context, context.CancelFunc) {
	// Handlers arm their own timeouts where the contract documents one.
	return ctx, func() {}
}

func (avenorAdapter) Validate(inv clicontract.Invocation) error { return nil }

// Write emits one indented JSON value for any non-nil machine-readable
// result a handler returns — maps, slices, or future types — so an
// unrecognized result can never be silently dropped. Handlers that print
// their own output return nil or an exitCode, which Write skips.
func (avenorAdapter) Write(w io.Writer, result any) error {
	switch result.(type) {
	case exitCode, nil:
		return nil
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	w.Write(append(data, '\n'))
	return nil
}

func (avenorAdapter) ExitCode(inv clicontract.Invocation, result any, handlerErr error) int {
	if _, ok := result.(failureResult); ok {
		return 1
	}
	if ec, ok := result.(exitCode); ok {
		return int(ec)
	}
	if handlerErr != nil {
		var ue *clicontract.UsageError
		if errors.As(handlerErr, &ue) {
			return 2
		}
		return 1
	}
	return 0
}

// runDeclared is Avenor's one dispatch path: main passes the process streams,
// and tests inject their own to capture what the adapter writes. Handlers
// that print their own output, such as await's transition lines, write to
// os.Stdout directly, because the library's HandleFunc has no stdout.
func runDeclared(argv []string, stdout, stderr io.Writer) int {
	return clicontract.Run(app(), avenorAdapter{}, argv, stdout, stderr)
}

// usageErrorf builds a caller error with the adapter's usage classification.
func usageErrorf(format string, args ...any) error {
	return clicontract.UsageErrorf(format, args...)
}
