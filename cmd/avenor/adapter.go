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

// avenorAdapter routes the library's dispatch through Avenor's conventions.
// Handlers that produce machine-readable results return them and the adapter
// writes one indented JSON value; commands that print their own output
// (confirmations, verify's check lines) write directly and return nil.
// Usage-class failures exit 2, operational failures 1, and a handler-decided
// exitCode wins.
type avenorAdapter struct{}

func (avenorAdapter) ErrorResult(command string, err error) any { return nil }

func (avenorAdapter) Write(w io.Writer, result any) error {
	switch result := result.(type) {
	case map[string]any:
		data, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			return err
		}
		w.Write(append(data, '\n'))
	case []map[string]any:
		data, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			return err
		}
		w.Write(append(data, '\n'))
	case []any:
		data, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			return err
		}
		w.Write(append(data, '\n'))
	}
	return nil
}

func (avenorAdapter) BeforeHandler(inv clicontract.Invocation, stderr io.Writer) {}

func (avenorAdapter) Deadline(ctx context.Context, inv clicontract.Invocation) (context.Context, context.CancelFunc) {
	// Handlers arm their own timeouts where the contract documents one.
	return ctx, func() {}
}

func (avenorAdapter) Validate(inv clicontract.Invocation) error { return nil }

func (avenorAdapter) ExitCode(inv clicontract.Invocation, result any, handlerErr error) int {
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

// runDeclared dispatches through the same clicontract.Run path production
// uses, with injected streams for the run* wrappers the tests call.
func runDeclared(argv []string, stdout, stderr io.Writer) int {
	return clicontract.Run(app(), avenorAdapter{}, argv, stdout, stderr)
}

// parseInvocation parses args against the declared surface.
func parseInvocation(args []string) (clicontract.Invocation, error) {
	return clicontract.Parse(app(), args)
}

// usageErrorf builds a caller error with the adapter's usage classification.
func usageErrorf(format string, args ...any) error {
	return clicontract.UsageErrorf(format, args...)
}
