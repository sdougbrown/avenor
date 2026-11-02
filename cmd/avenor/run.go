package main

import (
	"context"
	"io"
	"os"

	"github.com/sdougbrown/avenor/internal/cli"
	"github.com/sdougbrown/cli-contract"
)

// handleRun dispatches a declared run invocation through the shared
// invocation-to-options path in internal/cli.
func handleRun(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	return exitCode(cli.RunInvocation(inv, os.Getenv, stderr)), nil
}
