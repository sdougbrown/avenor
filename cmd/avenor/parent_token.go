package main

import (
	"context"
	"fmt"
	"io"

	"github.com/sdougbrown/avenor/internal/token"
	"github.com/sdougbrown/cli-contract"
)

// runParentTokenTo is the testable form of runParentToken.
func runParentTokenTo(stdout, stderr io.Writer, args ...string) int {
	inv, err := parseInvocation(append([]string{"parent-token"}, args...))
	if err != nil {
		fmt.Fprintf(stderr, "avenor parent-token: %v\n", err)
		return 2
	}
	if err := handleParentToken(context.Background(), inv, stdout); err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return 1
	}
	return 0
}

// handleParentToken writes one fresh credential to stdout.
func handleParentToken(ctx context.Context, inv clicontract.Invocation, stdout io.Writer) error {
	credential, err := token.NewHexToken()
	if err != nil {
		return fmt.Errorf("avenor parent-token: %w", err)
	}
	fmt.Fprintln(stdout, credential)
	return nil
}
