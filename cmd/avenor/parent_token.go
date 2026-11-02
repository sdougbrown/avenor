package main

import (
	"context"
	"fmt"
	"io"

	"github.com/sdougbrown/avenor/internal/token"
	"github.com/sdougbrown/cli-contract"
)

// handleParentToken writes one fresh credential to stdout.
func handleParentToken(ctx context.Context, inv clicontract.Invocation, stdout io.Writer) error {
	credential, err := token.NewHexToken()
	if err != nil {
		return fmt.Errorf("avenor parent-token: %w", err)
	}
	fmt.Fprintln(stdout, credential)
	return nil
}
