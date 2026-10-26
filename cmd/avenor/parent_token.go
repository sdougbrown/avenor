package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/sdougbrown/avenor/internal/token"
)

// runParentToken implements `avenor parent-token`.
func runParentToken(args []string) int {
	return runParentTokenTo(os.Stdout, os.Stderr, args...)
}

// runParentTokenTo is the testable form of runParentToken.
func runParentTokenTo(stdout, stderr io.Writer, args ...string) int {
	fs := flag.NewFlagSet("parent-token", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if err := fs.Parse(args); err != nil {
		return 1
	}
	token, err := token.NewHexToken()
	if err != nil {
		fmt.Fprintf(stderr, "avenor parent-token: %v\n", err)
		return 1
	}
	fmt.Fprintln(stdout, token)
	return 0
}
