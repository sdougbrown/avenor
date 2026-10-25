package main

import (
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
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
	token, err := newParentToken()
	if err != nil {
		fmt.Fprintf(stderr, "avenor parent-token: %v\n", err)
		return 1
	}
	fmt.Fprintln(stdout, token)
	return 0
}

func newParentToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("crypto/rand: %w", err)
	}
	return hex.EncodeToString(b), nil
}
