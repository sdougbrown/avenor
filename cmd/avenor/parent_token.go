package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
)

// runParentToken implements `avenor parent-token`.
func runParentToken(args []string) int {
	return runParentTokenTo(os.Stdout, os.Stderr)
}

// runParentTokenTo is the testable form of runParentToken.
func runParentTokenTo(stdout, stderr io.Writer) int {
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
