package main

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"
)

func TestParentTokenPrintsFreshToken(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := runParentTokenTo(&stdout, &stderr); code != 0 {
		t.Fatalf("expected exit 0, got %d (stderr: %s)", code, stderr.String())
	}

	out := strings.TrimSuffix(stdout.String(), "\n")
	if stdout.String() != out+"\n" {
		t.Fatalf("expected exactly one line, got %q", stdout.String())
	}
	if len(out) != 64 {
		t.Fatalf("expected a 64-character hex token, got %d characters: %q", len(out), out)
	}
	if _, err := hex.DecodeString(out); err != nil {
		t.Fatalf("expected hex encoding, got %q: %v", out, err)
	}

	var second bytes.Buffer
	if code := runParentTokenTo(&second, &stderr); code != 0 {
		t.Fatalf("expected exit 0 on second run, got %d (stderr: %s)", code, stderr.String())
	}
	if strings.TrimSpace(second.String()) == out {
		t.Fatal("expected a fresh token on each run")
	}
}

func TestParentTokenRejectsUnknownFlags(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := runParentTokenTo(&stdout, &stderr, "--typo"); code != 1 {
		t.Fatalf("runParentTokenTo(--typo) = %d, want 1", code)
	}
	if stdout.Len() != 0 {
		t.Fatalf("expected no token on stdout, got %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "flag provided but not defined") {
		t.Fatalf("expected a flag error on stderr, got %q", stderr.String())
	}
}
