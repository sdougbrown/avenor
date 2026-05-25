package main

import (
	"fmt"
	"os/exec"
	"strings"
	"testing"
)

func TestVersionFlag(t *testing.T) {
	cmd := exec.Command("go", "run", ".", "--version")
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("failed to run avenor --version: %v", err)
	}

	outputStr := strings.TrimSpace(string(output))
	expectedVersion := fmt.Sprintf("avenor v%s", Version)

	if outputStr != expectedVersion {
		t.Errorf("expected %q, got %q", expectedVersion, outputStr)
	}
}

func TestRunSubcommandAlias(t *testing.T) {
	// Before the fix, "avenor run --loop-file X" would stop parsing at the
	// positional "run" and complain that --prompt/--prompt-file/--loop-file
	// is required. After the fix, "run" is stripped and flags are parsed
	// correctly, so we should hit backend validation instead.
	cmd := exec.Command("go", "run", ".", "run", "--loop-file", "nonexistent-loop-file-test.json")
	output, err := cmd.CombinedOutput()
	outputStr := string(output)
	if err == nil {
		t.Fatal("expected non-zero exit")
	}

	// If "run" is NOT stripped, we get "--prompt, --prompt-file, or --loop-file is required".
	// If "run" IS stripped, we either load loop config (if file existed, which it won't)
	// or hit backend validation. Both prove flags are parsed correctly.
	if strings.Contains(outputStr, "--prompt, --prompt-file, or --loop-file is required") {
		t.Fatalf("'run' was not stripped before cli.Run; got flag parsing error: %s", outputStr)
	}
	// Should have reached loop config or backend validation, confirming --loop-file was parsed.
	if !strings.Contains(outputStr, "load loop config") && !strings.Contains(outputStr, "--server-url is required") {
		t.Fatalf("expected loop-config or backend validation error after strip, got: %s", outputStr)
	}
}
