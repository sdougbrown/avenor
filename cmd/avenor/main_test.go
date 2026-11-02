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

func TestBareInvocationPrintsHelp(t *testing.T) {
	var stdout, stderr strings.Builder
	code := runDeclared(nil, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("expected exit 0 for bare invocation, got %d; stderr: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "run") {
		t.Fatalf("expected root help on stdout to list the run command, got: %s", stdout.String())
	}
}

func TestRunSubcommandAlias(t *testing.T) {
	// Guards that "avenor run --loop-file X" dispatches to the run handler
	// with --loop-file parsed, rather than failing flag validation.
	cmd := exec.Command("go", "run", ".", "run", "--loop-file", "nonexistent-loop-file-test.json")
	output, err := cmd.CombinedOutput()
	outputStr := string(output)
	if err == nil {
		t.Fatal("expected non-zero exit")
	}

	if strings.Contains(outputStr, "--prompt, --prompt-file, or --loop-file is required") {
		t.Fatalf("run handler did not receive --loop-file; got flag parsing error: %s", outputStr)
	}
	// The missing file fails loop-config load, or backend validation fires first;
	// both prove flags were parsed and the run handler was reached.
	if !strings.Contains(outputStr, "load loop config") && !strings.Contains(outputStr, "--server-url is required") {
		t.Fatalf("expected loop-config or backend validation error, got: %s", outputStr)
	}
}
