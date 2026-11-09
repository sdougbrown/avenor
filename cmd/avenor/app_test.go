package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunDeclaredHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runDeclared([]string{"run", "--help"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run --help exit = %d, want 0; stderr: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "--prompt-file") {
		t.Fatalf("run --help stdout missing --prompt-file: %q", stdout.String())
	}
}

// TestRunDeclaredFlagSyntaxRejectsBeforeSentinel pins the flag-syntax contract:
// a command line that does not parse exits 2 and writes nothing, including the
// sentinel file.
func TestRunDeclaredFlagSyntaxRejectsBeforeSentinel(t *testing.T) {
	for name, extra := range map[string][]string{
		"unknown flag":       {"--nope"},
		"stray positional":   {"extra"},
		"malformed duration": {"--timeout=notaduration"},
		"malformed int":      {"--max-retries=notanint"},
	} {
		t.Run(name, func(t *testing.T) {
			sentinel := filepath.Join(t.TempDir(), "done.env")
			var stdout, stderr bytes.Buffer
			args := append([]string{"run", "--sentinel-file", sentinel, "--prompt", "x"}, extra...)
			code := runDeclared(args, &stdout, &stderr)
			if code != 2 {
				t.Fatalf("exit = %d, want 2; stderr: %s", code, stderr.String())
			}
			if _, err := os.Stat(sentinel); !os.IsNotExist(err) {
				t.Fatalf("sentinel %s exists after flag-syntax rejection", sentinel)
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
		})
	}
}

func TestRunConflictingPromptsRejectBeforeArtifacts(t *testing.T) {
	dir := t.TempDir()
	sentinel := filepath.Join(dir, "done.env")
	var stdout, stderr bytes.Buffer
	code := runDeclared([]string{"run", "--prompt=x", "--prompt-file=prompt.txt", "--sentinel-file=" + sentinel}, &stdout, &stderr)
	if code != 2 || !strings.Contains(stderr.String(), "mutually exclusive") {
		t.Fatalf("exit = %d, stderr = %q; want usage error", code, stderr.String())
	}
	if _, err := os.Stat(sentinel); !os.IsNotExist(err) {
		t.Fatalf("sentinel exists after rejection: %v", err)
	}
}

func TestSchemaCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runDeclared([]string{"schema"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("schema exit = %d, stderr: %s", code, stderr.String())
	}
	var raw map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &raw); err != nil {
		t.Fatalf("schema output is not one JSON value: %v (%q)", err, stdout.String())
	}
	if raw["schema"] != "avenor/cli-schema/v1" {
		t.Errorf("schema id = %v, want avenor/cli-schema/v1", raw["schema"])
	}
	cmds, ok := raw["commands"].(map[string]any)
	if !ok {
		t.Fatalf("commands = %T, want an object", raw["commands"])
	}
	for _, name := range []string{"probe", "stable", "schema"} {
		if _, ok := cmds[name]; !ok {
			t.Errorf("schema commands missing %q", name)
		}
	}
}
