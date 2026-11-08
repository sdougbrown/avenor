package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sdougbrown/cli-contract"
	umpire "github.com/umpire-tools/umpire-go"
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

func TestMessageSchemaDeclaresStdinExclusion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := runDeclared([]string{"schema"}, &stdout, &stderr); code != 0 {
		t.Fatalf("schema exit = %d, stderr: %s", code, stderr.String())
	}
	var schema struct {
		Commands map[string]struct {
			Flags      map[string]any `json:"flags"`
			InputRules []any          `json:"input_rules"`
		} `json:"commands"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &schema); err != nil {
		t.Fatal(err)
	}
	command := schema.Commands["message"]
	if len(command.InputRules) == 0 {
		t.Fatal("message schema does not declare stdin exclusion")
	}
	fields := make(map[string]any, len(command.Flags))
	for name := range command.Flags {
		fields[name] = map[string]any{}
	}
	parsed, err := umpire.SchemaFromObject(map[string]any{"version": float64(1), "fields": fields, "rules": command.InputRules})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := umpire.NewFromSchema(parsed)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		args  []string
		input map[string]any
		valid bool
	}{
		{"inline", []string{"--message=text"}, map[string]any{"message": "text"}, true},
		{"false stdin", []string{"--message=text", "--message-stdin=false"}, map[string]any{"message": "text", "message-stdin": false}, true},
		{"true stdin", []string{"--message=text", "--message-stdin"}, map[string]any{"message": "text", "message-stdin": true}, false},
		{"empty inline", []string{"--message=", "--message-stdin"}, map[string]any{"message": "", "message-stdin": true}, false},
		{"stdin only", []string{"--message-stdin"}, map[string]any{"message-stdin": true}, true},
		{"neither", nil, map[string]any{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"message", "--action=tell"}, tc.args...)
			_, parseErr := clicontract.Parse(app(), args)
			if (parseErr == nil) != tc.valid {
				t.Errorf("parse error = %v, want valid %v", parseErr, tc.valid)
			}
			status, err := engine.Check(tc.input, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			valid := true
			for name := range tc.input {
				if !status[name].Enabled {
					valid = false
				}
			}
			if valid != tc.valid {
				t.Errorf("schema accepts input = %v, want %v", valid, tc.valid)
			}
		})
	}
}
