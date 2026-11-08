package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/sdougbrown/cli-contract"
	umpire "github.com/umpire-tools/umpire-go"
)

func TestCommandInputRulesMatchParsing(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := runDeclared([]string{"schema"}, &stdout, &stderr); code != 0 {
		t.Fatalf("schema exit = %d: %s", code, stderr.String())
	}
	var schema struct {
		Commands map[string]struct {
			Flags map[string]any `json:"flags"`
			Rules []any          `json:"input_rules"`
		} `json:"commands"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &schema); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, command string
		args          []string
		input         map[string]any
		valid         bool
	}{
		{"mcp conflicting auth", "mcp", []string{"--auth-token=x", "--auth-token-file=y"}, map[string]any{"auth-token": "x", "auth-token-file": "y"}, false},
		{"mcp empty token", "mcp", []string{"--auth-token=", "--auth-token-file=y"}, map[string]any{"auth-token": "", "auth-token-file": "y"}, true},
		{"mcp empty file", "mcp", []string{"--auth-token=x", "--auth-token-file="}, map[string]any{"auth-token": "x", "auth-token-file": ""}, true},
		{"mcp token alone", "mcp", []string{"--auth-token=x"}, map[string]any{"auth-token": "x"}, true},
		{"mcp no autostart", "mcp", []string{"--no-autostart"}, map[string]any{"no-autostart": true}, false},
		{"mcp no autostart false", "mcp", []string{"--no-autostart=false"}, map[string]any{"no-autostart": false}, true},
		{"mcp explicit socket", "mcp", []string{"--no-autostart", "--supervisor-socket=/tmp/socket"}, map[string]any{"no-autostart": true, "supervisor-socket": "/tmp/socket"}, true},
		{"mcp socket alone", "mcp", []string{"--supervisor-socket=/tmp/socket"}, map[string]any{"supervisor-socket": "/tmp/socket"}, true},
		{"mcp empty socket", "mcp", []string{"--no-autostart", "--supervisor-socket="}, map[string]any{"no-autostart": true, "supervisor-socket": ""}, false},
		{"verify entry missing file", "verify", []string{"--roster-entry=x"}, map[string]any{"roster-entry": "x"}, false},
		{"verify empty entry", "verify", []string{"--roster-entry="}, map[string]any{"roster-entry": ""}, true},
		{"verify empty file", "verify", []string{"--roster-entry=x", "--roster-file="}, map[string]any{"roster-entry": "x", "roster-file": ""}, false},
		{"verify entry and file", "verify", []string{"--roster-entry=x", "--roster-file=y"}, map[string]any{"roster-entry": "x", "roster-file": "y"}, true},
		{"verify file alone", "verify", []string{"--roster-file=y"}, map[string]any{"roster-file": "y"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			command := schema.Commands[tc.command]
			if len(command.Rules) == 0 {
				t.Fatal("command has no input rules")
			}
			fields := make(map[string]any, len(command.Flags))
			for name := range command.Flags {
				fields[name] = map[string]any{}
			}
			parsed, err := umpire.SchemaFromObject(map[string]any{"version": float64(1), "fields": fields, "rules": command.Rules})
			if err != nil {
				t.Fatal(err)
			}
			engine, err := umpire.NewFromSchema(parsed)
			if err != nil {
				t.Fatal(err)
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
				t.Errorf("schema accepts = %v, want %v", valid, tc.valid)
			}
			_, err = clicontract.Parse(app(), append([]string{tc.command}, tc.args...))
			if (err == nil) != tc.valid {
				t.Errorf("parse error = %v, want valid %v", err, tc.valid)
			}
		})
	}
}

func TestMCPNoAutostartRequiresSupervisorSocket(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runDeclared([]string{"mcp", "--no-autostart"}, &stdout, &stderr)
	if code != 2 || !strings.Contains(stderr.String(), "--no-autostart requires --supervisor-socket") {
		t.Fatalf("exit = %d, stderr = %q", code, stderr.String())
	}
}
