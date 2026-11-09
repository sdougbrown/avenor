package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/sdougbrown/cli-contract"
	umpire "github.com/umpire-tools/umpire-go"
)

type inputRuleCommand struct {
	Flags       map[string]any              `json:"flags"`
	Rules       []any                       `json:"input_rules"`
	Subcommands map[string]inputRuleCommand `json:"subcommands"`
}

func loadInputRuleSchema(t *testing.T) map[string]inputRuleCommand {
	t.Helper()
	var stdout, stderr bytes.Buffer
	if code := runDeclared([]string{"schema"}, &stdout, &stderr); code != 0 {
		t.Fatalf("schema exit = %d: %s", code, stderr.String())
	}
	var schema struct {
		Commands map[string]inputRuleCommand `json:"commands"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &schema); err != nil {
		t.Fatal(err)
	}
	return schema.Commands
}

func inputRuleEngine(t *testing.T, commands map[string]inputRuleCommand, path []string) *umpire.Umpire {
	t.Helper()
	command, ok := commands[path[0]]
	if !ok {
		t.Fatalf("command %q is missing from schema", path[0])
	}
	for _, name := range path[1:] {
		command, ok = command.Subcommands[name]
		if !ok {
			t.Fatalf("subcommand %q is missing from schema", name)
		}
	}
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
	return engine
}

func TestCommandInputRulesMatchParsing(t *testing.T) {
	commands := loadInputRuleSchema(t)
	for _, tc := range []struct {
		name, command string
		args          []string
		input         map[string]any
		valid         bool
	}{
		{"message inline", "message", []string{"--action=tell", "--message=text"}, map[string]any{"message": "text"}, true},
		{"message false stdin", "message", []string{"--action=tell", "--message=text", "--message-stdin=false"}, map[string]any{"message": "text", "message-stdin": false}, true},
		{"message true stdin", "message", []string{"--action=tell", "--message=text", "--message-stdin"}, map[string]any{"message": "text", "message-stdin": true}, false},
		{"message empty inline", "message", []string{"--action=tell", "--message=", "--message-stdin"}, map[string]any{"message": "", "message-stdin": true}, false},
		{"message stdin alone", "message", []string{"--action=tell", "--message-stdin"}, map[string]any{"message-stdin": true}, true},
		{"message neither", "message", []string{"--action=tell"}, map[string]any{}, true},
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
		{"run both", "run", []string{"--prompt=hello", "--prompt-file=prompt.txt"}, map[string]any{"prompt": "hello", "prompt-file": "prompt.txt"}, false},
		{"run empty prompt", "run", []string{"--prompt=", "--prompt-file=prompt.txt"}, map[string]any{"prompt": "", "prompt-file": "prompt.txt"}, true},
		{"run empty file", "run", []string{"--prompt=hello", "--prompt-file="}, map[string]any{"prompt": "hello", "prompt-file": ""}, true},
		{"spawn both", "control spawn", []string{"--prompt=hello", "--prompt-file=prompt.txt"}, map[string]any{"prompt": "hello", "prompt-file": "prompt.txt"}, false},
		{"spawn empty prompt", "control spawn", []string{"--prompt=", "--prompt-file=prompt.txt"}, map[string]any{"prompt": "", "prompt-file": "prompt.txt"}, true},
		{"spawn empty file", "control spawn", []string{"--prompt=hello", "--prompt-file="}, map[string]any{"prompt": "hello", "prompt-file": ""}, true},
		{"spawn prompt alone", "control spawn", []string{"--prompt=hello"}, map[string]any{"prompt": "hello"}, true},
		{"spawn file alone", "control spawn", []string{"--prompt-file=prompt.txt"}, map[string]any{"prompt-file": "prompt.txt"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := strings.Fields(tc.command)
			engine := inputRuleEngine(t, commands, path)
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
			_, err = clicontract.Parse(app(), append(path, tc.args...))
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

func TestWorkflowRequiredFlagsRejectEmptyInSchemaAndParser(t *testing.T) {
	commands := loadInputRuleSchema(t)
	for _, tc := range []struct {
		command string
		args    []string
		flags   []string
	}{
		{"workflow create", nil, []string{"request-file"}},
		{"workflow instantiate", nil, []string{"template-id", "template-version", "request-file"}},
		{"workflow complete", []string{"wf", "node"}, []string{"activation-id", "attempt-id", "lease-id", "request-file"}},
		{"workflow gate", []string{"wf", "node", "gate"}, []string{"activation-id", "operation", "request-file"}},
		{"workflow skip", []string{"wf", "node"}, []string{"request-file"}},
		{"workflow cancel", []string{"wf"}, []string{"reason"}},
		{"workflow unblock", []string{"wf", "node"}, []string{"request-file"}},
		{"workflow controller create", nil, []string{"request-file"}},
		{"workflow controller disable", []string{"controller"}, []string{"reason"}},
	} {
		for _, empty := range tc.flags {
			t.Run(tc.command+"/"+empty, func(t *testing.T) {
				path := strings.Fields(tc.command)
				engine := inputRuleEngine(t, commands, path)
				input := make(map[string]any, len(tc.flags))
				args := append(append([]string{}, path...), tc.args...)
				for _, name := range tc.flags {
					value := "value"
					if name == empty {
						value = ""
					}
					input[name] = value
					args = append(args, "--"+name+"="+value)
				}
				validInput := make(map[string]any, len(input))
				for name, value := range input {
					validInput[name] = value
				}
				validInput[empty] = "value"
				validStatus, err := engine.Check(validInput, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				for _, name := range tc.flags {
					if !validStatus[name].Enabled {
						t.Errorf("schema rejects non-empty --%s", name)
					}
				}
				validArgs := append([]string{}, args...)
				for i, arg := range validArgs {
					if arg == "--"+empty+"=" {
						validArgs[i] += "value"
					}
				}
				if _, err := clicontract.Parse(app(), validArgs); err != nil {
					t.Errorf("parser rejects non-empty flags: %v", err)
				}
				status, err := engine.Check(input, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				if status[empty].Enabled {
					t.Errorf("schema accepts empty --%s", empty)
				}
				if _, err := clicontract.Parse(app(), args); err == nil {
					t.Errorf("parser accepts empty --%s", empty)
				}
			})
		}
	}
}

func TestWorkflowCancelRejectsEmptyReason(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runDeclared([]string{"workflow", "cancel", "wf", "--reason="}, &stdout, &stderr)
	if code != 2 || !strings.Contains(stderr.String(), "--reason must not be empty") {
		t.Fatalf("exit = %d, stderr = %q", code, stderr.String())
	}
}
