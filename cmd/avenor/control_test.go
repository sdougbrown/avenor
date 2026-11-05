package main

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/sdougbrown/cli-contract"
)

func TestSpawnParamsFromArgs(t *testing.T) {
	var stderr bytes.Buffer
	params, code := spawnParamsFromArgs([]string{
		"--prompt", "Review PR #42",
		"--dir", "/repo/A",
		"--label", "review-42",
		"--agent", "coder",
		"--model", "test-model",
		"--thinking", "medium",
		"--backend", "opencode-http",
		"--server-url", "http://127.0.0.1:9999",
		"--on-event", "/tmp/events.ndjson",
		"--sentinel-file", "/tmp/done.env",
		"--permission-handler", "file:/tmp/perm",
		"--auto-approve",
		"--persist-sessions",
		"--timeout", "30",
		"--max-retries", "2",
	}, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, stderr=%s", code, stderr.String())
	}

	want := map[string]any{
		"prompt":             "Review PR #42",
		"dir":                "/repo/A",
		"label":              "review-42",
		"agent":              "coder",
		"model":              "test-model",
		"thinking":           "medium",
		"backend":            "opencode-http",
		"server_url":         "http://127.0.0.1:9999",
		"on_event":           "/tmp/events.ndjson",
		"sentinel_file":      "/tmp/done.env",
		"permission_handler": "file:/tmp/perm",
		"auto_approve":       true,
		"persist_sessions":   true,
		"timeout":            30,
		"max_retries":        2,
	}
	for key, value := range want {
		if params[key] != value {
			t.Fatalf("params[%q] = %#v, want %#v; all params=%#v", key, params[key], value, params)
		}
	}
}

func TestSpawnMessagingFlagTriState(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		want    any
		present bool
	}{
		{"omitted", nil, nil, false},
		{"on", []string{"--messaging"}, true, true},
		{"off", []string{"--messaging=false"}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stderr bytes.Buffer
			params, code := spawnParamsFromArgs(append([]string{"--prompt", "work"}, tc.args...), &stderr)
			got, present := params["messaging"]
			if code != 0 || present != tc.present || got != tc.want {
				t.Fatalf("code=%d messaging=%v present=%v stderr=%s", code, got, present, stderr.String())
			}
		})
	}
}

func TestSpawnParamsFromArgsAllowsModelWithoutAgent(t *testing.T) {
	var stderr bytes.Buffer
	params, code := spawnParamsFromArgs([]string{
		"--prompt", "Use the runtime default agent",
		"--dir", "/repo/A",
		"--model", "test-model",
	}, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, stderr=%s", code, stderr.String())
	}
	if params["model"] != "test-model" {
		t.Fatalf("params[model] = %#v, want test-model", params["model"])
	}
	if _, ok := params["agent"]; ok {
		t.Fatalf("agent key should be absent, params=%#v", params)
	}
	if _, ok := params["thinking"]; ok {
		t.Fatalf("thinking key should be absent, params=%#v", params)
	}
}

func TestSpawnParamsFromArgsRejectsInvalidThinking(t *testing.T) {
	var stderr bytes.Buffer
	params, code := spawnParamsFromArgs([]string{"--prompt", "work", "--thinking", "HIGH"}, &stderr)
	if code == 0 || params != nil {
		t.Fatalf("code=%d params=%#v", code, params)
	}
	if !containsStr(stderr.String(), "invalid thinking value") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestSpawnParamsFromArgsAcceptsEveryThinkingValue(t *testing.T) {
	for _, value := range []string{"off", "minimal", "low", "medium", "high", "xhigh", "max"} {
		t.Run(value, func(t *testing.T) {
			var stderr bytes.Buffer
			params, code := spawnParamsFromArgs([]string{"--prompt", "work", "--thinking", value}, &stderr)
			if code != 0 || params["thinking"] != value {
				t.Fatalf("code=%d thinking=%v stderr=%s", code, params["thinking"], stderr.String())
			}
		})
	}
}

func TestSpawnParamsFromArgsRequiresPrompt(t *testing.T) {
	var stderr bytes.Buffer
	_, code := spawnParamsFromArgs([]string{"--dir", "/repo/A"}, &stderr)
	if code == 0 {
		t.Fatal("code = 0, want failure")
	}
	if !containsStr(stderr.String(), "--prompt or --prompt-file is required") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestSpawnParamsFromArgsAllowsBackendOnly(t *testing.T) {
	var stderr bytes.Buffer
	params, code := spawnParamsFromArgs([]string{
		"--prompt", "Use the backend default agent",
		"--backend", "agy",
	}, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, stderr=%s", code, stderr.String())
	}
	if params["backend"] != "agy" {
		t.Fatalf("params[backend] = %#v, want agy", params["backend"])
	}
	if _, ok := params["agent"]; ok {
		t.Fatalf("agent should be omitted: %#v", params)
	}
}

func TestSpawnParamsFromArgsRosterSelector(t *testing.T) {
	var stderr bytes.Buffer
	params, code := spawnParamsFromArgs([]string{
		"--prompt", "Plan the work",
		"--roster-file", "/repo/roster.json",
		"--roster-entry", "planner",
	}, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, stderr=%s", code, stderr.String())
	}
	if params["roster_file"] != "/repo/roster.json" || params["roster_entry"] != "planner" {
		t.Fatalf("roster params = %#v", params)
	}
	if _, ok := params["agent"]; ok {
		t.Fatalf("agent should be omitted in roster mode: %#v", params)
	}
}

func TestSpawnParamsFromArgsRejectsMixedRosterSelector(t *testing.T) {
	var stderr bytes.Buffer
	params, code := spawnParamsFromArgs([]string{
		"--prompt", "Plan the work",
		"--roster-file", "/repo/roster.json",
		"--roster-entry", "planner",
		"--backend", "pi",
	}, &stderr)
	if code == 0 || params != nil {
		t.Fatalf("code=%d params=%#v", code, params)
	}
	if !containsStr(stderr.String(), "direct identity fields are disabled in roster mode") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestSpawnParamsFromArgsRejectsPromptAndPromptFile(t *testing.T) {
	var stderr bytes.Buffer
	_, code := spawnParamsFromArgs([]string{"--prompt", "hello", "--prompt-file", "prompt.txt"}, &stderr)
	if code == 0 {
		t.Fatal("code = 0, want failure")
	}
	if !containsStr(stderr.String(), "mutually exclusive") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

// spawnParamsFromArgs is the test seam for spawnParamsFromInvocation: it
// parses raw argv the way the declared tree would and returns the exit-code
// style result the tests assert on.
func spawnParamsFromArgs(args []string, stderr io.Writer) (map[string]any, int) {
	inv, err := clicontract.Parse(app(), append([]string{"control", "spawn"}, args...))
	if err != nil {
		fmt.Fprintln(stderr, err)
		return nil, 2
	}
	params, err := spawnParamsFromInvocation(inv)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return nil, 2
	}
	return params, 0
}
