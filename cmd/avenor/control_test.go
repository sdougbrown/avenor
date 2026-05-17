package main

import (
	"bytes"
	"testing"
)

func TestSpawnParamsFromArgs(t *testing.T) {
	var stderr bytes.Buffer
	params, code := spawnParamsFromArgs([]string{
		"--prompt", "Review PR #42",
		"--dir", "/repo/A",
		"--label", "review-42",
		"--agent", "coder",
		"--model", "test-model",
		"--backend", "opencode-http",
		"--server-url", "http://127.0.0.1:9999",
		"--on-event", "/tmp/events.ndjson",
		"--sentinel-file", "/tmp/done.env",
		"--permission-handler", "file:/tmp/perm",
		"--auto-approve",
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
		"backend":            "opencode-http",
		"server_url":         "http://127.0.0.1:9999",
		"on_event":           "/tmp/events.ndjson",
		"sentinel_file":      "/tmp/done.env",
		"permission_handler": "file:/tmp/perm",
		"auto_approve":       true,
		"timeout":            30,
		"max_retries":        2,
	}
	for key, value := range want {
		if params[key] != value {
			t.Fatalf("params[%q] = %#v, want %#v; all params=%#v", key, params[key], value, params)
		}
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
