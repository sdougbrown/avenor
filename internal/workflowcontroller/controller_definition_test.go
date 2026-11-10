package workflowcontroller

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeDefinition(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

func TestLoadControllerDefinitionsMissingDir(t *testing.T) {
	t.Parallel()
	loaded, err := LoadControllerDefinitions(filepath.Join(t.TempDir(), "absent"))
	if err != nil {
		t.Fatalf("missing dir: %v", err)
	}
	if len(loaded.Definitions) != 0 || len(loaded.Failures) != 0 {
		t.Fatalf("missing dir loaded %+v, want empty", loaded)
	}
}

func TestLoadControllerDefinitions(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeDefinition(t, dir, "a.json", `{"controller_id": "alpha", "max_inflight": 3}`)
	writeDefinition(t, dir, "b.json", `{"controller_id": "beta", "max_inflight": 2, "desired_state": "enabled"}`)
	writeDefinition(t, dir, "notes.txt", "not json")
	if err := os.MkdirAll(filepath.Join(dir, "sub.json"), 0o755); err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadControllerDefinitions(dir)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded.Failures) != 0 {
		t.Fatalf("unexpected failures: %v", loaded.Failures)
	}
	if len(loaded.Definitions) != 2 {
		t.Fatalf("definitions = %+v, want alpha and beta", loaded.Definitions)
	}
	byID := map[string]ControllerDefinition{}
	for _, def := range loaded.Definitions {
		byID[def.ControllerID] = def
	}
	if def := byID["alpha"]; def.MaxInflight != 3 || def.DesiredState != "" || def.File != "a.json" {
		t.Fatalf("alpha = %+v", def)
	}
	if def := byID["beta"]; def.MaxInflight != 2 || def.DesiredState != DesiredEnabled {
		t.Fatalf("beta = %+v", def)
	}
}

func TestLoadControllerDefinitionsValidation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeDefinition(t, dir, "bad-limit.json", `{"controller_id": "x", "max_inflight": 0}`)
	writeDefinition(t, dir, "bad-state.json", `{"controller_id": "x", "max_inflight": 1, "desired_state": "paused"}`)
	writeDefinition(t, dir, "unknown-field.json", `{"controller_id": "x", "max_inflight": 1, "extra": true}`)
	writeDefinition(t, dir, "not-json.json", `nope`)
	writeDefinition(t, dir, "trailing.json", `{"controller_id": "x", "max_inflight": 1} trailing junk`)
	writeDefinition(t, dir, "trailing-brace.json", `{"controller_id": "x", "max_inflight": 1}}`)

	loaded, err := LoadControllerDefinitions(dir)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded.Definitions) != 0 {
		t.Fatalf("definitions = %+v, want none", loaded.Definitions)
	}
	if len(loaded.Failures) != 6 {
		t.Fatalf("failures = %d, want 6: %v", len(loaded.Failures), loaded.Failures)
	}
	for _, failure := range loaded.Failures {
		if !errors.Is(failure, ErrInvalidController) && !strings.Contains(failure.Err.Error(), "parse") &&
			!strings.Contains(failure.Err.Error(), "unknown field") && !strings.Contains(failure.Err.Error(), "trailing data") {
			t.Fatalf("unexpected failure %v", failure)
		}
	}
}

func TestLoadControllerDefinitionsDirIsFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	file := filepath.Join(dir, "not-a-dir")
	if err := os.WriteFile(file, []byte("not a dir"), 0o600); err != nil {
		t.Fatalf("write regular file: %v", err)
	}
	if _, err := LoadControllerDefinitions(file); err == nil {
		t.Fatal("LoadControllerDefinitions on a regular file: err = nil, want non-nil")
	}
}

func TestLoadControllerDefinitionsDuplicateIDs(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeDefinition(t, dir, "one.json", `{"controller_id": "dup", "max_inflight": 1}`)
	writeDefinition(t, dir, "two.json", `{"controller_id": "dup", "max_inflight": 2}`)

	loaded, err := LoadControllerDefinitions(dir)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded.Definitions) != 0 {
		t.Fatalf("definitions = %+v, want none (duplicates reject every file)", loaded.Definitions)
	}
	if len(loaded.Failures) != 1 || !strings.Contains(loaded.Failures[0].Err.Error(), "dup") {
		t.Fatalf("failures = %v, want one rejection naming dup", loaded.Failures)
	}
	if !errors.Is(loaded.Failures[0], ErrInvalidController) {
		t.Fatalf("duplicate rejection err = %v, want ErrInvalidController", loaded.Failures[0].Err)
	}
}
