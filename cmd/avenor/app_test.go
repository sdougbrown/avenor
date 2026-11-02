package main

import (
	"bytes"
	"encoding/json"
	"testing"
)

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
