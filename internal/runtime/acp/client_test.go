package acp

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestMergeEnvironReplacesExistingKeys(t *testing.T) {
	base := []string{
		"PATH=/usr/bin",
		"HOME=/home/user",
		"OPENCODE_CONFIG=/home/user/.config/opencode/opencode.local.json",
	}
	overrides := map[string]string{
		"OPENCODE_CONFIG": "/tmp/avenor-opencode-acp-run_1.json",
		"AVENOR_RUN_ID":   "run_1",
	}

	merged := mergeEnviron(base, overrides)

	// Must not contain duplicate OPENCODE_CONFIG
	count := 0
	for _, entry := range merged {
		if len(entry) >= len("OPENCODE_CONFIG=") && entry[:len("OPENCODE_CONFIG=")] == "OPENCODE_CONFIG=" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("OPENCODE_CONFIG appears %d times, want 1", count)
	}

	// The override value must win
	for _, entry := range merged {
		if entry == "OPENCODE_CONFIG=/tmp/avenor-opencode-acp-run_1.json" {
			return // found
		}
	}
	t.Fatalf("override value not found in merged env: %v", merged)
}

func TestMergeEnvironAppendsNewKeys(t *testing.T) {
	base := []string{"PATH=/usr/bin", "HOME=/home/user"}
	overrides := map[string]string{"NEW_KEY": "new_value"}

	merged := mergeEnviron(base, overrides)

	found := false
	for _, entry := range merged {
		if entry == "NEW_KEY=new_value" {
			found = true
		}
	}
	if !found {
		t.Fatalf("NEW_KEY not found in merged env: %v", merged)
	}
	if len(merged) != 3 {
		t.Fatalf("len(merged) = %d, want 3", len(merged))
	}
}

func TestMergeEnvironEmptyOverrides(t *testing.T) {
	base := []string{"PATH=/usr/bin"}
	merged := mergeEnviron(base, nil)
	if len(merged) != 1 || merged[0] != "PATH=/usr/bin" {
		t.Fatalf("mergeEnviron with nil overrides changed base: %v", merged)
	}
}

func TestMergeEnvironReplacesMultiple(t *testing.T) {
	base := []string{
		"A=1",
		"B=2",
		"C=3",
	}
	overrides := map[string]string{
		"A": "10",
		"C": "30",
		"D": "40",
	}
	merged := mergeEnviron(base, overrides)

	expect := map[string]string{"A": "10", "B": "2", "C": "30", "D": "40"}
	for key, want := range expect {
		found := false
		for _, entry := range merged {
			prefix := key + "="
			if len(entry) >= len(prefix) && entry[:len(prefix)] == prefix {
				if entry != prefix+want {
					t.Fatalf("%s = %q, want %q", key, entry[len(prefix):], want)
				}
				found = true
			}
		}
		if !found {
			t.Fatalf("%s not found in merged env", key)
		}
	}
	if len(merged) != 4 {
		t.Fatalf("len(merged) = %d, want 4", len(merged))
	}
}

// TestClientPermissionRequestThenImmediateEOFDoesNotRace verifies that a child
// emitting session/request_permission right before exiting never triggers a
// send-on-closed-channel panic in handleRequest vs readLoop.
func TestClientPermissionRequestThenImmediateEOFDoesNotRace(t *testing.T) {
	script := filepath.Join(t.TempDir(), "perm-eof.sh")
	body := `#!/bin/sh
for i in 1 2 3 4 5 6 7 8; do
  printf '{"jsonrpc":"2.0","id":%d,"method":"session/request_permission","params":{"sessionId":"ses_race","options":[{"optionId":"allow","kind":"allow"}]}}\n' "$i"
done
exit 0
`
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}

	for iter := 0; iter < 20; iter++ {
		client, err := NewClient(context.Background(), ClientConfig{
			Bin: script,
		})
		if err != nil {
			t.Fatalf("iter %d NewClient: %v", iter, err)
		}
		for range client.Events() {
		}
		_ = client.Close()
	}
}
