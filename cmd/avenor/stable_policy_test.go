package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRunStableRejectsMalformedMCPLaunchWrapperPolicy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.json")
	if err := os.WriteFile(path, []byte(`{"allowed_argv": [["nice"]]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	// newSupervisorFn is never reached: the malformed policy must fail startup.
	exit := runStable([]string{"--control-socket", filepath.Join(t.TempDir(), "s.sock"), "--mcp-launch-wrapper-policy", path})
	if exit != 1 {
		t.Fatalf("runStable() = %d, want 1", exit)
	}
}

func TestRunStableMissingPolicyFileFails(t *testing.T) {
	exit := runStable([]string{"--control-socket", filepath.Join(t.TempDir(), "s.sock"), "--mcp-launch-wrapper-policy", filepath.Join(t.TempDir(), "absent.json")})
	if exit != 1 {
		t.Fatalf("runStable() = %d, want 1", exit)
	}
}
