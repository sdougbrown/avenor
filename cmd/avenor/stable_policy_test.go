package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sdougbrown/avenor/internal/stable"
)

func writeValidPolicyFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policy.json")
	content := `{"allowed_argv": [["/usr/bin/nice", "-n", "5"]]}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRunStableForwardsValidPolicyToSupervisorConfig(t *testing.T) {
	path := writeValidPolicyFile(t)
	var captured stable.Config
	orig := newSupervisorFn
	newSupervisorFn = func(cfg stable.Config) *stable.Supervisor {
		captured = cfg
		// Never actually run: return nil after capturing the config. The
		// malformed/missing-policy tests above exit before this point, and
		// this test must too — return before Run is invoked.
		return nil
	}
	defer func() { newSupervisorFn = orig }()

	// Run panics or fails on the nil supervisor; capture happens first.
	func() {
		defer func() { _ = recover() }()
		_ = runStable([]string{"--control-socket", filepath.Join(t.TempDir(), "s.sock"), "--mcp-launch-wrapper-policy", path})
	}()
	if captured.MCPLaunchWrapperPolicy == nil {
		t.Fatal("Config.MCPLaunchWrapperPolicy = nil, want loaded policy")
	}
	if len(captured.MCPLaunchWrapperPolicy.AllowedArgv) != 1 {
		t.Fatalf("AllowedArgv entries = %d, want 1", len(captured.MCPLaunchWrapperPolicy.AllowedArgv))
	}
}

func TestRunStableOmitsPolicyByDefault(t *testing.T) {
	var captured stable.Config
	orig := newSupervisorFn
	newSupervisorFn = func(cfg stable.Config) *stable.Supervisor {
		captured = cfg
		return nil
	}
	defer func() { newSupervisorFn = orig }()
	func() {
		defer func() { _ = recover() }()
		_ = runStable([]string{"--control-socket", filepath.Join(t.TempDir(), "s.sock")})
	}()
	if captured.MCPLaunchWrapperPolicy != nil {
		t.Fatal("Config.MCPLaunchWrapperPolicy set without the flag, want nil")
	}
}

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
