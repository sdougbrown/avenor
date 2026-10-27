package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sdougbrown/avenor/internal/mcpserver"
	"github.com/sdougbrown/avenor/internal/stable"
)

// stubPolicyEnv replaces the environment and home directory the pickup chain
// reads, so each test describes its own machine-local config.
func stubPolicyEnv(t *testing.T, env map[string]string, home string) {
	t.Helper()
	origGetenv, origHomeDir := mcpLaunchWrapperGetenv, mcpLaunchWrapperHomeDir
	mcpLaunchWrapperGetenv = func(key string) string { return env[key] }
	mcpLaunchWrapperHomeDir = func() (string, error) { return home, nil }
	t.Cleanup(func() {
		mcpLaunchWrapperGetenv, mcpLaunchWrapperHomeDir = origGetenv, origHomeDir
	})
}

func writePolicyIn(t *testing.T, dir, name, executable string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	content := `{"allowed_argv": [["` + executable + `"]]}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func stableArgs(t *testing.T, args ...string) []string {
	t.Helper()
	return append([]string{"--control-socket", filepath.Join(t.TempDir(), "s.sock")}, args...)
}

// captureStablePolicy runs runStable with a patched supervisor constructor and
// returns the Config it was handed. The constructor returns nil, so runStable's
// panic on the nil supervisor is swallowed after the capture.
func captureStablePolicy(t *testing.T, args ...string) stable.Config {
	t.Helper()
	var captured stable.Config
	orig := newSupervisorFn
	newSupervisorFn = func(cfg stable.Config) *stable.Supervisor {
		captured = cfg
		return nil
	}
	t.Cleanup(func() { newSupervisorFn = orig })
	func() {
		defer func() { _ = recover() }()
		_ = runStable(stableArgs(t, args...))
	}()
	return captured
}

// captureMCPPolicyPath runs runMCP with a patched server constructor and
// returns the policy path forwarded in the MCP server options.
func captureMCPPolicyPath(t *testing.T, args ...string) string {
	t.Helper()
	var captured mcpserver.Options
	orig := newMCPServerFn
	newMCPServerFn = func(opts mcpserver.Options) (*mcpserver.Server, error) {
		captured = opts
		return nil, errors.New("capture only")
	}
	t.Cleanup(func() { newMCPServerFn = orig })
	if exit := runMCP(args); exit != 1 {
		t.Fatalf("runMCP() = %d, want 1 from the capture constructor", exit)
	}
	return captured.MCPLaunchWrapperPolicyPath
}

func TestCommandsResolveLaunchWrapperPolicyThroughSameChain(t *testing.T) {
	home := t.TempDir()
	envPath := writePolicyIn(t, t.TempDir(), "env.json", "/env/exec")
	flagPath := writePolicyIn(t, t.TempDir(), "flag.json", "/flag/exec")
	wellKnownPath := writePolicyIn(t, home, ".avenor/launch-wrapper-policy.json", "/well-known/exec")

	tests := []struct {
		name     string
		env      map[string]string
		args     []string
		wantPath string
		wantExec string
	}{
		{"flag wins", map[string]string{stable.EnvMCPLaunchWrapperPolicy: envPath}, []string{"--mcp-launch-wrapper-policy", flagPath}, flagPath, "/flag/exec"},
		{"environment wins over the well-known file", map[string]string{stable.EnvMCPLaunchWrapperPolicy: envPath}, nil, envPath, "/env/exec"},
		{"the well-known file is the default", nil, nil, wellKnownPath, "/well-known/exec"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stubPolicyEnv(t, tt.env, home)

			cfg := captureStablePolicy(t, tt.args...)
			if cfg.MCPLaunchWrapperPolicy == nil {
				t.Fatalf("stable: MCPLaunchWrapperPolicy = nil, want the policy from %s", tt.wantPath)
			}
			if got := cfg.MCPLaunchWrapperPolicy.AllowedArgv[0][0]; got != tt.wantExec {
				t.Fatalf("stable: allowed executable = %q, want %q", got, tt.wantExec)
			}
			if got := captureMCPPolicyPath(t, tt.args...); got != tt.wantPath {
				t.Fatalf("mcp: forwarded policy path = %q, want %q (stable and mcp must agree)", got, tt.wantPath)
			}
		})
	}
}

func TestCommandsDisableLaunchWrapperPolicyWithoutAnySource(t *testing.T) {
	stubPolicyEnv(t, nil, t.TempDir())

	if cfg := captureStablePolicy(t); cfg.MCPLaunchWrapperPolicy != nil {
		t.Fatalf("stable: MCPLaunchWrapperPolicy = %#v, want nil", cfg.MCPLaunchWrapperPolicy)
	}
	if got := captureMCPPolicyPath(t); got != "" {
		t.Fatalf("mcp: forwarded policy path = %q, want empty", got)
	}
}

func TestCommandsFailOnInvalidWellKnownPolicy(t *testing.T) {
	home := t.TempDir()
	path := filepath.Join(home, ".avenor", "launch-wrapper-policy.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"allowed_argv": [["nice"]]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	stubPolicyEnv(t, nil, home)

	if exit := runStable(stableArgs(t)); exit != 1 {
		t.Fatalf("runStable() = %d, want 1 for an invalid well-known policy", exit)
	}
	if exit := runMCP(nil); exit != 1 {
		t.Fatalf("runMCP() = %d, want 1 for an invalid well-known policy", exit)
	}
}

func TestCommandsFailOnMissingEnvPolicyFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent.json")
	stubPolicyEnv(t, map[string]string{stable.EnvMCPLaunchWrapperPolicy: missing}, t.TempDir())

	if exit := runStable(stableArgs(t)); exit != 1 {
		t.Fatalf("runStable() = %d, want 1 for a missing env policy file", exit)
	}
	if exit := runMCP(nil); exit != 1 {
		t.Fatalf("runMCP() = %d, want 1 for a missing env policy file", exit)
	}
}
