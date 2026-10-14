package stable

import (
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
)

func mcpTestSupervisor(t *testing.T, policy *MCPLaunchWrapperPolicy) (*Supervisor, *[]string) {
	t.Helper()
	sup := NewSupervisor(Config{
		ControlSocket:          "/tmp/launch-wrapper-mcp-test.sock",
		MaxRuntimes:            1,
		MCPLaunchWrapperPolicy: policy,
	})
	var got []string
	sup.newProviderFunc = func(startOpts runtime.StartOptions, _ string) (runtime.Provider, error) {
		got = startOpts.LaunchWrapper
		return scriptedStage5Provider("ses_mcp_wrapper", "end_turn"), nil
	}
	t.Cleanup(func() { sup.Shutdown("kill") })
	return sup, &got
}

func runtimeCount(sup *Supervisor) int {
	sup.controlMu.Lock()
	defer sup.controlMu.Unlock()
	return len(sup.runtimes)
}

// TestSpawnMCPDeniesWrapperWithoutPolicy pins the default-deny posture: a
// supervisor without an MCP policy rejects every MCP spawn carrying a
// launch_wrapper, creates no process, and registers no runtime.
func TestSpawnMCPDeniesWrapperWithoutPolicy(t *testing.T) {
	sup, got := mcpTestSupervisor(t, nil)
	_, err := sup.SpawnMCP([]byte(`{"dir":"` + t.TempDir() + `","launch_wrapper":["nice","-n","5"]}`))
	if err == nil {
		t.Fatal("SpawnMCP() = nil, want disabled error")
	}
	if err.Error() != "launch_wrapper: disabled for MCP on this supervisor" {
		t.Fatalf("error = %q, want exact disabled error", err)
	}
	if len(*got) != 0 {
		t.Fatal("provider was created for a denied spawn")
	}
	if runtimeCount(sup) != 0 {
		t.Fatal("runtime was registered for a denied spawn")
	}
}

// TestSpawnMCPAllowlistEnforcement covers exact-match authorization and the
// denial of every non-exact variant.
func TestSpawnMCPAllowlistEnforcement(t *testing.T) {
	policy := &MCPLaunchWrapperPolicy{AllowedArgv: [][]string{{"/usr/bin/nice", "-n", "5"}}}
	cases := []struct {
		name    string
		wrapper []string
		wantErr string
	}{
		{"exact match", []string{"/usr/bin/nice", "-n", "5"}, ""},
		{"basename vs absolute", []string{"nice", "-n", "5"}, "launch_wrapper: argv is not allowed by this supervisor"},
		{"changed flag value", []string{"/usr/bin/nice", "-n", "10"}, "launch_wrapper: argv is not allowed by this supervisor"},
		{"reordered args", []string{"-n", "5", "/usr/bin/nice"}, "launch_wrapper: argv is not allowed by this supervisor"},
		{"extra arg", []string{"/usr/bin/nice", "-n", "5", "x"}, "launch_wrapper: argv is not allowed by this supervisor"},
		{"missing arg", []string{"/usr/bin/nice", "-n"}, "launch_wrapper: argv is not allowed by this supervisor"},
		{"different executable", []string{"/bin/sh", "-n", "5"}, "launch_wrapper: argv is not allowed by this supervisor"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			sup, got := mcpTestSupervisor(t, policy)
			raw := `{"dir":"` + t.TempDir() + `","prompt":"hi","launch_wrapper":[`
			for i, el := range tt.wrapper {
				if i > 0 {
					raw += ","
				}
				raw += `"` + el + `"`
			}
			raw += `]}`
			_, err := sup.SpawnMCP([]byte(raw))
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("SpawnMCP() = %v, want nil", err)
				}
				if strings.Join(*got, " ") != strings.Join(tt.wrapper, " ") {
					t.Fatalf("provider wrapper = %v, want %v", *got, tt.wrapper)
				}
				return
			}
			if err == nil || err.Error() != tt.wantErr {
				t.Fatalf("SpawnMCP() error = %v, want %q", err, tt.wantErr)
			}
			if len(*got) != 0 {
				t.Fatal("provider was created for a denied wrapper")
			}
			if runtimeCount(sup) != 0 {
				t.Fatal("runtime was registered for a denied wrapper")
			}
		})
	}
}

// TestSpawnMCPUnwrappedAlwaysAllowed proves an MCP spawn without a wrapper is
// never gated by the policy.
func TestSpawnMCPUnwrappedAlwaysAllowed(t *testing.T) {
	sup, got := mcpTestSupervisor(t, nil)
	if _, err := sup.SpawnMCP([]byte(`{"dir":"` + t.TempDir() + `","prompt":"hi"}`)); err != nil {
		t.Fatalf("SpawnMCP() = %v, want nil", err)
	}
	if len(*got) != 0 {
		t.Fatal("unwrapped spawn must not carry a wrapper")
	}
}
