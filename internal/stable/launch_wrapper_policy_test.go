package stable

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writePolicyFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policy.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadMCPLaunchWrapperPolicyAcceptsValidAllowlist(t *testing.T) {
	t.Parallel()
	path := writePolicyFile(t, `{"allowed_argv": [["/usr/bin/nice", "-n", "5"]]}`)
	policy, err := LoadMCPLaunchWrapperPolicy(path)
	if err != nil {
		t.Fatalf("LoadMCPLaunchWrapperPolicy() = %v, want nil", err)
	}
	if len(policy.AllowedArgv) != 1 || len(policy.AllowedArgv[0]) != 3 {
		t.Fatalf("policy = %#v, want one 3-element entry", policy.AllowedArgv)
	}
	if !mcpLaunchWrapperAllowed(policy, []string{"/usr/bin/nice", "-n", "5"}) {
		t.Fatal("exact-match wrapper was not allowed")
	}
}

func TestLoadMCPLaunchWrapperPolicyRejects(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{"unknown field", `{"allowed_argv": [["/usr/bin/nice"]], "extra": 1}`, "unknown field"},
		{"trailing data", `{"allowed_argv": [["/usr/bin/nice"]]} {"x":1}`, "trailing data"},
		{"empty allowlist", `{"allowed_argv": []}`, "at least one entry"},
		{"missing allowlist", `{}`, "at least one entry"},
		{"empty entry", `{"allowed_argv": [[]]}`, "entry 0 is empty"},
		{"empty element", `{"allowed_argv": [["/usr/bin/nice", ""]]}`, "element 1 is empty"},
		{"NUL byte", "{\"allowed_argv\": [[\"/usr/bin/n\\u0000\"]]}", "NUL byte"},
		{"NUL byte in non-executable element", "{\"allowed_argv\": [[\"/usr/bin/nice\", \"a\\u0000b\"]]}", "NUL byte"},
		{"relative executable", `{"allowed_argv": [["nice", "-n", "5"]]}`, "absolute path"},
		{"not JSON", `nope`, "invalid character"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writePolicyFile(t, tt.content)
			_, err := LoadMCPLaunchWrapperPolicy(path)
			if err == nil {
				t.Fatalf("LoadMCPLaunchWrapperPolicy() = nil, want error containing %q", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %q, want containing %q", err, tt.want)
			}
		})
	}
}

func TestLoadMCPLaunchWrapperPolicyMissingFile(t *testing.T) {
	t.Parallel()
	if _, err := LoadMCPLaunchWrapperPolicy(filepath.Join(t.TempDir(), "absent.json")); err == nil {
		t.Fatal("missing file accepted")
	}
}

func TestLoadMCPLaunchWrapperPolicyAcceptsRelativeNonExecutableArgs(t *testing.T) {
	t.Parallel()
	path := writePolicyFile(t, `{"allowed_argv": [["/usr/bin/nice", "relative-arg"], ["/bin/sh"]]}`)
	policy, err := LoadMCPLaunchWrapperPolicy(path)
	if err != nil {
		t.Fatalf("LoadMCPLaunchWrapperPolicy() = %v, want nil", err)
	}
	if len(policy.AllowedArgv) != 2 {
		t.Fatalf("policy entries = %d, want 2 (multi-entry allowlists preserved)", len(policy.AllowedArgv))
	}
	if !mcpLaunchWrapperAllowed(policy, []string{"/bin/sh"}) {
		t.Fatal("second allowlist entry not matched")
	}
	if mcpLaunchWrapperAllowed(policy, []string{"/usr/bin/nice", "other"}) {
		t.Fatal("unmatched wrapper allowed")
	}
}

func TestMcpLaunchWrapperAllowedRequiresExactMatch(t *testing.T) {
	t.Parallel()
	policy := &MCPLaunchWrapperPolicy{AllowedArgv: [][]string{{"/usr/bin/nice", "-n", "5"}}}
	cases := []struct {
		name    string
		wrapper []string
		want    bool
	}{
		{"exact", []string{"/usr/bin/nice", "-n", "5"}, true},
		{"basename vs absolute", []string{"nice", "-n", "5"}, false},
		{"changed flag value", []string{"/usr/bin/nice", "-n", "10"}, false},
		{"reordered", []string{"-n", "5", "/usr/bin/nice"}, false},
		{"extra arg", []string{"/usr/bin/nice", "-n", "5", "x"}, false},
		{"missing arg", []string{"/usr/bin/nice", "-n"}, false},
		{"different executable", []string{"/bin/sh", "-n", "5"}, false},
		{"empty wrapper", nil, false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if got := mcpLaunchWrapperAllowed(policy, tt.wrapper); got != tt.want {
				t.Fatalf("mcpLaunchWrapperAllowed(%v) = %v, want %v", tt.wrapper, got, tt.want)
			}
		})
	}
	if mcpLaunchWrapperAllowed(nil, []string{"/usr/bin/nice"}) {
		t.Fatal("nil policy must not allow anything")
	}
}
