package stable

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writePolicyAt writes a valid one-entry policy file named name inside dir.
func writePolicyAt(t *testing.T, dir, name, executable string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	content := fmt.Sprintf(`{"allowed_argv": [[%q]]}`, executable)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func fixedGetenv(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func fixedHomeDir(home string) func() (string, error) {
	return func() (string, error) { return home, nil }
}

func wellKnownPolicyPath(home string) string {
	return filepath.Join(home, ".avenor", "launch-wrapper-policy.json")
}

func TestResolveMCPLaunchWrapperPolicyPriority(t *testing.T) {
	dir := t.TempDir()
	flagPath := writePolicyAt(t, dir, "flag.json", "/flag/exec")
	envPath := writePolicyAt(t, dir, "env.json", "/env/exec")
	home := filepath.Join(dir, "home")
	wellKnownPath := writePolicyAt(t, home, ".avenor/launch-wrapper-policy.json", "/well-known/exec")

	env := fixedGetenv(map[string]string{EnvMCPLaunchWrapperPolicy: envPath})
	homeDir := fixedHomeDir(home)

	tests := []struct {
		name         string
		explicitPath string
		wantPath     string
	}{
		{"flag beats env and well-known", flagPath, flagPath},
		{"env beats well-known", "", envPath},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy, path, err := ResolveMCPLaunchWrapperPolicy(tt.explicitPath, env, homeDir)
			if err != nil {
				t.Fatalf("ResolveMCPLaunchWrapperPolicy() error = %v, want nil", err)
			}
			if path != tt.wantPath {
				t.Fatalf("path = %q, want %q", path, tt.wantPath)
			}
			if policy == nil || len(policy.AllowedArgv) != 1 {
				t.Fatalf("policy = %#v, want one entry", policy)
			}
		})
	}

	t.Run("well-known when nothing else names a path", func(t *testing.T) {
		policy, path, err := ResolveMCPLaunchWrapperPolicy("", fixedGetenv(nil), homeDir)
		if err != nil {
			t.Fatalf("ResolveMCPLaunchWrapperPolicy() error = %v, want nil", err)
		}
		if path != wellKnownPath {
			t.Fatalf("path = %q, want %q", path, wellKnownPath)
		}
		if policy == nil || policy.AllowedArgv[0][0] != "/well-known/exec" {
			t.Fatalf("policy = %#v, want the well-known file's entry", policy)
		}
	})
}

func TestResolveMCPLaunchWrapperPolicyAbsentWellKnownFileDisables(t *testing.T) {
	policy, path, err := ResolveMCPLaunchWrapperPolicy("", fixedGetenv(nil), fixedHomeDir(t.TempDir()))
	if err != nil {
		t.Fatalf("ResolveMCPLaunchWrapperPolicy() error = %v, want nil", err)
	}
	if policy != nil || path != "" {
		t.Fatalf("policy = %#v, path = %q, want nil policy and empty path", policy, path)
	}
}

func TestResolveMCPLaunchWrapperPolicyWhitespaceOnlyEnvDisables(t *testing.T) {
	env := fixedGetenv(map[string]string{EnvMCPLaunchWrapperPolicy: "  "})
	policy, path, err := ResolveMCPLaunchWrapperPolicy("", env, fixedHomeDir(t.TempDir()))
	if err != nil {
		t.Fatalf("ResolveMCPLaunchWrapperPolicy() error = %v, want nil", err)
	}
	if policy != nil || path != "" {
		t.Fatalf("policy = %#v, path = %q, want nil policy and empty path", policy, path)
	}
}

func TestResolveMCPLaunchWrapperPolicyUnresolvableHomeDisables(t *testing.T) {
	homeErr := func() (string, error) { return "", os.ErrNotExist }
	policy, path, err := ResolveMCPLaunchWrapperPolicy("", fixedGetenv(nil), homeErr)
	if err != nil {
		t.Fatalf("ResolveMCPLaunchWrapperPolicy() error = %v, want nil", err)
	}
	if policy != nil || path != "" {
		t.Fatalf("policy = %#v, path = %q, want nil policy and empty path", policy, path)
	}
}

func TestResolveMCPLaunchWrapperPolicyInvalidWellKnownFileErrors(t *testing.T) {
	home := t.TempDir()
	path := wellKnownPolicyPath(home)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	// Relative executable: rejected by strict validation.
	if err := os.WriteFile(path, []byte(`{"allowed_argv": [["nice"]]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	policy, resolved, err := ResolveMCPLaunchWrapperPolicy("", fixedGetenv(nil), fixedHomeDir(home))
	if err == nil {
		t.Fatal("ResolveMCPLaunchWrapperPolicy() = nil error, want validation failure")
	}
	if !strings.Contains(err.Error(), "absolute path") {
		t.Fatalf("error = %q, want containing %q", err, "absolute path")
	}
	if !strings.Contains(err.Error(), path) {
		t.Fatalf("error = %q, want it to name %q", err, path)
	}
	if policy != nil || resolved != "" {
		t.Fatalf("policy = %#v, path = %q, want nil policy and empty path on error", policy, resolved)
	}
}

func TestResolveMCPLaunchWrapperPolicyMissingNamedFileErrors(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent.json")
	home := t.TempDir()
	writePolicyAt(t, home, ".avenor/launch-wrapper-policy.json", "/well-known/exec")

	t.Run("flag", func(t *testing.T) {
		// A missing flag path fails even though the well-known file would load.
		_, _, err := ResolveMCPLaunchWrapperPolicy(missing, fixedGetenv(nil), fixedHomeDir(home))
		if err == nil {
			t.Fatal("ResolveMCPLaunchWrapperPolicy() = nil error, want missing-file failure")
		}
		if !strings.Contains(err.Error(), missing) {
			t.Fatalf("error = %q, want it to name %q", err, missing)
		}
	})

	t.Run("env", func(t *testing.T) {
		env := fixedGetenv(map[string]string{EnvMCPLaunchWrapperPolicy: missing})
		_, _, err := ResolveMCPLaunchWrapperPolicy("", env, fixedHomeDir(home))
		if err == nil {
			t.Fatal("ResolveMCPLaunchWrapperPolicy() = nil error, want missing-file failure")
		}
		if !strings.Contains(err.Error(), missing) {
			t.Fatalf("error = %q, want it to name %q", err, missing)
		}
	})
}
