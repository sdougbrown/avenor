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

func fixedConfigHome(dir string) func() string {
	return func() string { return dir }
}

func wellKnownPolicyPath(configHome string) string {
	return filepath.Join(configHome, "launch-wrapper-policy.json")
}

func TestResolveMCPLaunchWrapperPolicyPriority(t *testing.T) {
	dir := t.TempDir()
	flagPath := writePolicyAt(t, dir, "flag.json", "/flag/exec")
	envPath := writePolicyAt(t, dir, "env.json", "/env/exec")
	configHome := filepath.Join(dir, "config-home")
	wellKnownPath := writePolicyAt(t, configHome, "launch-wrapper-policy.json", "/well-known/exec")

	env := fixedGetenv(map[string]string{EnvMCPLaunchWrapperPolicy: envPath})
	configHomeFn := fixedConfigHome(configHome)

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
			policy, path, err := ResolveMCPLaunchWrapperPolicy(tt.explicitPath, env, configHomeFn)
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
		policy, path, err := ResolveMCPLaunchWrapperPolicy("", fixedGetenv(nil), configHomeFn)
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
	policy, path, err := ResolveMCPLaunchWrapperPolicy("", fixedGetenv(nil), fixedConfigHome(t.TempDir()))
	if err != nil {
		t.Fatalf("ResolveMCPLaunchWrapperPolicy() error = %v, want nil", err)
	}
	if policy != nil || path != "" {
		t.Fatalf("policy = %#v, path = %q, want nil policy and empty path", policy, path)
	}
}

func TestResolveMCPLaunchWrapperPolicyWhitespaceOnlyEnvDisables(t *testing.T) {
	env := fixedGetenv(map[string]string{EnvMCPLaunchWrapperPolicy: "  "})
	policy, path, err := ResolveMCPLaunchWrapperPolicy("", env, fixedConfigHome(t.TempDir()))
	if err != nil {
		t.Fatalf("ResolveMCPLaunchWrapperPolicy() error = %v, want nil", err)
	}
	if policy != nil || path != "" {
		t.Fatalf("policy = %#v, path = %q, want nil policy and empty path", policy, path)
	}
}

func TestResolveMCPLaunchWrapperPolicyEmptyConfigHomeDisables(t *testing.T) {
	policy, path, err := ResolveMCPLaunchWrapperPolicy("", fixedGetenv(nil), func() string { return "" })
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
	policy, resolved, err := ResolveMCPLaunchWrapperPolicy("", fixedGetenv(nil), fixedConfigHome(home))
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
	writePolicyAt(t, home, "launch-wrapper-policy.json", "/well-known/exec")

	t.Run("flag", func(t *testing.T) {
		// A missing flag path fails even though the well-known file would load.
		_, _, err := ResolveMCPLaunchWrapperPolicy(missing, fixedGetenv(nil), fixedConfigHome(home))
		if err == nil {
			t.Fatal("ResolveMCPLaunchWrapperPolicy() = nil error, want missing-file failure")
		}
		if !strings.Contains(err.Error(), missing) {
			t.Fatalf("error = %q, want it to name %q", err, missing)
		}
	})

	t.Run("env", func(t *testing.T) {
		env := fixedGetenv(map[string]string{EnvMCPLaunchWrapperPolicy: missing})
		_, _, err := ResolveMCPLaunchWrapperPolicy("", env, fixedConfigHome(home))
		if err == nil {
			t.Fatal("ResolveMCPLaunchWrapperPolicy() = nil error, want missing-file failure")
		}
		if !strings.Contains(err.Error(), missing) {
			t.Fatalf("error = %q, want it to name %q", err, missing)
		}
	})
}

func TestResolveMCPLaunchWrapperPolicyWellKnownStatErrorFails(t *testing.T) {
	dir := t.TempDir()
	notADir := filepath.Join(dir, "not-a-dir")
	if err := os.WriteFile(notADir, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Joining a file as a directory yields ENOTDIR, which is not fs.ErrNotExist:
	// the chain must fail closed rather than silently disable the policy.
	policy, path, err := ResolveMCPLaunchWrapperPolicy("", fixedGetenv(nil), fixedConfigHome(notADir))
	if err == nil {
		t.Fatal("ResolveMCPLaunchWrapperPolicy() = nil error, want stat failure")
	}
	if policy != nil || path != "" {
		t.Fatalf("policy = %#v, path = %q, want nil policy and empty path on error", policy, path)
	}
}
