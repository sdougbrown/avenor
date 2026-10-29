package appdir

import (
	"os"
	"path/filepath"
	"testing"
)

// The no-home fallback: neither XDG_CONFIG_HOME nor HOME resolves, and the
// relative "avenor" default keeps the caller's documented shape.
func TestConfigHomeNoHomeFallback(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("HOME", "")
	if got, want := ConfigHome(), filepath.Join("avenor"); got != want {
		t.Fatalf("ConfigHome() = %q, want %q", got, want)
	}
}

// XDG_STATE_HOME set to an absolute path wins over the home default; a
// relative value is ignored, matching ConfigHome's rule.
func TestStateHomeXDGAbsolute(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", "/state")
	if got, want := StateHome(), filepath.Join("/state", "avenor"); got != want {
		t.Fatalf("StateHome() = %q, want %q", got, want)
	}
	t.Setenv("XDG_STATE_HOME", "relative/state")
	t.Setenv("HOME", t.TempDir())
	want := filepath.Join(os.Getenv("HOME"), ".avenor")
	if got := StateHome(); got != want {
		t.Fatalf("StateHome() = %q, want the home default %q for a relative XDG_STATE_HOME", got, want)
	}
}

// No XDG_STATE_HOME and no HOME: the relative "avenor" keeps the documented
// fallback shape. The guard skips hosts where os.UserHomeDir() still resolves
// with HOME unset (a populated /etc/passwd), since the fallback then cannot
// be exercised.
func TestStateHomeNoHomeFallback(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", "")
	t.Setenv("HOME", "")
	if u, err := os.UserHomeDir(); err == nil && u != "" {
		t.Skipf("os.UserHomeDir resolves %q without HOME; the fallback is unreachable here", u)
	}
	if got, want := StateHome(), filepath.Join("avenor"); got != want {
		t.Fatalf("StateHome() = %q, want %q", got, want)
	}
}
