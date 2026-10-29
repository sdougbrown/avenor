package appdir

import (
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
	if got := StateHome(); got == "relative/state" {
		t.Fatalf("StateHome() = %q, want the home default for a relative XDG_STATE_HOME", got)
	}
}

// No XDG_STATE_HOME and no HOME: the relative "avenor" keeps the documented
// fallback shape.
func TestStateHomeNoHomeFallback(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", "")
	t.Setenv("HOME", "")
	if got, want := StateHome(), filepath.Join("avenor"); got != want {
		t.Fatalf("StateHome() = %q, want %q", got, want)
	}
}
