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
