package stable

import (
	"path/filepath"
	"testing"
)

func TestResolveWorkflowControllerDir(t *testing.T) {
	t.Run("configured wins", func(t *testing.T) {
		t.Setenv("XDG_CONFIG_HOME", "/xdg/config")
		t.Setenv("HOME", "/home/u")
		if got := resolveWorkflowControllerDir("/custom/controllers"); got != "/custom/controllers" {
			t.Fatalf("got %q", got)
		}
	})
	t.Run("xdg default", func(t *testing.T) {
		t.Setenv("XDG_CONFIG_HOME", "/xdg/config")
		t.Setenv("HOME", "/home/u")
		want := filepath.Join("/xdg/config", "avenor", "workflow-controllers")
		if got := resolveWorkflowControllerDir(""); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})
	t.Run("home fallback", func(t *testing.T) {
		t.Setenv("XDG_CONFIG_HOME", "")
		t.Setenv("HOME", "/home/u")
		want := filepath.Join("/home/u", ".config", "avenor", "workflow-controllers")
		if got := resolveWorkflowControllerDir(""); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})
	t.Run("relative xdg ignored", func(t *testing.T) {
		t.Setenv("XDG_CONFIG_HOME", "relative/config")
		t.Setenv("HOME", "/home/u")
		want := filepath.Join("/home/u", ".config", "avenor", "workflow-controllers")
		if got := resolveWorkflowControllerDir(""); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})
}
