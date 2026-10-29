// Package appdir resolves avenor's user-level directories: config under
// $XDG_CONFIG_HOME/avenor when XDG_CONFIG_HOME is set to an absolute path,
// else ~/.config/avenor, and state under $XDG_STATE_HOME/avenor, else
// ~/.local/state/avenor. Every consumer of a file under one of these
// directories (workflow adapters, the model provider registry, persisted pi
// sessions) resolves through here so the fallback rules cannot silently
// diverge.
package appdir

import (
	"os"
	"path/filepath"
)

// ConfigHome returns the avenor config directory, never creating it. When
// neither XDG_CONFIG_HOME nor the home directory resolves, the relative
// "avenor" keeps the caller's documented default shape.
func ConfigHome() string {
	if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" && filepath.IsAbs(xdg) {
		return filepath.Join(xdg, "avenor")
	}
	return underHome(".config", "avenor")
}

// homeDir resolves the user's home directory: HOME, then os.UserHomeDir,
// then the empty string that makes homePath fall back to the bare relative
// shape.
func homeDir() string {
	home := os.Getenv("HOME")
	if home == "" {
		if u, err := os.UserHomeDir(); err == nil && u != "" {
			home = u
		}
	}
	return home
}

// underHome joins the home directory with the relative path. When no home
// resolves, the relative "avenor" is the documented default shape, shared by
// both directories.
func underHome(rel ...string) string {
	home := homeDir()
	if home == "" {
		return "avenor"
	}
	return filepath.Join(append([]string{home}, rel...)...)
}

// StateHome returns the avenor state directory, never creating it. Files
// here outlive any run and are never pruned with run artifacts, so only
// data a user may want later belongs under it.
func StateHome() string {
	if xdg := os.Getenv("XDG_STATE_HOME"); xdg != "" && filepath.IsAbs(xdg) {
		return filepath.Join(xdg, "avenor")
	}
	return underHome(".local", "state", "avenor")
}
