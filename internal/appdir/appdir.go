// Package appdir resolves avenor's user-level config directory:
// $XDG_CONFIG_HOME/avenor when XDG_CONFIG_HOME is set to an absolute path,
// else ~/.config/avenor. Every consumer of a file under that directory
// (workflow adapters, the model provider registry) resolves through here so
// the fallback rules cannot silently diverge.
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
	home := os.Getenv("HOME")
	if home == "" {
		if u, err := os.UserHomeDir(); err == nil && u != "" {
			home = u
		}
	}
	if home == "" {
		return "avenor"
	}
	return filepath.Join(home, ".config", "avenor")
}
