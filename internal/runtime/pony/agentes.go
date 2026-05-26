package pony

import (
	"os"
	"path/filepath"
)

// LoadAgentsMD walks up from dir to find an AGENTS.md file.
// Returns the content if found, or empty string if not found (not an error).
func LoadAgentsMD(dir string) string {
	current := dir
	for {
		path := filepath.Join(current, "AGENTS.md")
		if data, err := os.ReadFile(path); err == nil {
			return string(data)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "" // reached filesystem root
		}
		current = parent
	}
}
