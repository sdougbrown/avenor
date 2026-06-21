package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadAgentModel(t *testing.T) {
	dir := t.TempDir()
	writeJSON := func(name string, v any) string {
		path := filepath.Join(dir, name)
		data, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		return path
	}

	t.Run("finds agent model", func(t *testing.T) {
		path := writeJSON("opencode.json", map[string]any{
			"agent": map[string]any{
				"jockey": map[string]any{"model": "deepseek-v4"},
			},
		})
		model, err := readAgentModel(path, "jockey")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if model != "deepseek-v4" {
			t.Errorf("model = %q, want deepseek-v4", model)
		}
	})

	t.Run("missing file returns empty", func(t *testing.T) {
		model, err := readAgentModel(filepath.Join(dir, "nonexistent.json"), "jockey")
		if err != nil {
			t.Fatalf("missing file should not error (ENOENT is expected): %v", err)
		}
		if model != "" {
			t.Errorf("model = %q, want empty", model)
		}
	})

	t.Run("unreadable file returns error", func(t *testing.T) {
		// Create a directory where a file would be — ReadFile on a dir fails with EISDIR
		subdir := filepath.Join(dir, "not-a-file")
		if err := os.Mkdir(subdir, 0o700); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		_, err := readAgentModel(subdir, "jockey")
		if err == nil {
			t.Fatal("expected read error for unreadable path, got nil")
		}
		if !strings.Contains(err.Error(), "read opencode config") {
			t.Errorf("error = %v, want 'read opencode config' prefix", err)
		}
	})

	t.Run("malformed JSON returns parse error", func(t *testing.T) {
		path := filepath.Join(dir, "broken.json")
		if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		_, err := readAgentModel(path, "jockey")
		if err == nil {
			t.Fatal("expected parse error, got nil")
		}
		if !strings.Contains(err.Error(), "parse opencode config") {
			t.Errorf("error = %v, want 'parse opencode config' prefix", err)
		}
	})

	t.Run("agent not configured returns empty", func(t *testing.T) {
		path := writeJSON("opencode.json", map[string]any{
			"agent": map[string]any{
				"other": map[string]any{"model": "gpt-4"},
			},
		})
		model, err := readAgentModel(path, "jockey")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if model != "" {
			t.Errorf("model = %q, want empty", model)
		}
	})

	t.Run("agent has no model returns empty", func(t *testing.T) {
		path := writeJSON("opencode.json", map[string]any{
			"agent": map[string]any{
				"jockey": map[string]any{},
			},
		})
		model, err := readAgentModel(path, "jockey")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if model != "" {
			t.Errorf("model = %q, want empty", model)
		}
	})
}

func TestOpenCodeConfigPaths(t *testing.T) {
	t.Run("uses env var when set", func(t *testing.T) {
		got := opencodeConfigPaths(func(key string) string {
			if key == "OPENCODE_CONFIG_DIR" {
				return "/custom/config"
			}
			return ""
		})
		want := []string{filepath.Join("/custom/config", "opencode.json"), filepath.Join("/custom/config", "opencode.jsonc")}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("opencodeConfigPaths() = %v, want %v", got, want)
		}
	})

	t.Run("uses explicit config file first", func(t *testing.T) {
		got := opencodeConfigPaths(func(key string) string {
			if key == "OPENCODE_CONFIG" {
				return "/custom/opencode.json"
			}
			if key == "OPENCODE_CONFIG_DIR" {
				return "/custom/config"
			}
			return ""
		})
		if len(got) != 1 || got[0] != "/custom/opencode.json" {
			t.Errorf("opencodeConfigPaths() = %v, want explicit file", got)
		}
	})

	t.Run("uses XDG_CONFIG_HOME", func(t *testing.T) {
		got := opencodeConfigPaths(func(key string) string {
			if key == "XDG_CONFIG_HOME" {
				return "/xdg"
			}
			return ""
		})
		want := []string{filepath.Join("/xdg", "opencode", "opencode.json"), filepath.Join("/xdg", "opencode", "opencode.jsonc")}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("opencodeConfigPaths() = %v, want %v", got, want)
		}
	})

	t.Run("falls back to ~/.config/opencode", func(t *testing.T) {
		got := opencodeConfigPaths(func(string) string { return "" })
		home, _ := os.UserHomeDir()
		want := []string{filepath.Join(home, ".config", "opencode", "opencode.json"), filepath.Join(home, ".config", "opencode", "opencode.jsonc")}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("opencodeConfigPaths() = %v, want %v", got, want)
		}
	})
}

func TestResolveAgentModel(t *testing.T) {
	dir := t.TempDir()
	writeConfig := func(name string, v any) string {
		path := filepath.Join(dir, name)
		data, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		return dir
	}

	t.Run("resolves model from config", func(t *testing.T) {
		dir := writeConfig("opencode.json", map[string]any{
			"agent": map[string]any{
				"jockey": map[string]any{"model": "deepseek-v4-flash"},
			},
		})

		model, err := resolveAgentModel("jockey", "opencode-acp", func(key string) string {
			if key == "OPENCODE_CONFIG_DIR" {
				return dir
			}
			return ""
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if model != "deepseek-v4-flash" {
			t.Errorf("model = %q, want deepseek-v4-flash", model)
		}
	})

	t.Run("tries .jsonc after .json", func(t *testing.T) {
		dir := writeConfig("opencode.jsonc", map[string]any{
			"agent": map[string]any{
				"horse": map[string]any{"model": "gpt-5"},
			},
		})

		model, err := resolveAgentModel("horse", "opencode-acp", func(key string) string {
			if key == "OPENCODE_CONFIG_DIR" {
				return dir
			}
			return ""
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if model != "gpt-5" {
			t.Errorf("model = %q, want gpt-5", model)
		}
	})

	t.Run("returns error on malformed config", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "opencode.json"), []byte("{broken"), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}

		_, err := resolveAgentModel("jockey", "opencode-acp", func(key string) string {
			if key == "OPENCODE_CONFIG_DIR" {
				return dir
			}
			return ""
		})
		if err == nil {
			t.Fatal("expected parse error, got nil")
		}
	})

	t.Run("returns empty when no config dir", func(t *testing.T) {
		model, err := resolveAgentModel("jockey", "opencode-acp", func(key string) string {
			if key == "OPENCODE_CONFIG_DIR" {
				return "/nonexistent/dir"
			}
			return ""
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if model != "" {
			t.Errorf("model = %q, want empty", model)
		}
	})

	t.Run("returns empty when agent not in config", func(t *testing.T) {
		dir := writeConfig("opencode.json", map[string]any{
			"agent": map[string]any{},
		})

		model, err := resolveAgentModel("nonexistent", "opencode-acp", func(key string) string {
			if key == "OPENCODE_CONFIG_DIR" {
				return dir
			}
			return ""
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if model != "" {
			t.Errorf("model = %q, want empty", model)
		}
	})

	t.Run("resolves from pi agents.json", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "agents.json")
		data, _ := json.Marshal(map[string]any{
			"reviewer": map[string]any{"model": "anthropic/claude-sonnet-4"},
		})
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatalf("write agents.json: %v", err)
		}

		model, err := resolveAgentModel("reviewer", "pi", func(key string) string {
			if key == "PI_CODING_AGENT_DIR" {
				return dir
			}
			return ""
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if model != "anthropic/claude-sonnet-4" {
			t.Errorf("model = %q, want anthropic/claude-sonnet-4", model)
		}
	})

	t.Run("pi agents.json missing", func(t *testing.T) {
		dir := t.TempDir()
		// Don't create agents.json — dir exists but file doesn't.

		model, err := resolveAgentModel("reviewer", "pi", func(key string) string {
			if key == "PI_CODING_AGENT_DIR" {
				return dir
			}
			return ""
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if model != "" {
			t.Errorf("model = %q, want empty", model)
		}
	})

	t.Run("pi agents.json malformed", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "agents.json")
		if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
			t.Fatalf("write agents.json: %v", err)
		}

		model, err := resolveAgentModel("reviewer", "pi", func(key string) string {
			if key == "PI_CODING_AGENT_DIR" {
				return dir
			}
			return ""
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if model != "" {
			t.Errorf("model = %q, want empty (malformed config should not break resolution)", model)
		}
	})

	t.Run("pi agent not in config", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "agents.json")
		data, _ := json.Marshal(map[string]any{
			"other-agent": map[string]any{"model": "some-model"},
		})
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatalf("write agents.json: %v", err)
		}

		model, err := resolveAgentModel("reviewer", "pi", func(key string) string {
			if key == "PI_CODING_AGENT_DIR" {
				return dir
			}
			return ""
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if model != "" {
			t.Errorf("model = %q, want empty", model)
		}
	})
}
