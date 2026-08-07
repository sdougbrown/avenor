package teamrunner

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLoadTeamConfigYAMLAndTOML confirms that team configs authored in YAML
// and TOML produce the same decoded result as the JSON equivalent.
func TestLoadTeamConfigYAMLAndTOML(t *testing.T) {
	t.Parallel()

	jsonData := `{
		"pre": [{"name":"setup","prompt":"init"}],
		"team": [{"name":"work","prompt":"do work"}]
	}`
	yamlData := "pre:\n  - name: setup\n    prompt: init\nteam:\n  - name: work\n    prompt: do work\n"
	tomlData := `[[pre]]
name = "setup"
prompt = "init"
[[team]]
name = "work"
prompt = "do work"
`

	for _, tc := range []struct {
		name string
		ext  string
		data string
	}{
		{"json", ".json", jsonData},
		{"yaml", ".yaml", yamlData},
		{"toml", ".toml", tomlData},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			path := filepath.Join(dir, "config"+tc.ext)
			if err := os.WriteFile(path, []byte(tc.data), 0o644); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadTeamConfig(path)
			if err != nil {
				t.Fatalf("LoadTeamConfig() error = %v", err)
			}
			if len(cfg.Pre) != 1 || cfg.Pre[0].Name != "setup" {
				t.Fatalf("pre = %+v, want 1 phase named setup", cfg.Pre)
			}
			if len(cfg.Team) != 1 || cfg.Team[0].Name != "work" {
				t.Fatalf("team = %+v, want 1 phase named work", cfg.Team)
			}
		})
	}
}

// TestLoadTeamConfigYAMLRejectsUnknownFields confirms that DisallowUnknownFields
// semantics carry through the YAML normalization path.
func TestLoadTeamConfigYAMLRejectsUnknownFields(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	data := "team:\n  - name: work\n    prompt: work\nbogus: true\n"
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadTeamConfig(path); err == nil {
		t.Fatal("expected unknown-field error for YAML, got nil")
	}
}