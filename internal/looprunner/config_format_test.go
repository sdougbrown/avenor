package looprunner

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLoadLoopConfigYAMLAndTOML confirms that loop configs authored in YAML
// and TOML produce the same decoded result as the JSON equivalent.
func TestLoadLoopConfigYAMLAndTOML(t *testing.T) {
	t.Parallel()

	jsonData := `{
		"max_iterations": 3,
		"loop": [{"name":"test","prompt":"run tests"}]
	}`
	yamlData := "max_iterations: 3\nloop:\n  - name: test\n    prompt: run tests\n"
	tomlData := `max_iterations = 3
[[loop]]
name = "test"
prompt = "run tests"
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
			cfg, err := LoadLoopConfig(path)
			if err != nil {
				t.Fatalf("LoadLoopConfig() error = %v", err)
			}
			if cfg.MaxIterations != 3 {
				t.Fatalf("MaxIterations = %d, want 3", cfg.MaxIterations)
			}
			if len(cfg.Loop) != 1 || cfg.Loop[0].Name != "test" {
				t.Fatalf("loop = %+v, want 1 phase named test", cfg.Loop)
			}
		})
	}
}

// TestLoadLoopConfigYAMLRejectsUnknownFields confirms that DisallowUnknownFields
// semantics carry through the YAML normalization path.
func TestLoadLoopConfigYAMLRejectsUnknownFields(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	data := "max_iterations: 3\nloop:\n  - name: test\n    prompt: run tests\nbogus: true\n"
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadLoopConfig(path); err == nil {
		t.Fatal("expected unknown-field error for YAML, got nil")
	}
}