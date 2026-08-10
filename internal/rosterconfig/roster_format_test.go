package rosterconfig

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLoadYAMLAndTOML confirms that roster files authored in YAML and TOML
// produce the same decoded result as the JSON equivalent.
func TestLoadYAMLAndTOML(t *testing.T) {
	jsonData := `{
		"planner": {"backend": "opencode-acp", "agent": "planner", "model": "provider/planner"},
		"executor": {"backend": "agy", "agent": "windsurf-swe"}
	}`
	yamlData := "planner:\n  backend: opencode-acp\n  agent: planner\n  model: provider/planner\nexecutor:\n  backend: agy\n  agent: windsurf-swe\n"
	tomlData := `[planner]
backend = "opencode-acp"
agent = "planner"
model = "provider/planner"
[executor]
backend = "agy"
agent = "windsurf-swe"
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
			path := filepath.Join(t.TempDir(), "roster"+tc.ext)
			if err := os.WriteFile(path, []byte(tc.data), 0o644); err != nil {
				t.Fatal(err)
			}
			config, err := Load(path)
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}

			planner, err := config.Lookup("planner")
			if err != nil {
				t.Fatalf("Lookup(planner) error = %v", err)
			}
			want := Entry{Backend: "opencode-acp", Agent: "planner", Model: "provider/planner"}
			if planner != want {
				t.Fatalf("planner = %+v, want %+v", planner, want)
			}

			executor, err := config.Lookup("executor")
			if err != nil {
				t.Fatalf("Lookup(executor) error = %v", err)
			}
			if executor.Backend != "agy" || executor.Agent != "windsurf-swe" || executor.Model != "" {
				t.Fatalf("executor = %+v, want agent-only entry", executor)
			}
		})
	}
}

// TestLoadYAMLRejectsUnknownFields confirms that DisallowUnknownFields
// semantics carry through the YAML normalization path.
func TestLoadYAMLRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "roster.yaml")
	data := "planner:\n  backend: agy\n  agent: planner\n  bogus: true\n"
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected unknown-field error for YAML, got nil")
	}
}

// TestLoadTOMLRejectsUnknownFields confirms that DisallowUnknownFields
// semantics carry through the TOML normalization path.
func TestLoadTOMLRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "roster.toml")
	data := `[planner]
backend = "agy"
agent = "planner"
bogus = true
`
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected unknown-field error for TOML, got nil")
	}
}
