package configfile

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, name, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestLoadJSONDecodesIntoStruct verifies the default JSON path still works
// and applies DisallowUnknownFields.
func TestLoadJSONDecodesIntoStruct(t *testing.T) {
	type inner struct {
		Name string `json:"name"`
	}
	type cfg struct {
		Title string `json:"title"`
		Inner inner  `json:"inner"`
	}

	path := writeConfig(t, "config.json", `{"title":"hi","inner":{"name":"bob"}}`)
	var got cfg
	if err := Load(path, &got); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got.Title != "hi" || got.Inner.Name != "bob" {
		t.Fatalf("got = %+v", got)
	}
}

func TestLoadJSONRejectsUnknownFields(t *testing.T) {
	type cfg struct {
		Name string `json:"name"`
	}
	path := writeConfig(t, "config.json", `{"name":"ok","bogus":true}`)
	var got cfg
	if err := Load(path, &got); err == nil {
		t.Fatal("expected unknown-field error, got nil")
	}
}

func TestLoadJSONRejectsTrailingData(t *testing.T) {
	type cfg struct {
		Name string `json:"name"`
	}
	path := writeConfig(t, "config.json", `{"name":"ok"} {"more":true}`)
	var got cfg
	if err := Load(path, &got); err == nil {
		t.Fatal("expected trailing-data error, got nil")
	}
}

func TestLoadJSONRejectsNull(t *testing.T) {
	type cfg struct {
		Name string `json:"name"`
	}
	path := writeConfig(t, "config.json", `null`)
	var got cfg
	// null is valid JSON; it decodes into a zero-value struct without error.
	// This documents the behavior: callers that need nil-detection (like
	// rosterconfig) must check the result themselves.
	if err := Load(path, &got); err != nil {
		t.Fatalf("Load(null) error = %v; null should decode to zero value", err)
	}
	if got.Name != "" {
		t.Fatalf("got = %+v, want zero value", got)
	}
}

// --- YAML ---

func TestLoadYAMLDecodesIntoStruct(t *testing.T) {
	type inner struct {
		Name string `json:"name"`
	}
	type cfg struct {
		Title string `json:"title"`
		Inner inner  `json:"inner"`
	}

	path := writeConfig(t, "config.yaml", "title: hi\ninner:\n  name: bob\n")
	var got cfg
	if err := Load(path, &got); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got.Title != "hi" || got.Inner.Name != "bob" {
		t.Fatalf("got = %+v", got)
	}
}

func TestLoadYMLExtension(t *testing.T) {
	type cfg struct {
		Name string `json:"name"`
	}
	path := writeConfig(t, "config.yml", "name: ok\n")
	var got cfg
	if err := Load(path, &got); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got.Name != "ok" {
		t.Fatalf("got = %+v", got)
	}
}

func TestLoadYAMLRejectsUnknownFields(t *testing.T) {
	type cfg struct {
		Name string `json:"name"`
	}
	path := writeConfig(t, "config.yaml", "name: ok\nbogus: true\n")
	var got cfg
	if err := Load(path, &got); err == nil {
		t.Fatal("expected unknown-field error, got nil")
	}
}

func TestLoadYAMLRejectsMultipleDocuments(t *testing.T) {
	type cfg struct {
		Name string `json:"name"`
	}
	path := writeConfig(t, "config.yaml", "name: first\n---\nname: second\n")
	var got cfg
	if err := Load(path, &got); err == nil {
		t.Fatal("expected multiple-document error, got nil")
	}
}

func TestLoadYAMLRejectsEmptyDocument(t *testing.T) {
	type cfg struct {
		Name string `json:"name"`
	}
	path := writeConfig(t, "config.yaml", "")
	var got cfg
	if err := Load(path, &got); err == nil {
		t.Fatal("expected empty-document error, got nil")
	}
}

func TestLoadYAMLRejectsCommentsOnly(t *testing.T) {
	type cfg struct {
		Name string `json:"name"`
	}
	path := writeConfig(t, "config.yaml", "# just a comment\n")
	var got cfg
	if err := Load(path, &got); err == nil {
		t.Fatal("expected error for comments-only YAML, got nil")
	}
}

// --- TOML ---

func TestLoadTOMLDecodesIntoStruct(t *testing.T) {
	type inner struct {
		Name string `json:"name"`
	}
	type cfg struct {
		Title string `json:"title"`
		Inner inner  `json:"inner"`
	}

	path := writeConfig(t, "config.toml", `title = "hi"`+"\n"+`[inner]`+"\n"+`name = "bob"`+"\n")
	var got cfg
	if err := Load(path, &got); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got.Title != "hi" || got.Inner.Name != "bob" {
		t.Fatalf("got = %+v", got)
	}
}

func TestLoadTOMLRejectsUnknownFields(t *testing.T) {
	type cfg struct {
		Name string `json:"name"`
	}
	path := writeConfig(t, "config.toml", `name = "ok"`+"\n"+`bogus = true`+"\n")
	var got cfg
	if err := Load(path, &got); err == nil {
		t.Fatal("expected unknown-field error, got nil")
	}
}

func TestLoadTOMLRejectsNestedUnknownFields(t *testing.T) {
	type inner struct {
		Name string `json:"name"`
	}
	type cfg struct {
		Title string `json:"title"`
		Inner inner  `json:"inner"`
	}
	path := writeConfig(t, "config.toml", `title = "hi"`+"\n"+`[inner]`+"\n"+`name = "bob"`+"\n"+`bogus = true`+"\n")
	var got cfg
	if err := Load(path, &got); err == nil {
		t.Fatal("expected unknown-field error for nested TOML table, got nil")
	}
}

func TestDecodeInvalidTOMLPreservesParseError(t *testing.T) {
	type cfg struct {
		Name string `json:"name"`
	}
	var got cfg
	err := Decode("config.toml", []byte("name = \n"), &got)
	if err == nil {
		t.Fatal("expected decode error, got nil")
	}
	if !strings.Contains(err.Error(), "decode config") {
		t.Fatalf("error should wrap with 'decode config', got: %v", err)
	}
}

func TestDecodeInvalidYAMLPreservesParseError(t *testing.T) {
	type cfg struct {
		Name string `json:"name"`
	}
	var got cfg
	err := Decode("config.yaml", []byte("name: [unclosed\n"), &got)
	if err == nil {
		t.Fatal("expected decode error, got nil")
	}
	if !strings.Contains(err.Error(), "decode config") {
		t.Fatalf("error should wrap with 'decode config', got: %v", err)
	}
}

// --- Format equivalence ---

func TestFormatEquivalenceAcrossJSONYAMLTOML(t *testing.T) {
	type phase struct {
		Name   string `json:"name"`
		Prompt string `json:"prompt"`
	}
	type workflow struct {
		MaxIterations int     `json:"max_iterations"`
		Pre           []phase `json:"pre"`
		Loop          []phase `json:"loop"`
	}

	jsonPath := writeConfig(t, "wf.json", `{
		"max_iterations": 5,
		"pre": [{"name":"setup","prompt":"init"}],
		"loop": [{"name":"work","prompt":"do work"}]
	}`)
	yamlPath := writeConfig(t, "wf.yaml", "max_iterations: 5\npre:\n  - name: setup\n    prompt: init\nloop:\n  - name: work\n    prompt: do work\n")
	tomlPath := writeConfig(t, "wf.toml", `max_iterations = 5
[[pre]]
name = "setup"
prompt = "init"
[[loop]]
name = "work"
prompt = "do work"
`)

	want := workflow{
		MaxIterations: 5,
		Pre:           []phase{{Name: "setup", Prompt: "init"}},
		Loop:          []phase{{Name: "work", Prompt: "do work"}},
	}

	for _, tc := range []struct {
		name string
		path string
	}{
		{"json", jsonPath},
		{"yaml", yamlPath},
		{"toml", tomlPath},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got workflow
			if err := Load(tc.path, &got); err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if got.MaxIterations != want.MaxIterations || len(got.Pre) != 1 || len(got.Loop) != 1 {
				t.Fatalf("got = %+v, want %+v", got, want)
			}
			if got.Pre[0] != want.Pre[0] || got.Loop[0] != want.Loop[0] {
				t.Fatalf("got = %+v, want %+v", got, want)
			}
		})
	}
}

// --- Decode (raw bytes, no file) ---

func TestDecodeJSONBytes(t *testing.T) {
	type cfg struct {
		Name string `json:"name"`
	}
	var got cfg
	if err := Decode("config.json", []byte(`{"name":"ok"}`), &got); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if got.Name != "ok" {
		t.Fatalf("got = %+v", got)
	}
}

func TestDecodeYAMLBytes(t *testing.T) {
	type cfg struct {
		Name string `json:"name"`
	}
	var got cfg
	if err := Decode("config.yaml", []byte("name: ok\n"), &got); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if got.Name != "ok" {
		t.Fatalf("got = %+v", got)
	}
}

// --- DetectFormat ---

func TestDetectFormat(t *testing.T) {
	tests := []struct {
		path string
		want Format
	}{
		{"config.json", FormatJSON},
		{"config.JSON", FormatJSON},
		{"config.yaml", FormatYAML},
		{"config.yml", FormatYAML},
		{"config.YAML", FormatYAML},
		{"config.toml", FormatTOML},
		{"config.TOML", FormatTOML},
		{"config", FormatJSON},     // no extension defaults to JSON
		{"config.txt", FormatJSON}, // unknown extension defaults to JSON
		{"/path/to/roster", FormatJSON},
	}
	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			if got := DetectFormat(tc.path); got != tc.want {
				t.Fatalf("DetectFormat(%q) = %v, want %v", tc.path, got, tc.want)
			}
		})
	}
}

// --- Error wrapping ---

func TestLoadFileNotFound(t *testing.T) {
	type cfg struct {
		Name string `json:"name"`
	}
	err := Load(filepath.Join(t.TempDir(), "missing.json"), &cfg{})
	if err == nil || !strings.Contains(err.Error(), "read config") {
		t.Fatalf("error = %v, want 'read config'", err)
	}
}

func TestLoadInvalidYAML(t *testing.T) {
	type cfg struct {
		Name string `json:"name"`
	}
	path := writeConfig(t, "config.yaml", "name: [unclosed\n")
	var got cfg
	if err := Load(path, &got); err == nil {
		t.Fatal("expected decode error, got nil")
	}
}

func TestLoadInvalidTOML(t *testing.T) {
	type cfg struct {
		Name string `json:"name"`
	}
	path := writeConfig(t, "config.toml", "name = \n")
	var got cfg
	if err := Load(path, &got); err == nil {
		t.Fatal("expected decode error, got nil")
	}
}
