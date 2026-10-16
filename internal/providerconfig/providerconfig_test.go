package providerconfig

import (
	"os"
	"path/filepath"
	"testing"
)

func writeRegistry(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadDirJSON(t *testing.T) {
	dir := t.TempDir()
	writeRegistry(t, dir, "providers.json", `{
		"providers": {
			"groq": {"base_url": "https://api.groq.com/openai/v1", "api_key_env": "GROQ_API_KEY"},
			"local": {"base_url": "http://localhost:8080/v1", "api_key": "lit"}
		}
	}`)
	cfg, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	if len(cfg.Providers) != 2 {
		t.Fatalf("providers = %d, want 2", len(cfg.Providers))
	}
	if got := cfg.Providers["groq"].BaseURL; got != "https://api.groq.com/openai/v1" {
		t.Fatalf("groq base_url = %q", got)
	}
}

func TestLoadDirYAMLAndTOML(t *testing.T) {
	yamlDir := t.TempDir()
	writeRegistry(t, yamlDir, "providers.yaml", `
providers:
  groq:
    base_url: https://api.groq.com/openai/v1
    api_key_env: GROQ_API_KEY
`)
	cfg, err := LoadDir(yamlDir)
	if err != nil {
		t.Fatalf("LoadDir yaml: %v", err)
	}
	if _, err := cfg.Lookup("groq"); err != nil {
		t.Fatalf("lookup groq: %v", err)
	}

	tomlDir := t.TempDir()
	writeRegistry(t, tomlDir, "providers.toml", `
[providers.local]
base_url = "http://localhost:8080/v1"
api_key = "lit"
`)
	cfg, err = LoadDir(tomlDir)
	if err != nil {
		t.Fatalf("LoadDir toml: %v", err)
	}
	if _, err := cfg.Lookup("local"); err != nil {
		t.Fatalf("lookup local: %v", err)
	}
}

func TestLoadDirJSONWinsOverYAML(t *testing.T) {
	dir := t.TempDir()
	writeRegistry(t, dir, "providers.yaml", "providers:\n  only: {base_url: 'http://y/v1', api_key: k}\n")
	writeRegistry(t, dir, "providers.json", `{"providers":{"first":{"base_url":"http://j/v1","api_key":"k"}}}`)
	cfg, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	if _, err := cfg.Lookup("first"); err != nil {
		t.Fatalf("json registry not preferred: %v", err)
	}
	if _, err := cfg.Lookup("only"); err == nil {
		t.Fatal("yaml registry should not be read when json exists")
	}
}

func TestLoadDirMissingRegistryIsNotAnError(t *testing.T) {
	cfg, err := LoadDir(t.TempDir())
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	if cfg != nil {
		t.Fatalf("cfg = %+v, want nil", cfg)
	}
	if _, err := cfg.Lookup("groq"); err == nil {
		t.Fatal("lookup on nil config should fail")
	}
}

func TestLoadDirRejectsUnknownFields(t *testing.T) {
	dir := t.TempDir()
	writeRegistry(t, dir, "providers.json", `{"providers":{"groq":{"base_url":"http://x/v1","api_key":"k","api_kye":"oops"}}}`)
	if _, err := LoadDir(dir); err == nil {
		t.Fatal("unknown field should be rejected")
	}
}

func TestLoadDirRejectsBadProviders(t *testing.T) {
	cases := []struct {
		name    string
		content string
	}{
		{"no base_url", `{"providers":{"g":{"api_key":"k"}}}`},
		{"bad scheme", `{"providers":{"g":{"base_url":"ftp://x","api_key":"k"}}}`},
		{"no key", `{"providers":{"g":{"base_url":"http://x/v1"}}}`},
		{"empty name", `{"providers":{"":{"base_url":"http://x/v1","api_key":"k"}}}`},
	}
	for _, tc := range cases {
		dir := t.TempDir()
		writeRegistry(t, dir, "providers.json", tc.content)
		if _, err := LoadDir(dir); err == nil {
			t.Errorf("%s: want error, got nil", tc.name)
		}
	}
}

func TestResolveKeyEnvWinsOverLiteral(t *testing.T) {
	t.Setenv("MODELAPI_TEST_KEY", "from-env")
	p := Provider{BaseURL: "http://x/v1", APIKey: "literal", APIKeyEnv: "MODELAPI_TEST_KEY"}
	key, err := p.ResolveKey()
	if err != nil {
		t.Fatalf("ResolveKey: %v", err)
	}
	if key != "from-env" {
		t.Fatalf("key = %q, want from-env", key)
	}
}

func TestResolveKeyLiteral(t *testing.T) {
	p := Provider{BaseURL: "http://x/v1", APIKey: "literal"}
	key, err := p.ResolveKey()
	if err != nil || key != "literal" {
		t.Fatalf("ResolveKey = %q, %v; want literal", key, err)
	}
}

func TestResolveKeyUnsetEnvFails(t *testing.T) {
	p := Provider{BaseURL: "http://x/v1", APIKeyEnv: "MODELAPI_TEST_UNSET_ENV"}
	if _, err := p.ResolveKey(); err == nil {
		t.Fatal("unset api_key_env should fail")
	}
}

func TestDefaultDirXDG(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", "/xdg/abs")
	if got := DefaultDir(); got != filepath.Join("/xdg/abs", "avenor") {
		t.Fatalf("DefaultDir = %q", got)
	}
	t.Setenv("XDG_CONFIG_HOME", "relative")
	t.Setenv("HOME", "/home/tester")
	if got := DefaultDir(); got != filepath.Join("/home/tester", ".config", "avenor") {
		t.Fatalf("relative XDG should fall back to HOME: %q", got)
	}
}

func TestLoadDirRejectsEndpointPathAsBaseURL(t *testing.T) {
	dir := t.TempDir()
	writeRegistry(t, dir, "providers.json", `{"providers":{"g":{"base_url":"https://x/v1/chat/completions","api_key":"k"}}}`)
	if _, err := LoadDir(dir); err == nil {
		t.Fatal("a full chat-completions endpoint as base_url should be rejected")
	}
}

func TestLoadDirYML(t *testing.T) {
	dir := t.TempDir()
	writeRegistry(t, dir, "providers.yml", "providers:\n  groq:\n    base_url: https://api.groq.com/openai/v1\n    api_key_env: GROQ_API_KEY\n")
	cfg, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir yml: %v", err)
	}
	if _, err := cfg.Lookup("groq"); err != nil {
		t.Fatalf("lookup groq from .yml registry: %v", err)
	}
}
