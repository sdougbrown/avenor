// Package providerconfig reads the user-level model-provider registry:
// named OpenAI-compatible endpoints avenor can call directly, outside any
// agent runtime. The registry lives under $XDG_CONFIG_HOME/avenor (else
// ~/.config/avenor) as providers.json, providers.yaml, or providers.toml,
// so a template can name a provider without embedding endpoints or
// credentials, and a user can add or rotate one without touching templates.
package providerconfig

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/sdougbrown/avenor/internal/appdir"
	"github.com/sdougbrown/avenor/internal/configfile"
)

// Provider is one named model-API endpoint. Only OpenAI-compatible
// chat-completions endpoints are supported: base_url is the API root (with
// or without a trailing /v1) and the key authenticates as a bearer token.
type Provider struct {
	// BaseURL is the OpenAI-compatible API root, for example
	// "https://api.groq.com/openai/v1".
	BaseURL string `json:"base_url"`
	// APIKey is a literal bearer token. Prefer APIKeyEnv so the secret stays
	// out of config files; APIKeyEnv wins when both are set.
	APIKey string `json:"api_key,omitempty"`
	// APIKeyEnv names the environment variable holding the bearer token.
	// It wins over APIKey when both are set.
	APIKeyEnv string `json:"api_key_env,omitempty"`
}

// Config is the decoded provider registry.
type Config struct {
	Providers map[string]Provider `json:"providers"`
}

// DefaultDir returns the directory the provider registry is read from: the
// shared avenor config home ($XDG_CONFIG_HOME/avenor, else ~/.config/avenor).
func DefaultDir() string {
	return appdir.ConfigHome()
}

// Load reads the provider registry from DefaultDir. The file may be
// providers.json, providers.yaml, or providers.toml; the first present name
// wins (JSON first). No registry at all is not an error: (nil, nil) means no
// providers are configured and every lookup will fail with a clear message.
func Load() (*Config, error) {
	return LoadDir(DefaultDir())
}

// LoadDir reads the provider registry from an explicit directory, mirroring
// Load's format detection and missing-file semantics.
func LoadDir(dir string) (*Config, error) {
	for _, name := range []string{"providers.json", "providers.yaml", "providers.yml", "providers.toml"} {
		path := filepath.Join(dir, name)
		if _, err := os.Stat(path); err != nil {
			continue
		}
		var cfg Config
		if err := configfile.Load(path, &cfg); err != nil {
			return nil, fmt.Errorf("load provider registry %s: %w", path, err)
		}
		for name, provider := range cfg.Providers {
			if strings.TrimSpace(name) == "" {
				return nil, fmt.Errorf("load provider registry %s: empty provider name", path)
			}
			if err := validateProvider(name, provider); err != nil {
				return nil, fmt.Errorf("load provider registry %s: %w", path, err)
			}
		}
		return &cfg, nil
	}
	return nil, nil
}

// Lookup returns the named provider's endpoint configuration.
func (c *Config) Lookup(name string) (Provider, error) {
	if c == nil {
		return Provider{}, fmt.Errorf("provider %q is not configured: no provider registry found", name)
	}
	provider, ok := c.Providers[name]
	if !ok {
		return Provider{}, fmt.Errorf("provider %q is not configured", name)
	}
	return provider, nil
}

// validateProvider rejects a provider entry that could never make a call.
func validateProvider(name string, p Provider) error {
	if strings.TrimSpace(p.BaseURL) == "" {
		return fmt.Errorf("provider %q: base_url is required", name)
	}
	if !strings.HasPrefix(p.BaseURL, "http://") && !strings.HasPrefix(p.BaseURL, "https://") {
		return fmt.Errorf("provider %q: base_url %q must be an http:// or https:// URL", name, p.BaseURL)
	}
	if strings.TrimSpace(p.APIKey) == "" && strings.TrimSpace(p.APIKeyEnv) == "" {
		return fmt.Errorf("provider %q: api_key or api_key_env is required", name)
	}
	return nil
}

// ResolveKey returns the bearer token for a call to this provider. A
// declared api_key_env wins over a literal api_key so a rotated environment
// variable takes effect without editing the registry.
func (p Provider) ResolveKey() (string, error) {
	if env := strings.TrimSpace(p.APIKeyEnv); env != "" {
		key := os.Getenv(env)
		if strings.TrimSpace(key) == "" {
			return "", fmt.Errorf("provider api_key_env %q is unset or empty", env)
		}
		return key, nil
	}
	if key := strings.TrimSpace(p.APIKey); key != "" {
		return key, nil
	}
	return "", fmt.Errorf("provider has neither api_key nor api_key_env")
}
