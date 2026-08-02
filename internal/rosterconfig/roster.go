// Package rosterconfig loads and resolves backend/agent/model roster entries.
package rosterconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Entry is one complete roster identity. Backend is required; an entry must
// also provide an agent, a model, or both. An agent may intentionally omit a
// model so the selected backend can apply its default.
type Entry struct {
	Backend string `json:"backend"`
	Agent   string `json:"agent,omitempty"`
	Model   string `json:"model,omitempty"`
}

// Config is the top-level roster map, keyed by roster entry name.
type Config map[string]Entry

// Load reads and validates a roster file. The top-level value must be a JSON
// object and all objects are decoded with unknown fields rejected. This keeps
// deferred fields such as system and thinking from silently becoming no-ops.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read roster config %s: %w", path, err)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	var config Config
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("decode roster config %s: %w", path, err)
	}
	if config == nil {
		return nil, fmt.Errorf("decode roster config %s: top-level value must be an object", path)
	}

	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("decode roster config %s: multiple JSON values", path)
		}
		return nil, fmt.Errorf("decode roster config %s: trailing JSON: %w", path, err)
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("validate roster config %s: %w", path, err)
	}
	return &config, nil
}

// LoadForConfig resolves the effective roster for a workflow config. A
// declared roster path is relative to configPath; when absent, inherited takes
// precedence over fallbackPath.
func LoadForConfig(configPath, declaredPath string, inherited *Config, fallbackPath string) (*Config, error) {
	rosterPath := declaredPath
	if rosterPath != "" {
		if !filepath.IsAbs(rosterPath) {
			rosterPath = filepath.Join(filepath.Dir(configPath), rosterPath)
		}
	} else if inherited != nil {
		return inherited, nil
	} else {
		rosterPath = fallbackPath
	}
	if rosterPath == "" {
		return nil, nil
	}
	return Load(rosterPath)
}

// Validate checks every roster name and entry in the configuration.
func (c Config) Validate() error {
	for name, entry := range c {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("entry name must not be empty")
		}
		if err := entry.Validate(); err != nil {
			return fmt.Errorf("entry %q: %w", name, err)
		}
	}
	return nil
}

// Validate checks the structural requirements for an entry. It does not
// verify whether Backend is supported or instantiate any provider.
func (e Entry) Validate() error {
	if strings.TrimSpace(e.Backend) == "" {
		return fmt.Errorf("backend must not be empty")
	}
	if strings.TrimSpace(e.Agent) == "" && strings.TrimSpace(e.Model) == "" {
		return fmt.Errorf("at least one of agent or model must be set")
	}
	return nil
}

// Lookup returns the named roster entry. Missing names are reported as
// configuration errors rather than falling back to a different identity.
func (c Config) Lookup(entryName string) (Entry, error) {
	if strings.TrimSpace(entryName) == "" {
		return Entry{}, fmt.Errorf("roster entry name must not be empty")
	}
	entry, ok := c[entryName]
	if !ok {
		return Entry{}, fmt.Errorf("roster entry %q not found", entryName)
	}
	if err := entry.Validate(); err != nil {
		return Entry{}, fmt.Errorf("roster entry %q: %w", entryName, err)
	}
	return entry, nil
}

// ResolveInput contains the run-level identity context and any phase-local
// inline overrides. AgentProfile and Thinking are deliberately context only:
// neither is part of the resolved identity and neither is read or changed by
// Resolve.
type ResolveInput struct {
	Backend      string
	Agent        string
	Model        string
	AgentProfile string
	Thinking     string

	InlineAgent string
	InlineModel string

	// Roster selects a complete identity when non-nil.
	Roster *Entry
	// Loop rejects inline phase agent/model overrides. A roster entry remains
	// valid for a loop phase.
	Loop bool
}

// ResolvedSelection is the effective backend/agent/model identity. Run-level
// agent_profile and thinking remain outside this type and must be carried by
// the caller as orthogonal execution context.
type ResolvedSelection struct {
	Backend string
	Agent   string
	Model   string
}

// Resolve applies roster and phase selection precedence without mutating any
// serialized configuration:
//
//   - a roster entry supplies the complete identity and wins over run-level
//     identity;
//   - phase-local inline agent/model fields are rejected with a roster entry;
//   - without a roster, inline team overrides retain their existing behavior;
//   - loop phases reject inline agent/model fields.
func Resolve(input ResolveInput) (ResolvedSelection, error) {
	if input.Roster != nil {
		if hasValue(input.InlineAgent) || hasValue(input.InlineModel) {
			return ResolvedSelection{}, fmt.Errorf("inline agent/model overrides are not allowed with a roster entry")
		}
		if err := input.Roster.Validate(); err != nil {
			return ResolvedSelection{}, fmt.Errorf("roster entry: %w", err)
		}
		return ResolvedSelection{
			Backend: input.Roster.Backend,
			Agent:   input.Roster.Agent,
			Model:   input.Roster.Model,
		}, nil
	}

	if input.Loop && (hasValue(input.InlineAgent) || hasValue(input.InlineModel)) {
		return ResolvedSelection{}, fmt.Errorf("loop phases do not support inline agent/model overrides")
	}

	selection := ResolvedSelection{
		Backend: input.Backend,
		Agent:   input.Agent,
		Model:   input.Model,
	}
	if hasValue(input.InlineAgent) {
		selection.Agent = input.InlineAgent
	}
	if hasValue(input.InlineModel) {
		selection.Model = input.InlineModel
	}
	return selection, nil
}

func hasValue(value string) bool {
	return strings.TrimSpace(value) != ""
}
