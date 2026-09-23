package rosterconfig

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeRoster(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "roster.json")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadMissingRosterFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "nonexistent.json"))
	if err == nil {
		t.Fatal("Load() error = nil, want error for missing roster")
	}
	errStr := err.Error()
	if !strings.Contains(errStr, "load roster config") {
		t.Fatalf("Load() error = %v, want roster-config boundary", err)
	}
	// The underlying cause (e.g. ENOENT) must be preserved via %w.
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Load() error = %v, expected to preserve underlying ENOENT via %%w", err)
	}
}

func TestLoadValidRosterAndLookup(t *testing.T) {
	config, err := Load(writeRoster(t, `{
		"planner": {"backend": "opencode-acp", "agent": "planner", "model": "provider/planner"},
		"executor": {"backend": "agy", "agent": "windsurf-swe"}
	}`))
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
}

func TestLoadForConfigResolvesDeclaredInheritedAndFallbackRoster(t *testing.T) {
	dir := t.TempDir()
	writeFile := func(name, contents string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}

	configPath := filepath.Join(dir, "workflow.json")
	fallbackPath := writeFile("fallback.json", `{"entry":{"backend":"fallback","agent":"fallback"}}`)
	writeFile("declared.json", `{"entry":{"backend":"declared","agent":"declared"}}`)
	inherited := Config{"entry": {Backend: "inherited", Agent: "inherited"}}

	declared, err := LoadForConfig(configPath, "declared.json", &inherited, fallbackPath)
	if err != nil {
		t.Fatalf("declared roster = %v", err)
	}
	if entry, _ := declared.Lookup("entry"); entry.Backend != "declared" {
		t.Fatalf("declared roster = %+v, want declared", entry)
	}

	gotInherited, err := LoadForConfig(configPath, "", &inherited, fallbackPath)
	if err != nil {
		t.Fatalf("inherited roster = %v", err)
	}
	if gotInherited != &inherited {
		t.Fatal("inherited roster was not preserved")
	}

	fallback, err := LoadForConfig(configPath, "", nil, fallbackPath)
	if err != nil {
		t.Fatalf("fallback roster = %v", err)
	}
	if entry, _ := fallback.Lookup("entry"); entry.Backend != "fallback" {
		t.Fatalf("fallback roster = %+v, want fallback", entry)
	}

	empty, err := LoadForConfig(configPath, "", nil, "")
	if err != nil {
		t.Fatalf("empty roster = %v", err)
	}
	if empty != nil {
		t.Fatalf("empty roster = %+v, want nil", empty)
	}
}

func TestLoadRejectsInvalidEntries(t *testing.T) {
	tests := []struct {
		name    string
		config  string
		wantErr string
	}{
		{
			name:    "missing backend",
			config:  `{"planner":{"agent":"planner"}}`,
			wantErr: "backend",
		},
		{
			name:    "missing identity",
			config:  `{"planner":{"backend":"agy"}}`,
			wantErr: "agent or model",
		},
		{
			name:    "empty entry name",
			config:  `{"":{"backend":"agy","agent":"planner"}}`,
			wantErr: "entry name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeRoster(t, tt.config))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Load() error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestLoadRejectsUnknownAndDeferredFields(t *testing.T) {
	// "thinking" is no longer in this list: it is a supported entry field.
	for _, field := range []string{"system", "misspelled"} {
		t.Run(field, func(t *testing.T) {
			_, err := Load(writeRoster(t, `{"planner":{"backend":"agy","agent":"planner","`+field+`":"deferred"}}`))
			if err == nil {
				t.Fatalf("Load() accepted deferred/unknown field %q", field)
			}
		})
	}
}

func TestLoadRejectsTrailingJSONAndNonObject(t *testing.T) {
	for _, contents := range []string{
		`{"planner":{"backend":"agy","agent":"planner"}} {}`,
		`null`,
	} {
		t.Run(contents, func(t *testing.T) {
			if _, err := Load(writeRoster(t, contents)); err == nil {
				t.Fatalf("Load() accepted %q", contents)
			}
		})
	}
}

func TestLookupUnknownEntry(t *testing.T) {
	config, err := Load(writeRoster(t, `{"planner":{"backend":"agy","agent":"planner"}}`))
	if err != nil {
		t.Fatal(err)
	}

	_, err = config.Lookup("missing")
	if err == nil || !strings.Contains(err.Error(), `roster entry "missing" not found`) {
		t.Fatalf("Lookup(missing) error = %v", err)
	}
}

func TestResolvePrecedenceAndContextSeparation(t *testing.T) {
	roster := &Entry{Backend: "agy", Agent: "roster-agent", Model: "roster-model"}
	input := ResolveInput{
		Backend:      "opencode-acp",
		Agent:        "run-agent",
		Model:        "run-model",
		AgentProfile: "cloud",
		Thinking:     "high",
		Roster:       roster,
	}

	resolved, err := Resolve(input)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	want := ResolvedSelection{Backend: "agy", Agent: "roster-agent", Model: "roster-model"}
	if resolved != want {
		t.Fatalf("resolved = %+v, want %+v", resolved, want)
	}
	if input.AgentProfile != "cloud" || input.Thinking != "high" {
		t.Fatalf("orthogonal run context changed: profile=%q thinking=%q", input.AgentProfile, input.Thinking)
	}
}

func TestResolveRejectsInlineOverridesWithRoster(t *testing.T) {
	for _, input := range []ResolveInput{
		{Roster: &Entry{Backend: "agy", Agent: "roster-agent"}, InlineAgent: "inline-agent"},
		{Roster: &Entry{Backend: "agy", Agent: "roster-agent"}, InlineModel: "inline-model"},
	} {
		if _, err := Resolve(input); err == nil || !strings.Contains(err.Error(), "inline agent/model") {
			t.Fatalf("Resolve(%+v) error = %v, want inline override rejection", input, err)
		}
	}
}

func TestResolveWithoutRosterPreservesDirectAndTeamBehavior(t *testing.T) {
	selection, err := Resolve(ResolveInput{
		Backend: "opencode-acp",
		Agent:   "run-agent",
		Model:   "run-model",
	})
	if err != nil {
		t.Fatalf("direct Resolve() error = %v", err)
	}
	if want := (ResolvedSelection{Backend: "opencode-acp", Agent: "run-agent", Model: "run-model"}); selection != want {
		t.Fatalf("direct selection = %+v, want %+v", selection, want)
	}

	selection, err = Resolve(ResolveInput{
		Backend:     "opencode-acp",
		Agent:       "run-agent",
		Model:       "run-model",
		InlineAgent: "team-agent",
		InlineModel: "team-model",
	})
	if err != nil {
		t.Fatalf("team Resolve() error = %v", err)
	}
	if want := (ResolvedSelection{Backend: "opencode-acp", Agent: "team-agent", Model: "team-model"}); selection != want {
		t.Fatalf("team selection = %+v, want %+v", selection, want)
	}
}

func TestResolveRejectsLoopInlineOverrides(t *testing.T) {
	for _, input := range []ResolveInput{
		{Loop: true, InlineAgent: "inline-agent"},
		{Loop: true, InlineModel: "inline-model"},
	} {
		if _, err := Resolve(input); err == nil || !strings.Contains(err.Error(), "loop") {
			t.Fatalf("Resolve(%+v) error = %v, want loop override rejection", input, err)
		}
	}

	selection, err := Resolve(ResolveInput{
		Loop:   true,
		Roster: &Entry{Backend: "agy", Agent: "loop-agent"},
	})
	if err != nil {
		t.Fatalf("loop roster Resolve() error = %v", err)
	}
	if selection != (ResolvedSelection{Backend: "agy", Agent: "loop-agent"}) {
		t.Fatalf("loop roster selection = %+v", selection)
	}
}

func TestLoadAcceptsThinkingOnAnEntry(t *testing.T) {
	config, err := Load(writeRoster(t, `{
		"horse": {"backend": "codex-app-server", "model": "gpt-5.6-terra", "thinking": "high"},
		"mule": {"backend": "codex-app-server", "model": "gpt-5.6-luna", "thinking": "low"},
		"plain": {"backend": "agy", "agent": "windsurf-swe"}
	}`))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	horse, err := config.Lookup("horse")
	if err != nil {
		t.Fatalf("Lookup(horse) error = %v", err)
	}
	if horse.Thinking != "high" {
		t.Fatalf("horse.Thinking = %q, want %q", horse.Thinking, "high")
	}

	// Thinking stays optional; an entry without it is unchanged.
	plain, err := config.Lookup("plain")
	if err != nil {
		t.Fatalf("Lookup(plain) error = %v", err)
	}
	if plain.Thinking != "" {
		t.Fatalf("plain.Thinking = %q, want empty", plain.Thinking)
	}
}

func TestLoadRejectsAnUnknownThinkingLevelAndNamesTheEntry(t *testing.T) {
	// A misspelled level used to be impossible to express. Now that it is, it
	// must fail at load with the offending entry named, rather than surfacing
	// much later with only a backend for context.
	_, err := Load(writeRoster(t, `{"horse":{"backend":"codex-app-server","model":"m","thinking":"hgih"}}`))
	if err == nil {
		t.Fatal("Load() accepted an unknown thinking level")
	}
	if !strings.Contains(err.Error(), "horse") {
		t.Fatalf("Load() error = %v, want the offending entry named", err)
	}
	if !strings.Contains(err.Error(), "thinking") {
		t.Fatalf("Load() error = %v, want the offending field named", err)
	}
}

func TestLoadAcceptsEveryCanonicalThinkingLevel(t *testing.T) {
	for _, level := range []string{"off", "minimal", "low", "medium", "high", "xhigh", "max"} {
		t.Run(level, func(t *testing.T) {
			if _, err := Load(writeRoster(t,
				`{"e":{"backend":"codex-app-server","model":"m","thinking":"`+level+`"}}`)); err != nil {
				t.Fatalf("Load() rejected canonical level %q: %v", level, err)
			}
		})
	}
}

func TestResolveStillLeavesRosterThinkingOutOfTheIdentity(t *testing.T) {
	// Thinking is a default carried alongside the identity, not part of it.
	// Phase selection does not apply it yet; this pins that boundary so the
	// follow-up work has to change the test deliberately.
	roster := &Entry{Backend: "agy", Agent: "roster-agent", Thinking: "max"}

	resolved, err := Resolve(ResolveInput{Roster: roster, Thinking: "low"})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	want := ResolvedSelection{Backend: "agy", Agent: "roster-agent"}
	if resolved != want {
		t.Fatalf("resolved = %+v, want %+v", resolved, want)
	}
}
