package rosterconfig

import (
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
	for _, field := range []string{"system", "thinking", "misspelled"} {
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
