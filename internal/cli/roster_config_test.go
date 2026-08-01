package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
)

func TestRunRejectsRosterEntryWithWorkflow(t *testing.T) {
	var stderr strings.Builder
	if got := run([]string{"--loop-file", "/missing/loop.json", "--roster-entry", "planner"}, func(string) string { return "" }, &stderr); got != 1 {
		t.Fatalf("run() = %d, want 1; stderr=%s", got, stderr.String())
	}
	if !strings.Contains(stderr.String(), "--roster-entry is not valid") {
		t.Fatalf("stderr = %q, want workflow roster-entry rejection", stderr.String())
	}
}

func TestRunDirectRosterSelection(t *testing.T) {
	oldNewProvider := newProvider
	provider := newScriptedProvider()
	var gotBackend string
	newProvider = func(_ runtime.StartOptions, backend string) (runtime.Provider, error) {
		gotBackend = backend
		return provider, nil
	}
	t.Cleanup(func() { newProvider = oldNewProvider })

	rosterPath := filepath.Join(t.TempDir(), "roster.json")
	if err := os.WriteFile(rosterPath, []byte(`{"planner":{"backend":"gemini-acp","agent":"planner","model":"provider/planner"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	if got := run([]string{
		"--prompt", "analyze",
		"--roster-file", rosterPath,
		"--roster-entry", "planner",
	}, func(string) string { return "" }, &stderr); got != 0 {
		t.Fatalf("run() = %d, want 0; stderr=%s", got, stderr.String())
	}
	if gotBackend != "gemini-acp" {
		t.Fatalf("backend = %q, want roster backend", gotBackend)
	}
	sessions := provider.snapshotSessions()
	if len(sessions) != 1 {
		t.Fatalf("sessions = %d, want 1", len(sessions))
	}
	if sessions[0].opts.Agent != "planner" || sessions[0].opts.Model != "provider/planner" {
		t.Fatalf("start options = %+v, want roster identity", sessions[0].opts)
	}
}

func TestRunWorkflowRosterFallbackUsesSelectedDirectoryAndDeclaredFileWins(t *testing.T) {
	oldNewProvider := newProvider
	provider := newScriptedProvider()
	newProvider = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) { return provider, nil }
	t.Cleanup(func() { newProvider = oldNewProvider })

	dir := t.TempDir()
	fallbackPath := filepath.Join(dir, "fallback.json")
	if err := os.WriteFile(fallbackPath, []byte(`{"other":{"backend":"gemini-acp","agent":"fallback"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	configDir := t.TempDir()
	declaredPath := filepath.Join(configDir, "declared.json")
	if err := os.WriteFile(declaredPath, []byte(`{"planner":{"backend":"gemini-acp","agent":"declared"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	loopPath := filepath.Join(configDir, "loop.json")
	if err := os.WriteFile(loopPath, []byte(`{"roster_file":"declared.json","pre":[{"name":"work","prompt":"work","roster_entry":"planner"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}

	var stderr strings.Builder
	if got := run([]string{
		"--backend", "gemini-acp",
		"--dir", dir,
		"--loop-file", loopPath,
		"--roster-file", "fallback.json",
	}, func(string) string { return "" }, &stderr); got != 0 {
		t.Fatalf("run() = %d, want 0; stderr=%s", got, stderr.String())
	}
	if len(provider.snapshotSessions()) != 1 {
		t.Fatalf("sessions = %d, want declared config to load before fallback", len(provider.snapshotSessions()))
	}
}
