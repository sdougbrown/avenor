package cli

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
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

func TestRunDirectRosterRejectsExplicitBackend(t *testing.T) {
	rosterPath := filepath.Join(t.TempDir(), "roster.json")
	if err := os.WriteFile(rosterPath, []byte(`{"planner":{"backend":"gemini-acp","agent":"planner"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	if got := run([]string{"--prompt", "work", "--backend", "pi", "--roster-file", rosterPath, "--roster-entry", "planner"}, func(string) string { return "" }, &stderr); got != 1 {
		t.Fatalf("run() = %d, want 1; stderr=%s", got, stderr.String())
	}
	if !strings.Contains(stderr.String(), "mutually exclusive") {
		t.Fatalf("stderr = %q, want direct selector conflict", stderr.String())
	}
}

func TestRunDirectBackendOnlyRemainsCompatible(t *testing.T) {
	oldNewProvider := newProvider
	provider := newScriptedProvider()
	newProvider = func(_ runtime.StartOptions, backend string) (runtime.Provider, error) {
		if backend != "gemini-acp" {
			t.Fatalf("backend = %q, want gemini-acp", backend)
		}
		return provider, nil
	}
	t.Cleanup(func() { newProvider = oldNewProvider })
	var stderr strings.Builder
	if got := run([]string{"--prompt", "work", "--backend", "gemini-acp"}, func(string) string { return "" }, &stderr); got != 0 {
		t.Fatalf("run() = %d, want 0; stderr=%s", got, stderr.String())
	}
	sessions := provider.snapshotSessions()
	if len(sessions) != 1 || sessions[0].opts.Agent != "" || sessions[0].opts.Model != "" {
		t.Fatalf("backend-only start options = %+v, want empty direct identity", sessions)
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

func TestRunWorkflowRosterResolvesEachPhaseAgainstLoadedContext(t *testing.T) {
	oldNewProvider := newProvider
	provider := newScriptedProvider()
	var mu sync.Mutex
	var backends []string
	var options []runtime.StartOptions
	newProvider = func(opts runtime.StartOptions, backend string) (runtime.Provider, error) {
		mu.Lock()
		backends = append(backends, backend)
		options = append(options, opts)
		mu.Unlock()
		return provider, nil
	}
	t.Cleanup(func() { newProvider = oldNewProvider })

	dir := t.TempDir()
	rosterPath := filepath.Join(dir, "roster.json")
	if err := os.WriteFile(rosterPath, []byte(`{
		"plan":{"backend":"gemini-acp","agent":"planner","model":"planner-model"},
		"ship":{"backend":"agy","agent":"executor","model":"executor-model"}
	}`), 0o600); err != nil {
		t.Fatal(err)
	}
	loopPath := filepath.Join(dir, "loop.json")
	if err := os.WriteFile(loopPath, []byte(`{
		"pre":[
			{"name":"plan","prompt":"plan","roster_entry":"plan"},
			{"name":"ship","prompt":"ship","roster_entry":"ship"}
		]
	}`), 0o600); err != nil {
		t.Fatal(err)
	}

	var stderr strings.Builder
	if got := run([]string{"--dir", dir, "--loop-file", loopPath, "--roster-file", rosterPath}, func(string) string { return "" }, &stderr); got != 0 {
		t.Fatalf("run() = %d, want 0; stderr=%s", got, stderr.String())
	}
	mu.Lock()
	defer mu.Unlock()
	if want := []string{"gemini-acp", "agy"}; len(backends) != len(want) || backends[0] != want[0] || backends[1] != want[1] {
		t.Fatalf("backends = %v, want %v", backends, want)
	}
	if len(options) != 2 || options[0].Agent != "planner" || options[0].Model != "planner-model" || options[1].Agent != "executor" || options[1].Model != "executor-model" {
		t.Fatalf("phase options = %+v, want roster identities", options)
	}
}

func TestRunWorkflowRosterThinkingUsesEffectiveBackend(t *testing.T) {
	oldNewProvider := newProvider
	called := false
	newProvider = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		called = true
		return newScriptedProvider(), nil
	}
	t.Cleanup(func() { newProvider = oldNewProvider })

	dir := t.TempDir()
	rosterPath := filepath.Join(dir, "roster.json")
	if err := os.WriteFile(rosterPath, []byte(`{"slow":{"backend":"agy","agent":"executor"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	loopPath := filepath.Join(dir, "loop.json")
	if err := os.WriteFile(loopPath, []byte(`{"pre":[{"name":"work","prompt":"work","roster_entry":"slow"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}

	var stderr strings.Builder
	if got := run([]string{"--dir", dir, "--loop-file", loopPath, "--roster-file", rosterPath, "--thinking", "low"}, func(string) string { return "" }, &stderr); got != 1 {
		t.Fatalf("run() = %d, want 1; stderr=%s", got, stderr.String())
	}
	if called || !strings.Contains(stderr.String(), "agy") || !strings.Contains(stderr.String(), "thinking") {
		t.Fatalf("called=%v stderr=%q, want effective-backend thinking rejection", called, stderr.String())
	}
}
