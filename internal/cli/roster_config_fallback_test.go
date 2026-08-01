package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
)

func TestRunWorkflowRosterFallbackUsesSelectedDirectory(t *testing.T) {
	oldNewProvider := newProvider
	provider := newScriptedProvider()
	newProvider = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) { return provider, nil }
	t.Cleanup(func() { newProvider = oldNewProvider })

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "fallback.json"), []byte(`{"planner":{"backend":"gemini-acp","agent":"fallback"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "loop.json")
	if err := os.WriteFile(configPath, []byte(`{"pre":[{"name":"work","prompt":"work","roster_entry":"planner"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}

	var stderr strings.Builder
	if got := run([]string{
		"--backend", "gemini-acp",
		"--dir", dir,
		"--loop-file", configPath,
		"--roster-file", "fallback.json",
	}, func(string) string { return "" }, &stderr); got != 0 {
		t.Fatalf("run() = %d, want 0; stderr=%s", got, stderr.String())
	}
	if len(provider.snapshotSessions()) != 1 {
		t.Fatalf("sessions = %d, want fallback roster to validate workflow", len(provider.snapshotSessions()))
	}
}
