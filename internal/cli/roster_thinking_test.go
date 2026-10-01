package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
)

// A roster entry supplies a default thinking level; an explicit --thinking
// wins. Asserted on the StartOptions the runtime actually receives, so the
// value is proven to travel, not merely to parse.
func TestRosterThinkingIsADefaultAndExplicitThinkingWins(t *testing.T) {
	for _, tc := range []struct {
		name          string
		entryThinking string
		flagThinking  string
		want          string
	}{
		{name: "roster supplies the level", entryThinking: `,"thinking":"high"`, want: "high"},
		{name: "explicit flag overrides the roster", entryThinking: `,"thinking":"high"`, flagThinking: "low", want: "low"},
		{name: "explicit flag with no roster level", want: "low", flagThinking: "low"},
		{name: "neither supplies a level", want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got runtime.StartOptions
			oldNewProvider := newProvider
			provider := newScriptedProvider()
			newProvider = func(opts runtime.StartOptions, _ string) (runtime.Provider, error) {
				got = opts
				return provider, nil
			}
			t.Cleanup(func() { newProvider = oldNewProvider })

			dir := t.TempDir()
			rosterPath := filepath.Join(dir, "roster.json")
			contents := `{"horse":{"backend":"codex-app-server","model":"gpt-5.6-terra"` + tc.entryThinking + `}}`
			if err := os.WriteFile(rosterPath, []byte(contents), 0o600); err != nil {
				t.Fatal(err)
			}

			args := []string{
				"--dir", dir,
				"--roster-file", rosterPath,
				"--roster-entry", "horse",
				"--prompt", "noop",
			}
			if tc.flagThinking != "" {
				args = append(args, "--thinking", tc.flagThinking)
			}

			var stderr strings.Builder
			if code := run(args, func(string) string { return "" }, &stderr); code != 0 {
				t.Fatalf("run() = %d, want 0; stderr=%s", code, stderr.String())
			}
			if got.Thinking != tc.want {
				t.Fatalf("StartOptions.Thinking = %q, want %q", got.Thinking, tc.want)
			}
		})
	}
}

// Per-phase roster entries in a loop config resolve through
// ResolvedSelection, so the entry's thinking level must merge with the
// run-level flag (an explicit flag wins) before the attempt starts.
func TestRunWorkflowPhaseRosterSuppliesThinkingUnlessFlagOverridesIt(t *testing.T) {
	for _, tc := range []struct {
		name          string
		entryThinking string
		flagThinking  string
		want          string
	}{
		{name: "roster supplies the level", entryThinking: `,"thinking":"high"`, want: "high"},
		{name: "explicit flag overrides the roster", entryThinking: `,"thinking":"high"`, flagThinking: "low", want: "low"},
		{name: "entry without a level keeps the flag", flagThinking: "low", want: "low"},
		{name: "neither supplies a level", want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oldNewProvider := newProvider
			provider := newScriptedProvider()
			var got runtime.StartOptions
			newProvider = func(opts runtime.StartOptions, _ string) (runtime.Provider, error) {
				got = opts
				return provider, nil
			}
			t.Cleanup(func() { newProvider = oldNewProvider })

			dir := t.TempDir()
			rosterPath := filepath.Join(dir, "roster.json")
			contents := `{"horse":{"backend":"codex-app-server","model":"gpt-5.6-terra"` + tc.entryThinking + `}}`
			if err := os.WriteFile(rosterPath, []byte(contents), 0o600); err != nil {
				t.Fatal(err)
			}
			loopPath := filepath.Join(dir, "loop.json")
			if err := os.WriteFile(loopPath, []byte(`{"pre":[{"name":"work","prompt":"work","roster_entry":"horse"}]}`), 0o600); err != nil {
				t.Fatal(err)
			}

			args := []string{"--dir", dir, "--loop-file", loopPath, "--roster-file", rosterPath}
			if tc.flagThinking != "" {
				args = append(args, "--thinking", tc.flagThinking)
			}
			var stderr strings.Builder
			if code := run(args, func(string) string { return "" }, &stderr); code != 0 {
				t.Fatalf("run() = %d, want 0; stderr=%s", code, stderr.String())
			}
			if got.Thinking != tc.want {
				t.Fatalf("StartOptions.Thinking = %q, want %q", got.Thinking, tc.want)
			}
		})
	}
}

// A phase roster entry's thinking level is a default that must be validated
// against the phase's resolved backend, just like a flag's. A regression that
// validates only the flag (or reorders the check after the attempt) would let
// a roster-supplied level for a thinking-rejecting backend reach the attempt.
func TestRunWorkflowPhaseRosterThinkingIsCheckedAgainstTheBackend(t *testing.T) {
	oldNewProvider := newProvider
	called := false
	newProvider = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		called = true
		return newScriptedProvider(), nil
	}
	t.Cleanup(func() { newProvider = oldNewProvider })

	dir := t.TempDir()
	rosterPath := filepath.Join(dir, "roster.json")
	if err := os.WriteFile(rosterPath, []byte(`{"slow":{"backend":"agy","agent":"executor","thinking":"low"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	loopPath := filepath.Join(dir, "loop.json")
	if err := os.WriteFile(loopPath, []byte(`{"pre":[{"name":"work","prompt":"work","roster_entry":"slow"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}

	var stderr strings.Builder
	if got := run([]string{"--dir", dir, "--loop-file", loopPath, "--roster-file", rosterPath}, func(string) string { return "" }, &stderr); got != 1 {
		t.Fatalf("run() = %d, want 1; stderr=%s", got, stderr.String())
	}
	if called || !strings.Contains(stderr.String(), "agy") || !strings.Contains(stderr.String(), "thinking") {
		t.Fatalf("called=%v stderr=%q, want effective-backend thinking rejection", called, stderr.String())
	}
}

// A roster entry's thinking level is a default that must be validated against
// the backend on the direct CLI path, just like a flag's. A regression that
// validates only the flag (or reorders the check after the attempt) would let
// a roster-supplied level for a thinking-rejecting backend reach the attempt;
// failing before the attempt is invoked pins the check to the roster value too.
func TestRosterSuppliedThinkingIsCheckedAgainstTheBackend(t *testing.T) {
	oldRunAttempt := runAttempt
	t.Cleanup(func() { runAttempt = oldRunAttempt })
	called := false
	runAttempt = func(context.Context, attemptConfig, attemptDeps) attemptResult {
		called = true
		return attemptResult{}
	}

	dir := t.TempDir()
	rosterPath := filepath.Join(dir, "roster.json")
	contents := `{"horse":{"backend":"agy","agent":"windsurf-swe","thinking":"low"}}`
	if err := os.WriteFile(rosterPath, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}

	var stderr strings.Builder
	if code := run([]string{
		"--dir", dir,
		"--roster-file", rosterPath,
		"--roster-entry", "horse",
		"--prompt", "work",
	}, func(string) string { return "" }, &stderr); code == 0 {
		t.Fatalf("run = %d, want non-zero; stderr=%s", code, stderr.String())
	}
	if called || !strings.Contains(stderr.String(), "agy") || !strings.Contains(stderr.String(), "thinking") {
		t.Fatalf("called=%v stderr=%q", called, stderr.String())
	}
}
