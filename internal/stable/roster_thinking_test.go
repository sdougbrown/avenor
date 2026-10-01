package stable

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/looprunner"
	"github.com/sdougbrown/avenor/internal/phaseconfig"
	"github.com/sdougbrown/avenor/internal/rosterconfig"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/teamrunner"
)

// The spawn path is what MCP-driven delegation uses, so the roster default
// has to survive the trip to StartOptions here too, not only through the CLI.
func TestStableDirectRosterSuppliesThinkingUnlessSpawnOverridesIt(t *testing.T) {
	for _, tc := range []struct {
		name          string
		entry         string
		spawnThinking string
		want          string
	}{
		{
			name:  "roster supplies the level",
			entry: `{"horse":{"backend":"codex-app-server","model":"gpt-5.6-terra","thinking":"high"}}`,
			want:  "high",
		},
		{
			name:          "explicit spawn thinking overrides the roster",
			entry:         `{"horse":{"backend":"codex-app-server","model":"gpt-5.6-terra","thinking":"high"}}`,
			spawnThinking: "low",
			want:          "low",
		},
		{
			name:  "entry without a level leaves thinking to the backend default",
			entry: `{"horse":{"backend":"codex-app-server","model":"gpt-5.6-terra"}}`,
			want:  "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sup := NewSupervisor(Config{ControlSocket: "/tmp/roster-thinking.sock", MaxRuntimes: 1})
			provider := scriptedStage5Provider("ses_thinking", "end_turn")
			var gotOpts runtime.StartOptions
			sup.newProviderFunc = func(opts runtime.StartOptions, _ string) (runtime.Provider, error) {
				gotOpts = opts
				return provider, nil
			}

			rosterPath := writeStage5Roster(t, t.TempDir(), tc.entry)
			result, err := sup.spawn(SpawnParams{
				Prompt:      "work",
				Dir:         t.TempDir(),
				RosterFile:  rosterPath,
				RosterEntry: "horse",
				Thinking:    tc.spawnThinking,
			})
			if err != nil {
				t.Fatalf("spawn: %v", err)
			}
			defer sup.cancelRuntime(result.RuntimeID)

			if gotOpts.Thinking != tc.want {
				t.Fatalf("StartOptions.Thinking = %q, want %q", gotOpts.Thinking, tc.want)
			}
		})
	}
}

// Per-phase roster entries resolve through ResolvedSelection, so the entry's
// thinking level must surface there and merge with the run-level level
// (an explicit run-level level wins) before StartOptions. Covers both the
// loop and the team child paths.
func TestStablePhaseRosterSuppliesThinkingUnlessRunOverridesIt(t *testing.T) {
	for _, tc := range []struct {
		name        string
		entry       string
		runThinking string
		want        string
	}{
		{
			name:  "roster supplies the level",
			entry: `{"horse":{"backend":"codex-app-server","model":"gpt-5.6-terra","thinking":"high"}}`,
			want:  "high",
		},
		{
			name:        "run-level thinking overrides the roster",
			entry:       `{"horse":{"backend":"codex-app-server","model":"gpt-5.6-terra","thinking":"high"}}`,
			runThinking: "low",
			want:        "low",
		},
		{
			name:        "entry without a level keeps the run-level level",
			entry:       `{"horse":{"backend":"codex-app-server","model":"gpt-5.6-terra"}}`,
			runThinking: "low",
			want:        "low",
		},
		{
			name:  "neither supplies a level",
			entry: `{"horse":{"backend":"codex-app-server","model":"gpt-5.6-terra"}}`,
			want:  "",
		},
	} {
		for _, kind := range []string{"loop", "team"} {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				sup := NewSupervisor(Config{ControlSocket: "/tmp/phase-roster-thinking.sock", MaxRuntimes: 1})
				provider := scriptedStage5Provider("ses_phase_thinking", "end_turn")
				var gotOpts runtime.StartOptions
				sup.newProviderFunc = func(opts runtime.StartOptions, _ string) (runtime.Provider, error) {
					gotOpts = opts
					return provider, nil
				}

				roster, err := rosterconfig.Load(writeStage5Roster(t, t.TempDir(), tc.entry))
				if err != nil {
					t.Fatal(err)
				}

				child := &childRuntime{
					id:          "rt_phase_thinking",
					done:        make(chan struct{}),
					promptCh:    make(chan struct{}, 1),
					eventWriter: stableTestSink{},
					cancelFn:    func() {},
					roster:      roster,
					dir:         t.TempDir(),
				}
				if kind == "loop" {
					cfg := &looprunner.LoopConfig{MaxIterations: 1, Pre: []phaseconfig.Phase{{Name: "work", Prompt: "work", RosterEntry: "horse"}}}
					go sup.runLoopChild(context.Background(), child, cfg, 1, "", "", "", tc.runThinking, "", "")
				} else {
					cfg := &teamrunner.TeamConfig{Team: []phaseconfig.Phase{{Name: "work", Prompt: "work", RosterEntry: "horse"}}}
					go sup.runTeamChild(context.Background(), child, cfg, 1, "", "", "", tc.runThinking, "", "")
				}
				select {
				case <-child.done:
				case <-time.After(5 * time.Second):
					t.Fatal("child did not complete")
				}
				if gotOpts.Thinking != tc.want {
					t.Fatalf("StartOptions.Thinking = %q, want %q", gotOpts.Thinking, tc.want)
				}
			})
		}
	}
}

// A roster entry's thinking level is a default that must be validated against
// the backend on the spawn path, just like a spawn-supplied level. A
// regression that validates only the spawn value (or moves the check after
// reservation) would let a roster-supplied level for a thinking-rejecting
// backend consume a runtime; failing with nothing reserved pins the check to
// the merged roster value too.
func TestStableRosterThinkingIsCheckedAgainstTheBackend(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/roster-thinking-agy.sock", MaxRuntimes: 1})
	providerCalled := false
	sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) {
		providerCalled = true
		return scriptedStage5Provider("ses_roster_agy", "end_turn"), nil
	}

	rosterPath := writeStage5Roster(t, t.TempDir(),
		`{"horse":{"backend":"agy","agent":"windsurf-swe","thinking":"low"}}`)
	_, err := sup.spawn(SpawnParams{
		Prompt:      "work",
		Dir:         t.TempDir(),
		RosterFile:  rosterPath,
		RosterEntry: "horse",
	})
	if err == nil || !strings.Contains(err.Error(), "agy") || !strings.Contains(err.Error(), "thinking") {
		t.Fatalf("spawn error = %v, want a thinking-rejection naming agy", err)
	}
	if providerCalled || len(sup.runtimes) != 0 || sup.nextID != 0 {
		t.Fatalf("provider=%v runtimes=%d nextID=%d", providerCalled, len(sup.runtimes), sup.nextID)
	}
}
