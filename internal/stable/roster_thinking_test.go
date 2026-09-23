package stable

import (
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
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
