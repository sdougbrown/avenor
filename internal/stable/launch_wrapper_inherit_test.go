package stable

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
)

// TestStableLoopAndTeamInheritLaunchWrapper proves phase starts inside a loop
// and a team inherit the parent run's wrapper, mirroring how dir inherits.
func TestStableLoopAndTeamInheritLaunchWrapper(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"loop", "team"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			configPath := filepath.Join(dir, mode+".json")
			contents := `{"pre":[{"name":"work","prompt":"work"}]}`
			if mode == "team" {
				contents = `{"team":[{"name":"work","prompt":"work"}]}`
			}
			if err := os.WriteFile(configPath, []byte(contents), 0o600); err != nil {
				t.Fatal(err)
			}
			sup := newTestSupervisor(t, Config{ControlSocket: "/tmp/launch-wrapper-inherit-" + mode + ".sock", MaxRuntimes: 1})
			var got []string
			sup.newProviderFunc = func(startOpts runtime.StartOptions, _ string) (runtime.Provider, error) {
				got = startOpts.LaunchWrapper
				return scriptedStage5Provider("ses_wrapper", "end_turn"), nil
			}
			params := SpawnParams{Dir: dir, LaunchWrapper: []string{"nice", "-n", "5"}}
			if mode == "loop" {
				params.LoopFile = configPath
			} else {
				params.TeamFile = configPath
			}
			result, err := sup.spawn(params)
			if err != nil {
				t.Fatalf("spawn: %v", err)
			}
			sup.controlMu.Lock()
			child := sup.runtimes[result.RuntimeID]
			sup.controlMu.Unlock()
			waitStage5Done(t, child)
			if !slices.Equal(got, []string{"nice", "-n", "5"}) {
				t.Fatalf("phase LaunchWrapper = %#v, want [nice -n 5]", got)
			}
		})
	}
}
