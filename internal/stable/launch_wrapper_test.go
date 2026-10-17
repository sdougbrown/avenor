package stable

import (
	"path/filepath"
	"slices"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
)

func TestStableSpawnThreadsLaunchWrapperToProvider(t *testing.T) {
	dir := t.TempDir()
	sentinelPath := filepath.Join(dir, "wrap.done")
	provider := &stableDeferredProvider{
		provisionalID: "pending-wrap", realID: "wrap-session", backend: "pi",
		events: deferredStartEndEvents("wrap-session", nil),
	}
	var got runtime.StartOptions
	sup := newTestSupervisor(t, Config{ControlSocket: "/tmp/launch-wrapper-thread.sock", MaxRuntimes: 1})
	sup.newProviderFunc = func(startOpts runtime.StartOptions, _ string) (runtime.Provider, error) {
		got = startOpts
		return provider, nil
	}
	if _, err := sup.spawn(SpawnParams{Prompt: "work", Dir: dir, SentinelFile: sentinelPath, LaunchWrapper: []string{"nice", "-n", "5"}}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got.LaunchWrapper, []string{"nice", "-n", "5"}) {
		t.Fatalf("provider LaunchWrapper = %#v, want [nice -n 5]", got.LaunchWrapper)
	}
}

func TestStableSpawnRejectsEmptyLaunchWrapperElement(t *testing.T) {
	sup := newTestSupervisor(t, Config{ControlSocket: "/tmp/launch-wrapper-empty.sock", MaxRuntimes: 1})
	_, err := sup.spawn(SpawnParams{Prompt: "work", Dir: ".", LaunchWrapper: []string{"nice", ""}})
	if err == nil {
		t.Fatal("spawn with an empty launch wrapper element succeeded, want error")
	}
	if err.Error() != "launch_wrapper: launch wrapper elements must be non-empty" {
		t.Fatalf("spawn error = %q, want %q", err, "launch_wrapper: launch wrapper elements must be non-empty")
	}
}
