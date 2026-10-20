package stable

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
)

// TestMain keeps root-budget runtime state out of a developer's real home
// directory and the package's runtime artifacts out of the shared temporary
// directory. Production roots still use the user's Avenor runtime-state
// directory; this only isolates the package's many direct NewSupervisor tests.
func TestMain(m *testing.M) {
	root, err := os.MkdirTemp("", "avenor-stable-test-")
	if err != nil {
		panic(err)
	}
	home := filepath.Join(root, "home")
	tmp := filepath.Join(root, "tmp")
	for _, dir := range []string{home, tmp} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			panic(err)
		}
	}
	oldHome, hadHome := os.LookupEnv("HOME")
	oldTmp, hadTmp := os.LookupEnv("TMPDIR")
	for _, env := range []struct{ key, value string }{{"HOME", home}, {"TMPDIR", tmp}} {
		if err := os.Setenv(env.key, env.value); err != nil {
			panic(err)
		}
	}
	code := m.Run()
	if hadHome {
		_ = os.Setenv("HOME", oldHome)
	} else {
		_ = os.Unsetenv("HOME")
	}
	if hadTmp {
		_ = os.Setenv("TMPDIR", oldTmp)
	} else {
		_ = os.Unsetenv("TMPDIR")
	}
	_ = os.RemoveAll(root)
	os.Exit(code)
}

// TestShutdownRemovesCreatedArtifactDirectories proves the lifetime of the
// default artifact paths: they survive a finished turn (callers read the
// sentinel after it ends), they are removed when the supervisor shuts down,
// and a caller-supplied path is never touched.
func TestShutdownRemovesCreatedArtifactDirectories(t *testing.T) {
	sup := newTestSupervisor(t, Config{
		ControlSocket: newStableSocketPath(t, "artifact-cleanup"),
		MaxRuntimes:   2,
	})
	var spawnSeq int64
	sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) {
		return newParkedScriptedProvider(fmt.Sprintf("ses_artifact_%d", atomic.AddInt64(&spawnSeq, 1)), 1), nil
	}
	dir := t.TempDir()
	callerSentinel := filepath.Join(t.TempDir(), "caller-sentinel.env")

	defaultRes, err := sup.spawn(SpawnParams{Prompt: "hello", Dir: dir})
	if err != nil {
		t.Fatalf("spawn with default artifact paths: %v", err)
	}
	callerRes, err := sup.spawn(SpawnParams{Prompt: "hello", Dir: dir, SentinelFile: callerSentinel})
	if err != nil {
		t.Fatalf("spawn with a caller-supplied sentinel: %v", err)
	}
	waitForActiveRuntimeCount(t, sup, 0)
	for _, id := range []string{defaultRes.RuntimeID, callerRes.RuntimeID} {
		if !childParked(fetchChild(t, sup, id)) {
			t.Fatalf("runtime %s did not park after its turn ended", id)
		}
	}

	if _, err := os.Stat(defaultRes.SentinelFile); err != nil {
		t.Fatalf("default sentinel after the turn ended: %v", err)
	}
	artifactDir := filepath.Dir(defaultRes.SentinelFile)
	runDir := filepath.Dir(artifactDir)

	if code := sup.shutdown("graceful"); code != 0 {
		t.Fatalf("shutdown() = %d, want 0", code)
	}
	if _, err := os.Stat(artifactDir); !os.IsNotExist(err) {
		t.Fatalf("artifact directory %s survived shutdown: %v", artifactDir, err)
	}
	if _, err := os.Stat(runDir); !os.IsNotExist(err) {
		t.Fatalf("run artifact directory %s survived shutdown: %v", runDir, err)
	}
	if _, err := os.Stat(callerSentinel); err != nil {
		t.Fatalf("caller-supplied sentinel after shutdown: %v", err)
	}
}

func TestRootTreeBudgetUsesAvenorRuntimeStateAndCleansUp(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "project", "control.sock")
	sup := newTestSupervisor(t, Config{
		ControlSocket: socketPath,
		MaxRuntimes:   1,
		MaxTreeBudget: 1,
	})
	defer func() {
		sup.stopReaper()
		_ = sup.broker.Stop()
	}()

	budgetPath := sup.TreeBudgetPath()
	if budgetPath == "" {
		t.Fatal("root tree budget path is empty")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("get home directory: %v", err)
	}
	wantDir := filepath.Join(home, ".avenor", "sockets")
	if got := filepath.Dir(budgetPath); got != wantDir {
		t.Fatalf("tree budget directory = %q, want %q", got, wantDir)
	}
	if _, err := os.Stat(budgetPath); err != nil {
		t.Fatalf("stat root tree budget: %v", err)
	}
	if _, err := os.Stat(socketPath + ".tree-budget"); !os.IsNotExist(err) {
		t.Fatalf("control socket sidecar exists or could not be checked: %v", err)
	}

	// A nested supervisor receives the root's explicit path and therefore joins
	// the same budget even though its own control socket is elsewhere.
	nested := newTestSupervisor(t, Config{
		ControlSocket:  filepath.Join(t.TempDir(), "worktree", "nested.sock"),
		MaxRuntimes:    1,
		TreeBudgetFile: budgetPath,
	})
	defer func() {
		nested.stopReaper()
		nested.closeTreeBudget()
		_ = nested.broker.Stop()
	}()
	if got := nested.TreeBudgetPath(); got != budgetPath {
		t.Fatalf("nested tree budget path = %q, want root path %q", got, budgetPath)
	}

	if code := sup.shutdown("graceful"); code != 0 {
		t.Fatalf("shutdown() = %d, want 0", code)
	}
	if _, err := os.Stat(budgetPath); !os.IsNotExist(err) {
		t.Fatalf("root tree budget remains after cleanup or could not be checked: %v", err)
	}
}
