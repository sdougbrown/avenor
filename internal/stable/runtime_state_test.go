package stable

import (
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
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

// TestSupervisorStartupPrunesStaleArtifactDirectories proves the retention
// policy of the default artifact tree: startup removes a run directory whose
// newest entry is older than the retention window, keeps a fresh one, and
// never follows a symlink out of the tree.
func TestSupervisorStartupPrunesStaleArtifactDirectories(t *testing.T) {
	root := artifactRoot()
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	staleRun := filepath.Join(root, "run_stale")
	freshRun := filepath.Join(root, "run_fresh")
	outside := filepath.Join(t.TempDir(), "outside")
	for _, dir := range []string{staleRun, freshRun, outside} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _ = os.RemoveAll(freshRun) })
	staleSentinel := filepath.Join(staleRun, "sentinel.env")
	if err := os.WriteFile(staleSentinel, []byte("status=DONE\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(freshRun, "sentinel.env"), []byte("status=DONE\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	outsideFile := filepath.Join(outside, "keep.env")
	if err := os.WriteFile(outsideFile, []byte("keep\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(staleRun, "linked-run")); err != nil {
		t.Fatal(err)
	}
	stale := time.Now().Add(-artifactRetention - time.Hour)
	for _, path := range []string{staleSentinel, staleRun} {
		if err := os.Chtimes(path, stale, stale); err != nil {
			t.Fatal(err)
		}
	}

	newTestSupervisor(t, Config{Stderr: io.Discard, MaxRuntimes: 1})

	if _, err := os.Stat(staleRun); !os.IsNotExist(err) {
		t.Fatalf("stale run directory after startup: %v", err)
	}
	if _, err := os.Stat(filepath.Join(freshRun, "sentinel.env")); err != nil {
		t.Fatalf("fresh run directory after startup: %v", err)
	}
	if _, err := os.Stat(outsideFile); err != nil {
		t.Fatalf("artifacts outside the tree after startup: %v", err)
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
