package stable

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
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

// TestPruneArtifactDirs covers the retention policy of the per-user artifact
// root: a stale run directory is removed, a fresh one is kept, and an entry
// that is not a run directory is never a candidate.
func TestPruneArtifactDirs(t *testing.T) {
	root, err := ensureArtifactRoot()
	if err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(root); err != nil {
		t.Fatal(err)
	} else if got := info.Mode().Perm(); got != 0o700 {
		t.Fatalf("artifact root mode = %04o, want 0700", got)
	}

	cases := []struct {
		name     string
		entry    string
		modTime  time.Time
		wantKept bool
	}{
		{"stale run directory", "run_stale", time.Now().Add(-artifactRetention - time.Hour), false},
		{"fresh run directory", "run_fresh", time.Now(), true},
		{"not a run directory", "stale_elsewhere", time.Now().Add(-artifactRetention - time.Hour), true},
	}
	for _, tc := range cases {
		dir := filepath.Join(root, tc.entry)
		writeArtifactRun(t, dir, tc.modTime)
		t.Cleanup(func() { _ = os.RemoveAll(dir) })
	}

	sup := newTestSupervisor(t, Config{Stderr: io.Discard, MaxRuntimes: 1})
	sup.pruneArtifactDirs()

	for _, tc := range cases {
		_, err := os.Stat(filepath.Join(root, tc.entry))
		if kept := err == nil; kept != tc.wantKept {
			t.Fatalf("%s: kept = %v, want %v (err %v)", tc.name, kept, tc.wantKept, err)
		}
	}
}

// TestPruneArtifactDirsKeepsLiveRunDirectory proves the liveness signal: a run
// directory whose modification time alone says stale survives because its
// supervisor refreshes it before pruning.
func TestPruneArtifactDirsKeepsLiveRunDirectory(t *testing.T) {
	sup := newTestSupervisor(t, Config{Stderr: io.Discard, MaxRuntimes: 1})
	dir := artifactRunDir(sup.runID)
	writeArtifactRun(t, dir, time.Now().Add(-artifactRetention-time.Hour))
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	sup.touchArtifactRunDir()
	sup.pruneArtifactDirs()

	if _, err := os.Stat(filepath.Join(dir, "sentinel.env")); err != nil {
		t.Fatalf("live run directory after prune: %v", err)
	}
}

// TestArtifactRootRefusal pins the roots retention refuses: one another user
// pre-created as a symlink, and one left group-writable. Neither is read or
// pruned, a spawn that needs the default paths fails naming the root, and the
// refusal is reported once instead of on every hourly pass.
func TestArtifactRootRefusal(t *testing.T) {
	cases := []struct {
		name    string
		prepare func(t *testing.T, root string) string
	}{
		{
			name: "symlinked root",
			prepare: func(t *testing.T, root string) string {
				target := t.TempDir()
				if err := os.Symlink(target, root); err != nil {
					t.Fatal(err)
				}
				return target
			},
		},
		{
			name: "group writable root",
			prepare: func(t *testing.T, root string) string {
				if err := os.Mkdir(root, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(root, 0o770); err != nil {
					t.Fatal(err)
				}
				return root
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := artifactRoot()
			if err := os.RemoveAll(root); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(root) })
			victim := filepath.Join(tc.prepare(t, root), "run_victim")
			writeArtifactRun(t, victim, time.Now().Add(-artifactRetention-time.Hour))

			if _, err := ensureArtifactRoot(); err == nil || !strings.Contains(err.Error(), root) {
				t.Fatalf("ensureArtifactRoot error = %v, want one naming %s", err, root)
			}

			var stderr bytes.Buffer
			sup := newTestSupervisor(t, Config{Stderr: &stderr, MaxRuntimes: 1})
			sup.touchArtifactRunDir()
			sup.pruneArtifactDirs()
			sup.pruneArtifactDirs()

			if _, err := os.Stat(filepath.Join(victim, "sentinel.env")); err != nil {
				t.Fatalf("run directory under a refused root was touched: %v", err)
			}
			if got := strings.Count(stderr.String(), "artifact retention disabled"); got != 1 {
				t.Fatalf("refusal diagnostics = %d, want 1: %q", got, stderr.String())
			}
		})
	}
}

// writeArtifactRun creates a run directory holding one artifact and sets the
// modification times the retention policy reads.
func writeArtifactRun(t *testing.T, dir string, modTime time.Time) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(dir, "sentinel.env")
	if err := os.WriteFile(sentinel, []byte("status=DONE\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{sentinel, dir} {
		if err := os.Chtimes(path, modTime, modTime); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRootTreeBudgetUsesAvenorRuntimeStateAndCleansUp(t *testing.T) {
	socketPath := newStableSocketPath(t, "root-tree-budget")
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
		ControlSocket:  newStableSocketPath(t, "nested-tree-budget"),
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
