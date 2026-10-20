package stable

import (
	"bytes"
	"encoding/json"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

// TestPruneArtifactDirs covers the retention policy of the per-user artifact
// root: a stale run directory is removed, a fresh one is kept, an entry that is
// not a run directory is never a candidate, a fresh entry nested in the real
// run_<id>/<rt_id> layout keeps a stale run directory, and a symlink is never
// followed.
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

	stale := time.Now().Add(-artifactRetention - time.Hour)
	now := time.Now()
	cases := []struct {
		name     string
		entry    string
		prepare  func(t *testing.T, dir string) string
		wantKept bool
		outside  string
	}{
		{
			name:  "stale run directory",
			entry: "run_stale",
			prepare: func(t *testing.T, dir string) string {
				writeArtifactRun(t, dir, stale)
				return ""
			},
			wantKept: false,
		},
		{
			name:  "fresh run directory",
			entry: "run_fresh",
			prepare: func(t *testing.T, dir string) string {
				writeArtifactRun(t, dir, now)
				return ""
			},
			wantKept: true,
		},
		{
			name:  "not a run directory",
			entry: "stale_elsewhere",
			prepare: func(t *testing.T, dir string) string {
				writeArtifactRun(t, dir, stale)
				return ""
			},
			wantKept: true,
		},
		{
			// Both directories look stale and only the nested file is fresh, so
			// the run survives only if the walk descends into it.
			name:  "stale run directory with a fresh nested artifact",
			entry: "run_nested",
			prepare: func(t *testing.T, dir string) string {
				rtDir := filepath.Join(dir, "rt_1")
				writeArtifactRun(t, rtDir, stale)
				fresh := filepath.Join(rtDir, "events.ndjson")
				if err := os.WriteFile(fresh, []byte("{}\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Chtimes(fresh, now, now); err != nil {
					t.Fatal(err)
				}
				// Written entries changed both directories' own mtimes; restore
				// them so only the nested file is fresh.
				for _, path := range []string{rtDir, dir} {
					if err := os.Chtimes(path, stale, stale); err != nil {
						t.Fatal(err)
					}
				}
				return ""
			},
			wantKept: true,
		},
		{
			// A run entry that is a symlink is not a run directory, so the policy
			// leaves the link and the outside tree it names alone.
			name:  "symlinked run entry",
			entry: "run_link",
			prepare: func(t *testing.T, dir string) string {
				outside := filepath.Join(t.TempDir(), "run_elsewhere")
				writeArtifactRun(t, outside, stale)
				if err := os.Symlink(outside, dir); err != nil {
					t.Fatal(err)
				}
				return outside
			},
			wantKept: true,
		},
		{
			// A fresh tree behind a link inside a stale run directory must not
			// keep that directory alive, and the removal stops at the link.
			name:  "symlink inside a stale run directory",
			entry: "run_escape",
			prepare: func(t *testing.T, dir string) string {
				outside := filepath.Join(t.TempDir(), "run_elsewhere")
				writeArtifactRun(t, outside, now)
				writeArtifactRun(t, dir, stale)
				if err := os.Symlink(outside, filepath.Join(dir, "escape")); err != nil {
					t.Fatal(err)
				}
				// Creating the link changed the run directory's own mtime.
				if err := os.Chtimes(dir, stale, stale); err != nil {
					t.Fatal(err)
				}
				return outside
			},
			wantKept: false,
		},
	}
	for i := range cases {
		tc := &cases[i]
		dir := filepath.Join(root, tc.entry)
		tc.outside = tc.prepare(t, dir)
		t.Cleanup(func() { _ = os.RemoveAll(dir) })
	}

	sup := newTestSupervisor(t, Config{Stderr: io.Discard, MaxRuntimes: 1})
	sup.pruneArtifactDirs()

	for _, tc := range cases {
		_, err := os.Stat(filepath.Join(root, tc.entry))
		if kept := err == nil; kept != tc.wantKept {
			t.Fatalf("%s: kept = %v, want %v (err %v)", tc.name, kept, tc.wantKept, err)
		}
		if tc.outside == "" {
			continue
		}
		if _, err := os.Stat(filepath.Join(tc.outside, "sentinel.env")); err != nil {
			t.Fatalf("%s: content outside the artifact root after prune: %v", tc.name, err)
		}
	}
}

// TestArtifactRetentionTickPrunesOtherRunsKeepsLiveRun covers the retention
// loop: a run directory that is stale after the first pass is removed on a
// later tick, while the supervisor's own refreshed directory survives.
func TestArtifactRetentionTickPrunesOtherRunsKeepsLiveRun(t *testing.T) {
	root, err := ensureArtifactRoot()
	if err != nil {
		t.Fatal(err)
	}
	stale := time.Now().Add(-artifactRetention - time.Hour)
	socketPath := newStableSocketPath(t, "artifact-tick")
	sup := newTestSupervisor(t, Config{ControlSocket: socketPath, MaxRuntimes: 1, Stderr: io.Discard})
	sup.artifactPruneInterval = 20 * time.Millisecond
	own := artifactRunDir(sup.runID)
	writeArtifactRun(t, own, stale)
	t.Cleanup(func() { _ = os.RemoveAll(own) })
	t.Cleanup(func() { _ = sup.Shutdown("graceful") })

	done := startStableRun(t, sup, socketPath)

	// Wait for the startup pass, which refreshes this supervisor's own
	// directory: the directory below is then only ever removed by a tick.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if info, err := os.Stat(own); err == nil && time.Since(info.ModTime()) < time.Second {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the startup pass never refreshed the supervisor's run directory")
		}
		time.Sleep(5 * time.Millisecond)
	}

	other := filepath.Join(root, artifactRunPrefix+"other")
	writeArtifactRun(t, other, stale)
	t.Cleanup(func() { _ = os.RemoveAll(other) })

	deadline = time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(other); os.IsNotExist(err) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a stale run directory survived the retention ticker")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := os.Stat(filepath.Join(own, "sentinel.env")); err != nil {
		t.Fatalf("the supervisor's own run directory after a tick: %v", err)
	}

	if err := sup.Shutdown("graceful"); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("Run() = %d, want 0", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after shutdown")
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
// pre-created as a symlink, one left group-writable, and one that grants group
// or other permissions. None is read or pruned, the refusal is reported once
// instead of on every hourly pass, and a spawn that needs the default paths
// fails naming the root and what to do about it, while a spawn that supplies
// its own artifact paths is unaffected.
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
		{
			name: "root with group and other bits",
			prepare: func(t *testing.T, root string) string {
				if err := os.Mkdir(root, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(root, 0o755); err != nil {
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
			sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) {
				return &idempotencyTestProvider{}, nil
			}
			sup.touchArtifactRunDir()
			sup.pruneArtifactDirs()
			sup.pruneArtifactDirs()

			if _, err := os.Stat(filepath.Join(victim, "sentinel.env")); err != nil {
				t.Fatalf("run directory under a refused root was touched: %v", err)
			}
			if got := strings.Count(stderr.String(), "artifact retention disabled"); got != 1 {
				t.Fatalf("refusal diagnostics = %d, want 1: %q", got, stderr.String())
			}

			// A spawn that relies on the default paths fails, and the error names
			// the root together with the way out of the refusal.
			dir := t.TempDir()
			raw, err := json.Marshal(SpawnParams{Prompt: "hello", Dir: dir, Agent: "claude"})
			if err != nil {
				t.Fatal(err)
			}
			_, err = sup.Spawn(raw)
			if err == nil {
				t.Fatal("spawn without artifact paths under a refused root succeeded")
			}
			for _, want := range []string{root, "chmod it to 0700"} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("spawn error = %v, want it to mention %q", err, want)
				}
			}

			// A spawn that supplies both artifact paths writes them itself and
			// never reaches the refused root.
			raw, err = json.Marshal(SpawnParams{
				Prompt:       "hello",
				Dir:          dir,
				Agent:        "claude",
				OnEvent:      filepath.Join(dir, "events.ndjson"),
				SentinelFile: filepath.Join(dir, "sentinel.env"),
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := sup.Spawn(raw); err != nil {
				t.Fatalf("spawn with explicit artifact paths under a refused root: %v", err)
			}
		})
	}
}

// TestArtifactPermissionDenialReportedOncePerPath proves the suppression is
// keyed on the denied path: after the first path has been collapsed, a second
// path still reports its own denial once.
func TestArtifactPermissionDenialReportedOncePerPath(t *testing.T) {
	var stderr bytes.Buffer
	sup := newTestSupervisor(t, Config{Stderr: &stderr, MaxRuntimes: 1})
	for _, path := range []string{"/artifact-a", "/artifact-a", "/artifact-b", "/artifact-b"} {
		sup.reportArtifactError(path, fs.ErrPermission, "read artifact dir %s: permission denied", path)
	}
	for _, path := range []string{"/artifact-a", "/artifact-b"} {
		if got := strings.Count(stderr.String(), "read artifact dir "+path); got != 1 {
			t.Fatalf("%s reported %d times, want 1: %q", path, got, stderr.String())
		}
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
