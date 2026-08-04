package stable

import (
	"os"
	"path/filepath"
	"testing"
)

// TestMain keeps root-budget runtime state out of a developer's real home
// directory. Production roots still use the user's Avenor runtime-state
// directory; this only isolates the package's many direct NewSupervisor tests.
func TestMain(m *testing.M) {
	home, err := os.MkdirTemp("", "avenor-stable-home-")
	if err != nil {
		panic(err)
	}
	oldHome, hadHome := os.LookupEnv("HOME")
	if err := os.Setenv("HOME", home); err != nil {
		panic(err)
	}
	code := m.Run()
	if hadHome {
		_ = os.Setenv("HOME", oldHome)
	} else {
		_ = os.Unsetenv("HOME")
	}
	_ = os.RemoveAll(home)
	os.Exit(code)
}

func TestRootTreeBudgetUsesAvenorRuntimeStateAndCleansUp(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "project", "control.sock")
	sup := NewSupervisor(Config{
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
	nested := NewSupervisor(Config{
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
