package stable

import (
	"testing"
	"time"
)

// TestStableRunnerDepsRefreshSharesRebuilds proves the supervisor-wide
// refresh window: the first refresh after startup rebuilds, two
// controllers' back-to-back refreshes cause one rebuild, and a refresh
// after the window elapses rebuilds again.
func TestStableRunnerDepsRefreshSharesRebuilds(t *testing.T) {
	f := newRunnerFixture(t, "refresh-share", "", 4, 0, false)
	depsA := &stableRunnerDeps{s: f.sup, controllerID: "c1"}
	depsB := &stableRunnerDeps{s: f.sup, controllerID: "c2"}

	if err := depsA.Refresh(); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	if n := f.sup.candidateRebuilds.Load(); n != 1 {
		t.Fatalf("rebuilds after first refresh = %d, want 1 (first refresh always rebuilds)", n)
	}
	if err := depsB.Refresh(); err != nil {
		t.Fatalf("second refresh: %v", err)
	}
	if n := f.sup.candidateRebuilds.Load(); n != 1 {
		t.Fatalf("rebuilds after back-to-back refresh = %d, want 1 (shared window)", n)
	}

	f.sup.candidateRefreshMu.Lock()
	f.sup.candidateRefreshWindow = 20 * time.Millisecond
	f.sup.candidateRefreshMu.Unlock()
	time.Sleep(30 * time.Millisecond)
	if err := depsB.Refresh(); err != nil {
		t.Fatalf("refresh after window: %v", err)
	}
	if n := f.sup.candidateRebuilds.Load(); n != 2 {
		t.Fatalf("rebuilds after window elapsed = %d, want 2", n)
	}
}
