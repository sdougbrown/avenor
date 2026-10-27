package stable

import (
	"testing"
	"time"
)

func TestTurnGuardCallbackMayAcquireControlLock(t *testing.T) {
	child := &childRuntime{sentinelFile: "turn-a"}
	s := &Supervisor{runtimes: map[string]*childRuntime{"rt": child}}
	done := make(chan error, 1)
	go func() {
		done <- s.WithTurn("rt", "turn-a", func() error { s.controlMu.Lock(); defer s.controlMu.Unlock(); return nil })
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("turn callback deadlocked acquiring control lock")
	}
}

func TestTurnGuardRejectsReusedRuntime(t *testing.T) {
	child := &childRuntime{sentinelFile: "turn-a"}
	s := &Supervisor{runtimes: map[string]*childRuntime{"rt": child}}
	called := false
	if err := s.WithTurn("rt", "turn-a", func() error { called = true; return nil }); err != nil || !called {
		t.Fatalf("current turn: called=%v err=%v", called, err)
	}
	child.sentinelFile = "turn-b"
	called = false
	if err := s.WithTurn("rt", "turn-a", func() error { called = true; return nil }); err == nil || called {
		t.Fatalf("stale turn: called=%v err=%v", called, err)
	}
}

// TestTurnGuardDoesNotBlockOtherRuntimes pins the fence as a per-runtime lock:
// a turn-bound operation held open on one runtime must not block turn-bound
// work on another.
func TestTurnGuardDoesNotBlockOtherRuntimes(t *testing.T) {
	a := &childRuntime{sentinelFile: "turn-a"}
	b := &childRuntime{sentinelFile: "turn-b"}
	s := &Supervisor{runtimes: map[string]*childRuntime{"rt_a": a, "rt_b": b}}
	release := make(chan struct{})
	doneA := make(chan error, 1)
	go func() {
		doneA <- s.WithTurn("rt_a", "turn-a", func() error { <-release; return nil })
	}()
	doneB := make(chan error, 1)
	go func() {
		doneB <- s.WithTurn("rt_b", "turn-b", func() error { return nil })
	}()
	select {
	case err := <-doneB:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime B's turn operation blocked behind runtime A's held turn")
	}
	close(release)
	if err := <-doneA; err != nil {
		t.Fatal(err)
	}
}
