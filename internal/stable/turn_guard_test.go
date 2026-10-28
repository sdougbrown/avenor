package stable

import (
	"sync"
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

// TestTurnGuardBlocksSameRuntimeTurnWhileOperationOpen pins WithTurn's own
// serialization: a turn-bound operation held open on one runtime must block a
// second WithTurn on that runtime until it returns, while another runtime's
// turn proceeds independently.
func TestTurnGuardBlocksSameRuntimeTurnWhileOperationOpen(t *testing.T) {
	a := &childRuntime{sentinelFile: "turn-a"}
	b := &childRuntime{sentinelFile: "turn-b"}
	s := &Supervisor{runtimes: map[string]*childRuntime{"rt_a": a, "rt_b": b}}
	entered, release := make(chan struct{}), make(chan struct{})
	doneA := make(chan error, 1)
	go func() {
		doneA <- s.WithTurn("rt_a", "turn-a", func() error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	sameDone := make(chan error, 1)
	go func() {
		sameDone <- s.WithTurn("rt_a", "turn-a", func() error { return nil })
	}()
	otherDone := make(chan error, 1)
	go func() {
		otherDone <- s.WithTurn("rt_b", "turn-b", func() error { return nil })
	}()
	select {
	case err := <-otherDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime B's turn operation blocked behind runtime A's held turn")
	}
	// The other runtime's turn ran to completion, so the pending same-runtime
	// admission has been scheduled; it must still be waiting on rt_a's held turn.
	select {
	case err := <-sameDone:
		t.Fatalf("same-runtime turn proceeded while the operation was open: err=%v", err)
	default:
	}
	close(release)
	select {
	case err := <-sameDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("same-runtime turn operation deadlocked behind the held turn")
	}
	if err := <-doneA; err != nil {
		t.Fatal(err)
	}
}

func TestTurnGuardRefusesAnUnknownRuntime(t *testing.T) {
	child := &childRuntime{id: "rt", sentinelFile: "turn-a", mu: sync.Mutex{}}
	sup := &Supervisor{runtimes: map[string]*childRuntime{"rt": child}}
	called := false
	err := sup.WithTurn("missing", "turn-a", func() error { called = true; return nil })
	if err == nil {
		t.Fatal("WithTurn succeeded for an unknown runtime")
	}
	if called {
		t.Fatal("WithTurn ran the operation for an unknown runtime")
	}
}
