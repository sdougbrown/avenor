package control

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestCancelAcknowledgesBeforeTheRunStopsTheServer pins the order of a run-level
// cancel: the owner is answered before the run is cancelled, because cancelling
// starts a teardown that stops the server and closes the connection. A cancel
// that stopped the server first would answer an owner whose cancel succeeded
// with a closed connection.
func TestCancelAcknowledgesBeforeTheRunStopsTheServer(t *testing.T) {
	state := NewState("run_1", "", 0)
	s, cap := NewTestServer(state)
	if err := s.Start(testSocketPath(t)); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()
	cancelled := make(chan struct{})
	s.SetCancelFunc(func() {
		_ = s.Stop()
		close(cancelled)
	})

	owner := dialOwner(t, s, cap)
	defer owner.Close()
	if err := writeReq(t, owner, Request{JSONRPC: "2.0", ID: 1, Method: "cancel"}); err != nil {
		t.Fatalf("write cancel: %v", err)
	}
	if resp := readResp(t, owner); resp.Error != nil {
		t.Fatalf("cancel answered %+v, want an acknowledgement", resp.Error)
	}
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("the cancel never ran the run's cancel function")
	}
}

// TestRunLevelCancelAnswersOnce pins the observable half of the async response
// contract: a run-level cancel writes one response, not two. The helper that
// acknowledges and returns the dispatch sentinel exists so the write and the
// sentinel cannot drift apart; a handler that wrote inline and returned a normal
// response would answer twice, which is what this test reads for.
func TestRunLevelCancelAnswersOnce(t *testing.T) {
	state := NewState("run_1", "", 0)
	s, cap := NewTestServer(state)
	if err := s.Start(testSocketPath(t)); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	cancelled := make(chan struct{})
	s.SetCancelFunc(func() { close(cancelled) })

	owner := dialOwner(t, s, cap)
	defer owner.Close()
	if err := writeReq(t, owner, Request{JSONRPC: "2.0", ID: 1, Method: "cancel"}); err != nil {
		t.Fatalf("write cancel: %v", err)
	}
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("the cancel never ran the run's cancel function")
	}

	// Drain whatever the server wrote until the deadline: exactly one line.
	_ = owner.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	buf := make([]byte, 0, 1024)
	chunk := make([]byte, 256)
	for {
		n, err := owner.Read(chunk)
		buf = append(buf, chunk[:n]...)
		if err != nil {
			break
		}
	}
	written := 0
	for _, line := range strings.Split(strings.TrimSpace(string(buf)), "\n") {
		if strings.TrimSpace(line) != "" {
			written++
		}
	}
	if written != 1 {
		t.Fatalf("cancel wrote %d response lines, want 1: %q", written, string(buf))
	}
}

func TestAnswerThenRunRunsAfterTheAnswer(t *testing.T) {
	answered := false
	var runs atomic.Int32
	answerThenRun(func() { answered = true }, func() {
		if !answered {
			t.Error("run started before the answer was written")
		}
		runs.Add(1)
	}, time.Minute)
	if got := runs.Load(); got != 1 {
		t.Fatalf("run called %d times, want 1", got)
	}
}

// TestAnswerThenRunDoesNotWaitOnAnAnswerThatBlocks pins that a client that stops
// reading its socket cannot keep the action from happening: run starts after the
// wait while the answer is still blocked, and only once.
func TestAnswerThenRunDoesNotWaitOnAnAnswerThatBlocks(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 2)
	var runs atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		answerThenRun(func() { <-release }, func() {
			runs.Add(1)
			started <- struct{}{}
		}, 20*time.Millisecond)
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("run never started while the answer was blocked")
	}
	close(release)
	<-done
	if got := runs.Load(); got != 1 {
		t.Fatalf("run called %d times, want exactly 1", got)
	}
}

func TestAnswerThenRunWithNoActionOnlyAnswers(t *testing.T) {
	answered := false
	answerThenRun(func() { answered = true }, nil, time.Minute)
	if !answered {
		t.Fatal("the answer was not written")
	}
}
