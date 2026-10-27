package acp

import (
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime/warmqueue"
)

func TestWarmAdmissionRejectsQueuedAndInjectingMessages(t *testing.T) {
	p := &Provider{Queue: warmqueue.NewQueue()}
	p.Queue.Enqueue("session", "message")
	if _, ok := p.LockWarmMessageAdmission("session"); ok {
		t.Fatal("queued message admitted")
	}
	msgs, done := p.Queue.Take("session")
	if len(msgs) != 1 {
		t.Fatalf("taken messages = %#v", msgs)
	}
	if _, ok := p.LockWarmMessageAdmission("session"); ok {
		t.Fatal("in-flight injection admitted")
	}
	done()
	unlock, ok := p.LockWarmMessageAdmission("session")
	if !ok {
		t.Fatal("empty queue rejected")
	}
	unlock()
}
