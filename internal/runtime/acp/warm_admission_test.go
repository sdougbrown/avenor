package acp

import (
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime/warmqueue"
)

// TestWarmAdmissionRejectsQueuedAndInjectingMessages proves method promotion
// of the embedded *warmqueue.Queue: a provider that embeds the queue satisfies
// runtime.WarmMessageAdmission with its own method set, and the guard
// refuses while prompts are queued or in flight. The guard's behavior itself
// is pinned in internal/runtime/warmqueue, and the supervisor-level refusal in
// TestWarmResumeRefusesPendingProviderMessage.
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
