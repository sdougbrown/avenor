package acp

import "testing"

func TestWarmAdmissionRejectsQueuedAndInjectingMessages(t *testing.T) {
	p := &Provider{pendingMessages: map[string][]string{"session": {"message"}}, injectingMessages: map[string]int{}}
	if _, ok := p.LockWarmMessageAdmission("session"); ok {
		t.Fatal("queued message admitted")
	}
	p.pendingMessages["session"] = nil
	p.injectingMessages["session"] = 1
	if _, ok := p.LockWarmMessageAdmission("session"); ok {
		t.Fatal("in-flight injection admitted")
	}
	p.injectingMessages["session"] = 0
	unlock, ok := p.LockWarmMessageAdmission("session")
	if !ok {
		t.Fatal("empty queue rejected")
	}
	unlock()
}
