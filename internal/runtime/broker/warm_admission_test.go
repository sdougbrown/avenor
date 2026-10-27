package broker

import (
	"encoding/json"
	"testing"
	"time"
)

func TestWarmAdmissionRejectsMessageBeingDeliveredToProvider(t *testing.T) {
	b := New("")
	b.CreateRun("rt", "")
	if err := b.Send("rt", "agent_message", json.RawMessage(`{"message":"hello"}`), "hello"); err != nil {
		t.Fatal(err)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() { b.pollAgentMessagesOnce("rt", func(string) { close(entered); <-release }); close(done) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("delivery did not start")
	}
	if unlock, err := b.LockWarmAdmission("rt", ""); err == nil {
		unlock(false)
		t.Error("admitted while message delivery was in flight")
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("delivery did not finish")
	}
	unlock, err := b.LockWarmAdmission("rt", "")
	if err != nil {
		t.Fatal(err)
	}
	unlock(false)
}
