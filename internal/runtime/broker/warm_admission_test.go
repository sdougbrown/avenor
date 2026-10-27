package broker

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
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

// TestWarmAdmissionRejectsRunNotFound pins the not-found branch: a resume
// naming a run the broker does not hold is refused, not silently admitted as
// an empty-name lock would be.
func TestWarmAdmissionRejectsRunNotFound(t *testing.T) {
	b := New("")
	_, err := b.LockWarmAdmission("missing", "resume-label")
	if err == nil || !strings.Contains(err.Error(), "run not found: missing") {
		t.Fatalf("LockWarmAdmission on a missing run = %v, want run not found", err)
	}
}

// TestWarmAdmissionHarnessNameTakesPrecedence pins the harness-name branch: a
// resume with an explicit label that conflicts with the runtime's own
// HarnessName is refused, the rejected attempt leaves the run unlocked, and a
// label matching the harness name still admits.
func TestWarmAdmissionHarnessNameTakesPrecedence(t *testing.T) {
	b := New("")
	if _, err := b.CreateRun("rt", ""); err != nil {
		t.Fatal(err)
	}
	if err := b.SetHarnessName("rt", "harness-name"); err != nil {
		t.Fatal(err)
	}
	_, err := b.LockWarmAdmission("rt", "resume-label")
	if err == nil || !strings.Contains(err.Error(), `harness name "harness-name" takes precedence`) {
		t.Fatalf("conflicting label = %v, want harness name precedence", err)
	}
	if got := b.DisplayName("rt"); got != "harness-name" {
		t.Fatalf("rejected attempt changed the label to %q", got)
	}
	unlock, err := b.LockWarmAdmission("rt", "harness-name")
	if err != nil {
		t.Fatalf("matching label refused: %v", err)
	}
	unlock(false)
}

// TestWarmAdmissionRejectsDescendantRun pins the descendant branch: a run a
// child broker owns cannot be renamed by an admission on this broker, because
// the next claim from that branch would revert the write.
func TestWarmAdmissionRejectsDescendantRun(t *testing.T) {
	b := New("")
	b.ownRun(messaging.ClaimedRun{Key: "desc", RunClaim: messaging.RunClaim{RunID: "desc", Label: "branch-label"}}, true)
	_, err := b.LockWarmAdmission("desc", "resume-label")
	if err == nil || !strings.Contains(err.Error(), "owned by a child broker") {
		t.Fatalf("descendant admission = %v, want owned by a child broker", err)
	}
	if got := b.DisplayName("desc"); got != "branch-label" {
		t.Fatalf("rejected attempt changed the label to %q", got)
	}
}
