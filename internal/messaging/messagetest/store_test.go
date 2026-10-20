package messagetest

import (
	"encoding/json"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestMemoryBackendRemovesExactEntry pins that a removal closure retracts the
// entry it published, not an earlier one that shares its message ID.
func TestMemoryBackendRemovesExactEntry(t *testing.T) {
	backend := NewMemoryBackend()
	first, err := backend.Publish("bob", messaging.InboundMessage{MessageID: "shared-id", Payload: json.RawMessage(`{"n":1}`)})
	if err != nil {
		t.Fatalf("first Publish: %v", err)
	}
	second, err := backend.Publish("bob", messaging.InboundMessage{MessageID: "shared-id", Payload: json.RawMessage(`{"n":2}`)})
	if err != nil {
		t.Fatalf("second Publish: %v", err)
	}

	second()
	second()
	if removed := backend.Removed(); removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}
	received, err := backend.Receive("bob", 8)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if len(received) != 1 || string(received[0].Payload) != `{"n":1}` {
		t.Fatalf("Receive = %+v, want the first entry", received)
	}
	first()
	if removed := backend.Removed(); removed != 1 {
		t.Fatalf("removed = %d, want 1 after removing an already-drained entry", removed)
	}
}
