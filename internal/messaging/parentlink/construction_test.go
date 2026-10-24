package parentlink

import (
	"errors"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestNewRefusesTheCallbacksTheRelayReads pins the construction contract: the
// relay reads the generation on every hop, so a partially populated option set
// is refused when the messenger is built rather than at the first relay.
func TestNewRefusesTheCallbacksTheRelayReads(t *testing.T) {
	local := newStubMessenger()

	if _, err := New(Options{}, local); !errors.Is(err, messaging.ErrInvalidRequest) {
		t.Fatalf("New without a generation = %v, want ErrInvalidRequest", err)
	}

	if _, err := New(Options{Generation: func() string { return "gen-a" }}, nil); !errors.Is(err, messaging.ErrInvalidRequest) {
		t.Fatalf("New without a local seam = %v, want ErrInvalidRequest", err)
	}

	messenger, err := New(Options{
		Generation: func() string { return "gen-a" },
	}, local)
	if err != nil {
		t.Fatalf("New with the relay's callbacks: %v", err)
	}
	t.Cleanup(func() { _ = messenger.Close() })
}
