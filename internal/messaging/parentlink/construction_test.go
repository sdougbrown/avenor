package parentlink

import (
	"errors"
	"net/http"
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

	// The built messenger relays: a send to a run a registered child owns
	// reaches the child's queue end to end, not just past the refusals above.
	child := newStubBroker(t, func(req stubRequest) (int, string) {
		return http.StatusOK, `{"outcome":"queued"}`
	})
	local.sendResult = messaging.SendResult{Outcome: messaging.OutcomeSent, MessageID: "msg-1"}
	if _, err := messenger.Register(messaging.DescendantRegistration{
		ChildID:    "child-one",
		Endpoint:   child.URL(),
		Generation: "gen-a",
		Runs:       []messaging.RunClaim{{RunID: "run-b", Token: "tok-b"}},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	sent, err := messenger.Send(testContext(t), askRequest("msg-1"), false)
	if err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("Send = (%v, %v), want the relayed ask delivered", sent, err)
	}
	if child.countPath(pathRelay) != 1 {
		t.Fatalf("the child saw %d relays, want the constructed relay to deliver", child.countPath(pathRelay))
	}
}
