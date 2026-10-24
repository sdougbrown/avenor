package parentlink

import (
	"errors"
	"net/http"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestLinkRefusesABodyThatIsNotOneJSONValue pins the payload the link puts on
// the wire: a body that is not one JSON value cannot be rendered into the
// request, so the ask is refused here and the fault is the caller's, not the
// parent's reachability.
func TestLinkRefusesABodyThatIsNotOneJSONValue(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		}
		return http.StatusOK, `{}`
	})
	ctx := testContext(t)
	link := newTestLink(t, stub, "gen-a")

	request := askRequest("msg-1")
	request.Body = `{"message":"ping"},"from_run_id":"run-victim"`
	sent, err := link.Send(ctx, request)
	if !errors.Is(err, messaging.ErrInvalidRequest) {
		t.Fatalf("Send error = %v, want ErrInvalidRequest", err)
	}
	if sent.Outcome != messaging.OutcomeNotSent {
		t.Fatalf("Send outcome = %q, want %q", sent.Outcome, messaging.OutcomeNotSent)
	}
	if got := stub.countPath(pathSend); got != 0 {
		t.Fatalf("the parent saw %d sends, want none", got)
	}
}

// TestLinkRefusesANonJSONBody pins the same rule for a body that is not JSON
// at all, the way the local transport refuses one.
func TestLinkRefusesANonJSONBody(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		}
		return http.StatusOK, `{}`
	})
	ctx := testContext(t)
	link := newTestLink(t, stub, "gen-a")

	request := askRequest("msg-1")
	request.Body = `hello`
	sent, err := link.Send(ctx, request)
	if !errors.Is(err, messaging.ErrInvalidRequest) {
		t.Fatalf("Send error = %v, want ErrInvalidRequest", err)
	}
	if sent.Outcome != messaging.OutcomeNotSent {
		t.Fatalf("Send outcome = %q, want %q", sent.Outcome, messaging.OutcomeNotSent)
	}
}

// TestRelayRefusesABodyThatIsNotOneJSONValue pins the same rule on the hop
// down to a child: a relayed body that is not one JSON value never reaches
// the child.
func TestRelayRefusesABodyThatIsNotOneJSONValue(t *testing.T) {
	child := newStubBroker(t, func(req stubRequest) (int, string) {
		return http.StatusOK, `{"outcome":"sent"}`
	})
	registry := NewRegistry()
	relay := newTestRelay(t, registry, "child-one", child.URL(), "gen-a", "run-b")

	request := askRequest("msg-1")
	request.Body = `{"message":"ping"},"to_run_id":"run-victim"`
	err := relay.Ask(testContext(t), request)
	if !errors.Is(err, messaging.ErrInvalidRequest) {
		t.Fatalf("Ask = %v, want ErrInvalidRequest", err)
	}
	if got := child.countPath(pathRelay); got != 0 {
		t.Fatalf("the child saw %d relays, want none", got)
	}
}
