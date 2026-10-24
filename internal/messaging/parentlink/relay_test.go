package parentlink

import (
	"errors"
	"net/http"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// newTestRelay builds one relay over a registry that already owns a child
// broker with the given runs.
func newTestRelay(t *testing.T, registry *Registry, childID, endpoint, generation string, runs ...string) *relay {
	t.Helper()
	claims := make([]messaging.RunClaim, 0, len(runs))
	for _, runID := range runs {
		claims = append(claims, messaging.RunClaim{RunID: runID})
	}
	if _, err := registry.Register(messaging.DescendantRegistration{
		ChildID:    childID,
		Endpoint:   endpoint,
		Generation: generation,
		Runs:       claims,
	}); err != nil {
		t.Fatalf("register child %q: %v", childID, err)
	}
	relay, err := newRelay(Options{
		RunToken:   func(string) (string, bool) { return "tok-b", true },
		Generation: func() string { return generation },
	}, registry)
	if err != nil {
		t.Fatalf("newRelay: %v", err)
	}
	t.Cleanup(func() { _ = relay.Close() })
	return relay
}

// TestRelayDropsTheWholeChildWhenItRefusesTheBranch pins the relay's refusal
// rule: a child that rejects the relay because its registration was replaced
// or its credentials were refused is no longer the branch it registered as, so
// every run it owns stops being routed instead of one run at a time.
func TestRelayDropsTheWholeChildWhenItRefusesTheBranch(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		return http.StatusConflict, `{"code":"generation-mismatch","error":"the branch was replaced"}`
	})
	registry := NewRegistry()
	relay := newTestRelay(t, registry, "child-one", stub.URL(), "gen-a", "run-b", "run-c")

	err := relay.Ask(testContext(t), messaging.SendRequest{
		From:      messaging.Identity{RunID: "run-a", Sender: "alice"},
		To:        messaging.Identity{RunID: "run-b", Sender: "bob"},
		MessageID: "msg-1",
	})
	if !errors.Is(err, messaging.ErrTargetUnavailable) {
		t.Fatalf("Ask = %v, want ErrTargetUnavailable", err)
	}
	if _, ok := registry.Hop("run-b"); ok {
		t.Fatal("the refused branch still routes the run that was relayed to")
	}
	if _, ok := registry.Hop("run-c"); ok {
		t.Fatal("the refused branch still routes its other run")
	}
}
