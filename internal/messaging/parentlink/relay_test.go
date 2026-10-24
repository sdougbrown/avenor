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
		claims = append(claims, messaging.RunClaim{RunID: runID, Token: "tok-b"})
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
		Generation: func() string { return generation },
	}, registry)
	if err != nil {
		t.Fatalf("newRelay: %v", err)
	}
	t.Cleanup(func() { _ = relay.Close() })
	return relay
}

// TestRelayRetiresOnlyTheRefusedRunsRoute pins the relay's refusal rule: a
// child that rejects the relay stops routing the run the relay was posted for,
// while the branch's other runs keep their route: retirement is scoped to the
// run whose relay was refused, and it only lands while the claim still resolves
// to the hop the relay was posted against.
func TestRelayRetiresOnlyTheRefusedRunsRoute(t *testing.T) {
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
	if got, _ := stub.lastRequest(t, pathRelay).Body["token"].(string); got != "tok-b" {
		t.Fatalf("the relay posted the token %q, want the credential the child registered", got)
	}
	if _, ok := registry.Hop("run-b"); ok {
		t.Fatal("the refused branch still routes the run that was relayed to")
	}
	if _, ok := registry.Hop("run-c"); !ok {
		t.Fatal("the refusal retired the child's other run's route")
	}
}
