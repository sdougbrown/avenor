package parentlink

import (
	"net/http"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestRegistrationRouteAdmitsATokenlessRunAndTheRelayRefusesItBeforeAnyHTTP
// pins the credential guard end to end: the registration route accepts a claim
// whose run carries no credential, and the relay refuses that target before it
// builds or sends any HTTP, so the tokenless ask neither reaches a descendant
// nor retires the sibling that did present a credential. The sibling then
// relays exactly once, with the credential its registration carried.
func TestRegistrationRouteAdmitsATokenlessRunAndTheRelayRefusesItBeforeAnyHTTP(t *testing.T) {
	parent := startBranch(t, "", "orchestrator")
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		return http.StatusOK, `{"outcome":"sent"}`
	})
	registerDescendant(t, parent, "branch-delta", stub.URL(), "gen-a", "target", "", "sibling", "tok-b")
	if !parent.link.Registry().Owns(ParentRunKey("branch-delta", "target")) {
		t.Fatal("the registration route never adopted the tokenless claim")
	}

	generation := parent.broker.Store().Generation()
	asker := parent.token(t, "orchestrator")

	refusedID := newMsgID(t)
	refused := sendAsk(parent.broker.Addr(), "orchestrator", asker, ParentRunKey("branch-delta", "target"), refusedID, generation)
	if refused.err != nil {
		t.Fatalf("ask the tokenless run: %v", refused.err)
	}
	if refused.status != http.StatusNotFound || refused.body["code"] != "target-unavailable" {
		t.Fatalf("ask the tokenless run = (%d, %v), want 404 target-unavailable", refused.status, refused.body)
	}
	if got := stub.countPath(pathRelay); got != 0 {
		t.Fatalf("the tokenless target saw %d relays, want the preflight guard to fire before any HTTP call", got)
	}
	// The refused ask still owned an edge at this broker; withdraw it so the
	// fence does not outlive the test.
	if _, err := parent.broker.LocalMessenger().Withdraw(t.Context(), messaging.WithdrawRequest{
		Sender: messaging.Identity{RunID: "orchestrator", Sender: "orchestrator"}, MessageID: refusedID, Generation: generation,
	}); err != nil {
		t.Fatalf("withdraw the refused ask: %v", err)
	}

	siblingID := newMsgID(t)
	sibling := sendAsk(parent.broker.Addr(), "orchestrator", asker, ParentRunKey("branch-delta", "sibling"), siblingID, generation)
	if sibling.err != nil {
		t.Fatalf("ask the credentialed sibling: %v", sibling.err)
	}
	if sibling.status != http.StatusOK || sibling.body["outcome"] != string(messaging.OutcomeSent) {
		t.Fatalf("ask the credentialed sibling = (%d, %v), want 200 sent", sibling.status, sibling.body)
	}
	if got := stub.countPath(pathRelay); got != 1 {
		t.Fatalf("the sibling saw %d relays, want exactly the one successful delivery", got)
	}
	if got, _ := stub.lastRequest(t, pathRelay).Body["token"].(string); got != "tok-b" {
		t.Fatalf("the sibling's relay carried the token %q, want the credential its registration presented", got)
	}
}
