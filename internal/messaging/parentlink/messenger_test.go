package parentlink

import (
	"net/http"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// newTestMessenger builds one parent-aware messenger over a stub local seam
// and, when a parent is given, a stub parent broker.
func newTestMessenger(t *testing.T, local *stubMessenger, parent *stubBroker) *Messenger {
	t.Helper()
	opts := Options{
		Endpoint:   "http://child-one",
		Runs:       testRuns,
		Generation: func() string { return "gen-a" },
	}
	if parent != nil {
		opts.ParentURL = parent.URL()
	}
	messenger, err := New(opts, local)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = messenger.Close() })
	return messenger
}

// TestMessengerWithdrawsTheEdgeWhenTheRelayFails pins the relay failure's
// contract: the edge this broker admitted is retracted, so a later wait does
// not block on a reply that cannot arrive.
func TestMessengerWithdrawsTheEdgeWhenTheRelayFails(t *testing.T) {
	child := newStubBroker(t, func(req stubRequest) (int, string) {
		return http.StatusInternalServerError, `{}`
	})
	local := newStubMessenger()
	local.sendResult = messaging.SendResult{Outcome: messaging.OutcomeSent, MessageID: "msg-1"}
	messenger := newTestMessenger(t, local, nil)
	if _, err := messenger.Register(messaging.DescendantRegistration{
		ChildID:    "child-one",
		Endpoint:   child.URL(),
		Generation: "gen-a",
		Runs:       []messaging.RunClaim{{RunID: "run-b", Token: "tok-b"}},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	sent, err := messenger.Send(testContext(t), askRequest("msg-1"), false)
	if err == nil {
		t.Fatal("Send = nil error, want the relay failure")
	}
	if sent.Outcome != messaging.OutcomeNotSent {
		t.Fatalf("Send outcome = %q, want %q", sent.Outcome, messaging.OutcomeNotSent)
	}
	withdrawn := local.withdrawnMessages()
	if len(withdrawn) != 1 || withdrawn[0] != "msg-1" {
		t.Fatalf("withdrawn = %v, want the admitted edge retracted", withdrawn)
	}
}

// TestMessengerRoutesALocalTargetLocally pins the routing rule: a target this
// broker can answer stays here, so nothing reaches the parent.
func TestMessengerRoutesALocalTargetLocally(t *testing.T) {
	parent := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		}
		return http.StatusOK, `{}`
	})
	local := newStubMessenger()
	local.reachable["run-local"] = true
	local.sendResult = messaging.SendResult{Outcome: messaging.OutcomeSent, MessageID: "msg-1"}
	messenger := newTestMessenger(t, local, parent)

	request := askRequest("msg-1")
	request.To = messaging.Identity{RunID: "run-local", Sender: "carol"}
	if _, err := messenger.Send(testContext(t), request, false); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if targets := local.sentRuns(); len(targets) != 1 || targets[0] != "run-local" {
		t.Fatalf("the local seam was asked for %v, want the local target", targets)
	}
	if got := parent.countPath(pathSend); got != 0 {
		t.Fatalf("the parent saw %d sends, want none", got)
	}
}

// TestMessengerRoutesAnUnknownTargetToTheParent pins the routing rule's other
// half: a target no local run owns and no registered child owns is the
// parent's to answer.
func TestMessengerRoutesAnUnknownTargetToTheParent(t *testing.T) {
	parent := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		case pathSend:
			return http.StatusOK, `{"outcome":"sent"}`
		}
		return http.StatusOK, `{}`
	})
	local := newStubMessenger()
	messenger := newTestMessenger(t, local, parent)

	sent, err := messenger.Send(testContext(t), askRequest("msg-1"), false)
	if err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("Send = (%+v, %v), want the ask forwarded to the parent", sent, err)
	}
	if got := parent.countPath(pathSend); got != 1 {
		t.Fatalf("the parent saw %d sends, want the ask forwarded", got)
	}
	if targets := local.sentRuns(); len(targets) != 0 {
		t.Fatalf("the local seam was asked for %v, want none", targets)
	}
}

// TestMessengerReachableFollowsTheParent pins the reachability contract: a
// branch with a parent reports every target it cannot answer itself as
// reachable, because the parent is the authority on what its descendants own.
func TestMessengerReachableFollowsTheParent(t *testing.T) {
	parent := newStubBroker(t, func(req stubRequest) (int, string) {
		return http.StatusOK, `{"generation":"gen-a"}`
	})
	local := newStubMessenger()
	local.reachable["run-local"] = true

	withParent := newTestMessenger(t, local, parent)
	if !withParent.Reachable("run-local") {
		t.Fatal("a local target is not reachable")
	}
	if !withParent.Reachable("run-elsewhere") {
		t.Fatal("a target the parent may own is not reachable")
	}

	withoutParent := newTestMessenger(t, local, nil)
	if !withoutParent.Reachable("run-local") {
		t.Fatal("a local target is not reachable")
	}
	if withoutParent.Reachable("run-elsewhere") {
		t.Fatal("a target no parent can own is reachable")
	}
}

// TestMessengerPeersUnionsTheParentsPeersWithoutDuplicates pins the peer
// listing: the parent's peers join this broker's own peers, and a run both
// lists is listed once.
func TestMessengerPeersUnionsTheParentsPeersWithoutDuplicates(t *testing.T) {
	parent := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		case pathSessions:
			return http.StatusOK, `{"generation":"gen-a","sessions":[{"run_id":"run-a"},{"run_id":"run-y"}]}`
		}
		return http.StatusOK, `{}`
	})
	local := newStubMessenger()
	local.peers = []messaging.Peer{{RunID: "run-a"}, {RunID: "run-x"}}
	messenger := newTestMessenger(t, local, parent)
	ctx := testContext(t)
	// The claim publishes this branch's runs, which are the credentials the
	// parent listing is made with.
	if err := messenger.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	peers, err := messenger.Peers(ctx)
	if err != nil {
		t.Fatalf("Peers: %v", err)
	}
	seen := map[string]int{}
	for _, peer := range peers {
		seen[peer.RunID]++
	}
	for _, runID := range []string{"run-a", "run-x", "run-y"} {
		if seen[runID] != 1 {
			t.Fatalf("peers = %v, want %q listed once", seen, runID)
		}
	}
}
