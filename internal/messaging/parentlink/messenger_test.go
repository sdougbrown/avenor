package parentlink

import (
	"errors"
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
		ChildID:    "child-one",
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

	request := askRequest("msg-1")
	request.To = messaging.Identity{RunID: ParentRunKey("child-one", "run-b"), Sender: "bob"}
	sent, err := messenger.Send(testContext(t), request)
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
	if _, err := messenger.Send(testContext(t), request); err != nil {
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

	sent, err := messenger.Send(testContext(t), askRequest("msg-1"))
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

// TestMessengerSendsANoReplyAskToAChildWithoutAnEdge pins the tell's relay
// into a child branch: the hop carries no reply expectation, the send is
// reported as sent, and this broker admits no edge of its own for it.
func TestMessengerSendsANoReplyAskToAChildWithoutAnEdge(t *testing.T) {
	child := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRelay:
			return http.StatusOK, `{}`
		}
		return http.StatusOK, `{}`
	})
	local := newStubMessenger()
	messenger := newTestMessenger(t, local, nil)
	if _, err := messenger.Register(messaging.DescendantRegistration{
		ChildID:    "child-one",
		Endpoint:   child.URL(),
		Generation: "gen-a",
		Runs:       []messaging.RunClaim{{RunID: "run-b", Token: "tok-b"}},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	tell := askRequest("msg-1")
	tell.To = messaging.Identity{RunID: ParentRunKey("child-one", "run-b"), Sender: "bob"}
	tell.NoReply = true
	sent, err := messenger.Send(testContext(t), tell)
	if err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("Send = (%+v, %v), want the tell relayed and sent", sent, err)
	}
	relayed := child.lastRequest(t, pathRelay)
	if relayed.Body["expects_reply"] != false {
		t.Fatalf("relayed tell = %v, want no reply expectation", relayed.Body)
	}
	if len(local.sends) != 0 {
		t.Fatalf("the local seam admitted %d edges, want none for a tell", len(local.sends))
	}
}

// TestMessengerForwardsAReplyWhenTheLocalEdgeIsAbsent pins the reply's
// upward fallback: a reply to an ask no local edge owns follows the relay
// route it arrived on, so the asker in the ancestor branch sees its edge
// complete.
func TestMessengerForwardsAReplyWhenTheLocalEdgeIsAbsent(t *testing.T) {
	parent := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		case pathSend:
			return http.StatusOK, `{"outcome":"replied"}`
		}
		return http.StatusOK, `{}`
	})
	local := newStubMessenger()
	messenger := newTestMessenger(t, local, parent)
	ctx := testContext(t)
	if err := messenger.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := messenger.RecordRelay(messaging.RelayRoute{
		MessageID:      "msg-1",
		AskerRunID:     "run-x",
		EdgeGeneration: "gen-a",
	}); err != nil {
		t.Fatalf("RecordRelay: %v", err)
	}

	outcome, err := messenger.Reply(ctx, messaging.ReplyRequest{
		From:      messaging.Identity{RunID: "run-a", Sender: "alice"},
		ToRunID:   "run-x",
		MessageID: "msg-1",
		Body:      `{"message":"pong"}`,
	})
	if err != nil || outcome != messaging.OutcomeReplied {
		t.Fatalf("Reply = (%q, %v), want the relayed edge completed", outcome, err)
	}
	forwarded := parent.lastRequest(t, pathSend)
	if forwarded.Body["reply_to"] != "msg-1" {
		t.Fatalf("forwarded reply = %v, want a reply to the relayed ask", forwarded.Body)
	}
}

// TestMessengerPeersJoinsTheParentsIncompleteListing pins the join at the
// messenger: a parent listing that failed still contributes the peers it
// reached, and the failure is joined onto the listing's error.
func TestMessengerPeersJoinsTheParentsIncompleteListing(t *testing.T) {
	parent := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		case pathSessions:
			return http.StatusOK, `{"generation":"gen-a","sessions":[{"run_id":"run-y"}],"peers_error":"one branch did not answer"}`
		}
		return http.StatusOK, `{}`
	})
	local := newStubMessenger()
	local.peers = []messaging.Peer{{RunID: "run-x"}}
	messenger := newTestMessenger(t, local, parent)
	ctx := testContext(t)
	if err := messenger.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	peers, err := messenger.Peers(ctx)
	if !errors.Is(err, messaging.ErrTargetUnavailable) {
		t.Fatalf("Peers error = %v, want the parent's incomplete listing joined", err)
	}
	seen := map[string]bool{}
	for _, peer := range peers {
		seen[peer.RunID] = true
	}
	if !seen["run-x"] || !seen["run-y"] {
		t.Fatalf("peers = %v, want this broker's run and the parent's reached run", seen)
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
