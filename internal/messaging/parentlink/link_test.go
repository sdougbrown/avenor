package parentlink

import (
	"errors"
	"net/http"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestLinkSendForwardsTheAskToTheParent pins the forward of one ask: the
// parent receives the ask under the generation it pinned, and the ask is
// admitted.
func TestLinkSendForwardsTheAskToTheParent(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		case pathSend:
			return http.StatusOK, `{"outcome":"sent","message_id":"msg-1"}`
		}
		return http.StatusOK, `{}`
	})
	ctx := testContext(t)
	link := newTestLink(t, stub, "gen-a")

	sent, err := link.Send(ctx, askRequest("msg-1"))
	if err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("Send = (%+v, %v), want the ask admitted", sent, err)
	}
	if sent.Generation != "gen-a" {
		t.Fatalf("Send generation = %q, want the generation the parent pinned", sent.Generation)
	}
	forwarded := stub.lastRequest(t, pathSend)
	if forwarded.Body["from_run_id"] != "run-a" || forwarded.Body["to_run_id"] != "run-b" {
		t.Fatalf("forwarded ask = %v, want the asker's and the target's run", forwarded.Body)
	}
	if forwarded.Body["generation"] != "gen-a" {
		t.Fatalf("forwarded generation = %v, want the generation the parent pinned", forwarded.Body["generation"])
	}
	if forwarded.Body["expects_reply"] != true {
		t.Fatalf("forwarded expects_reply = %v, want true", forwarded.Body["expects_reply"])
	}
	if forwarded.Body["run_id"] != "run-a" || forwarded.Body["token"] != "tok-a" {
		t.Fatalf("forwarded credentials = %v, want the asker's own credential", forwarded.Body)
	}
}

// TestLinkRecordsTheRouteOnlyForAnAskThatExpectsAReply pins the route
// bookkeeping: only an ask that owns a reply edge records the hop it was
// admitted on, so a wait or a withdrawal on a tell finds no route.
func TestLinkRecordsTheRouteOnlyForAnAskThatExpectsAReply(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		case pathSend:
			return http.StatusOK, `{"outcome":"sent"}`
		}
		return http.StatusOK, `{}`
	})
	ctx := testContext(t)
	link := newTestLink(t, stub, "gen-a")

	tell := askRequest("msg-tell")
	tell.NoReply = true
	sent, err := link.Send(ctx, tell)
	if err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("Send = (%+v, %v), want the tell delivered", sent, err)
	}
	waited, err := link.Wait(ctx, waitRequest("msg-tell"))
	if waited.Outcome != messaging.OutcomeEdgeAbsent {
		t.Fatalf("Wait outcome = %q, want %q for a tell", waited.Outcome, messaging.OutcomeEdgeAbsent)
	}
	if err != nil {
		t.Fatalf("Wait error = %v, want no error", err)
	}
	if stub.countPath(pathWait) != 0 {
		t.Fatal("a wait on a tell reached the parent")
	}
}

// TestLinkWaitReportsAnUnknownRouteWithoutAskingTheParent pins the routing
// contract: an operation on a message this branch never forwarded finds no
// route and costs the parent nothing.
func TestLinkWaitReportsAnUnknownRouteWithoutAskingTheParent(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		return http.StatusOK, `{"generation":"gen-a"}`
	})
	ctx := testContext(t)
	link := newTestLink(t, stub, "gen-a")

	waited, err := link.Wait(ctx, waitRequest("msg-unknown"))
	if waited.Outcome != messaging.OutcomeEdgeAbsent || err != nil {
		t.Fatalf("Wait = (%+v, %v), want the edge absent", waited, err)
	}
	withdrawn, err := link.Withdraw(ctx, withdrawRequest("msg-unknown"))
	if withdrawn.Outcome != messaging.OutcomeEdgeAbsent || err != nil {
		t.Fatalf("Withdraw = (%+v, %v), want the edge absent", withdrawn, err)
	}
	if stub.countPath(pathWait) != 0 || stub.countPath(pathWithdraw) != 0 {
		t.Fatal("an operation on an unknown route reached the parent")
	}
}

// TestLinkWaitReadsTheRepliedVerdictAndConsumesTheRoute pins the wait's read
// of a completed edge: the reply is returned to the caller and the route is
// consumed, so a later wait finds nothing here.
func TestLinkWaitReadsTheRepliedVerdictAndConsumesTheRoute(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		case pathSend:
			return http.StatusOK, `{"outcome":"sent"}`
		case pathWait:
			return http.StatusOK, `{"outcome":"replied","message_id":"msg-1","from_run_id":"run-b",` +
				`"sender":"bob","kind":"agent_message","payload":{"message":"pong"}}`
		}
		return http.StatusOK, `{}`
	})
	ctx := testContext(t)
	link := newTestLink(t, stub, "gen-a")

	if sent, err := link.Send(ctx, askRequest("msg-1")); err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("Send = (%+v, %v), want the ask admitted", sent, err)
	}

	waited, err := link.Wait(ctx, waitRequest("msg-1"))
	if err != nil {
		t.Fatalf("Wait error = %v, want no error", err)
	}
	if waited.Outcome != messaging.OutcomeReplied {
		t.Fatalf("Wait outcome = %q, want %q", waited.Outcome, messaging.OutcomeReplied)
	}
	if waited.Reply == nil || waited.Reply.FromRunID != "run-b" || waited.Reply.Sender != "bob" {
		t.Fatalf("Wait reply = %+v, want the parent's reply", waited.Reply)
	}
	forwarded := stub.lastRequest(t, pathWait)
	if forwarded.Body["waiting_for"] != "msg-1" || forwarded.Body["generation"] != "gen-a" {
		t.Fatalf("forwarded wait = %v, want the message and the admitted generation", forwarded.Body)
	}

	// The edge is consumed, so a later wait finds no route here.
	again, err := link.Wait(ctx, waitRequest("msg-1"))
	if again.Outcome != messaging.OutcomeEdgeAbsent || err != nil {
		t.Fatalf("second Wait = (%+v, %v), want the edge absent", again, err)
	}
}

// TestLinkWaitKeepsTheRouteAfterATimeout pins the terminal fence the parent
// keeps: the route stays, so a repeat wait reads the parent's verdict again
// instead of one this branch invented.
func TestLinkWaitKeepsTheRouteAfterATimeout(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		case pathSend:
			return http.StatusOK, `{"outcome":"sent"}`
		case pathWait:
			return http.StatusOK, `{"outcome":"timed-out","message_id":"msg-1"}`
		}
		return http.StatusOK, `{}`
	})
	ctx := testContext(t)
	link := newTestLink(t, stub, "gen-a")

	if sent, err := link.Send(ctx, askRequest("msg-1")); err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("Send = (%+v, %v), want the ask admitted", sent, err)
	}
	waited, err := link.Wait(ctx, waitRequest("msg-1"))
	if err != nil {
		t.Fatalf("Wait error = %v, want no error", err)
	}
	if waited.Outcome != messaging.OutcomeTimedOut {
		t.Fatalf("Wait outcome = %q, want %q", waited.Outcome, messaging.OutcomeTimedOut)
	}
	if stub.countPath(pathWait) != 1 {
		t.Fatalf("the parent saw %d waits, want one", stub.countPath(pathWait))
	}
	again, err := link.Wait(ctx, waitRequest("msg-1"))
	if err != nil {
		t.Fatalf("second Wait error = %v, want no error", err)
	}
	if again.Outcome != messaging.OutcomeTimedOut {
		t.Fatalf("second Wait outcome = %q, want %q", again.Outcome, messaging.OutcomeTimedOut)
	}
	if stub.countPath(pathWait) != 2 {
		t.Fatalf("the parent saw %d waits, want the route to reach it twice", stub.countPath(pathWait))
	}
}

// TestLinkWithdrawReadsTheTerminalFence pins the withdrawal's read of the
// parent's fence: the terminal fence comes back with the verdict.
func TestLinkWithdrawReadsTheTerminalFence(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		case pathSend:
			return http.StatusOK, `{"outcome":"sent"}`
		case pathWithdraw:
			return http.StatusOK, `{"outcome":"withdrawn","message_id":"msg-1","fence":{"sender":"run-a",` +
				`"message_id":"msg-1","generation":"gen-a","deadline":"2030-01-01T00:00:00Z","terminal":true}}`
		}
		return http.StatusOK, `{}`
	})
	ctx := testContext(t)
	link := newTestLink(t, stub, "gen-a")

	if sent, err := link.Send(ctx, askRequest("msg-1")); err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("Send = (%+v, %v), want the ask admitted", sent, err)
	}
	withdrawn, err := link.Withdraw(ctx, withdrawRequest("msg-1"))
	if err != nil {
		t.Fatalf("Withdraw error = %v, want no error", err)
	}
	if withdrawn.Outcome != messaging.OutcomeWithdrawn {
		t.Fatalf("Withdraw outcome = %q, want %q", withdrawn.Outcome, messaging.OutcomeWithdrawn)
	}
	if !withdrawn.Fence.Terminal || withdrawn.Fence.MessageID != "msg-1" {
		t.Fatalf("Withdraw fence = %+v, want the parent's terminal fence", withdrawn.Fence)
	}
	forwarded := stub.lastRequest(t, pathWithdraw)
	if forwarded.Body["cancel_message_id"] != "msg-1" || forwarded.Body["generation"] != "gen-a" {
		t.Fatalf("forwarded withdrawal = %v, want the message and the admitted generation", forwarded.Body)
	}
}

// TestLinkSendClassifiesAnUnreachableParent pins the send's transport
// classification: a request that provably never reached the parent was never
// sent, while a request that failed after it left is possibly pending.
func TestLinkSendClassifiesAnUnreachableParent(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		return http.StatusOK, `{"generation":"gen-a"}`
	})
	closed := stub.URL()
	stub.server.Close()

	link, err := NewLink(Options{
		ParentURL:  closed,
		Endpoint:   "http://child-one",
		Runs:       testRuns,
		Generation: func() string { return "gen-a" },
	})
	if err != nil {
		t.Fatalf("NewLink: %v", err)
	}
	t.Cleanup(func() { _ = link.Close() })

	sent, err := link.Send(testContext(t), askRequest("msg-1"))
	if !errors.Is(err, ErrParentUnavailable) {
		t.Fatalf("Send error = %v, want ErrParentUnavailable", err)
	}
	if sent.Outcome != messaging.OutcomeNotSent {
		t.Fatalf("Send outcome = %q, want %q", sent.Outcome, messaging.OutcomeNotSent)
	}
}

// TestLinkSendClassifiesAFailedTransmission pins the send's classification of
// a transmission that failed after it left: the ask's fate is unknown, so it
// is possibly pending and must never be re-sent.
func TestLinkSendClassifiesAFailedTransmission(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		case pathSend:
			return -1, ""
		}
		return http.StatusOK, `{}`
	})
	ctx := testContext(t)
	link := newTestLink(t, stub, "gen-a")

	sent, err := link.Send(ctx, askRequest("msg-1"))
	if !errors.Is(err, ErrParentUnavailable) {
		t.Fatalf("Send error = %v, want ErrParentUnavailable", err)
	}
	if sent.Outcome != messaging.OutcomePossiblyPending {
		t.Fatalf("Send outcome = %q, want %q", sent.Outcome, messaging.OutcomePossiblyPending)
	}
}

// TestLinkPeersSyncsBeforeItLists pins the listing's recovery: a Peers call
// before the first successful registration syncs this branch's claim once and
// then lists, instead of answering a silent empty list.
func TestLinkPeersSyncsBeforeItLists(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		case pathSessions:
			return http.StatusOK, `{"generation":"gen-a","sessions":[{"run_id":"run-b"}]}`
		}
		return http.StatusOK, `{}`
	})
	link := newTestLink(t, stub, "gen-a")

	peers, err := link.Peers(testContext(t))
	if err != nil {
		t.Fatalf("Peers: %v", err)
	}
	if len(peers) != 1 || peers[0].RunID != "run-b" {
		t.Fatalf("peers = %v, want the parent's run", peers)
	}
	if got := stub.countPath(pathRegister); got != 1 {
		t.Fatalf("the parent saw %d registrations, want the listing's automatic sync", got)
	}
}

// TestLinkPeersRefusesWithoutAListingRun pins the other recovery: a claim the
// parent cannot accept as credentials is not a listing, so Peers reports the
// state instead of returning an empty list.
func TestLinkPeersRefusesWithoutAListingRun(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		}
		return http.StatusOK, `{}`
	})
	link, err := NewLink(Options{
		ParentURL:  stub.URL(),
		Endpoint:   "http://child-one",
		ChildID:    "child-one",
		Runs:       func() []messaging.RunClaim { return []messaging.RunClaim{{RunID: "run-a"}} },
		Generation: func() string { return "gen-a" },
	})
	if err != nil {
		t.Fatalf("NewLink: %v", err)
	}
	t.Cleanup(func() { _ = link.Close() })

	if peers, err := link.Peers(testContext(t)); err == nil {
		t.Fatalf("Peers = %v with no run registered, want an error", peers)
	} else if !errors.Is(err, messaging.ErrTargetUnavailable) {
		t.Fatalf("Peers = %v, want ErrTargetUnavailable", err)
	}
}
