package parentlink

import (
	"errors"
	"net/http"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestLinkReplyResultCarriesTheRequestIdentity pins the reply's result
// identity: the result names the request's message id on every path and
// carries the edge generation the reply followed, so a caller that only sees
// the result can tell which edge it completed and under which generation.
func TestLinkReplyResultCarriesTheRequestIdentity(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		case pathSend:
			// The parent answers without its own generation, so the result
			// falls back to the edge generation the reply was admitted under.
			return http.StatusOK, `{"outcome":"replied"}`
		}
		return http.StatusOK, `{}`
	})
	ctx := testContext(t)
	link := newTestLink(t, stub, "gen-a")
	if err := link.RecordRelay(messaging.RelayRoute{
		MessageID:      "msg-1",
		AskerRunID:     "run-x",
		EdgeGeneration: "gen-a",
	}); err != nil {
		t.Fatalf("RecordRelay: %v", err)
	}

	res, err := link.Reply(ctx, replyRequest("msg-1"))
	if err != nil {
		t.Fatalf("Reply error = %v, want no error", err)
	}
	if res.Outcome != messaging.OutcomeReplied {
		t.Fatalf("Reply outcome = %q, want %q", res.Outcome, messaging.OutcomeReplied)
	}
	if res.MessageID != "msg-1" {
		t.Fatalf("Reply message id = %q, want the request's id", res.MessageID)
	}
	if res.Generation != "gen-a" {
		t.Fatalf("Reply generation = %q, want the edge generation the reply followed", res.Generation)
	}
}

// TestLinkReplyResultCarriesTheDecodedGeneration pins the reply's generation
// preference: a parent that answers with its own generation reports it, so the
// result carries the generation the parent named rather than emptying it.
func TestLinkReplyResultCarriesTheDecodedGeneration(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		case pathSend:
			return http.StatusOK, `{"outcome":"replied","generation":"gen-b"}`
		}
		return http.StatusOK, `{}`
	})
	ctx := testContext(t)
	link := newTestLink(t, stub, "gen-a")
	if err := link.RecordRelay(messaging.RelayRoute{
		MessageID:      "msg-1",
		AskerRunID:     "run-x",
		EdgeGeneration: "gen-a",
	}); err != nil {
		t.Fatalf("RecordRelay: %v", err)
	}

	res, err := link.Reply(ctx, replyRequest("msg-1"))
	if err != nil {
		t.Fatalf("Reply error = %v, want no error", err)
	}
	if res.Outcome != messaging.OutcomeReplied {
		t.Fatalf("Reply outcome = %q, want %q", res.Outcome, messaging.OutcomeReplied)
	}
	if res.Generation != "gen-b" {
		t.Fatalf("Reply generation = %q, want the generation the parent named", res.Generation)
	}
}

// TestLinkReplyResultForAnAbsentRoute pins the absent path's identity: a reply
// to an ask no relay brought here reports the edge absent, names the request's
// message id, and carries no generation because no edge was followed.
func TestLinkReplyResultForAnAbsentRoute(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		}
		return http.StatusOK, `{}`
	})
	link := newTestLink(t, stub, "gen-a")

	res, err := link.Reply(testContext(t), replyRequest("msg-unknown"))
	if err != nil {
		t.Fatalf("Reply error = %v, want no error", err)
	}
	if res.Outcome != messaging.OutcomeEdgeAbsent {
		t.Fatalf("Reply outcome = %q, want %q", res.Outcome, messaging.OutcomeEdgeAbsent)
	}
	if res.MessageID != "msg-unknown" {
		t.Fatalf("Reply message id = %q, want the request's id", res.MessageID)
	}
	if res.Generation != "" {
		t.Fatalf("Reply generation = %q, want none for an edge that was never followed", res.Generation)
	}
}

// TestLinkReplyResultForALostRoute pins the lost path's identity: a reply whose
// parent was replaced reports the edge lost, names the request's message id, and
// carries the edge generation the reply was admitted under, so the caller knows
// which generation the loss belongs to.
func TestLinkReplyResultForALostRoute(t *testing.T) {
	generation := &generationStub{generation: "gen-a"}
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"` + generation.get() + `"}`
		}
		return http.StatusOK, `{}`
	})
	ctx := testContext(t)
	link := newTestLink(t, stub, "gen-a")
	if err := link.RecordRelay(messaging.RelayRoute{
		MessageID:      "msg-1",
		AskerRunID:     "run-a",
		EdgeGeneration: "gen-a",
	}); err != nil {
		t.Fatalf("RecordRelay: %v", err)
	}
	if err := link.Sync(ctx); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	generation.set("gen-b")
	if err := link.Sync(ctx); err != nil {
		t.Fatalf("Sync after the replacement: %v", err)
	}

	res, err := link.Reply(ctx, messaging.ReplyRequest{
		From:      messaging.Identity{RunID: "run-b", Sender: "bob"},
		ToRunID:   "run-a",
		MessageID: "msg-1",
	})
	if err != nil {
		t.Fatalf("Reply error = %v, want no error", err)
	}
	if res.Outcome != messaging.OutcomeLost {
		t.Fatalf("Reply outcome = %q, want %q", res.Outcome, messaging.OutcomeLost)
	}
	if res.MessageID != "msg-1" {
		t.Fatalf("Reply message id = %q, want the request's id", res.MessageID)
	}
	if res.Generation != "gen-a" {
		t.Fatalf("Reply generation = %q, want the edge generation the reply was admitted under", res.Generation)
	}
}

// TestLinkReplyResultForAMismatchedGeneration pins the decode branch's identity:
// a parent that pins the generation the reply names but refuses it on the wire
// reports the edge lost, names the request's message id, and carries the edge
// generation the refusal belongs to.
func TestLinkReplyResultForAMismatchedGeneration(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		case pathSend:
			return http.StatusConflict, `{"code":"generation-mismatch","error":"the ask store was replaced"}`
		}
		return http.StatusOK, `{}`
	})
	ctx := testContext(t)
	link := newTestLink(t, stub, "gen-a")
	if err := link.RecordRelay(messaging.RelayRoute{
		MessageID:      "msg-1",
		AskerRunID:     "run-a",
		EdgeGeneration: "gen-a",
	}); err != nil {
		t.Fatalf("RecordRelay: %v", err)
	}

	res, err := link.Reply(ctx, messaging.ReplyRequest{
		From:      messaging.Identity{RunID: "run-a", Sender: "alice"},
		ToRunID:   "run-a",
		MessageID: "msg-1",
	})
	if !errors.Is(err, messaging.ErrGenerationMismatch) {
		t.Fatalf("Reply error = %v, want ErrGenerationMismatch", err)
	}
	if res.Outcome != messaging.OutcomeLost {
		t.Fatalf("Reply outcome = %q, want %q", res.Outcome, messaging.OutcomeLost)
	}
	if res.MessageID != "msg-1" {
		t.Fatalf("Reply message id = %q, want the request's id", res.MessageID)
	}
	if res.Generation != "gen-a" {
		t.Fatalf("Reply generation = %q, want the edge generation the refusal belongs to", res.Generation)
	}
}

// TestLinkPeersCarriesTheParentGeneration pins the listing's result identity:
// the result carries the generation the parent answered with, so a caller that
// only sees the result knows which parent generation the listing is from.
func TestLinkPeersCarriesTheParentGeneration(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		case pathSessions:
			return http.StatusOK, `{"generation":"gen-parent","sessions":[{"run_id":"run-b"}]}`
		}
		return http.StatusOK, `{}`
	})
	link := newTestLink(t, stub, "gen-a")

	res, err := link.Peers(testContext(t))
	if err != nil {
		t.Fatalf("Peers: %v", err)
	}
	if len(res.Peers) != 1 || res.Peers[0].RunID != "run-b" {
		t.Fatalf("peers = %v, want the parent's run", res.Peers)
	}
	if res.Generation != "gen-parent" {
		t.Fatalf("Peers generation = %q, want the generation the parent answered with", res.Generation)
	}
}

// TestLinkPeersCarriesThePartialListingAndGeneration pins the incomplete
// listing's result identity: a listing the parent could not finish still carries
// the peers it reached and the generation it answered with, beside the error, so
// a partial list never reads as a complete one and its generation is known.
func TestLinkPeersCarriesThePartialListingAndGeneration(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		case pathSessions:
			return http.StatusOK, `{"generation":"gen-parent","sessions":[{"run_id":"run-b"}],"peers_error":"one branch did not answer"}`
		}
		return http.StatusOK, `{}`
	})
	link := newTestLink(t, stub, "gen-a")

	res, err := link.Peers(testContext(t))
	if !errors.Is(err, messaging.ErrTargetUnavailable) {
		t.Fatalf("Peers error = %v, want ErrTargetUnavailable", err)
	}
	if len(res.Peers) != 1 || res.Peers[0].RunID != "run-b" {
		t.Fatalf("peers = %v, want the peers the parent reached", res.Peers)
	}
	if res.Generation != "gen-parent" {
		t.Fatalf("Peers generation = %q, want the generation the parent answered with", res.Generation)
	}
}

// TestMessengerPeersKeepsTheLocalGeneration pins the join's generation rule: the
// merged listing carries this broker's own generation, never the parent's,
// because the preflight negotiates the local store. The parent's peers join the
// local ones, but the generation stays the local one.
func TestMessengerPeersKeepsTheLocalGeneration(t *testing.T) {
	parent := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-parent"}`
		case pathSessions:
			return http.StatusOK, `{"generation":"gen-parent","sessions":[{"run_id":"run-y"}]}`
		}
		return http.StatusOK, `{}`
	})
	local := newStubMessenger()
	local.peers = []messaging.Peer{{RunID: "run-x"}}
	local.peersGen = "gen-local"
	messenger := newTestMessenger(t, local, parent)
	ctx := testContext(t)
	if err := messenger.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	res, err := messenger.Peers(ctx, messaging.PeersRequest{})
	if err != nil {
		t.Fatalf("Peers: %v", err)
	}
	seen := map[string]int{}
	for _, peer := range res.Peers {
		seen[peer.RunID]++
	}
	if seen["run-x"] != 1 || seen["run-y"] != 1 {
		t.Fatalf("peers = %v, want this broker's run and the parent's run, once each", seen)
	}
	if res.Generation != "gen-local" {
		t.Fatalf("Peers generation = %q, want the local generation, never the parent's", res.Generation)
	}
}

// TestMessengerReceivePassesThroughToTheLocalSeam pins the receive's pass-
// through: the messenger serves a receive from the local seam, preserving the
// request it hands down, the result it returns, and the error it reports.
func TestMessengerReceivePassesThroughToTheLocalSeam(t *testing.T) {
	local := newStubMessenger()
	local.receiveResult = messaging.ReceiveResult{
		Messages:   []messaging.InboundMessage{{MessageID: "msg-in", FromRunID: "run-b"}},
		Generation: "gen-local",
	}
	messenger := newTestMessenger(t, local, nil)

	res, err := messenger.Receive(testContext(t), messaging.ReceiveRequest{
		Sender:     messaging.Identity{RunID: "run-a", Sender: "alice"},
		Generation: "gen-local",
		Limit:      8,
	})
	if err != nil {
		t.Fatalf("Receive error = %v, want no error", err)
	}
	if len(res.Messages) != 1 || res.Messages[0].MessageID != "msg-in" {
		t.Fatalf("Receive = %+v, want the local seam's messages", res)
	}
	if res.Generation != "gen-local" {
		t.Fatalf("Receive generation = %q, want the local seam's generation", res.Generation)
	}
	requests := local.receivedRequests()
	if len(requests) != 1 {
		t.Fatalf("the local seam saw %d receives, want the one the messenger handed down", len(requests))
	}
	if requests[0].Sender.RunID != "run-a" || requests[0].Limit != 8 {
		t.Fatalf("the local seam saw %+v, want the request the messenger handed down", requests[0])
	}
}

// TestMessengerReceiveReportsTheLocalSeamsError pins the receive's error
// pass-through: a receive the local seam refuses is reported as the refusal, so
// the messenger never invents a verdict for a drain it did not perform.
func TestMessengerReceiveReportsTheLocalSeamsError(t *testing.T) {
	local := newStubMessenger()
	local.receiveErr = messaging.ErrTransportClosed
	messenger := newTestMessenger(t, local, nil)

	res, err := messenger.Receive(testContext(t), messaging.ReceiveRequest{
		Sender: messaging.Identity{RunID: "run-a", Sender: "alice"},
	})
	if !errors.Is(err, messaging.ErrTransportClosed) {
		t.Fatalf("Receive error = %v, want the local seam's refusal", err)
	}
	if res.Messages != nil {
		t.Fatalf("Receive = %+v, want no messages for a refused drain", res)
	}
}

// TestMessengerPreflightPassesThroughToTheLocalSeam pins the preflight's pass-
// through: the messenger reports the local seam's protocol and generation, and
// the local seam is the one prefetched, so a branch never reports a generation
// it did not negotiate.
func TestMessengerPreflightPassesThroughToTheLocalSeam(t *testing.T) {
	local := newStubMessenger()
	local.preflightResult = messaging.PreflightResult{Protocol: messaging.ProtocolVersion, Generation: "gen-local"}
	messenger := newTestMessenger(t, local, nil)

	res, err := messenger.Preflight(testContext(t))
	if err != nil {
		t.Fatalf("Preflight error = %v, want no error", err)
	}
	if res.Protocol != messaging.ProtocolVersion || res.Generation != "gen-local" {
		t.Fatalf("Preflight = %+v, want the local seam's protocol and generation", res)
	}
	if got := local.preflightCount(); got != 1 {
		t.Fatalf("the local seam was prefetched %d times, want the one the messenger handed down", got)
	}
}

// TestMessengerPreflightReportsTheLocalSeamsError pins the preflight's error
// pass-through: a preflight the local seam refuses is reported as the refusal, so
// the messenger never reports a generation it could not negotiate.
func TestMessengerPreflightReportsTheLocalSeamsError(t *testing.T) {
	local := newStubMessenger()
	local.preflightErr = messaging.ErrUnsupportedProtocol
	messenger := newTestMessenger(t, local, nil)

	res, err := messenger.Preflight(testContext(t))
	if !errors.Is(err, messaging.ErrUnsupportedProtocol) {
		t.Fatalf("Preflight error = %v, want the local seam's refusal", err)
	}
	if res.Generation != "" {
		t.Fatalf("Preflight = %+v, want no generation for a refused negotiation", res)
	}
}
