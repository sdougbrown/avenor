package parentlink

import (
	"errors"
	"net/http"
	"sync"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// generationStub reports the generation a stub parent currently answers
// with, so a test can replace the parent between two operations.
type generationStub struct {
	mu         sync.Mutex
	generation string
}

func (g *generationStub) set(generation string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.generation = generation
}

func (g *generationStub) get() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.generation
}

// TestLinkReportsALostAskWhenTheParentWasReplaced pins the replacement
// parent's safety invariant: a parent that answers with another generation
// was replaced, so no edge from the pinned generation can exist. The wait
// reports the ask lost and drops the route, and a later operation on the
// same message finds nothing.
func TestLinkReportsALostAskWhenTheParentWasReplaced(t *testing.T) {
	generation := &generationStub{generation: "gen-a"}
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"` + generation.get() + `"}`
		case pathSend:
			return http.StatusOK, `{"outcome":"sent"}`
		case pathWait, pathWithdraw:
			return http.StatusConflict, `{"code":"generation-mismatch","error":"the ask store was replaced"}`
		}
		return http.StatusOK, `{}`
	})
	ctx := testContext(t)
	link := newTestLink(t, stub, "gen-a")

	if sent, err := link.Send(ctx, askRequest("msg-1")); err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("Send = (%+v, %v), want the ask admitted", sent, err)
	}

	// The parent is replaced, so the generation it pins changes.
	generation.set("gen-b")

	waited, err := link.Wait(ctx, waitRequest("msg-1"))
	if err != nil {
		t.Fatalf("Wait error = %v, want no error", err)
	}
	if waited.Outcome != messaging.OutcomeLost {
		t.Fatalf("Wait outcome = %q, want %q", waited.Outcome, messaging.OutcomeLost)
	}
	again, err := link.Wait(ctx, waitRequest("msg-1"))
	if again.Outcome != messaging.OutcomeEdgeAbsent || err != nil {
		t.Fatalf("second Wait = (%+v, %v), want the edge absent", again, err)
	}
}

// TestLinkWithdrawReportsALostAskWhenTheParentWasReplaced pins the same
// invariant for the withdrawal: a replaced parent admits no edge from the
// pinned generation, so the withdrawal reports the ask lost and drops the
// route.
func TestLinkWithdrawReportsALostAskWhenTheParentWasReplaced(t *testing.T) {
	generation := &generationStub{generation: "gen-a"}
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"` + generation.get() + `"}`
		case pathSend:
			return http.StatusOK, `{"outcome":"sent"}`
		case pathWithdraw:
			return http.StatusConflict, `{"code":"generation-mismatch","error":"the ask store was replaced"}`
		}
		return http.StatusOK, `{}`
	})
	ctx := testContext(t)
	link := newTestLink(t, stub, "gen-a")

	if sent, err := link.Send(ctx, askRequest("msg-1")); err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("Send = (%+v, %v), want the ask admitted", sent, err)
	}
	generation.set("gen-b")

	withdrawn, err := link.Withdraw(ctx, withdrawRequest("msg-1"))
	if err != nil {
		t.Fatalf("Withdraw error = %v, want no error", err)
	}
	if withdrawn.Outcome != messaging.OutcomeLost {
		t.Fatalf("Withdraw outcome = %q, want %q", withdrawn.Outcome, messaging.OutcomeLost)
	}
	again, err := link.Withdraw(ctx, withdrawRequest("msg-1"))
	if again.Outcome != messaging.OutcomeEdgeAbsent || err != nil {
		t.Fatalf("second Withdraw = (%+v, %v), want the edge absent", again, err)
	}
}

// TestLinkReplyReportsALostRelayWhenTheParentWasReplaced pins the reply's
// replacement rule: a reply that names a generation the parent no longer
// pins is never replayed onto the replacement.
func TestLinkReplyReportsALostRelayWhenTheParentWasReplaced(t *testing.T) {
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

	outcome, err := link.Reply(ctx, messaging.ReplyRequest{
		From:      messaging.Identity{RunID: "run-b", Sender: "bob"},
		ToRunID:   "run-a",
		MessageID: "msg-1",
	})
	if err != nil {
		t.Fatalf("Reply error = %v, want no error", err)
	}
	if outcome != messaging.OutcomeLost {
		t.Fatalf("Reply outcome = %q, want %q", outcome, messaging.OutcomeLost)
	}
	if stub.countPath(pathSend) != 0 {
		t.Fatal("a reply on a replaced parent was sent onward")
	}
}

// TestLinkSendClassifiesARefusedGeneration pins the send's replacement
// rule: a parent that refuses the generation the ask was forwarded under
// reports what the refusal proves. A replaced parent admits no edge from
// the pinned generation, so the ask is lost; an unchanged parent leaves
// the ask uncertain, and neither refusal records a route.
func TestLinkSendClassifiesARefusedGeneration(t *testing.T) {
	generation := &generationStub{generation: "gen-a"}
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"` + generation.get() + `"}`
		case pathSend:
			return http.StatusConflict, `{"code":"generation-mismatch","error":"the ask store was replaced"}`
		}
		return http.StatusOK, `{}`
	})
	ctx := testContext(t)
	link := newTestLink(t, stub, "gen-a")

	sent, err := link.Send(ctx, askRequest("msg-1"))
	if !errors.Is(err, messaging.ErrGenerationMismatch) {
		t.Fatalf("Send error = %v, want ErrGenerationMismatch", err)
	}
	if sent.Outcome != messaging.OutcomePossiblyPending {
		t.Fatalf("Send outcome = %q, want %q while the parent keeps its generation", sent.Outcome, messaging.OutcomePossiblyPending)
	}
	if _, ok := link.hopOf("run-a", "msg-1"); ok {
		t.Fatal("a refused ask recorded a route, want none")
	}

	// The parent is replaced, so the generation it pins changes and the
	// next refused ask proves the edge cannot exist.
	generation.set("gen-b")
	sent, err = link.Send(ctx, askRequest("msg-1"))
	if !errors.Is(err, messaging.ErrGenerationMismatch) {
		t.Fatalf("Send after the replacement error = %v, want ErrGenerationMismatch", err)
	}
	if sent.Outcome != messaging.OutcomeLost {
		t.Fatalf("Send after the replacement outcome = %q, want %q", sent.Outcome, messaging.OutcomeLost)
	}
	if _, ok := link.hopOf("run-a", "msg-1"); ok {
		t.Fatal("a lost ask recorded a route, want none")
	}
}

// TestLinkReplyReportsALostRelayWhenTheParentRefusesThePinnedGeneration pins
// the decode branch of the reply's replacement rule: a parent that pins the
// generation the reply names but refuses it on the wire still proves the edge
// cannot be replayed, so the reply reports lost and drops the route.
func TestLinkReplyReportsALostRelayWhenTheParentRefusesThePinnedGeneration(t *testing.T) {
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

	outcome, err := link.Reply(ctx, messaging.ReplyRequest{
		From:      messaging.Identity{RunID: "run-a", Sender: "alice"},
		ToRunID:   "run-a",
		MessageID: "msg-1",
	})
	if outcome != messaging.OutcomeLost || err == nil {
		t.Fatalf("Reply = (%q, %v), want lost with the parent's refusal", outcome, err)
	}
	if !errors.Is(err, messaging.ErrGenerationMismatch) {
		t.Fatalf("Reply error = %v, want ErrGenerationMismatch", err)
	}
	again, err := link.Reply(ctx, messaging.ReplyRequest{
		From:      messaging.Identity{RunID: "run-a", Sender: "alice"},
		ToRunID:   "run-a",
		MessageID: "msg-1",
	})
	if again != messaging.OutcomeEdgeAbsent || err != nil {
		t.Fatalf("second Reply = (%q, %v), want the dropped route", again, err)
	}
}

// TestLinkWithdrawKeepsTheAskUncertainWhenTheParentKeepsItsGeneration pins the
// decode branch of the withdrawal's replacement rule: a parent that refuses
// the pinned generation but pins the same one again was not replaced, so the
// withdrawal reports the refusal without inventing a verdict.
func TestLinkWithdrawKeepsTheAskUncertainWhenTheParentKeepsItsGeneration(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		case pathSend:
			return http.StatusOK, `{"outcome":"sent"}`
		case pathWithdraw:
			return http.StatusConflict, `{"code":"generation-mismatch","error":"the ask store was replaced"}`
		}
		return http.StatusOK, `{}`
	})
	ctx := testContext(t)
	link := newTestLink(t, stub, "gen-a")

	if sent, err := link.Send(ctx, askRequest("msg-1")); err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("Send = (%+v, %v), want the ask admitted", sent, err)
	}

	withdrawn, err := link.Withdraw(ctx, withdrawRequest("msg-1"))
	if err == nil {
		t.Fatal("Withdraw error = nil, want the parent's refusal")
	}
	if !errors.Is(err, messaging.ErrGenerationMismatch) {
		t.Fatalf("Withdraw error = %v, want ErrGenerationMismatch", err)
	}
	if withdrawn.Outcome != messaging.OutcomePossiblyPending {
		t.Fatalf("Withdraw outcome = %q, want %q", withdrawn.Outcome, messaging.OutcomePossiblyPending)
	}
	// The refusal was not a replacement, so the route stays and a later
	// operation still follows the pinned hop.
	if _, ok := link.hopOf("run-a", "msg-1"); !ok {
		t.Fatal("the route was dropped, want it kept for the unchanged parent")
	}
}

// TestLinkKeepsTheAskUncertainWhenTheParentKeepsItsGeneration pins the other
// half of the replacement rule: a parent that refuses the pinned generation
// but pins the same one again was not replaced, so the ask's fate stays
// unknown.
func TestLinkKeepsTheAskUncertainWhenTheParentKeepsItsGeneration(t *testing.T) {
	generation := &generationStub{generation: "gen-a"}
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"` + generation.get() + `"}`
		case pathSend:
			return http.StatusOK, `{"outcome":"sent"}`
		case pathWait:
			return http.StatusConflict, `{"code":"generation-mismatch","error":"the ask store was replaced"}`
		}
		return http.StatusOK, `{}`
	})
	ctx := testContext(t)
	link := newTestLink(t, stub, "gen-a")

	if sent, err := link.Send(ctx, askRequest("msg-1")); err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("Send = (%+v, %v), want the ask admitted", sent, err)
	}

	waited, err := link.Wait(ctx, waitRequest("msg-1"))
	if err == nil {
		t.Fatal("Wait error = nil, want the parent's refusal")
	}
	if waited.Outcome != messaging.OutcomePossiblyPending {
		t.Fatalf("Wait outcome = %q, want %q", waited.Outcome, messaging.OutcomePossiblyPending)
	}
}
