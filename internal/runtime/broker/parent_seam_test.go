package broker

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// parentLinkStub is a parent-link seam that records nothing and refuses every
// method a caller is not allowed to reach: a broker that calls the link outside
// its own registration routes fails loudly instead of quietly doing I/O under
// the ask store's lock.
type parentLinkStub struct {
	owned        string
	recordRelay  func(messaging.RelayRoute) error
	recorded     []messaging.RelayRoute
	deregistered bool
}

func (p *parentLinkStub) Owns(runID string) bool { return runID == p.owned }

func (p *parentLinkStub) Register(messaging.DescendantRegistration) (messaging.DescendantClaim, error) {
	panic("parentlink: registration is not part of an admission")
}

func (p *parentLinkStub) Deregister(childID string) ([]string, error) {
	p.deregistered = true
	return nil, nil
}

func (p *parentLinkStub) RecordRelay(route messaging.RelayRoute) error {
	p.recorded = append(p.recorded, route)
	if p.recordRelay != nil {
		return p.recordRelay(route)
	}
	return nil
}

// TestAdmissionForADescendantRunTouchesNoLink pins the admission boundary: a
// publication for a run a child broker owns is accepted without a local queue
// and without entering the link, so the relay that reaches the child always runs
// after the store has finished admitting.
func TestAdmissionForADescendantRunTouchesNoLink(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	link := &parentLinkStub{owned: "remote"}
	b.SetParentLink(link)

	now := time.Now().UTC()
	msgID, err := messaging.NewMsgID(now, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("mint message id: %v", err)
	}
	res, err := b.LocalMessenger().Send(context.Background(), messaging.SendRequest{
		From:       messaging.Identity{RunID: "local", Sender: "local"},
		To:         messaging.Identity{RunID: "remote", Sender: "remote"},
		MessageID:  msgID,
		Generation: b.Store().Generation(),
		Body:       `{"message":"cross-branch"}`,
	}, false)
	if err != nil || res.Outcome != messaging.OutcomeSent {
		t.Fatalf("admission for a descendant = (%+v, %v), want sent", res, err)
	}
	if b.GetRun("remote") != nil {
		t.Fatal("the descendant run got a local queue record")
	}
	if edges := b.Store().ActiveCount(); edges != 1 {
		t.Fatalf("store holds %d ask edges, want the one the cross-branch ask owns", edges)
	}
	if len(link.recorded) != 0 {
		t.Fatalf("admission recorded %d relay routes, want none", len(link.recorded))
	}
}

// TestRelayRecordsTheReplyRouteBeforeExposingTheAsk pins the ordering rule: a
// relayed ask whose reply route cannot be recorded is never queued, so a consumer
// can never answer an ask whose route is unbound.
func TestRelayRecordsTheReplyRouteBeforeExposingTheAsk(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	token, err := b.CreateRun("branch-run", "branch-run")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	link := &parentLinkStub{recordRelay: func(messaging.RelayRoute) error {
		return messaging.ErrTransportClosed
	}}
	b.SetParentLink(link)

	status, body := postAuthBroker(t, b.Addr(), "/relay", "branch-run", token, map[string]any{
		"from_run_id":     "asker",
		"to_run_id":       "branch-run",
		"type":            "agent_message",
		"payload":         map[string]any{"message": "relayed"},
		"message_id":      "av1.abc.def",
		"generation":      b.Store().Generation(),
		"edge_generation": "edge-gen",
		"expects_reply":   true,
	})
	if status != http.StatusConflict {
		t.Fatalf("relay with an unrecordable route = (%d, %v), want 409", status, body)
	}
	if queued := countAgentMessages(t, b, "branch-run"); queued != 0 {
		t.Fatalf("run queue = %d messages, want the ask withheld until its route is bound", queued)
	}

	// With the route accepted, the same relay is exposed exactly once and the
	// route carries the asker and the generation the ancestor admitted it under.
	link.recordRelay = nil
	link.recorded = nil
	status, body = postAuthBroker(t, b.Addr(), "/relay", "branch-run", token, map[string]any{
		"from_run_id":     "asker",
		"to_run_id":       "branch-run",
		"type":            "agent_message",
		"payload":         map[string]any{"message": "relayed"},
		"message_id":      "av1.abc.def",
		"generation":      b.Store().Generation(),
		"edge_generation": "edge-gen",
		"expects_reply":   true,
	})
	if status != http.StatusOK {
		t.Fatalf("relay = (%d, %v), want 200", status, body)
	}
	if queued := countAgentMessages(t, b, "branch-run"); queued != 1 {
		t.Fatalf("run queue = %d messages, want the relayed ask", queued)
	}
	if len(link.recorded) != 1 || link.recorded[0].AskerRunID != "asker" ||
		link.recorded[0].EdgeGeneration != "edge-gen" {
		t.Fatalf("recorded relay routes = %+v, want the asker and edge generation", link.recorded)
	}
	if edges := b.Store().ActiveCount(); edges != 0 {
		t.Fatalf("a relayed ask admitted %d local edges, want 0", edges)
	}
}

// TestRelayRefusesAReplacementGeneration pins that a relay names the generation
// the branch pinned when it registered, so a relay prepared for a replaced broker
// is refused instead of landing in the replacement.
func TestRelayRefusesAReplacementGeneration(t *testing.T) {
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	token, err := b.CreateRun("branch-run", "branch-run")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	b.SetParentLink(&parentLinkStub{})

	status, body := postAuthBroker(t, b.Addr(), "/relay", "branch-run", token, map[string]any{
		"from_run_id":   "asker",
		"to_run_id":     "branch-run",
		"type":          "agent_message",
		"payload":       map[string]any{"message": "stale"},
		"message_id":    "av1.abc.def",
		"generation":    "a-generation-from-before-the-restart",
		"expects_reply": true,
	})
	if status != http.StatusConflict || body["code"] != "generation-mismatch" {
		t.Fatalf("relay for a replacement generation = (%d, %v), want 409 generation-mismatch", status, body)
	}
	if queued := countAgentMessages(t, b, "branch-run"); queued != 0 {
		t.Fatalf("run queue = %d messages, want the refused relay withheld", queued)
	}
}
