package broker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestMessengerReplyRejectsNonJSONWithoutConsumingTheEdge pins the reply
// validation on the broker-routed path: the local messenger stamps the reply
// body before the store sees it, so a body that is not JSON is refused
// not-sent there and the edge stays live for the reply that parses. The bare
// in-process transport shares the store's validation.
func TestMessengerReplyRejectsNonJSONWithoutConsumingTheEdge(t *testing.T) {
	b, _, _ := startSeamBroker(t)
	m := b.LocalMessenger()
	ctx := context.Background()

	now := time.Now().UTC()
	msgID, err := messaging.NewMsgID(now, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("message id: %v", err)
	}
	sent, err := m.Send(ctx, messaging.SendRequest{
		From:       messaging.Identity{RunID: "sender", Sender: "sender"},
		To:         messaging.Identity{RunID: "target", Sender: "target"},
		MessageID:  msgID,
		Generation: b.Store().Generation(),
		Body:       `{"message":"ping"}`,
	})
	if err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("Send = (%+v, %v), want sent", sent, err)
	}

	outcome, err := m.Reply(ctx, messaging.ReplyRequest{
		From:      messaging.Identity{RunID: "target", Sender: "target"},
		ToRunID:   "sender",
		MessageID: msgID,
		Body:      `{"payload":`,
	})
	if outcome.Outcome != messaging.OutcomeNotSent || !errors.Is(err, messaging.ErrInvalidRequest) {
		t.Fatalf("Reply(not JSON) = (%q, %v), want not-sent invalid-request", outcome, err)
	}

	outcome, err = m.Reply(ctx, messaging.ReplyRequest{
		From:      messaging.Identity{RunID: "target", Sender: "target"},
		ToRunID:   "sender",
		MessageID: msgID,
		Body:      `{"payload":"pong"}`,
	})
	if err != nil || outcome.Outcome != messaging.OutcomeReplied {
		t.Fatalf("Reply(valid) after the refusal = (%q, %v), want replied", outcome, err)
	}
	waited, err := m.Wait(ctx, messaging.WaitRequest{
		Sender:     messaging.Identity{RunID: "sender", Sender: "sender"},
		MessageID:  msgID,
		Generation: b.Store().Generation(),
		Timeout:    time.Second,
	})
	if err != nil || waited.Outcome != messaging.OutcomeReplied {
		t.Fatalf("Wait = (%+v, %v), want the replied edge", waited, err)
	}
	var replyBody map[string]any
	if waited.Reply == nil {
		t.Fatal("reply payload = nil, want the valid reply stored")
	}
	if err := json.Unmarshal(waited.Reply.Payload, &replyBody); err != nil {
		t.Fatalf("reply payload %s: %v", waited.Reply.Payload, err)
	}
	if replyBody["payload"] != "pong" {
		t.Fatalf("reply payload = %+v, want the valid reply stored", waited.Reply)
	}
}
