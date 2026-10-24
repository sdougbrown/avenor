package broker

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/messagetest"
)

// countAgentMessages reports how many agent messages are queued for runID.
// Publication happens before the handler answers, so a caller observes a
// settled queue without waiting.
func countAgentMessages(t *testing.T, b *Broker, runID string) int {
	t.Helper()
	st := b.GetRun(runID)
	if st == nil {
		t.Fatalf("run %s is not registered", runID)
	}
	st.Mu.Lock()
	defer st.Mu.Unlock()
	count := 0
	for _, msg := range st.ControlQueue {
		if msg.Type == "agent_message" {
			count++
		}
	}
	return count
}

func startSeamBroker(t *testing.T) (*Broker, string, string) {
	t.Helper()
	b := New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	senderToken, err := b.CreateRun("sender", "sender")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	targetToken, err := b.CreateRun("target", "target")
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	return b, senderToken, targetToken
}

// TestLocalMessagingSeamServesTheHTTPChildPaths pins that the authenticated
// local HTTP router publishes every messaging path through the broker's seam:
// a tell and an absent-edge reply as deliveries that allocate no ask edge, a
// live-edge reply as an edge completion, an ask under the legacy contract, and
// a withdrawal. Each call keeps its existing wire status and outcome.
func TestLocalMessagingSeamServesTheHTTPChildPaths(t *testing.T) {
	b, senderToken, targetToken := startSeamBroker(t)
	rec := &messagetest.Recorder{Local: b.LocalMessenger()}
	b.SetMessenger(rec)

	status, body := postAuthBroker(t, b.Addr(), "/send", "sender", senderToken, map[string]any{
		"from_run_id": "sender",
		"to_run_id":   "target",
		"type":        "agent_message",
		"payload":     map[string]any{"message": "notice"},
	})
	if status != http.StatusOK || body["outcome"] != string(messaging.OutcomeSent) {
		t.Fatalf("tell = (%d, %v), want 200 sent", status, body)
	}
	delivered := rec.Delivers()
	if len(delivered) != 1 || delivered[0].ToRunID != "target" || delivered[0].Kind != "agent_message" {
		t.Fatalf("deliveries = %+v, want the tell through the seam", delivered)
	}
	if edges := b.Store().ActiveCount(); edges != 0 {
		t.Fatalf("tell allocated %d ask edges, want 0", edges)
	}
	if queued := countAgentMessages(t, b, "target"); queued != 1 {
		t.Fatalf("target queue = %d messages, want the single tell", queued)
	}

	// A legacy caller names an unversioned message id without a generation.
	status, body = postAuthBroker(t, b.Addr(), "/send", "sender", senderToken, map[string]any{
		"from_run_id":   "sender",
		"to_run_id":     "target",
		"type":          "agent_message",
		"message_id":    "legacy-ask-1",
		"expects_reply": true,
		"payload":       map[string]any{"message": "ping"},
	})
	if status != http.StatusOK || body["outcome"] != string(messaging.OutcomeSent) {
		t.Fatalf("legacy ask = (%d, %v), want 200 sent", status, body)
	}
	sent := rec.Sends()
	if len(sent) != 1 || !sent[0].Legacy || sent[0].Request.MessageID != "legacy-ask-1" {
		t.Fatalf("sends = %+v, want one legacy ask through the seam", sent)
	}
	if queued := countAgentMessages(t, b, "target"); queued != 2 {
		t.Fatalf("target queue = %d messages, want the tell and the ask", queued)
	}
	if edges := b.Store().ActiveCount(); edges != 1 {
		t.Fatalf("active ask edges = %d, want the legacy ask", edges)
	}
	// A replied edge is retained for duplicate detection, so the reply
	// completes it instead of allocating another one.
	edgesAfterAsk := b.Store().ActiveCount()

	// The ask's target answers the live edge.
	status, body = postAuthBroker(t, b.Addr(), "/send", "target", targetToken, map[string]any{
		"from_run_id": "target",
		"to_run_id":   "sender",
		"type":        "agent_message",
		"reply_to":    "legacy-ask-1",
		"payload":     map[string]any{"message": "all good"},
	})
	if status != http.StatusOK || body["outcome"] != string(messaging.OutcomeReplied) {
		t.Fatalf("live reply = (%d, %v), want 200 replied", status, body)
	}
	if replies := rec.Replies(); len(replies) != 1 || replies[0].MessageID != "legacy-ask-1" {
		t.Fatalf("replies = %+v, want the live reply through the seam", replies)
	}
	if edges := b.Store().ActiveCount(); edges != 1 {
		t.Fatalf("active ask edges = %d, want the completed ask", edges)
	}

	// A reply with no live edge degrades to a delivery instead of an edge.
	status, body = postAuthBroker(t, b.Addr(), "/send", "target", targetToken, map[string]any{
		"from_run_id": "target",
		"to_run_id":   "sender",
		"type":        "agent_message",
		"reply_to":    "edge-already-retired",
		"payload":     map[string]any{"message": "late"},
	})
	if status != http.StatusOK || body["outcome"] != string(messaging.OutcomeEdgeAbsent) {
		t.Fatalf("absent-edge reply = (%d, %v), want 200 edge-absent", status, body)
	}
	if delivered := rec.Delivers(); len(delivered) != 2 || delivered[1].ToRunID != "sender" {
		t.Fatalf("deliveries = %+v, want the absent-edge fallback through the seam", delivered)
	}
	if edges := b.Store().ActiveCount(); edges != edgesAfterAsk {
		t.Fatalf("absent-edge reply allocated an ask edge (%d, want %d)", edges, edgesAfterAsk)
	}
	if queued := countAgentMessages(t, b, "sender"); queued != 1 {
		t.Fatalf("sender queue = %d messages, want the fallback delivery", queued)
	}

	// A withdrawal reaches the seam and retires the edge it names.
	status, body = postAuthBroker(t, b.Addr(), "/send", "sender", senderToken, map[string]any{
		"from_run_id":   "sender",
		"to_run_id":     "target",
		"type":          "agent_message",
		"message_id":    "legacy-ask-2",
		"expects_reply": true,
		"payload":       map[string]any{"message": "cancel me"},
	})
	if status != http.StatusOK || body["outcome"] != string(messaging.OutcomeSent) {
		t.Fatalf("second legacy ask = (%d, %v), want 200 sent", status, body)
	}
	status, body = postAuthBroker(t, b.Addr(), "/cancel_message", "sender", senderToken, map[string]any{
		"cancel_message_id": "legacy-ask-2",
	})
	if status != http.StatusOK || body["outcome"] != string(messaging.OutcomeWithdrawn) {
		t.Fatalf("withdraw = (%d, %v), want 200 withdrawn", status, body)
	}
	if withdraws := rec.Withdraws(); len(withdraws) != 1 || withdraws[0].MessageID != "legacy-ask-2" {
		t.Fatalf("withdrawals = %+v, want the withdrawal through the seam", withdraws)
	}
	if edges := b.Store().ActiveCount(); edges != 1 {
		t.Fatalf("active ask edges = %d after the withdrawal, want only the completed ask", edges)
	}
}

// TestLocalMessagingSeamAnswersAskTargetLookup pins that the ask target
// pre-check asks the seam, so a host that can reach a target the broker does
// not own decides its reachability.
func TestLocalMessagingSeamAnswersAskTargetLookup(t *testing.T) {
	b, senderToken, _ := startSeamBroker(t)
	rec := &messagetest.Recorder{
		Local:         b.LocalMessenger(),
		ReachableFunc: func(string) bool { return false },
	}
	b.SetMessenger(rec)

	status, body := postAuthBroker(t, b.Addr(), "/send", "sender", senderToken, map[string]any{
		"from_run_id":   "sender",
		"to_run_id":     "target",
		"type":          "agent_message",
		"message_id":    "legacy-ask-1",
		"expects_reply": true,
		"payload":       map[string]any{"message": "ping"},
	})
	if status != http.StatusNotFound || body["code"] != "target-unavailable" {
		t.Fatalf("ask with an unreachable target = (%d, %v), want 404 target-unavailable", status, body)
	}
	if sends := rec.Sends(); len(sends) != 0 {
		t.Fatalf("sends = %+v, want none for an unreachable target", sends)
	}
	if queued := countAgentMessages(t, b, "target"); queued != 0 {
		t.Fatalf("target queue = %d messages, want none", queued)
	}
}

// TestLocalMessengerKeepsAskAdmissionContracts pins the one admission
// difference between the broker's two local callers: the legacy HTTP router
// admits an unversioned message id, and a versioned caller does not.
func TestLocalMessengerKeepsAskAdmissionContracts(t *testing.T) {
	b, _, _ := startSeamBroker(t)
	ctx := context.Background()
	req := messaging.SendRequest{
		From:      messaging.Identity{RunID: "sender", Sender: "sender"},
		To:        messaging.Identity{RunID: "target", Sender: "target"},
		MessageID: "unversioned-ask",
		Body:      `{"message":"ping"}`,
	}

	res, err := b.LocalMessenger().Send(ctx, req, false)
	if !errors.Is(err, messaging.ErrGenerationMismatch) {
		t.Fatalf("versioned send of an unversioned id = (%+v, %v), want a generation mismatch", res, err)
	}

	res, err = b.LocalMessenger().Send(ctx, req, true)
	if err != nil || res.Outcome != messaging.OutcomeSent {
		t.Fatalf("legacy send of an unversioned id = (%+v, %v), want sent", res, err)
	}
	if queued := countAgentMessages(t, b, "target"); queued != 1 {
		t.Fatalf("target queue = %d messages, want the single legacy ask", queued)
	}
}

// TestMessengerTransportResolvesTheSeamPerCall pins that the in-process
// transport built for the owner enters the messenger installed after it was
// built, and that its ask lifecycle stays joined to the store's generation.
func TestMessengerTransportResolvesTheSeamPerCall(t *testing.T) {
	b, _, _ := startSeamBroker(t)
	transport := b.MessengerTransport()
	t.Cleanup(func() { _ = transport.Close() })

	rec := &messagetest.Recorder{Local: b.LocalMessenger()}
	b.SetMessenger(rec)

	ctx := context.Background()
	pre, err := transport.Preflight(ctx)
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	now := time.Now().UTC()
	msgID, err := messaging.NewMsgID(now, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("message id: %v", err)
	}
	res, err := transport.Send(ctx, messaging.SendRequest{
		From:       messaging.Identity{RunID: "sender"},
		To:         messaging.Identity{RunID: "target"},
		MessageID:  msgID,
		Generation: pre.Generation,
		Body:       `{"message":"ping"}`,
	})
	if err != nil || res.Outcome != messaging.OutcomeSent {
		t.Fatalf("send = (%+v, %v), want sent", res, err)
	}
	sent := rec.Sends()
	if len(sent) != 1 || sent[0].Request.MessageID != msgID || sent[0].Legacy {
		t.Fatalf("sends = %+v, want one versioned ask through the seam", sent)
	}
	if queued := countAgentMessages(t, b, "target"); queued != 1 {
		t.Fatalf("target queue = %d messages, want the single ask", queued)
	}

	waitCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	waited, err := transport.Wait(waitCtx, messaging.WaitRequest{
		Sender:     messaging.Identity{RunID: "sender"},
		MessageID:  msgID,
		Generation: pre.Generation,
		Timeout:    50 * time.Millisecond,
	})
	if err != nil || waited.Outcome != messaging.OutcomeTimedOut {
		t.Fatalf("wait = (%+v, %v), want a bounded timeout", waited, err)
	}
	if waits := rec.Waits(); len(waits) != 1 || waits[0].MessageID != msgID {
		t.Fatalf("waits = %+v, want the wait through the seam", waits)
	}
}
