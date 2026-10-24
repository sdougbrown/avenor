package stable

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/localhttp"
	"github.com/sdougbrown/avenor/internal/messaging/messagetest"
	"github.com/sdougbrown/avenor/internal/runtime/broker/brokertest"
)

// TestOwnerMessagingEntersTheBrokerSeam pins that the supervisor's own
// transport reaches a messenger installed after the supervisor was built, so a
// host that owns a parent link hooks owner asks, replies, waits and peer
// lookups instead of reaching the local store directly. Each hook still leaves
// the local ask intact: the ask is queued once, the child answers it over HTTP
// and the owner's wait receives that reply.
func TestOwnerMessagingEntersTheBrokerSeam(t *testing.T) {
	sup, _ := newStage2Supervisor(t, 4)
	c := ownerControlClient(t, sup, "seam-owner")

	rec := &messagetest.Recorder{Local: sup.broker.LocalMessenger()}
	sup.broker.SetMessenger(rec)
	t.Cleanup(func() { sup.broker.SetMessenger(nil) })

	token, err := sup.broker.CreateRun("rt_child", "worker")
	if err != nil {
		t.Fatalf("create child run: %v", err)
	}
	child, err := localhttp.New(sup.broker.Addr(), messaging.LocalCredentials{RunID: "rt_child", Token: token})
	if err != nil {
		t.Fatalf("child transport: %v", err)
	}
	defer child.Close()

	pre, err := c.MessagePreflight()
	if err != nil {
		t.Fatalf("control preflight: %v", err)
	}
	now := time.Now().UTC()
	msgID, err := messaging.NewMsgID(now, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("message id: %v", err)
	}
	body, err := messaging.AskBody(msgID, "supervisor", "rt_child", "status?", "host")
	if err != nil {
		t.Fatalf("ask body: %v", err)
	}
	sent, err := c.MessageSend(messaging.SendRequest{
		From:       messaging.Identity{RunID: "supervisor"},
		To:         messaging.Identity{RunID: "rt_child"},
		MessageID:  msgID,
		Generation: pre.Generation,
		Body:       string(body),
	})
	if err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("owner ask = (%+v, %v), want sent", sent, err)
	}
	sends := rec.Sends()
	if len(sends) != 1 || sends[0].Request.MessageID != msgID || sends[0].Request.Legacy {
		t.Fatalf("owner sends = %+v, want one versioned ask through the seam", sends)
	}
	if queued := brokertest.WaitForAgentMessage(t, sup.broker, "rt_child"); queued.ID != msgID {
		t.Fatalf("queued ask = %+v, want the owner's %s", queued, msgID)
	}

	ctx := context.Background()
	replied, err := child.Reply(ctx, messaging.ReplyRequest{
		From:          messaging.Identity{RunID: "rt_child"},
		ToRunID:       "supervisor",
		MessageID:     msgID,
		CorrelationID: msgID,
		Body:          `{"message":"all good","role":"agent"}`,
	})
	if err != nil || replied.Outcome != messaging.OutcomeReplied {
		t.Fatalf("child reply = (%+v, %v), want replied", replied, err)
	}
	if replies := rec.Replies(); len(replies) != 1 || replies[0].MessageID != msgID {
		t.Fatalf("replies = %+v, want the child's reply through the seam", replies)
	}

	waited, err := c.MessageWait(messaging.WaitRequest{
		Sender:     messaging.Identity{RunID: "supervisor"},
		MessageID:  msgID,
		Generation: pre.Generation,
		Timeout:    2 * time.Second,
	})
	if err != nil || waited.Outcome != messaging.OutcomeReplied || waited.Reply == nil {
		t.Fatalf("owner wait = (%+v, %v), want the child's reply", waited, err)
	}
	if waits := rec.Waits(); len(waits) != 1 || waits[0].MessageID != msgID {
		t.Fatalf("waits = %+v, want the owner wait through the seam", waits)
	}
	var reply struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(waited.Reply.Payload, &reply); err != nil || reply.Message != "all good" {
		t.Fatalf("reply payload = %s (%v), want all good", waited.Reply.Payload, err)
	}

	peers, err := c.MessagePeers(messaging.PeersRequest{
		Sender:     messaging.Identity{RunID: messaging.SupervisorRunID},
		Generation: pre.Generation,
	})
	if err != nil {
		t.Fatalf("message_peers: %v", err)
	}
	if rec.PeerCalls() != 1 {
		t.Fatalf("peer lookups = %d, want the owner lookup through the seam", rec.PeerCalls())
	}
	visible := map[string]bool{}
	for _, peer := range peers.Peers {
		visible[peer.RunID] = true
	}
	if !visible[messaging.SupervisorRunID] || !visible["rt_child"] {
		t.Fatalf("peers = %+v, want the supervisor and its child", peers.Peers)
	}
}
