package messagetest

import (
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/askstore"
)

// TransportFactory builds a transport bound to id's credentials.
type TransportFactory func(t *testing.T, id messaging.Identity) messaging.Transport

// RunTransportSuite exercises the shared transport contract. Every transport
// implementation must pass it unchanged.
func RunTransportSuite(t *testing.T, newTransport TransportFactory) {
	t.Helper()

	alice := messaging.Identity{RunID: "suite-alice", Sender: "alice"}
	bob := messaging.Identity{RunID: "suite-bob", Sender: "bob"}
	at := newTransport(t, alice)
	bt := newTransport(t, bob)
	t.Cleanup(func() {
		_ = at.Close()
		_ = bt.Close()
	})

	pre, err := at.Preflight(t.Context())
	if err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	if pre.Protocol != messaging.ProtocolVersion {
		t.Fatalf("protocol = %d, want %d", pre.Protocol, messaging.ProtocolVersion)
	}
	if pre.Generation == "" {
		t.Fatal("Preflight returned an empty generation")
	}
	bpre, err := bt.Preflight(t.Context())
	if err != nil {
		t.Fatalf("target Preflight: %v", err)
	}

	mint := func(t *testing.T, ttl time.Duration) string {
		t.Helper()
		now := time.Now().UTC()
		msgID, err := messaging.NewMsgID(now, now.Add(ttl))
		if err != nil {
			t.Fatalf("NewMsgID: %v", err)
		}
		return msgID
	}

	t.Run("send receive reply wait", func(t *testing.T) {
		msgID := mint(t, time.Minute)
		sent, err := at.Send(t.Context(), messaging.SendRequest{
			From: alice, To: bob, MessageID: msgID, Generation: pre.Generation, Body: `{"message":"ping"}`,
		})
		if err != nil || sent.Outcome != messaging.OutcomeSent {
			t.Fatalf("Send = (%+v, %v)", sent, err)
		}
		received, err := bt.Receive(t.Context(), messaging.ReceiveRequest{
			Sender: bob, Generation: bpre.Generation, Limit: 8,
		})
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		if len(received.Messages) != 1 || received.Messages[0].MessageID != msgID {
			t.Fatalf("Receive = %+v", received.Messages)
		}
		msg := received.Messages[0]
		if msg.FromRunID != alice.RunID || msg.Sender != alice.Sender {
			t.Fatalf("received sender = %q/%q, want %q/%q", msg.FromRunID, msg.Sender, alice.RunID, alice.Sender)
		}
		if msg.Kind != "agent_message" || string(msg.Payload) != `{"message":"ping"}` {
			t.Fatalf("received message = %+v", msg)
		}
		replied, err := bt.Reply(t.Context(), messaging.ReplyRequest{
			From: bob, ToRunID: alice.RunID, MessageID: msgID, Body: `{"message":"pong"}`,
		})
		if err != nil || replied.Outcome != messaging.OutcomeReplied {
			t.Fatalf("Reply = (%+v, %v)", replied, err)
		}
		waited, err := at.Wait(t.Context(), messaging.WaitRequest{
			Sender: alice, MessageID: msgID, Generation: pre.Generation, Timeout: 2 * time.Second,
		})
		if err != nil {
			t.Fatalf("Wait: %v", err)
		}
		if waited.Outcome != messaging.OutcomeReplied || waited.Reply == nil {
			t.Fatalf("Wait = %+v", waited)
		}
		if waited.Reply.MessageID != msgID || waited.Reply.FromRunID != bob.RunID {
			t.Fatalf("reply identity = %+v", waited.Reply)
		}
		if waited.Reply.Kind != "agent_message" || string(waited.Reply.Payload) != `{"message":"pong"}` {
			t.Fatalf("reply = %+v", waited.Reply)
		}
	})

	t.Run("withdraw fences the ask", func(t *testing.T) {
		msgID := mint(t, time.Minute)
		if _, err := at.Send(t.Context(), messaging.SendRequest{
			From: alice, To: bob, MessageID: msgID, Generation: pre.Generation, Body: `{"message":"ping"}`,
		}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		withdrawn, err := at.Withdraw(t.Context(), messaging.WithdrawRequest{
			Sender: alice, MessageID: msgID, Generation: pre.Generation,
		})
		if err != nil {
			t.Fatalf("Withdraw: %v", err)
		}
		if withdrawn.Outcome != messaging.OutcomeWithdrawn || !withdrawn.Fence.Terminal {
			t.Fatalf("Withdraw = %+v", withdrawn)
		}
		waited, err := at.Wait(t.Context(), messaging.WaitRequest{
			Sender: alice, MessageID: msgID, Generation: pre.Generation, Timeout: time.Second,
		})
		if err != nil || waited.Outcome != messaging.OutcomeWithdrawn {
			t.Fatalf("Wait = (%+v, %v)", waited, err)
		}
	})

	t.Run("wait timeout", func(t *testing.T) {
		msgID := mint(t, time.Minute)
		if _, err := at.Send(t.Context(), messaging.SendRequest{
			From: alice, To: bob, MessageID: msgID, Generation: pre.Generation, Body: `{"message":"ping"}`,
		}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		waited, err := at.Wait(t.Context(), messaging.WaitRequest{
			Sender: alice, MessageID: msgID, Generation: pre.Generation, Timeout: 25 * time.Millisecond,
		})
		if err != nil || waited.Outcome != messaging.OutcomeTimedOut {
			t.Fatalf("Wait = (%+v, %v)", waited, err)
		}
	})

	t.Run("wait for an unknown ask", func(t *testing.T) {
		waited, err := at.Wait(t.Context(), messaging.WaitRequest{
			Sender: alice, MessageID: mint(t, time.Minute), Generation: pre.Generation, Timeout: 20 * time.Millisecond,
		})
		if err != nil || waited.Outcome != messaging.OutcomeEdgeAbsent {
			t.Fatalf("Wait = (%+v, %v)", waited, err)
		}
	})

	t.Run("peers", func(t *testing.T) {
		peers, err := at.Peers(t.Context(), messaging.PeersRequest{Sender: alice, Generation: pre.Generation})
		if err != nil {
			t.Fatalf("Peers: %v", err)
		}
		if peers.Generation == "" {
			t.Fatal("Peers returned an empty generation")
		}
	})
}

// NewMemoryStore builds an askstore backed by a fresh MemoryBackend.
func NewMemoryStore(t *testing.T) (*askstore.Store, *MemoryBackend) {
	t.Helper()
	backend := NewMemoryBackend()
	store := askstore.New(askstore.Options{})
	store.SetBackend(backend)
	return store, backend
}
