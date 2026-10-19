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

	t.Run("receive honors the limit", func(t *testing.T) {
		ids := make([]string, 3)
		for i := range ids {
			ids[i] = mint(t, time.Minute)
			if _, err := at.Send(t.Context(), messaging.SendRequest{
				From: alice, To: bob, MessageID: ids[i], Generation: pre.Generation, Body: `{"message":"ping"}`,
			}); err != nil {
				t.Fatalf("Send %d: %v", i, err)
			}
		}
		first, err := bt.Receive(t.Context(), messaging.ReceiveRequest{
			Sender: bob, Generation: bpre.Generation, Limit: 2,
		})
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		if len(first.Messages) != 2 || first.Messages[0].MessageID != ids[0] || first.Messages[1].MessageID != ids[1] {
			t.Fatalf("first Receive = %+v, want the first two in order", first.Messages)
		}
		second, err := bt.Receive(t.Context(), messaging.ReceiveRequest{
			Sender: bob, Generation: bpre.Generation, Limit: 2,
		})
		if err != nil {
			t.Fatalf("second Receive: %v", err)
		}
		if len(second.Messages) != 1 || second.Messages[0].MessageID != ids[2] {
			t.Fatalf("second Receive = %+v, want the message the limit left queued", second.Messages)
		}
	})

	t.Run("zero limit takes the default", func(t *testing.T) {
		ids := make([]string, messaging.DefaultReceiveLimit+1)
		for i := range ids {
			ids[i] = mint(t, time.Minute)
			if _, err := at.Send(t.Context(), messaging.SendRequest{
				From: alice, To: bob, MessageID: ids[i], Generation: pre.Generation, Body: `{"message":"ping"}`,
			}); err != nil {
				t.Fatalf("Send %d: %v", i, err)
			}
		}
		first, err := bt.Receive(t.Context(), messaging.ReceiveRequest{
			Sender: bob, Generation: bpre.Generation, Limit: 0,
		})
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		if len(first.Messages) != messaging.DefaultReceiveLimit {
			t.Fatalf("Receive with no limit returned %d messages, want %d", len(first.Messages), messaging.DefaultReceiveLimit)
		}
		if first.Messages[0].MessageID != ids[0] || first.Messages[len(first.Messages)-1].MessageID != ids[messaging.DefaultReceiveLimit-1] {
			t.Fatalf("Receive with no limit = %+v, want the first %d in order", first.Messages, messaging.DefaultReceiveLimit)
		}
		last, err := bt.Receive(t.Context(), messaging.ReceiveRequest{
			Sender: bob, Generation: bpre.Generation, Limit: 0,
		})
		if err != nil {
			t.Fatalf("second Receive: %v", err)
		}
		if len(last.Messages) != 1 || last.Messages[0].MessageID != ids[messaging.DefaultReceiveLimit] {
			t.Fatalf("second Receive = %+v, want the message the default left queued", last.Messages)
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
		for _, timeout := range []time.Duration{25 * time.Millisecond, 500 * time.Microsecond} {
			msgID := mint(t, time.Minute)
			if _, err := at.Send(t.Context(), messaging.SendRequest{
				From: alice, To: bob, MessageID: msgID, Generation: pre.Generation, Body: `{"message":"ping"}`,
			}); err != nil {
				t.Fatalf("Send: %v", err)
			}
			waited, err := at.Wait(t.Context(), messaging.WaitRequest{
				Sender: alice, MessageID: msgID, Generation: pre.Generation, Timeout: timeout,
			})
			if err != nil || waited.Outcome != messaging.OutcomeTimedOut {
				t.Fatalf("wait with %v timeout = (%+v, %v)", timeout, waited, err)
			}
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

// ReplyRoutingHooks expose the store operations a transport cannot perform, so
// the reply-routing suite can set up the edges it checks.
type ReplyRoutingHooks struct {
	// AdmitLegacy admits an unversioned ask, which the store keys without a
	// generation.
	AdmitLegacy func(t *testing.T, sender messaging.Identity, messageID, target string)
	// Rotate retires the generation that holds live ask state.
	Rotate func(t *testing.T)
}

// RunReplyRoutingSuite pins reply routing for the keys a transport cannot
// create itself: a reply answers an edge admitted on the legacy path, and a
// reply whose generation a rotation retired finds no edge.
func RunReplyRoutingSuite(t *testing.T, newTransport TransportFactory, hooks ReplyRoutingHooks) {
	t.Helper()

	alice := messaging.Identity{RunID: "reply-alice", Sender: "alice"}
	bob := messaging.Identity{RunID: "reply-bob", Sender: "bob"}
	at := newTransport(t, alice)
	bt := newTransport(t, bob)
	t.Cleanup(func() {
		_ = at.Close()
		_ = bt.Close()
	})

	t.Run("reply answers a legacy edge", func(t *testing.T) {
		msgID := "legacy-edge-id"
		hooks.AdmitLegacy(t, alice, msgID, bob.RunID)
		replied, err := bt.Reply(t.Context(), messaging.ReplyRequest{
			From: bob, ToRunID: alice.RunID, MessageID: msgID, Body: `{"message":"pong"}`,
		})
		if err != nil || replied.Outcome != messaging.OutcomeReplied {
			t.Fatalf("Reply = (%+v, %v), want the legacy edge answered", replied, err)
		}
	})

	t.Run("reply after a rotation finds no edge", func(t *testing.T) {
		pre, err := at.Preflight(t.Context())
		if err != nil {
			t.Fatalf("Preflight: %v", err)
		}
		now := time.Now().UTC()
		msgID, err := messaging.NewMsgID(now, now.Add(time.Minute))
		if err != nil {
			t.Fatalf("NewMsgID: %v", err)
		}
		if _, err := at.Send(t.Context(), messaging.SendRequest{
			From: alice, To: bob, MessageID: msgID, Generation: pre.Generation, Body: `{"message":"ping"}`,
		}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		hooks.Rotate(t)
		replied, err := bt.Reply(t.Context(), messaging.ReplyRequest{
			From: bob, ToRunID: alice.RunID, MessageID: msgID, Body: `{"message":"pong"}`,
		})
		if err != nil || replied.Outcome != messaging.OutcomeEdgeAbsent {
			t.Fatalf("Reply after rotation = (%+v, %v), want edge-absent", replied, err)
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
