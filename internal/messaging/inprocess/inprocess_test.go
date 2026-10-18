package inprocess

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/messagetest"
)

var alice = messaging.Identity{RunID: "alice", Sender: "alice"}
var bob = messaging.Identity{RunID: "bob", Sender: "bob"}

func suiteFactory(t *testing.T) messagetest.TransportFactory {
	t.Helper()
	store, _ := messagetest.NewMemoryStore(t)
	return func(t *testing.T, id messaging.Identity) messaging.Transport {
		return New(store)
	}
}

func TestInProcessTransportContract(t *testing.T) {
	messagetest.RunTransportSuite(t, suiteFactory(t))
}

func TestAskFailureMatrix(t *testing.T) {
	messagetest.RunFailureMatrix(t, suiteFactory(t))
}

func mintID(t *testing.T, ttl time.Duration) string {
	t.Helper()
	now := time.Now().UTC()
	id, err := messaging.NewMsgID(now, now.Add(ttl))
	if err != nil {
		t.Fatalf("NewMsgID: %v", err)
	}
	return id
}

// TestWithdrawFencesPausedSend pauses a send before admission, withdraws the
// same key, and resumes the send. The resumed send must not deliver or leave an
// edge, and the fence must stay terminal.
func TestWithdrawFencesPausedSend(t *testing.T) {
	store, backend := messagetest.NewMemoryStore(t)
	tr := New(store)
	gen := store.Generation()
	msgID := mintID(t, time.Minute)
	key := messaging.FenceKey{Sender: alice.RunID, MessageID: msgID, Generation: gen}

	store.PauseAdmission(key)

	type sendOutcome struct {
		res messaging.SendResult
		err error
	}
	sent := make(chan sendOutcome, 1)
	go func() {
		res, err := tr.Send(context.Background(), messaging.SendRequest{
			From: alice, To: bob, MessageID: msgID, Generation: gen, Body: `{"message":"paused ask"}`,
		})
		sent <- sendOutcome{res: res, err: err}
	}()

	deadline := time.Now().Add(2 * time.Second)
	for !store.AdmissionReached(key) {
		if time.Now().After(deadline) {
			t.Fatal("send never reached the admission seam")
		}
		time.Sleep(time.Millisecond)
	}

	withdrawn, err := tr.Withdraw(context.Background(), messaging.WithdrawRequest{
		Sender: alice, MessageID: msgID, Generation: gen,
	})
	if err != nil {
		t.Fatalf("Withdraw: %v", err)
	}
	if !withdrawn.Fence.Terminal {
		t.Fatalf("withdrawal did not install a terminal fence: %+v", withdrawn)
	}

	store.ResumeAdmission(key)

	select {
	case out := <-sent:
		if out.err != nil {
			t.Fatalf("resumed send error: %v", out.err)
		}
		if out.res.Outcome != messaging.OutcomeWithdrawn {
			t.Fatalf("resumed send outcome = %q, want withdrawn", out.res.Outcome)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("resumed send did not return")
	}

	if delivered := backend.Queued(bob.RunID); delivered != 0 {
		t.Fatalf("withdrawn ask was delivered %d time(s)", delivered)
	}
	if store.Pending(alice.RunID, msgID) {
		t.Fatal("withdrawn ask left a pending edge")
	}
	if store.PendingCount(alice.RunID) != 0 {
		t.Fatalf("pending count = %d, want 0", store.PendingCount(alice.RunID))
	}
	ack, ok := store.Fence(key)
	if !ok || !ack.Terminal {
		t.Fatalf("fence = (%+v, %v), want terminal", ack, ok)
	}
}

func TestTransportCloseUnblocksWait(t *testing.T) {
	store, _ := messagetest.NewMemoryStore(t)
	tr := New(store)
	gen := store.Generation()
	msgID := mintID(t, time.Minute)
	if _, err := tr.Send(context.Background(), messaging.SendRequest{
		From: alice, To: bob, MessageID: msgID, Generation: gen, Body: `{"message":"ask"}`,
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	type waitOutcome struct {
		res messaging.WaitResult
		err error
	}
	waited := make(chan waitOutcome, 1)
	go func() {
		res, err := tr.Wait(context.Background(), messaging.WaitRequest{
			Sender: alice, MessageID: msgID, Generation: gen, Timeout: time.Minute,
		})
		waited <- waitOutcome{res: res, err: err}
	}()
	time.Sleep(20 * time.Millisecond)

	closed := make(chan error, 1)
	go func() { closed <- tr.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(messaging.CleanupDeadline + time.Second):
		t.Fatal("Close exceeded the cleanup deadline")
	}
	select {
	case out := <-waited:
		if out.err != nil {
			t.Fatalf("Wait: %v", out.err)
		}
		if out.res.Outcome != messaging.OutcomePossiblyPending {
			t.Fatalf("Wait outcome = %q, want possibly-pending", out.res.Outcome)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not unblock on Close")
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestAskEpochLost pins the confirmed-generation-loss path: a replaced
// generation terminates the waiter as lost and refuses the old generation's
// requests.
func TestAskEpochLost(t *testing.T) {
	store, _ := messagetest.NewMemoryStore(t)
	tr := New(store)
	oldGen := store.Generation()
	msgID := mintID(t, time.Minute)
	if _, err := tr.Send(context.Background(), messaging.SendRequest{
		From: alice, To: bob, MessageID: msgID, Generation: oldGen, Body: `{"message":"ask"}`,
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	waited := make(chan messaging.WaitResult, 1)
	go func() {
		res, _ := tr.Wait(context.Background(), messaging.WaitRequest{
			Sender: alice, MessageID: msgID, Generation: oldGen, Timeout: time.Minute,
		})
		waited <- res
	}()
	time.Sleep(20 * time.Millisecond)

	store.Reset()

	select {
	case res := <-waited:
		if res.Outcome != messaging.OutcomeLost {
			t.Fatalf("outcome = %q, want lost", res.Outcome)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter outlived the generation")
	}

	if pre, err := tr.Preflight(context.Background()); err != nil {
		t.Fatalf("Preflight: %v", err)
	} else if pre.Generation == oldGen {
		t.Fatal("Reset did not rotate the generation")
	}
	if _, err := tr.Send(context.Background(), messaging.SendRequest{
		From: alice, To: bob, MessageID: mintID(t, time.Minute), Generation: oldGen, Body: `{}`,
	}); !errors.Is(err, messaging.ErrGenerationMismatch) {
		t.Fatalf("old-generation send error = %v", err)
	}
	if _, ok := store.Fence(messaging.FenceKey{Sender: alice.RunID, MessageID: msgID, Generation: oldGen}); ok {
		t.Fatal("old-generation fence survived rotation")
	}
}
