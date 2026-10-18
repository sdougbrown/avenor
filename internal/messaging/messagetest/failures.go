package messagetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// RunFailureMatrix drives the shared failure classes through a transport.
// Each case asserts the typed outcome, that no replay occurred, and that any
// retained message ID still has a terminal path.
func RunFailureMatrix(t *testing.T, newTransport TransportFactory) {
	t.Helper()

	alice := messaging.Identity{RunID: "matrix-alice", Sender: "alice"}
	bob := messaging.Identity{RunID: "matrix-bob", Sender: "bob"}
	base := newTransport(t, alice)
	target := newTransport(t, bob)
	t.Cleanup(func() {
		_ = base.Close()
		_ = target.Close()
	})

	pre, err := base.Preflight(t.Context())
	if err != nil {
		t.Fatalf("Preflight: %v", err)
	}

	mint := func(t *testing.T) string {
		t.Helper()
		now := time.Now().UTC()
		msgID, err := messaging.NewMsgID(now, now.Add(time.Minute))
		if err != nil {
			t.Fatalf("NewMsgID: %v", err)
		}
		return msgID
	}

	newFault := func(t *testing.T, mode faultMode) *faultTransport {
		t.Helper()
		tr := newTransport(t, alice)
		t.Cleanup(func() { _ = tr.Close() })
		return &faultTransport{Transport: tr, mode: mode}
	}

	t.Run("credentials rejected", func(t *testing.T) {
		tr := newFault(t, faultAuthRejected)
		res, err := tr.Send(t.Context(), messaging.SendRequest{From: alice, To: bob, MessageID: mint(t), Generation: pre.Generation})
		if !errors.Is(err, messaging.ErrAuthRejected) || res.Outcome != messaging.OutcomeAuthRejected {
			t.Fatalf("Send = (%+v, %v)", res, err)
		}
		if tr.sends != 1 {
			t.Fatalf("sends = %d, want 1", tr.sends)
		}
	})

	t.Run("admission rejected", func(t *testing.T) {
		tr := newFault(t, faultNotSent)
		res, err := tr.Send(t.Context(), messaging.SendRequest{From: alice, To: bob, MessageID: mint(t), Generation: pre.Generation})
		if !errors.Is(err, messaging.ErrInvalidRequest) || res.Outcome != messaging.OutcomeNotSent {
			t.Fatalf("Send = (%+v, %v)", res, err)
		}
		if tr.sends != 1 {
			t.Fatalf("sends = %d, want 1", tr.sends)
		}
	})

	t.Run("accepted send with lost response", func(t *testing.T) {
		tr := newFault(t, faultLostResponse)
		msgID := mint(t)
		res, err := tr.Send(t.Context(), messaging.SendRequest{
			From: alice, To: bob, MessageID: msgID, Generation: pre.Generation, Body: `{"message":"ping"}`,
		})
		if err != nil || res.Outcome != messaging.OutcomePossiblyPending {
			t.Fatalf("Send = (%+v, %v)", res, err)
		}
		if tr.sends != 1 {
			t.Fatalf("sends = %d, want 1", tr.sends)
		}
		// The retained ID still has a terminal path.
		withdrawn, err := base.Withdraw(t.Context(), messaging.WithdrawRequest{
			Sender: alice, MessageID: msgID, Generation: pre.Generation,
		})
		if err != nil {
			t.Fatalf("Withdraw: %v", err)
		}
		if !withdrawn.Fence.Terminal || withdrawn.Outcome != messaging.OutcomeWithdrawn {
			t.Fatalf("Withdraw = %+v", withdrawn)
		}
	})

	t.Run("uncertain wait retains the id", func(t *testing.T) {
		tr := newFault(t, faultUncertainWait)
		msgID := mint(t)
		if _, err := tr.Send(t.Context(), messaging.SendRequest{
			From: alice, To: bob, MessageID: msgID, Generation: pre.Generation, Body: `{"message":"ping"}`,
		}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		waited, err := tr.Wait(t.Context(), messaging.WaitRequest{
			Sender: alice, MessageID: msgID, Generation: pre.Generation, Timeout: time.Second,
		})
		if err != nil || waited.Outcome != messaging.OutcomePossiblyPending {
			t.Fatalf("Wait = (%+v, %v)", waited, err)
		}
		withdrawn, err := base.Withdraw(t.Context(), messaging.WithdrawRequest{
			Sender: alice, MessageID: msgID, Generation: pre.Generation,
		})
		if err != nil || withdrawn.Outcome != messaging.OutcomeWithdrawn {
			t.Fatalf("Withdraw = (%+v, %v)", withdrawn, err)
		}
	})

	t.Run("generation mismatch", func(t *testing.T) {
		tr := newFault(t, faultGeneration)
		res, err := tr.Send(t.Context(), messaging.SendRequest{From: alice, To: bob, MessageID: mint(t), Generation: pre.Generation})
		if !errors.Is(err, messaging.ErrGenerationMismatch) || res.Outcome != messaging.OutcomeNotSent {
			t.Fatalf("Send = (%+v, %v)", res, err)
		}
	})

	t.Run("cancellation without a fence", func(t *testing.T) {
		withdrawn, err := base.Withdraw(t.Context(), messaging.WithdrawRequest{
			Sender: alice, MessageID: mint(t), Generation: pre.Generation,
		})
		if err != nil {
			t.Fatalf("Withdraw: %v", err)
		}
		if withdrawn.Outcome != messaging.OutcomeEdgeAbsent || !withdrawn.Fence.Terminal {
			t.Fatalf("Withdraw = %+v", withdrawn)
		}
	})

	t.Run("duplicate id is refused without a second edge", func(t *testing.T) {
		msgID := mint(t)
		if _, err := base.Send(t.Context(), messaging.SendRequest{
			From: alice, To: bob, MessageID: msgID, Generation: pre.Generation, Body: `{"message":"ping"}`,
		}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		res, err := base.Send(t.Context(), messaging.SendRequest{
			From: alice, To: bob, MessageID: msgID, Generation: pre.Generation, Body: `{"message":"again"}`,
		})
		if !errors.Is(err, messaging.ErrDuplicateMessageID) || res.Outcome != messaging.OutcomeNotSent {
			t.Fatalf("duplicate Send = (%+v, %v)", res, err)
		}
		if _, err := base.Withdraw(t.Context(), messaging.WithdrawRequest{
			Sender: alice, MessageID: msgID, Generation: pre.Generation,
		}); err != nil {
			t.Fatalf("Withdraw: %v", err)
		}
	})

	t.Run("non-target reply is refused", func(t *testing.T) {
		msgID := mint(t)
		if _, err := base.Send(t.Context(), messaging.SendRequest{
			From: alice, To: bob, MessageID: msgID, Generation: pre.Generation, Body: `{"message":"ping"}`,
		}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		_, err := target.Reply(t.Context(), messaging.ReplyRequest{
			From: messaging.Identity{RunID: "matrix-eve"}, ToRunID: alice.RunID, MessageID: msgID, Body: `{}`,
		})
		if !errors.Is(err, messaging.ErrNotAskTarget) {
			t.Fatalf("non-target reply error = %v", err)
		}
		if _, err := base.Withdraw(t.Context(), messaging.WithdrawRequest{
			Sender: alice, MessageID: msgID, Generation: pre.Generation,
		}); err != nil {
			t.Fatalf("Withdraw: %v", err)
		}
	})

	t.Run("timeout finishes the waiter once", func(t *testing.T) {
		msgID := mint(t)
		if _, err := base.Send(t.Context(), messaging.SendRequest{
			From: alice, To: bob, MessageID: msgID, Generation: pre.Generation, Body: `{"message":"ping"}`,
		}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		for i := 0; i < 2; i++ {
			waited, err := base.Wait(t.Context(), messaging.WaitRequest{
				Sender: alice, MessageID: msgID, Generation: pre.Generation, Timeout: 20 * time.Millisecond,
			})
			if err != nil || waited.Outcome != messaging.OutcomeTimedOut {
				t.Fatalf("Wait %d = (%+v, %v)", i, waited, err)
			}
		}
	})
}

type faultMode int

const (
	faultNone faultMode = iota
	faultAuthRejected
	faultNotSent
	faultLostResponse
	faultUncertainWait
	faultGeneration
)

// faultTransport injects one failure class while delegating everything else.
type faultTransport struct {
	messaging.Transport
	mode  faultMode
	sends int
}

func (f *faultTransport) Send(ctx context.Context, req messaging.SendRequest) (messaging.SendResult, error) {
	f.sends++
	switch f.mode {
	case faultAuthRejected:
		return messaging.SendResult{Outcome: messaging.OutcomeAuthRejected, MessageID: req.MessageID}, messaging.ErrAuthRejected
	case faultNotSent:
		return messaging.SendResult{Outcome: messaging.OutcomeNotSent, MessageID: req.MessageID}, messaging.ErrInvalidRequest
	case faultGeneration:
		return messaging.SendResult{Outcome: messaging.OutcomeNotSent, MessageID: req.MessageID}, messaging.ErrGenerationMismatch
	case faultLostResponse:
		if _, err := f.Transport.Send(ctx, req); err != nil {
			return messaging.SendResult{Outcome: messaging.OutcomeNotSent, MessageID: req.MessageID}, err
		}
		return messaging.SendResult{Outcome: messaging.OutcomePossiblyPending, MessageID: req.MessageID}, nil
	default:
		return f.Transport.Send(ctx, req)
	}
}

func (f *faultTransport) Wait(ctx context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	if f.mode == faultUncertainWait {
		return messaging.WaitResult{Outcome: messaging.OutcomePossiblyPending}, nil
	}
	return f.Transport.Wait(ctx, req)
}
