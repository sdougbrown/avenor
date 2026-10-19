package messagetest

import (
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// AdmissionGate is the ask store admission seam the paused-send fence
// regression drives. Every store-backed leg exposes it: the in-process wrapper
// directly, the local adapter through the broker's store, and the host fixture
// through the store it serves.
type AdmissionGate interface {
	PauseAdmission(key messaging.FenceKey)
	ResumeAdmission(key messaging.FenceKey)
	AdmissionReached(key messaging.FenceKey) bool
	Pending(sender, messageID string) bool
	Fence(key messaging.FenceKey) (messaging.FenceAck, bool)
}

// RunWithdrawFencesPausedSend pauses a send at the store's admission seam,
// withdraws that exact key through tr, then resumes the send. The resumed send
// must report the fence's withdrawal, and it must leave no delivery, edge or
// non-terminal fence behind.
func RunWithdrawFencesPausedSend(t *testing.T, tr messaging.Transport, gate AdmissionGate, sender, target messaging.Identity) {
	t.Helper()

	pre, err := tr.Preflight(t.Context())
	if err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	now := time.Now().UTC()
	msgID, err := messaging.NewMsgID(now, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("NewMsgID: %v", err)
	}
	key := messaging.FenceKey{Sender: sender.RunID, MessageID: msgID, Generation: pre.Generation}
	gate.PauseAdmission(key)

	type sendOutcome struct {
		res messaging.SendResult
		err error
	}
	sent := make(chan sendOutcome, 1)
	go func() {
		res, err := tr.Send(t.Context(), messaging.SendRequest{
			From: sender, To: target, MessageID: msgID, Generation: pre.Generation,
			Body: `{"message":"paused ask"}`,
		})
		sent <- sendOutcome{res: res, err: err}
	}()

	deadline := time.Now().Add(2 * time.Second)
	for !gate.AdmissionReached(key) {
		if time.Now().After(deadline) {
			t.Fatal("send never reached the store's admission seam")
		}
		time.Sleep(time.Millisecond)
	}

	withdrawn, err := tr.Withdraw(t.Context(), messaging.WithdrawRequest{
		Sender: sender, MessageID: msgID, Generation: pre.Generation,
	})
	if err != nil {
		t.Fatalf("Withdraw: %v", err)
	}
	if !withdrawn.Fence.Terminal {
		t.Fatalf("withdrawal did not install a terminal fence: %+v", withdrawn)
	}

	gate.ResumeAdmission(key)

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

	if gate.Pending(sender.RunID, msgID) {
		t.Fatal("withdrawn ask left a pending edge")
	}
	if ack, ok := gate.Fence(key); !ok || !ack.Terminal {
		t.Fatalf("fence = (%+v, %v), want terminal", ack, ok)
	}
}
