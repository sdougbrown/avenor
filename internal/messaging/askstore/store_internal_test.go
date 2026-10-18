package askstore

import (
	"errors"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// clock is a settable clock for deterministic prune tests.
type clock struct {
	now time.Time
}

func (c *clock) Now() time.Time { return c.now }
func (c *clock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
}

func TestStorePrunePreservesValidFence(t *testing.T) {
	base := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	clk := &clock{now: base}
	store := New(Options{Clock: clk.Now, PruneInterval: 30 * time.Second})
	sender := messaging.Identity{RunID: "alice"}
	msgID, err := messaging.NewMsgID(base, base.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("NewMsgID: %v", err)
	}
	key := messaging.FenceKey{Sender: sender.RunID, MessageID: msgID, Generation: store.Generation()}
	if _, err := store.Admit(t.Context(), AdmitRequest{
		Sender: sender, Target: "bob", MessageID: msgID, Generation: store.Generation(),
	}); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if _, err := store.Withdraw(key); err != nil {
		t.Fatalf("Withdraw: %v", err)
	}

	// A tick far shorter than the acceptance deadline must not reclaim the
	// fence or admit the same ID.
	clk.Advance(30 * time.Second)
	store.Prune(clk.Now())
	ack, ok := store.Fence(key)
	if !ok || !ack.Terminal {
		t.Fatal("prune reclaimed a fence before its acceptance deadline")
	}
	admitted, err := store.Admit(t.Context(), AdmitRequest{
		Sender: sender, Target: "bob", MessageID: msgID, Generation: store.Generation(),
	})
	if err != nil {
		t.Fatalf("fenced Admit: %v", err)
	}
	if admitted.Outcome != messaging.OutcomeWithdrawn {
		t.Fatalf("fenced Admit outcome = %q", admitted.Outcome)
	}

	// After the encoded deadline the fence is reclaimed and the ID expires.
	clk.Advance(2 * time.Minute)
	store.Prune(clk.Now())
	if _, ok := store.Fence(key); ok {
		t.Fatal("expired fence was retained")
	}
	if _, err := store.Admit(t.Context(), AdmitRequest{
		Sender: sender, Target: "bob", MessageID: msgID, Generation: store.Generation(),
	}); !errors.Is(err, messaging.ErrMessageIDExpired) {
		t.Fatalf("expired Admit error = %v", err)
	}
}

func TestStorePruneTerminatesExpiredWaiter(t *testing.T) {
	base := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	clk := &clock{now: base}
	store := New(Options{Clock: clk.Now})
	sender := messaging.Identity{RunID: "alice"}
	msgID, err := messaging.NewMsgID(base, base.Add(time.Minute))
	if err != nil {
		t.Fatalf("NewMsgID: %v", err)
	}
	key := messaging.FenceKey{Sender: sender.RunID, MessageID: msgID, Generation: store.Generation()}
	if _, err := store.Admit(t.Context(), AdmitRequest{
		Sender: sender, Target: "bob", MessageID: msgID, Generation: store.Generation(),
	}); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	done := make(chan messaging.WaitResult, 1)
	go func() {
		res, _ := store.Wait(t.Context(), key, time.Minute)
		done <- res
	}()
	// Let the waiter capture the edge before the prune terminates it.
	time.Sleep(20 * time.Millisecond)
	clk.Advance(2 * time.Minute)
	store.Prune(clk.Now())
	select {
	case res := <-done:
		if res.Outcome != messaging.OutcomeTimedOut {
			t.Fatalf("outcome = %q, want timed-out", res.Outcome)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter was not terminated by prune")
	}
	if store.Pending(sender.RunID, msgID) {
		t.Fatal("expired edge survived prune")
	}
}

func TestStoreResetRotatesGeneration(t *testing.T) {
	store := New(Options{})
	before := store.Generation()
	store.Reset()
	if after := store.Generation(); after == before || after == "" {
		t.Fatalf("generation = %q, want a rotation of %q", after, before)
	}
}
