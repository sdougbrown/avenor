package askstore

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// clock is a settable clock for deterministic prune tests. The store may read
// it from any goroutine, so it is locked.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
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

// TestStoreWithdrawAbsentIDPolicy pins which edge-absent withdrawals may
// install a fence and how those fences are charged.
func TestStoreWithdrawAbsentIDPolicy(t *testing.T) {
	base := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	sender := messaging.Identity{RunID: "alice"}
	mint := func(t *testing.T, now, deadline time.Time) string {
		t.Helper()
		msgID, err := messaging.NewMsgID(now, deadline)
		if err != nil {
			t.Fatalf("NewMsgID: %v", err)
		}
		return msgID
	}

	t.Run("expired versioned id is not fenced", func(t *testing.T) {
		clk := &clock{now: base}
		store := New(Options{Clock: clk.Now})
		key := messaging.FenceKey{
			Sender: sender.RunID, MessageID: mint(t, base, base.Add(30*time.Second)), Generation: store.Generation(),
		}
		clk.Advance(time.Minute)
		res, err := store.Withdraw(key)
		if err != nil || res.Outcome != messaging.OutcomeEdgeAbsent || res.Fence.Terminal {
			t.Fatalf("Withdraw = (%+v, %v)", res, err)
		}
		if _, ok := store.Fence(key); ok {
			t.Fatal("expired id was fenced")
		}
	})

	t.Run("unversioned id is not fenced", func(t *testing.T) {
		clk := &clock{now: base}
		store := New(Options{Clock: clk.Now})
		key := messaging.FenceKey{Sender: sender.RunID, MessageID: "legacy-absent"}
		res, err := store.Withdraw(key)
		if err != nil || res.Outcome != messaging.OutcomeEdgeAbsent || res.Fence.Terminal {
			t.Fatalf("Withdraw = (%+v, %v)", res, err)
		}
		if _, err := store.Admit(t.Context(), AdmitRequest{
			Sender: sender, Target: "bob", MessageID: key.MessageID, Legacy: true,
		}); err != nil {
			t.Fatalf("legacy Admit after absent withdrawal: %v", err)
		}
	})

	t.Run("reclaiming a withdrawn ask releases no absent quota", func(t *testing.T) {
		clk := &clock{now: base}
		store := New(Options{Clock: clk.Now})
		gen := store.Generation()
		// The withdrawn ask's fence is charged to the shared capacity, while the
		// absent withdrawal's is charged to the sender's absent quota.
		msgID := mint(t, base, base.Add(30*time.Second))
		if _, err := store.Admit(t.Context(), AdmitRequest{
			Sender: sender, Target: "bob", MessageID: msgID, Generation: gen,
		}); err != nil {
			t.Fatalf("Admit: %v", err)
		}
		if _, err := store.Withdraw(messaging.FenceKey{Sender: sender.RunID, MessageID: msgID, Generation: gen}); err != nil {
			t.Fatalf("Withdraw: %v", err)
		}
		if res, err := store.Withdraw(messaging.FenceKey{
			Sender: sender.RunID, MessageID: mint(t, base, base.Add(2*time.Minute)), Generation: gen,
		}); err != nil || !res.Fence.Terminal {
			t.Fatalf("absent withdrawal = (%+v, %v)", res, err)
		}

		clk.Advance(time.Minute)
		store.Prune(clk.Now())
		if store.absentFences != 1 || store.absentBySender[sender.RunID] != 1 {
			t.Fatalf("absent accounting = (%d, %d), want the surviving absent fence only",
				store.absentFences, store.absentBySender[sender.RunID])
		}
	})

	t.Run("absent fences are bounded per sender", func(t *testing.T) {
		clk := &clock{now: base}
		store := New(Options{Clock: clk.Now, FenceCapacity: 2})
		gen := store.Generation()
		for i := 0; i < absentFenceQuota; i++ {
			res, err := store.Withdraw(messaging.FenceKey{
				Sender: sender.RunID, MessageID: mint(t, base, base.Add(time.Minute)), Generation: gen,
			})
			if err != nil || !res.Fence.Terminal {
				t.Fatalf("absent withdrawal %d = (%+v, %v)", i, res, err)
			}
		}
		over, err := store.Withdraw(messaging.FenceKey{
			Sender: sender.RunID, MessageID: mint(t, base, base.Add(time.Minute)), Generation: gen,
		})
		if !errors.Is(err, messaging.ErrFenceCapacity) || over.Fence.Terminal {
			t.Fatalf("over-quota withdrawal = (%+v, %v)", over, err)
		}
		// Another sender's ask is unaffected by the absent fences, which are
		// charged per sender rather than against the shared capacity.
		res, err := store.Admit(t.Context(), AdmitRequest{
			Sender: messaging.Identity{RunID: "bob"}, Target: "alice",
			MessageID: mint(t, base, base.Add(time.Minute)), Generation: gen,
		})
		if err != nil || res.Outcome != messaging.OutcomeSent {
			t.Fatalf("bob Admit = (%+v, %v)", res, err)
		}
	})

	t.Run("absent fences are bounded globally", func(t *testing.T) {
		clk := &clock{now: base}
		store := New(Options{Clock: clk.Now})
		gen := store.Generation()
		withdraw := func(senderRunID string) (messaging.WithdrawResult, error) {
			return store.Withdraw(messaging.FenceKey{
				Sender: senderRunID, MessageID: mint(t, base, base.Add(time.Minute)), Generation: gen,
			})
		}
		senders := absentFenceCapacity / absentFenceQuota
		for i := 0; i < senders; i++ {
			sender := fmt.Sprintf("sender-%d", i)
			for j := 0; j < absentFenceQuota; j++ {
				if res, err := withdraw(sender); err != nil || !res.Fence.Terminal {
					t.Fatalf("absent withdrawal %d/%d = (%+v, %v)", i, j, res, err)
				}
			}
		}
		// The next sender is under its own quota, so only the global ceiling
		// can refuse it.
		over, err := withdraw("one-more-sender")
		if !errors.Is(err, messaging.ErrFenceCapacity) || over.Fence.Terminal {
			t.Fatalf("over-capacity withdrawal = (%+v, %v)", over, err)
		}
	})

	t.Run("reclaiming absent fences releases the quota", func(t *testing.T) {
		clk := &clock{now: base}
		store := New(Options{Clock: clk.Now})
		gen := store.Generation()
		for i := 0; i < absentFenceQuota; i++ {
			key := messaging.FenceKey{
				Sender: sender.RunID, MessageID: mint(t, base, base.Add(time.Minute)), Generation: gen,
			}
			if res, err := store.Withdraw(key); err != nil || !res.Fence.Terminal {
				t.Fatalf("absent withdrawal %d = (%+v, %v)", i, res, err)
			}
		}
		clk.Advance(2 * time.Minute)
		store.Prune(clk.Now())
		res, err := store.Withdraw(messaging.FenceKey{
			Sender: sender.RunID, MessageID: mint(t, clk.Now(), clk.Now().Add(time.Minute)), Generation: gen,
		})
		if err != nil || !res.Fence.Terminal {
			t.Fatalf("withdrawal after reclaim = (%+v, %v)", res, err)
		}
	})
}

// TestStoreRequiresGenerationForVersionedIDs pins that a versioned ID cannot
// bypass the fence by omitting its generation.
func TestStoreRequiresGenerationForVersionedIDs(t *testing.T) {
	store := New(Options{})
	sender := messaging.Identity{RunID: "alice"}
	now := time.Now().UTC()
	msgID, err := messaging.NewMsgID(now, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("NewMsgID: %v", err)
	}
	key := messaging.FenceKey{Sender: sender.RunID, MessageID: msgID}

	for _, legacy := range []bool{false, true} {
		if _, err := store.Admit(t.Context(), AdmitRequest{
			Sender: sender, Target: "bob", MessageID: msgID, Legacy: legacy,
		}); !errors.Is(err, messaging.ErrGenerationMismatch) {
			t.Fatalf("Admit(legacy=%v) error = %v, want generation mismatch", legacy, err)
		}
	}
	if _, err := store.Wait(t.Context(), key, time.Millisecond); !errors.Is(err, messaging.ErrGenerationMismatch) {
		t.Fatalf("Wait error = %v, want generation mismatch", err)
	}
	if _, err := store.Withdraw(key); !errors.Is(err, messaging.ErrGenerationMismatch) {
		t.Fatalf("Withdraw error = %v, want generation mismatch", err)
	}
	if _, err := store.Admit(t.Context(), AdmitRequest{
		Sender: sender, Target: "bob", MessageID: "legacy-id", Legacy: true,
	}); err != nil {
		t.Fatalf("legacy Admit: %v", err)
	}
}

// TestStorePruneFencesUnconsumedReply pins that a replied edge removed by prune
// still fences its ID, so the ID cannot be admitted a second time.
func TestStorePruneFencesUnconsumedReply(t *testing.T) {
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
	if outcome, err := store.Reply(key, "bob", []byte(`{"message":"pong"}`)); err != nil || outcome != messaging.OutcomeReplied {
		t.Fatalf("Reply = (%q, %v)", outcome, err)
	}
	// Prune sees the edge as expired while the store clock still accepts the ID.
	store.Prune(base.Add(2 * time.Minute))
	ack, ok := store.Fence(key)
	if !ok || !ack.Terminal {
		t.Fatalf("pruned reply fence = (%+v, %v), want terminal", ack, ok)
	}
	if _, err := store.Admit(t.Context(), AdmitRequest{
		Sender: sender, Target: "bob", MessageID: msgID, Generation: store.Generation(),
	}); !errors.Is(err, messaging.ErrDuplicateMessageID) {
		t.Fatalf("re-admit error = %v, want duplicate refusal", err)
	}
}

// recordingBackend records what the store publishes.
type recordingBackend struct {
	mu        sync.Mutex
	published []messaging.InboundMessage
}

func (r *recordingBackend) Publish(target string, msg messaging.InboundMessage) (func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.published = append(r.published, msg)
	return func() {}, nil
}

func (r *recordingBackend) Receive(sender string, limit int) ([]messaging.InboundMessage, error) {
	return nil, nil
}

func (r *recordingBackend) Peers() ([]messaging.Peer, error) { return nil, nil }

func TestStoreNoReplyCreatesNoEdge(t *testing.T) {
	store := New(Options{})
	backend := &recordingBackend{}
	store.SetBackend(backend)

	sender := messaging.Identity{RunID: "alice", Sender: "alice"}
	now := time.Now()
	msgID, err := messaging.NewMsgID(now, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("NewMsgID: %v", err)
	}
	key := messaging.FenceKey{Sender: sender.RunID, MessageID: msgID, Generation: store.Generation()}

	res, err := store.Admit(t.Context(), AdmitRequest{
		Sender: sender, Target: "bob", MessageID: msgID, Generation: store.Generation(),
		Body: `{"message":"notice"}`, NoReply: true,
	})
	if err != nil || res.Outcome != messaging.OutcomeSent {
		t.Fatalf("Admit = (%+v, %v), want sent", res, err)
	}
	if len(backend.published) != 1 || backend.published[0].MessageID != msgID {
		t.Fatalf("published = %+v, want the notice", backend.published)
	}
	if store.Pending(sender.RunID, msgID) {
		t.Fatal("no-reply admit left a live edge")
	}
	if ack, ok := store.Fence(key); !ok || !ack.Terminal {
		t.Fatalf("no-reply admit left no dedup fence (%+v, %v)", ack, ok)
	}
	duplicate, err := store.Admit(t.Context(), AdmitRequest{
		Sender: sender, Target: "bob", MessageID: msgID, Generation: store.Generation(),
		Body: `{"message":"notice"}`, NoReply: true,
	})
	if !errors.Is(err, messaging.ErrDuplicateMessageID) || duplicate.Outcome != messaging.OutcomeNotSent {
		t.Fatalf("duplicate no-reply Admit = (%+v, %v), want a duplicate refusal", duplicate, err)
	}
	if len(backend.published) != 1 {
		t.Fatalf("the same message id was delivered %d time(s), want 1", len(backend.published))
	}
	waited, err := store.Wait(t.Context(), key, 20*time.Millisecond)
	if err != nil || waited.Outcome != messaging.OutcomeEdgeAbsent {
		t.Fatalf("Wait = (%+v, %v), want edge-absent", waited, err)
	}
	withdrawn, err := store.Withdraw(key)
	if err != nil || withdrawn.Outcome != messaging.OutcomeEdgeAbsent {
		t.Fatalf("Withdraw = (%+v, %v), want edge-absent", withdrawn, err)
	}
}

// TestNoReplyCannotBypassWithdrawnFence pins the no-reply path against the
// terminal fence: an ID withdrawn before the message arrives is not delivered.
func TestNoReplyCannotBypassWithdrawnFence(t *testing.T) {
	store := New(Options{})
	backend := &recordingBackend{}
	store.SetBackend(backend)

	sender := messaging.Identity{RunID: "alice", Sender: "alice"}
	now := time.Now()
	msgID, err := messaging.NewMsgID(now, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("NewMsgID: %v", err)
	}
	key := messaging.FenceKey{Sender: sender.RunID, MessageID: msgID, Generation: store.Generation()}
	if withdrawn, err := store.Withdraw(key); err != nil || withdrawn.Outcome != messaging.OutcomeEdgeAbsent {
		t.Fatalf("Withdraw = (%+v, %v), want edge-absent", withdrawn, err)
	}

	res, err := store.Admit(t.Context(), AdmitRequest{
		Sender: sender, Target: "bob", MessageID: msgID, Generation: store.Generation(),
		Body: `{"message":"notice"}`, NoReply: true,
	})
	if err != nil || res.Outcome != messaging.OutcomeWithdrawn || res.Fence == nil {
		t.Fatalf("Admit past a withdrawal = (%+v, %v), want the terminal fence", res, err)
	}
	if len(backend.published) != 0 {
		t.Fatalf("withdrawn no-reply message was delivered %d time(s)", len(backend.published))
	}
}
