// Package messagetest holds the parameterized suites every messaging transport
// and store must pass.
package messagetest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/askstore"
)

// MemoryBackend is an in-memory queue used by transport and store tests.
type MemoryBackend struct {
	mu      sync.Mutex
	queues  map[string][]queuedEntry
	peers   []messaging.Peer
	next    uint64
	removed int
}

// queuedEntry carries the identity of one published message, so its removal
// closure deletes that entry and not an earlier one that shares its ID.
type queuedEntry struct {
	seq uint64
	msg messaging.InboundMessage
}

// NewMemoryBackend builds an empty backend.
func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{queues: make(map[string][]queuedEntry)}
}

// Publish appends msg to target's queue.
func (m *MemoryBackend) Publish(target string, msg messaging.InboundMessage) (func(), error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.next++
	seq := m.next
	m.queues[target] = append(m.queues[target], queuedEntry{seq: seq, msg: msg})
	removed := false
	return func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		if removed {
			return
		}
		removed = true
		queue := m.queues[target]
		for i, queued := range queue {
			if queued.seq != seq {
				continue
			}
			m.queues[target] = append(queue[:i], queue[i+1:]...)
			m.removed++
			return
		}
	}, nil
}

// Receive drains up to limit messages for sender.
func (m *MemoryBackend) Receive(sender string, limit int) ([]messaging.InboundMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	queue := m.queues[sender]
	if len(queue) == 0 {
		return nil, nil
	}
	if limit > 0 && len(queue) > limit {
		queue = queue[:limit]
	}
	out := make([]messaging.InboundMessage, 0, len(queue))
	for _, queued := range queue {
		out = append(out, queued.msg)
	}
	m.queues[sender] = m.queues[sender][len(queue):]
	return out, nil
}

// Peers returns the peers registered with AddPeer.
func (m *MemoryBackend) Peers() ([]messaging.Peer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]messaging.Peer(nil), m.peers...), nil
}

// AddPeer registers a peer for Peers.
func (m *MemoryBackend) AddPeer(runID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.peers = append(m.peers, messaging.Peer{RunID: runID})
}

// Queued reports how many messages are queued for target.
func (m *MemoryBackend) Queued(target string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.queues[target])
}

// Removed reports how many published entries were removed before delivery.
func (m *MemoryBackend) Removed() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.removed
}

// StoreFactory builds a fresh store for one subtest.
type StoreFactory func(t *testing.T) *askstore.Store

// RunStoreSuite exercises the shared ask lifecycle against a Store.
func RunStoreSuite(t *testing.T, newStore StoreFactory) {
	t.Helper()

	now := time.Now().UTC()
	id := func(t *testing.T, ttl time.Duration) string {
		t.Helper()
		msgID, err := messaging.NewMsgID(now, now.Add(ttl))
		if err != nil {
			t.Fatalf("NewMsgID: %v", err)
		}
		return msgID
	}
	sender := messaging.Identity{RunID: "alice", Sender: "alice"}
	admit := func(t *testing.T, store *askstore.Store, msgID, target string) askstore.AdmitResult {
		t.Helper()
		res, err := store.Admit(t.Context(), askstore.AdmitRequest{
			Sender: sender, Target: target, MessageID: msgID, Generation: store.Generation(),
		})
		if err != nil {
			t.Fatalf("Admit: %v", err)
		}
		return res
	}
	key := func(store *askstore.Store, msgID string) messaging.FenceKey {
		return messaging.FenceKey{Sender: sender.RunID, MessageID: msgID, Generation: store.Generation()}
	}
	admitLegacy := func(t *testing.T, store *askstore.Store, msgID, target string) askstore.AdmitResult {
		t.Helper()
		res, err := store.Admit(t.Context(), askstore.AdmitRequest{
			Sender: sender, Target: target, MessageID: msgID, Legacy: true,
		})
		if err != nil {
			t.Fatalf("legacy Admit: %v", err)
		}
		return res
	}

	t.Run("reply completes the wait", func(t *testing.T) {
		store := newStore(t)
		msgID := id(t, time.Minute)
		admit(t, store, msgID, "bob")
		if got := store.PendingCount(sender.RunID); got != 1 {
			t.Fatalf("pending = %d, want 1", got)
		}
		outcome, err := store.Reply(key(store, msgID), "bob", []byte(`{"message":"hi"}`))
		if err != nil || outcome != messaging.OutcomeReplied {
			t.Fatalf("Reply = (%q, %v)", outcome, err)
		}
		res, err := store.Wait(t.Context(), key(store, msgID), time.Second)
		if err != nil {
			t.Fatalf("Wait: %v", err)
		}
		if res.Outcome != messaging.OutcomeReplied || res.Reply == nil {
			t.Fatalf("Wait = %+v", res)
		}
		if store.PendingCount(sender.RunID) != 0 {
			t.Fatal("edge survived reply consumption")
		}
		if ack, ok := store.Fence(key(store, msgID)); !ok || !ack.Terminal {
			t.Fatal("replied ID has no terminal fence")
		}
	})

	t.Run("duplicate and mutual asks are refused", func(t *testing.T) {
		store := newStore(t)
		msgID := id(t, time.Minute)
		admit(t, store, msgID, "bob")
		if _, err := store.Admit(t.Context(), askstore.AdmitRequest{
			Sender: sender, Target: "bob", MessageID: msgID, Generation: store.Generation(),
		}); !errors.Is(err, messaging.ErrDuplicateMessageID) {
			t.Fatalf("duplicate error = %v", err)
		}
		if _, err := store.Admit(t.Context(), askstore.AdmitRequest{
			Sender: messaging.Identity{RunID: "bob"}, Target: "alice", MessageID: id(t, time.Minute), Generation: store.Generation(),
		}); !errors.Is(err, messaging.ErrMutualAsk) {
			t.Fatalf("mutual error = %v", err)
		}
	})

	t.Run("generation mismatch is refused", func(t *testing.T) {
		store := newStore(t)
		_, err := store.Admit(t.Context(), askstore.AdmitRequest{
			Sender: sender, Target: "bob", MessageID: id(t, time.Minute), Generation: "other",
		})
		if !errors.Is(err, messaging.ErrGenerationMismatch) {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("withdrawal fences a pending ask", func(t *testing.T) {
		store := newStore(t)
		msgID := id(t, time.Minute)
		admit(t, store, msgID, "bob")
		res, err := store.Withdraw(key(store, msgID))
		if err != nil {
			t.Fatalf("Withdraw: %v", err)
		}
		if res.Outcome != messaging.OutcomeWithdrawn || !res.Fence.Terminal {
			t.Fatalf("Withdraw = %+v", res)
		}
		wait, err := store.Wait(t.Context(), key(store, msgID), time.Second)
		if err != nil || wait.Outcome != messaging.OutcomeWithdrawn {
			t.Fatalf("Wait = (%+v, %v)", wait, err)
		}
	})

	t.Run("withdrawal without an edge still fences", func(t *testing.T) {
		store := newStore(t)
		msgID := id(t, time.Minute)
		res, err := store.Withdraw(key(store, msgID))
		if err != nil {
			t.Fatalf("Withdraw: %v", err)
		}
		if res.Outcome != messaging.OutcomeEdgeAbsent || !res.Fence.Terminal {
			t.Fatalf("Withdraw = %+v", res)
		}
		admitted, err := store.Admit(t.Context(), askstore.AdmitRequest{
			Sender: sender, Target: "bob", MessageID: msgID, Generation: store.Generation(),
		})
		if err != nil {
			t.Fatalf("Admit after fence: %v", err)
		}
		if admitted.Outcome != messaging.OutcomeWithdrawn {
			t.Fatalf("Admit outcome = %q, want withdrawn", admitted.Outcome)
		}
	})

	t.Run("wait timeout terminates once", func(t *testing.T) {
		store := newStore(t)
		msgID := id(t, time.Minute)
		admit(t, store, msgID, "bob")
		res, err := store.Wait(t.Context(), key(store, msgID), 20*time.Millisecond)
		if err != nil || res.Outcome != messaging.OutcomeTimedOut {
			t.Fatalf("Wait = (%+v, %v)", res, err)
		}
		again, err := store.Wait(t.Context(), key(store, msgID), time.Second)
		if err != nil || again.Outcome != messaging.OutcomeTimedOut {
			t.Fatalf("second Wait = (%+v, %v)", again, err)
		}
		if store.PendingCount(sender.RunID) != 0 {
			t.Fatal("timed-out edge is still live")
		}
	})

	t.Run("withdrawal retracts the queued ask", func(t *testing.T) {
		store := newStore(t)
		msgID := id(t, time.Minute)
		admit(t, store, msgID, "bob")
		if _, err := store.Withdraw(key(store, msgID)); err != nil {
			t.Fatalf("Withdraw: %v", err)
		}
		received, err := store.Receive("bob", 8)
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		if len(received) != 0 {
			t.Fatalf("withdrawn ask was still delivered: %+v", received)
		}
	})

	t.Run("wait timeout keeps the queued ask", func(t *testing.T) {
		store := newStore(t)
		msgID := id(t, time.Minute)
		admit(t, store, msgID, "bob")
		res, err := store.Wait(t.Context(), key(store, msgID), 20*time.Millisecond)
		if err != nil || res.Outcome != messaging.OutcomeTimedOut {
			t.Fatalf("Wait = (%+v, %v)", res, err)
		}
		received, err := store.Receive("bob", 8)
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		if len(received) != 1 || received[0].MessageID != msgID {
			t.Fatalf("target received %+v, want the timed-out ask", received)
		}
		// The timed-out ID is fenced, so a late reply cannot answer it and falls
		// back to fire-and-forget delivery for the sender.
		if outcome, err := store.Reply(key(store, msgID), "bob", []byte(`{"message":"late"}`)); err != nil || outcome != messaging.OutcomeEdgeAbsent {
			t.Fatalf("late Reply = (%q, %v), want edge-absent", outcome, err)
		}
		if ack, ok := store.Fence(key(store, msgID)); !ok || !ack.Terminal {
			t.Fatalf("fence = (%+v, %v), want terminal", ack, ok)
		}
		if _, err := store.Admit(t.Context(), askstore.AdmitRequest{
			Sender: sender, Target: "bob", MessageID: msgID, Generation: store.Generation(),
		}); !errors.Is(err, messaging.ErrDuplicateMessageID) {
			t.Fatalf("re-admit error = %v, want duplicate refusal", err)
		}
	})

	t.Run("wait abandonment keeps the queued ask", func(t *testing.T) {
		store := newStore(t)
		msgID := id(t, time.Minute)
		admit(t, store, msgID, "bob")
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		res, err := store.Wait(ctx, key(store, msgID), time.Second)
		if !errors.Is(err, context.Canceled) || res.Outcome != messaging.OutcomePossiblyPending {
			t.Fatalf("Wait = (%+v, %v)", res, err)
		}
		received, err := store.Receive("bob", 8)
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		if len(received) != 1 || received[0].MessageID != msgID {
			t.Fatalf("target received %+v, want the abandoned ask", received)
		}
		if outcome, err := store.Reply(key(store, msgID), "bob", []byte(`{"message":"late"}`)); err != nil || outcome != messaging.OutcomeEdgeAbsent {
			t.Fatalf("late Reply = (%q, %v), want edge-absent", outcome, err)
		}
		if ack, ok := store.Fence(key(store, msgID)); !ok || !ack.Terminal {
			t.Fatalf("fence = (%+v, %v), want terminal", ack, ok)
		}
	})

	t.Run("legacy wait timeout leaves no fence", func(t *testing.T) {
		store := newStore(t)
		msgID := "legacy-timeout-id"
		legacyKey := messaging.FenceKey{Sender: sender.RunID, MessageID: msgID}
		admitLegacy(t, store, msgID, "bob")
		res, err := store.Wait(t.Context(), legacyKey, 20*time.Millisecond)
		if err != nil || res.Outcome != messaging.OutcomeTimedOut {
			t.Fatalf("Wait = (%+v, %v)", res, err)
		}
		if _, ok := store.Fence(legacyKey); ok {
			t.Fatal("legacy timeout recorded a fence")
		}
		if received, err := store.Receive("bob", 8); err != nil || len(received) != 1 {
			t.Fatalf("Receive = (%+v, %v), want the timed-out ask retained", received, err)
		}
		// The same unversioned ID can be sent again immediately.
		admitLegacy(t, store, msgID, "bob")
	})

	t.Run("legacy reply leaves no fence", func(t *testing.T) {
		store := newStore(t)
		msgID := "legacy-replied-id"
		legacyKey := messaging.FenceKey{Sender: sender.RunID, MessageID: msgID}
		admitLegacy(t, store, msgID, "bob")
		// The reply names the current generation; the lookup falls back to the
		// generation-less key the legacy ask was registered under.
		if outcome, err := store.Reply(key(store, msgID), "bob", []byte(`{"message":"pong"}`)); err != nil || outcome != messaging.OutcomeReplied {
			t.Fatalf("Reply = (%q, %v)", outcome, err)
		}
		res, err := store.Wait(t.Context(), legacyKey, time.Second)
		if err != nil || res.Outcome != messaging.OutcomeReplied || res.Reply == nil {
			t.Fatalf("Wait = (%+v, %v)", res, err)
		}
		if _, ok := store.Fence(legacyKey); ok {
			t.Fatal("a consumed legacy reply recorded a fence")
		}
		// The same unversioned ID can be sent again immediately.
		admitLegacy(t, store, msgID, "bob")
	})

	t.Run("reply must name the edge generation", func(t *testing.T) {
		store := newStore(t)
		msgID := id(t, time.Minute)
		admit(t, store, msgID, "bob")
		generationless := messaging.FenceKey{Sender: sender.RunID, MessageID: msgID}
		outcome, err := store.Reply(generationless, "bob", []byte(`{"message":"pong"}`))
		if err != nil || outcome != messaging.OutcomeEdgeAbsent {
			t.Fatalf("generation-less Reply = (%q, %v), want edge-absent", outcome, err)
		}
		if !store.Pending(sender.RunID, msgID) {
			t.Fatal("generation-less reply disturbed the live edge")
		}
		if outcome, err := store.Reply(key(store, msgID), "bob", []byte(`{"message":"pong"}`)); err != nil || outcome != messaging.OutcomeReplied {
			t.Fatalf("Reply = (%q, %v)", outcome, err)
		}
		res, err := store.Wait(t.Context(), key(store, msgID), time.Second)
		if err != nil || res.Outcome != messaging.OutcomeReplied || res.Reply == nil {
			t.Fatalf("Wait = (%+v, %v)", res, err)
		}
	})

	t.Run("reply then removal completes once", func(t *testing.T) {
		store := newStore(t)
		msgID := id(t, time.Minute)
		admit(t, store, msgID, "bob")
		k := key(store, msgID)
		if outcome, err := store.Reply(k, "bob", []byte(`{"message":"pong"}`)); err != nil || outcome != messaging.OutcomeReplied {
			t.Fatalf("Reply = (%q, %v)", outcome, err)
		}
		store.RemoveRun("bob")
		res, err := store.Wait(t.Context(), k, time.Second)
		if err != nil || res.Outcome != messaging.OutcomeReplied || res.Reply == nil {
			t.Fatalf("Wait after removal = (%+v, %v)", res, err)
		}
		if store.PendingCount(sender.RunID) != 0 {
			t.Fatal("replied edge survived consumption")
		}
		store.Reset()
		if _, err := store.Wait(t.Context(), k, time.Second); !errors.Is(err, messaging.ErrGenerationMismatch) {
			t.Fatalf("old-generation Wait error = %v", err)
		}
	})

	t.Run("remove run keeps the queued ask", func(t *testing.T) {
		store := newStore(t)
		msgID := id(t, time.Minute)
		admit(t, store, msgID, "bob")
		store.RemoveRun(sender.RunID)
		if received, err := store.Receive("bob", 8); err != nil || len(received) != 1 {
			t.Fatalf("Receive = (%+v, %v), want the ask retained", received, err)
		}
	})

	t.Run("prune preserves a valid fence", func(t *testing.T) {
		store := newStore(t)
		msgID := id(t, time.Minute)
		admit(t, store, msgID, "bob")
		if _, err := store.Withdraw(key(store, msgID)); err != nil {
			t.Fatalf("Withdraw: %v", err)
		}
		store.Prune(now.Add(askstore.DefaultPruneInterval))
		ack, ok := store.Fence(key(store, msgID))
		if !ok || !ack.Terminal {
			t.Fatal("prune reclaimed a fence before its deadline")
		}
		admitted, err := store.Admit(t.Context(), askstore.AdmitRequest{
			Sender: sender, Target: "bob", MessageID: msgID, Generation: store.Generation(),
		})
		if err != nil || admitted.Outcome != messaging.OutcomeWithdrawn {
			t.Fatalf("fenced Admit = (%+v, %v)", admitted, err)
		}
	})

	t.Run("prune keeps an ask that is not yet due", func(t *testing.T) {
		store := newStore(t)
		minted := time.Now().UTC()
		msgID, err := messaging.NewMsgID(minted, minted.Add(time.Minute))
		if err != nil {
			t.Fatalf("NewMsgID: %v", err)
		}
		admit(t, store, msgID, "bob")

		store.Prune(minted.Add(30 * time.Second))
		if !store.Pending(sender.RunID, msgID) || store.PendingCount(sender.RunID) != 1 {
			t.Fatal("prune terminated an ask before its deadline")
		}
		if _, ok := store.Fence(key(store, msgID)); ok {
			t.Fatal("prune fenced an ask before its deadline")
		}
	})

	t.Run("prune expires a fenced id after its deadline", func(t *testing.T) {
		store := newStore(t)
		minted := time.Now().UTC()
		msgID, err := messaging.NewMsgID(minted, minted.Add(time.Minute))
		if err != nil {
			t.Fatalf("NewMsgID: %v", err)
		}
		withdrawn, err := store.Withdraw(key(store, msgID))
		if err != nil {
			t.Fatalf("Withdraw: %v", err)
		}
		if !withdrawn.Fence.Terminal {
			t.Fatalf("Withdraw = %+v, want a terminal fence", withdrawn)
		}

		store.Prune(minted.Add(2 * time.Minute))
		if _, ok := store.Fence(key(store, msgID)); ok {
			t.Fatal("expired fence was retained")
		}
	})

	t.Run("capacity refuses new work", func(t *testing.T) {
		store := askstore.New(askstore.Options{FenceCapacity: 2})
		store.SetBackend(NewMemoryBackend())
		for i := 0; i < 2; i++ {
			if _, err := store.Admit(t.Context(), askstore.AdmitRequest{
				Sender: sender, Target: "bob", MessageID: id(t, time.Minute), Generation: store.Generation(),
			}); err != nil {
				t.Fatalf("Admit %d: %v", i, err)
			}
		}
		if _, err := store.Admit(t.Context(), askstore.AdmitRequest{
			Sender: sender, Target: "bob", MessageID: id(t, time.Minute), Generation: store.Generation(),
		}); !errors.Is(err, messaging.ErrFenceCapacity) {
			t.Fatalf("capacity error = %v", err)
		}
	})

	t.Run("remove run terminates its waiters", func(t *testing.T) {
		store := newStore(t)
		msgID := id(t, time.Minute)
		admit(t, store, msgID, "bob")
		done := make(chan messaging.WaitResult, 1)
		go func() {
			res, _ := store.Wait(t.Context(), key(store, msgID), time.Minute)
			done <- res
		}()
		waitFor(t, func() bool { return store.PendingCount(sender.RunID) == 1 })
		store.RemoveRun("bob")
		select {
		case res := <-done:
			if res.Outcome != messaging.OutcomeLost {
				t.Fatalf("outcome = %q, want lost", res.Outcome)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("waiter was not terminated")
		}
	})
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not reached")
}
