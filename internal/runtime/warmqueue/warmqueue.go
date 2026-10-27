// Package warmqueue holds the pending-message queue shared by the
// channel-backed runtime providers: prompts received from the broker while a
// session is mid-turn are queued here and injected at the next turn boundary.
// The queue also carries the warm-resume admission guard, so a turn admitted
// by the stable supervisor can never overtake prompts that are still queued or
// being injected.
package warmqueue

import "sync"

// WarmMessageAdmission is the provider-side warm-resume guard. Providers that
// inject broker messages at turn boundaries implement it so the stable
// supervisor can refuse admission while prompts are queued or in flight for
// the session being resumed.
type WarmMessageAdmission interface {
	// LockWarmMessageAdmission reports whether a warm resume may be admitted
	// for the session. On success it returns the unlock for the held guard;
	// on refusal the guard is not held.
	LockWarmMessageAdmission(sessionID string) (func(), bool)
}

// Queue is a provider's per-session queue of channel-wrapped prompts awaiting
// injection, with the injection bookkeeping the admission guard reads. The
// zero value holds no prompts and admits only for an in-flight injection:
// Enqueue drops prompts, Take delivers nothing, and LockWarmMessageAdmission
// admits. Providers that accept prompts construct it with NewQueue.
type Queue struct {
	mu        sync.Mutex
	pending   map[string][]string
	injecting map[string]int
}

// NewQueue returns an empty queue that accepts prompts.
func NewQueue() *Queue {
	return &Queue{pending: map[string][]string{}}
}

// Enqueue queues a wrapped prompt for the session. It is a no-op once the
// queue has been discarded, and on a nil queue.
func (q *Queue) Enqueue(sessionID, wrapped string) {
	if q == nil {
		return
	}
	q.mu.Lock()
	if q.pending != nil {
		q.pending[sessionID] = append(q.pending[sessionID], wrapped)
	}
	q.mu.Unlock()
}

// Discard drops every pending prompt and stops accepting new ones, for a
// provider whose sessions are gone for good.
func (q *Queue) Discard() {
	if q == nil {
		return
	}
	q.mu.Lock()
	q.pending = nil
	q.mu.Unlock()
}

// Reset drops every pending prompt while continuing to accept new ones, for a
// provider that can start sessions again after close.
func (q *Queue) Reset() {
	if q == nil {
		return
	}
	q.mu.Lock()
	q.pending = map[string][]string{}
	q.mu.Unlock()
}

// LockWarmMessageAdmission implements WarmMessageAdmission: admission is
// refused while the session has queued prompts or an injection in flight. A
// nil queue always admits.
func (q *Queue) LockWarmMessageAdmission(sessionID string) (func(), bool) {
	if q == nil {
		return func() {}, true
	}
	q.mu.Lock()
	if len(q.pending[sessionID]) != 0 || q.injecting[sessionID] != 0 {
		q.mu.Unlock()
		return nil, false
	}
	return q.mu.Unlock, true
}

// Take removes the session's queued prompts and, when any were taken, marks an
// injection in flight until the returned done is called. A caller that
// received prompts must call done; done is nil when there was nothing to
// deliver, including on a nil queue.
func (q *Queue) Take(sessionID string) (msgs []string, done func()) {
	if q == nil {
		return nil, nil
	}
	q.mu.Lock()
	if q.pending != nil {
		msgs = q.pending[sessionID]
		q.pending[sessionID] = nil
	}
	if len(msgs) != 0 {
		if q.injecting == nil {
			q.injecting = make(map[string]int)
		}
		q.injecting[sessionID]++
	}
	q.mu.Unlock()
	if len(msgs) == 0 {
		return msgs, nil
	}
	return msgs, func() {
		q.mu.Lock()
		q.injecting[sessionID]--
		q.mu.Unlock()
	}
}
