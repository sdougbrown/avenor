// Package askstore owns the shared ask lifecycle: sender-scoped edges,
// waiters, terminal fences and bounded retention. It performs no HTTP and
// holds no execution state; queue publication is delegated to a Backend.
package askstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
)

const (
	// DefaultAskTimeout is the retention and wait bound for an unversioned ask.
	DefaultAskTimeout = 10 * time.Minute

	// DefaultPruneInterval is how often the prune loop reclaims expired state.
	DefaultPruneInterval = 30 * time.Second

	// DefaultFenceCapacity bounds the number of edge and fence records the
	// store will hold before refusing new work.
	DefaultFenceCapacity = 1024
)

// Options configures a Store. Every field has a usable default.
type Options struct {
	Clock         func() time.Time
	PruneInterval time.Duration
	FenceCapacity int
}

// Backend supplies the queue and peer state the store does not own. The broker
// implements it; a test backend can record deliveries instead.
type Backend interface {
	// Publish enqueues an admitted ask for its target. The returned function
	// removes the entry if it has not been observed yet; it is safe to call
	// more than once.
	Publish(target string, msg messaging.InboundMessage) (remove func(), err error)
	// Receive drains inbound messages queued for sender.
	Receive(sender string, limit int) ([]messaging.InboundMessage, error)
	// Peers lists the peers visible to this store.
	Peers() ([]messaging.Peer, error)
}

// Store owns the ask lifecycle for one generation.
type Store struct {
	mu            sync.Mutex
	now           func() time.Time
	pruneInterval time.Duration
	fenceCapacity int
	generation    string
	backend       Backend

	active   map[messaging.FenceKey]*edge
	terminal map[messaging.FenceKey]*fence
	paused   map[messaging.FenceKey]*gate
	reached  map[messaging.FenceKey]bool
}

// edge is the store's own ask record. It is deliberately not a copy of the
// broker's wire DTO: it carries the wait state and the fence deadline.
type edge struct {
	key       messaging.FenceKey
	target    string
	deadline  time.Time
	done      chan struct{}
	completed bool
	outcome   messaging.Outcome
	reply     *messaging.InboundMessage
	remove    func()
}

// fence is a terminal record retained until its deadline.
type fence struct {
	deadline time.Time
	outcome  messaging.Outcome
}

// gate pauses admission for one key until it is resumed.
type gate struct {
	reached chan struct{}
	resume  chan struct{}
	once    sync.Once
}

// AdmitRequest describes one ask admission attempt.
type AdmitRequest struct {
	Sender     messaging.Identity
	Target     string
	MessageID  string
	Generation string
	Body       string
	// Legacy admits IDs without an encoded deadline, which is the local HTTP
	// router's compatibility path. The shared client always leaves it false.
	Legacy bool
}

// AdmitResult reports the admission verdict and the acceptance deadline.
type AdmitResult struct {
	Outcome    messaging.Outcome
	MessageID  string
	Generation string
	Deadline   time.Time
	Fence      *messaging.FenceAck
}

// New builds a Store with its first generation.
func New(opts Options) *Store {
	s := &Store{
		now:           opts.Clock,
		pruneInterval: opts.PruneInterval,
		fenceCapacity: opts.FenceCapacity,
		generation:    messaging.NewGeneration(),
		active:        make(map[messaging.FenceKey]*edge),
		terminal:      make(map[messaging.FenceKey]*fence),
		paused:        make(map[messaging.FenceKey]*gate),
		reached:       make(map[messaging.FenceKey]bool),
	}
	if s.now == nil {
		s.now = time.Now
	}
	if s.pruneInterval <= 0 {
		s.pruneInterval = DefaultPruneInterval
	}
	if s.fenceCapacity <= 0 {
		s.fenceCapacity = DefaultFenceCapacity
	}
	return s
}

// SetBackend installs the queue and peer source used by Send, Receive and
// Peers.
func (s *Store) SetBackend(b Backend) {
	s.mu.Lock()
	s.backend = b
	s.mu.Unlock()
}

// Generation returns the store's current generation.
func (s *Store) Generation() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.generation
}

// Admit registers an ask and publishes it while holding the admission
// boundary, so a withdrawal either fences before the edge exists or observes
// and removes the queued delivery.
func (s *Store) Admit(ctx context.Context, req AdmitRequest) (AdmitResult, error) {
	result := AdmitResult{MessageID: req.MessageID, Generation: req.Generation}
	if req.Sender.RunID == "" || req.Target == "" || req.MessageID == "" {
		result.Outcome = messaging.OutcomeNotSent
		return result, fmt.Errorf("%w: sender, target and message id are required", messaging.ErrInvalidRequest)
	}
	key := messaging.FenceKey{Sender: req.Sender.RunID, MessageID: req.MessageID, Generation: req.Generation}

	if err := s.waitAdmissionGate(ctx, key); err != nil {
		result.Outcome = messaging.OutcomeNotSent
		return result, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Generation != "" && req.Generation != s.generation {
		result.Outcome = messaging.OutcomeNotSent
		return result, messaging.ErrGenerationMismatch
	}

	now := s.now()
	deadline, err := s.deadlineLocked(now, req)
	if err != nil {
		result.Outcome = messaging.OutcomeNotSent
		return result, err
	}
	result.Deadline = deadline

	s.reclaimLocked(now)

	if f, ok := s.terminal[key]; ok {
		ack := messaging.FenceAck{
			Sender: key.Sender, MessageID: key.MessageID, Generation: key.Generation,
			Deadline: f.deadline, Terminal: true,
		}
		result.Fence = &ack
		if f.outcome == messaging.OutcomeWithdrawn {
			result.Outcome = messaging.OutcomeWithdrawn
			return result, nil
		}
		result.Outcome = f.outcome
		return result, messaging.ErrDuplicateMessageID
	}
	if _, ok := s.active[key]; ok {
		result.Outcome = messaging.OutcomeNotSent
		return result, messaging.ErrDuplicateMessageID
	}
	for _, e := range s.active {
		if e.key.Sender == req.Target && e.target == req.Sender.RunID {
			result.Outcome = messaging.OutcomeNotSent
			return result, messaging.ErrMutualAsk
		}
	}
	if len(s.active)+len(s.terminal) >= s.fenceCapacity {
		result.Outcome = messaging.OutcomeNotSent
		return result, messaging.ErrFenceCapacity
	}

	msg := messaging.InboundMessage{
		MessageID: req.MessageID,
		FromRunID: req.Sender.RunID,
		Sender:    req.Sender.Sender,
		Kind:      "agent_message",
		Payload:   json.RawMessage(req.Body),
	}
	e := &edge{
		key:      key,
		target:   req.Target,
		deadline: deadline,
		done:     make(chan struct{}),
	}
	if s.backend != nil {
		remove, err := s.backend.Publish(req.Target, msg)
		if err != nil {
			result.Outcome = messaging.OutcomeNotSent
			return result, err
		}
		e.remove = remove
	}
	s.active[key] = e
	result.Outcome = messaging.OutcomeSent
	return result, nil
}

// Wait blocks until the ask reaches a terminal state or timeout expires.
func (s *Store) Wait(ctx context.Context, key messaging.FenceKey, timeout time.Duration) (messaging.WaitResult, error) {
	if timeout <= 0 || timeout > messaging.MaxWait {
		timeout = messaging.MaxWait
	}
	s.mu.Lock()
	if key.Generation != "" && key.Generation != s.generation {
		s.mu.Unlock()
		return messaging.WaitResult{Outcome: messaging.OutcomeNotSent}, messaging.ErrGenerationMismatch
	}
	if e := s.active[key]; e != nil {
		if e.completed {
			outcome, reply := e.outcome, e.reply
			if outcome == messaging.OutcomeReplied {
				s.consumeLocked(e)
			}
			s.mu.Unlock()
			return messaging.WaitResult{Outcome: outcome, Reply: reply}, nil
		}
		s.mu.Unlock()
		return s.waitOnEdge(ctx, e, timeout)
	}
	if f, ok := s.terminal[key]; ok {
		s.mu.Unlock()
		if f.outcome == messaging.OutcomeReplied {
			// The reply was already consumed; its payload is not retained.
			return messaging.WaitResult{Outcome: messaging.OutcomeEdgeAbsent}, nil
		}
		return messaging.WaitResult{Outcome: f.outcome}, nil
	}
	s.mu.Unlock()
	return messaging.WaitResult{Outcome: messaging.OutcomeEdgeAbsent}, nil
}

func (s *Store) waitOnEdge(ctx context.Context, e *edge, timeout time.Duration) (messaging.WaitResult, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-e.done:
		s.mu.Lock()
		outcome, reply := e.outcome, e.reply
		if outcome == messaging.OutcomeReplied {
			s.consumeLocked(e)
		}
		s.mu.Unlock()
		return messaging.WaitResult{Outcome: outcome, Reply: reply}, nil
	case <-timer.C:
		s.mu.Lock()
		s.completeLocked(e, messaging.OutcomeTimedOut)
		s.mu.Unlock()
		return messaging.WaitResult{Outcome: messaging.OutcomeTimedOut}, nil
	case <-ctx.Done():
		s.mu.Lock()
		s.completeLocked(e, messaging.OutcomePossiblyPending)
		s.mu.Unlock()
		return messaging.WaitResult{Outcome: messaging.OutcomePossiblyPending}, ctx.Err()
	}
}

// Reply routes a reply to the waiting sender and reports whether a live edge
// accepted it.
func (s *Store) Reply(key messaging.FenceKey, fromRunID string, payload json.RawMessage) (messaging.Outcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.findActiveLocked(key.Sender, key.MessageID)
	if e == nil {
		return messaging.OutcomeEdgeAbsent, nil
	}
	if e.target != fromRunID {
		return messaging.OutcomeEdgeAbsent, messaging.ErrNotAskTarget
	}
	if e.completed {
		return e.outcome, nil
	}
	reply := &messaging.InboundMessage{
		MessageID: e.key.MessageID,
		FromRunID: fromRunID,
		Sender:    fromRunID,
		Kind:      "agent_message",
		Payload:   payload,
	}
	e.reply = reply
	s.completeLocked(e, messaging.OutcomeReplied)
	return messaging.OutcomeReplied, nil
}

// Withdraw installs a terminal fence for the key. A withdrawal with no live
// edge still fences the ID; it reports edge-absent because nothing was pending.
func (s *Store) Withdraw(key messaging.FenceKey) (messaging.WithdrawResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if key.Generation != "" && key.Generation != s.generation {
		return messaging.WithdrawResult{Outcome: messaging.OutcomeNotSent}, messaging.ErrGenerationMismatch
	}
	now := s.now()
	s.reclaimLocked(now)
	if e := s.active[key]; e != nil {
		if e.completed {
			// A reply already won; withdrawal cannot undo it, but the edge is
			// consumed and its terminal outcome is reported.
			outcome := e.outcome
			if outcome == messaging.OutcomeReplied {
				s.consumeLocked(e)
			}
			return messaging.WithdrawResult{Outcome: outcome, Fence: s.ackLocked(key, e.deadline)}, nil
		}
		s.completeLocked(e, messaging.OutcomeWithdrawn)
		return messaging.WithdrawResult{Outcome: messaging.OutcomeWithdrawn, Fence: s.ackLocked(key, e.deadline)}, nil
	}
	if f, ok := s.terminal[key]; ok {
		return messaging.WithdrawResult{Outcome: f.outcome, Fence: s.ackLocked(key, f.deadline)}, nil
	}
	deadline, err := messaging.ParseMsgID(key.MessageID, now)
	if err != nil {
		deadline = now.Add(DefaultAskTimeout)
	}
	s.terminal[key] = &fence{deadline: deadline, outcome: messaging.OutcomeWithdrawn}
	return messaging.WithdrawResult{Outcome: messaging.OutcomeEdgeAbsent, Fence: s.ackLocked(key, deadline)}, nil
}

// RemoveRun terminates every edge the run owns or targets. A waiter sees
// OutcomeLost because no delivery can reach it afterwards.
func (s *Store) RemoveRun(runID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.active {
		if e.key.Sender == runID || e.target == runID {
			s.completeLocked(e, messaging.OutcomeLost)
		}
	}
}

// Reset terminates all waits, clears ask state and rotates the generation.
func (s *Store) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.active {
		s.completeLocked(e, messaging.OutcomeLost)
	}
	s.active = make(map[messaging.FenceKey]*edge)
	s.terminal = make(map[messaging.FenceKey]*fence)
	s.paused = make(map[messaging.FenceKey]*gate)
	s.reached = make(map[messaging.FenceKey]bool)
	s.generation = messaging.NewGeneration()
}

// Receive drains inbound messages for sender through the installed backend.
func (s *Store) Receive(sender string, limit int) ([]messaging.InboundMessage, error) {
	s.mu.Lock()
	backend := s.backend
	s.mu.Unlock()
	if backend == nil {
		return nil, nil
	}
	return backend.Receive(sender, limit)
}

// Peers lists visible peers through the installed backend.
func (s *Store) Peers() ([]messaging.Peer, error) {
	s.mu.Lock()
	backend := s.backend
	s.mu.Unlock()
	if backend == nil {
		return nil, nil
	}
	return backend.Peers()
}

// Pending reports whether a live edge exists for the sender and message ID.
func (s *Store) Pending(sender, messageID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.findActiveLocked(sender, messageID) != nil
}

// PendingCount reports the number of live edges owned by sender.
func (s *Store) PendingCount(sender string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, e := range s.active {
		if e.key.Sender == sender {
			n++
		}
	}
	return n
}

// ActiveCount reports the number of live edges across all senders.
func (s *Store) ActiveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.active)
}

// Fence returns the recorded terminal fence for a key, if any.
func (s *Store) Fence(key messaging.FenceKey) (messaging.FenceAck, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, ok := s.terminal[key]
	if !ok {
		return messaging.FenceAck{}, false
	}
	return s.ackLocked(key, f.deadline), true
}

// PauseAdmission blocks the next admission for key until ResumeAdmission is
// called. It is the shared test seam used by the paused-send fence regressions.
func (s *Store) PauseAdmission(key messaging.FenceKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.paused[key]; ok {
		return
	}
	s.paused[key] = &gate{reached: make(chan struct{}), resume: make(chan struct{})}
}

// ResumeAdmission releases a paused admission for key.
func (s *Store) ResumeAdmission(key messaging.FenceKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.paused[key]
	if !ok {
		return
	}
	close(g.resume)
	delete(s.paused, key)
}

// AdmissionReached reports whether an admission for key waited at the pause
// seam.
func (s *Store) AdmissionReached(key messaging.FenceKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reached[key]
}

func (s *Store) waitAdmissionGate(ctx context.Context, key messaging.FenceKey) error {
	s.mu.Lock()
	g := s.paused[key]
	var resume chan struct{}
	if g != nil {
		g.once.Do(func() {
			close(g.reached)
			s.reached[key] = true
		})
		resume = g.resume
	}
	s.mu.Unlock()
	if resume == nil {
		return nil
	}
	select {
	case <-resume:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Store) deadlineLocked(now time.Time, req AdmitRequest) (time.Time, error) {
	deadline, err := messaging.ParseMsgID(req.MessageID, now)
	if err == nil {
		return deadline, nil
	}
	if req.Legacy && errors.Is(err, messaging.ErrMalformedMessageID) {
		return now.Add(DefaultAskTimeout), nil
	}
	return time.Time{}, err
}

// completeLocked terminally completes an edge exactly once. A replied edge
// stays active until its waiter consumes the reply so a late wait can still
// read it.
func (s *Store) completeLocked(e *edge, outcome messaging.Outcome) {
	if e.completed {
		return
	}
	e.completed = true
	e.outcome = outcome
	close(e.done)
	if outcome == messaging.OutcomeReplied {
		return
	}
	delete(s.active, e.key)
	s.terminal[e.key] = &fence{deadline: e.deadline, outcome: outcome}
	if e.remove != nil {
		e.remove()
		e.remove = nil
	}
}

// consumeLocked removes a replied edge and records its terminal fence.
func (s *Store) consumeLocked(e *edge) {
	delete(s.active, e.key)
	s.terminal[e.key] = &fence{deadline: e.deadline, outcome: messaging.OutcomeReplied}
}

func (s *Store) findActiveLocked(sender, messageID string) *edge {
	for key, e := range s.active {
		if key.Sender == sender && key.MessageID == messageID {
			return e
		}
	}
	return nil
}

func (s *Store) reclaimLocked(now time.Time) {
	for key, f := range s.terminal {
		if !now.Before(f.deadline) {
			delete(s.terminal, key)
		}
	}
}

func (s *Store) ackLocked(key messaging.FenceKey, deadline time.Time) messaging.FenceAck {
	return messaging.FenceAck{
		Sender: key.Sender, MessageID: key.MessageID, Generation: key.Generation,
		Deadline: deadline, Terminal: true,
	}
}
