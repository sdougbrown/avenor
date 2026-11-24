package acp

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/sdougbrown/avenor/internal/events"
)

// eventhub.go holds the ACP provider's session event fan-out machinery: the
// per-session subscriber pump, the wildcard/session backlog, and the ordered
// broadcast surface the client pump feeds. Everything here is internal to the
// package; Provider owns the hub's lifecycle.

const (
	hubBacklogCap       = 256
	subscriberBufferCap = 128
)

type orderedEvent struct {
	event events.Event
	ack   chan struct{}
}

type eventSubscriber struct {
	sessionID     string
	ch            chan events.Event
	done          chan struct{}
	doneOnce      sync.Once
	mu            sync.Mutex
	queue         []events.Event
	eof           bool
	closed        bool
	stopAfterFunc func() bool
	wake          chan struct{}
}

func (s *eventSubscriber) matches(ev events.Event) bool {
	return s.sessionID == "" || ev.SessionID == "" || ev.SessionID == s.sessionID
}

func (s *eventSubscriber) closeDone() {
	s.doneOnce.Do(func() {
		close(s.done)
	})
}

func (s *eventSubscriber) signalWake() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *eventSubscriber) enqueue(ev events.Event) {
	s.mu.Lock()
	if s.closed || s.eof {
		s.mu.Unlock()
		return
	}
	s.queue = append(s.queue, ev)
	s.mu.Unlock()
	s.signalWake()
}

func (s *eventSubscriber) markEOF() {
	s.mu.Lock()
	stop := s.stopAfterFunc
	s.stopAfterFunc = nil
	s.eof = true
	s.mu.Unlock()
	if stop != nil {
		stop()
	}
	s.signalWake()
}

func (s *eventSubscriber) abort() {
	s.closeDone()
	s.mu.Lock()
	stop := s.stopAfterFunc
	s.stopAfterFunc = nil
	s.closed = true
	s.queue = nil
	s.mu.Unlock()
	if stop != nil {
		stop()
	}
	s.signalWake()
}

func (s *eventSubscriber) run(hubStop <-chan struct{}) {
	defer close(s.ch)
	for {
		select {
		case <-s.done:
			return
		case <-hubStop:
			return
		default:
		}

		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		if len(s.queue) > 0 {
			ev := s.queue[0]
			s.queue[0] = events.Event{}
			s.queue = s.queue[1:]
			if len(s.queue) == 0 {
				s.queue = nil
			}
			s.mu.Unlock()

			select {
			case s.ch <- ev:
			case <-s.done:
				return
			case <-hubStop:
				return
			}
			continue
		}
		if s.eof {
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()

		select {
		case <-s.wake:
		case <-s.done:
			return
		case <-hubStop:
			return
		}
	}
}

func isDroppableChunk(eventName string) bool {
	return eventName == "agent.message_chunk" || eventName == "agent.thought_chunk"
}

type eventHub struct {
	mu              sync.Mutex
	closed          bool
	wildcardSubSeen bool
	sessionSubSeen  map[string]bool
	backlog         []events.Event
	subs            []*eventSubscriber

	ordered   chan orderedEvent
	asyncWake chan struct{}
	asyncOps  atomic.Int32

	stop       chan struct{}
	stopOnce   sync.Once
	closedCh   chan struct{}
	closedOnce sync.Once
	pumpDone   chan struct{}
}

func newEventHub() *eventHub {
	return &eventHub{
		sessionSubSeen: map[string]bool{},
		ordered:        make(chan orderedEvent, 64),
		asyncWake:      make(chan struct{}, 1),
		stop:           make(chan struct{}),
		closedCh:       make(chan struct{}),
		pumpDone:       make(chan struct{}),
	}
}

func (h *eventHub) beginAsyncOp() {
	h.asyncOps.Add(1)
}

func (h *eventHub) endAsyncOp() {
	if h.asyncOps.Add(-1) <= 0 {
		select {
		case h.asyncWake <- struct{}{}:
		default:
		}
	}
}

func (h *eventHub) asyncOpsCount() int32 {
	return h.asyncOps.Load()
}

func (h *eventHub) requestStop() {
	h.closedOnce.Do(func() {
		close(h.closedCh)
	})
	h.stopOnce.Do(func() {
		close(h.stop)
	})
}

func (h *eventHub) finishPump(abort bool) {
	h.closeAll(abort)
	close(h.pumpDone)
}

func (h *eventHub) closeAll(abort bool) {
	h.closedOnce.Do(func() {
		close(h.closedCh)
	})

	h.mu.Lock()
	h.closed = true
	subs := h.subs
	h.subs = nil
	h.backlog = nil
	h.mu.Unlock()

	for _, sub := range subs {
		if abort {
			sub.abort()
		} else {
			sub.markEOF()
		}
	}
}

func (h *eventHub) subscribe(ctx context.Context, sessionID string) (<-chan events.Event, error) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		ch := make(chan events.Event)
		close(ch)
		return ch, nil
	}

	var backlog []events.Event
	if sessionID == "" {
		if !h.wildcardSubSeen {
			h.wildcardSubSeen = true
			backlog = h.backlog
			h.backlog = nil
			for _, ev := range backlog {
				if ev.SessionID != "" {
					h.sessionSubSeen[ev.SessionID] = true
				}
			}
		}
	} else if !h.sessionSubSeen[sessionID] {
		h.sessionSubSeen[sessionID] = true
		remaining := h.backlog[:0]
		for _, ev := range h.backlog {
			if ev.SessionID == "" || ev.SessionID == sessionID {
				backlog = append(backlog, ev)
			} else {
				remaining = append(remaining, ev)
			}
		}
		if len(remaining) == 0 {
			h.backlog = nil
		} else {
			clear(h.backlog[len(remaining):])
			h.backlog = remaining
		}
	}

	sub := &eventSubscriber{
		sessionID: sessionID,
		ch:        make(chan events.Event, subscriberBufferCap),
		done:      make(chan struct{}),
		wake:      make(chan struct{}, 1),
	}
	for _, ev := range backlog {
		if !sub.matches(ev) {
			continue
		}
		cloned := cloneEvent(ev)
		select {
		case sub.ch <- cloned:
		default:
			sub.queue = append(sub.queue, cloned)
		}
	}
	// Hold sub.mu before publishing sub into h.subs so concurrent broadcast,
	// closeAll, or an already-cancelled ctx callback cannot race with
	// stopAfterFunc registration or backlog ordering.
	sub.mu.Lock()
	h.subs = append(h.subs, sub)
	h.mu.Unlock()

	sub.stopAfterFunc = context.AfterFunc(ctx, func() {
		h.unsubscribe(sub)
	})
	sub.mu.Unlock()

	go sub.run(h.stop)
	return sub.ch, nil
}

func (h *eventHub) unsubscribe(sub *eventSubscriber) {
	sub.closeDone()

	h.mu.Lock()
	for i, candidate := range h.subs {
		if candidate == sub {
			h.subs = slices.Delete(h.subs, i, i+1)
			break
		}
	}
	h.mu.Unlock()

	sub.abort()
}

func (h *eventHub) appendBacklogLocked(ev events.Event) {
	cloned := cloneEvent(ev)
	if len(h.backlog) < hubBacklogCap {
		h.backlog = append(h.backlog, cloned)
		return
	}
	if isDroppableChunk(ev.Event) {
		return
	}
	// Preserve non-droppable lifecycle/permission events by evicting the oldest
	// droppable chunk (preferring thought chunks first) when at capacity. When
	// every retained event is non-droppable the cap can only hold by giving up
	// the oldest entry: the backlog is the window a late subscriber replays, and
	// the event just recorded is the more useful end of that window than the
	// stalest one, so keep the newest and drop it.
	evictIdx := -1
	for i, existing := range h.backlog {
		if existing.Event == "agent.thought_chunk" {
			evictIdx = i
			break
		}
		if evictIdx == -1 && isDroppableChunk(existing.Event) {
			evictIdx = i
		}
	}
	if evictIdx < 0 {
		evictIdx = 0
	}
	h.backlog = slices.Delete(h.backlog, evictIdx, evictIdx+1)
	h.backlog = append(h.backlog, cloned)
}

func (h *eventHub) broadcast(ev events.Event) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	var subs []*eventSubscriber
	for _, sub := range h.subs {
		if sub.matches(ev) {
			subs = append(subs, sub)
		}
	}
	shouldBacklog := false
	if ev.SessionID != "" {
		shouldBacklog = !h.sessionSubSeen[ev.SessionID] && !h.wildcardSubSeen
	} else {
		shouldBacklog = !h.wildcardSubSeen && len(h.sessionSubSeen) == 0
	}
	if shouldBacklog {
		h.appendBacklogLocked(ev)
	}
	h.mu.Unlock()

	for _, sub := range subs {
		sub.enqueue(cloneEvent(ev))
	}
}
