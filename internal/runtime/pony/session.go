package pony

import (
	"context"
	"sync"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/pony/model"
)

// sessionState holds the per-session data for a single conversation.
type sessionState struct {
	mu sync.Mutex

	history    []model.Message
	cancel     context.CancelFunc // cancels the in-flight Prompt call
	subs       []chan events.Event
	subsMu     sync.Mutex
	initialised  bool // true after Start has set up history
	droppedEvents int // count of events dropped due to full subscriber channels
	runtimeID     string // supervisor-assigned runtime ID (rt_N)

	// pendingPerm is set when a tool call is waiting for permission approval.
	pendingPerm struct {
		requestID string
		respond   chan<- runtime.PermissionResponse
	}
}

func newSessionState() *sessionState {
	return &sessionState{
		history: make([]model.Message, 0),
		subs:    make([]chan events.Event, 0),
	}
}

// addSubscriber registers a channel to receive events for this session.
// The channel should be buffered (at least 128).
func (s *sessionState) addSubscriber(ch chan events.Event) {
	s.subsMu.Lock()
	s.subs = append(s.subs, ch)
	s.subsMu.Unlock()
}

// removeSubscriber removes a channel from the subscriber list.
func (s *sessionState) removeSubscriber(ch chan events.Event) {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	for i, sub := range s.subs {
		if sub == ch {
			s.subs = append(s.subs[:i], s.subs[i+1:]...)
			return
		}
	}
}

// emit sends an event to all subscribers non-blocking.
func (s *sessionState) emit(evt events.Event) {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	for _, sub := range s.subs {
		select {
		case sub <- evt:
		default:
		}
	}
}
