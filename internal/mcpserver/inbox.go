package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sdougbrown/avenor/internal/appdir"
	"github.com/sdougbrown/avenor/internal/askclaim"
	"github.com/sdougbrown/avenor/internal/feed"
	"github.com/sdougbrown/avenor/internal/messaging"
)

const (
	// InboxResourceURI is the resource a bound conversation's broker inbox is
	// read from and subscribed to.
	InboxResourceURI = "avenor://inbox"
	// InboxMIMEType is what one inbox read carries: one JSON object holding the
	// messages that were queued when the read arrived.
	InboxMIMEType = "application/json"
	// maxInboxMessages and maxInboxBytes bound what the queue holds before the
	// worker pauses. A client that stops reading costs this much memory and then
	// stops the poll: backpressure reaches the broker instead of growing a queue
	// the broker cannot see.
	maxInboxMessages = 64
	maxInboxBytes    = 4 << 20
	// inboxNotifyTimeout bounds one resource-updated wake. The queue is the
	// source of truth for what has arrived, so a wake that cannot be delivered
	// inside this window is dropped rather than retried.
	inboxNotifyTimeout = 5 * time.Second
	// inboxModernProtocol is the first protocol version that subscribes with
	// subscriptions/listen instead of resources/subscribe. go-sdk keeps the same
	// threshold unexported and picks the notification path by it, so the inbox
	// classifies a session exactly the way the SDK does.
	inboxModernProtocol = "2026-07-28"
)

// inboxMessage is one queued broker message as a read reports it. ExpectsReply
// is the broker's ask bit and Answerable is the consumer's verdict on whether a
// reply to the ask can be routed, so the two are reported separately: an ask
// that arrived without a stamped sender expects a reply this run cannot send.
type inboxMessage struct {
	ID           string `json:"id"`
	From         string `json:"from"`
	Text         string `json:"text"`
	ExpectsReply bool   `json:"expects_reply"`
	Answerable   bool   `json:"answerable"`
}

// inboxPayload is the one JSON object a read returns. Messages is never null:
// an empty inbox reports an empty array, so a caller indexes it without a null
// check.
type inboxPayload struct {
	Messages []inboxMessage `json:"messages"`
}

// inboxState is the inbox worker's lifecycle. A worker exists only between the
// subscribe that found the inbox idle and the stop that ended it, and its exit
// is observed in one place, reconcileWorkerExit.
type inboxState int

const (
	// inboxIdle is an inbox with no worker: nothing is being polled, and the
	// queue holds whatever the last worker delivered.
	inboxIdle inboxState = iota
	// inboxRunning is an inbox whose worker is polling the broker.
	inboxRunning
	// inboxStopping is an inbox whose worker was told to stop and whose exit has
	// not landed yet. Its done handle is still open, so a subscribe arriving now
	// waits for the exit instead of contending for the consumer lock.
	inboxStopping
	// inboxClosed is an inbox whose server is closing for good. No worker may
	// start again.
	inboxClosed
)

// mcpInbox is a bound conversation's inbox: the queue a read drains, the
// sessions that asked to be woken when it grows, and the one worker that polls
// the broker for them.
//
// Every field below is written under mu, and the worker's exit is the only
// transition the worker itself performs. The worker is exclusive against every
// other consumer of the same run by the feed's advisory lock, which it holds
// from before it is published to until its poll loop has returned.
type mcpInbox struct {
	mu    sync.Mutex
	state inboxState
	// closing records a permanent stop, so a worker that exits during it settles
	// in inboxClosed instead of offering a restart the closing server would
	// refuse.
	closing bool
	// legacySubscribers are the sessions subscribed with resources/subscribe. A
	// repeat subscribe from one of them is idempotent, as that protocol expects.
	legacySubscribers map[*mcp.ServerSession]struct{}
	// modernSubscribers are the sessions listening with subscriptions/listen.
	// go-sdk keys its subscriptions by (uri, session), so one session holds at
	// most one listen per resource: a second is refused before the SDK can
	// overwrite the first.
	modernSubscribers map[*mcp.ServerSession]struct{}
	// queue holds messages the worker delivered and no read has drained yet.
	queue []inboxMessage
	// queueBytes is the text the queue holds, which is the memory backpressure
	// bounds; ids and senders are bookkeeping beside it.
	queueBytes int
	// drainCh is closed and replaced whenever the queue drains or the worker
	// stops, which wakes the BeforePoll that paused for room.
	drainCh chan struct{}
	// cancel ends the worker's poll context, and is nil when no worker is
	// stopping.
	cancel context.CancelFunc
	// done closes after the worker returned from its poll loop, released the
	// consumer lock, and settled the state. It stays set through inboxRunning and
	// inboxStopping so concurrent stops join the same completion.
	done chan struct{}
}

// newMCPInbox returns an inbox with no worker, no queue and no subscribers.
func newMCPInbox() *mcpInbox {
	return &mcpInbox{
		legacySubscribers: make(map[*mcp.ServerSession]struct{}),
		modernSubscribers: make(map[*mcp.ServerSession]struct{}),
		drainCh:           make(chan struct{}),
	}
}

// isModernSession reports whether a session subscribes with
// subscriptions/listen rather than resources/subscribe. The protocol version is
// fixed when the session initializes, and go-sdk reads the same field to choose
// how it delivers resource-updated notifications.
func isModernSession(sess *mcp.ServerSession) bool {
	if sess == nil {
		return false
	}
	params := sess.InitializeParams()
	return params != nil && params.ProtocolVersion >= inboxModernProtocol
}

// handleReadInbox drains the queue in arrival order and reports the messages it
// took. A read is the only way a queued message leaves the inbox, so a client
// that reads twice receives each message once.
func (s *Server) handleReadInbox(_ context.Context, _ *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	if s.inbox == nil {
		return nil, mcp.ResourceNotFoundError(InboxResourceURI)
	}
	encoded, err := json.Marshal(inboxPayload{Messages: s.inbox.drain()})
	if err != nil {
		return nil, fmt.Errorf("avenor://inbox: encode the drained messages: %w", err)
	}
	return &mcp.ReadResourceResult{
		Contents: []*mcp.ResourceContents{{
			URI:      InboxResourceURI,
			MIMEType: InboxMIMEType,
			Text:     string(encoded),
		}},
		// A drained queue is never cacheable, so a cached read cannot hide the
		// next message. The scope is stated rather than left empty because the
		// SDK materializes an absent scope as "public", which would let an
		// intermediary hold one conversation's messages.
		Cacheable: mcp.Cacheable{TTLMs: 0, CacheScope: "private"},
	}, nil
}

// handleResourceSubscribe starts the inbox worker for the session that asked,
// or joins the worker already running. The consumer lock is taken before the
// worker is published, so two subscribes cannot both believe they started it:
// the second sees the first's worker and joins it.
func (s *Server) handleResourceSubscribe(ctx context.Context, req *mcp.SubscribeRequest) error {
	uri := ""
	if req != nil && req.Params != nil {
		uri = req.Params.URI
	}
	if uri != InboxResourceURI {
		return mcp.ResourceNotFoundError(uri)
	}
	if s.inbox == nil {
		return errors.New("avenor://inbox: this server serves no bound conversation")
	}
	var bound boundIdentity
	boundOK := false
	if s.hostBind != nil {
		bound, boundOK = s.hostBind.boundState()
	}
	if !boundOK {
		return errors.New("avenor://inbox: the conversation is not bound")
	}
	// The lock key is derived from the bound run's coordinates, and the bind
	// already resolved them, so coordinates that cannot be keyed have no inbox
	// to serve. The refusal names that, never the run token.
	if _, err := askclaim.BrokerKey(bound.result.BrokerURL, bound.result.RunID); err != nil {
		return errors.New("avenor://inbox: the bound run's broker coordinates cannot be keyed")
	}
	lockDir := feed.DefaultLockDir(appdir.StateHome())
	inbox := s.inbox
	// The subscription style is fixed at initialization, so it is decided once,
	// outside the inbox lock.
	modern := isModernSession(req.Session)
	for {
		inbox.mu.Lock()
		if inbox.closing || inbox.state == inboxClosed {
			inbox.mu.Unlock()
			return errors.New("avenor://inbox: server is closed")
		}
		if inbox.state == inboxStopping {
			// The worker is retiring for the subscribers that left. Wait for that
			// exit and recheck rather than contending with it over the consumer
			// lock this inbox's own retiring worker still holds.
			waitDone := inbox.done
			inbox.mu.Unlock()
			select {
			case <-waitDone:
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		subscribers := inbox.legacySubscribers
		if modern {
			subscribers = inbox.modernSubscribers
		}
		if _, exists := subscribers[req.Session]; exists {
			inbox.mu.Unlock()
			if modern {
				return errors.New("avenor://inbox: overlapping subscription listen on the same session is not supported")
			}
			return nil
		}
		if inbox.state == inboxRunning {
			subscribers[req.Session] = struct{}{}
			inbox.mu.Unlock()
			return nil
		}
		unlock, held, err := feed.AcquireLock(lockDir, bound.result.BrokerURL, bound.result.RunID)
		if err != nil {
			inbox.mu.Unlock()
			return fmt.Errorf("avenor://inbox: take the inbox consumer lock: %w", err)
		}
		if held {
			inbox.mu.Unlock()
			return errors.New("avenor://inbox: the inbox consumer lock is held by another consumer, such as avenor feed broker")
		}
		// The poll context is the worker's alone: it is not the subscribe
		// request's context, because a resources/subscribe call returns while the
		// worker keeps polling, and a subscriptions/listen call is what keeps the
		// client waiting.
		pollCtx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		consumer := s.inboxConsumerConfig(pollCtx, bound)
		inbox.state = inboxRunning
		inbox.cancel = cancel
		inbox.done = done
		subscribers[req.Session] = struct{}{}
		go func() {
			// done closes last: strictly after feed.Consume and its claim pruner
			// have returned, after the consumer lock is released, and after the
			// state machine settled, so a stop that waits on done joins an inbox
			// nothing else is still reading.
			defer close(done)
			defer func() {
				_ = unlock()
				inbox.reconcileWorkerExit(done)
			}()
			_ = feed.Consume(pollCtx, consumer)
		}()
		inbox.mu.Unlock()
		return nil
	}
}

// inboxConsumerConfig is what the worker runs: the bound run's own broker
// credentials, the queue's backpressure hook, and the wake a delivered batch
// triggers.
//
// The delivery boundary is explicit. Every decoded batch is queued before the
// worker exits, so a poll that already drained the broker is never dropped on
// the floor. CancelInFlight abandons only a request still in flight: a client
// that disconnects or unsubscribes must not wait out an idle 15s long-poll, so
// the poll context reaches the wire and the worker retires promptly. The same
// context bounds the wake, whose failure is expected for a subscriber that
// already left.
//
// Diagnostics go to the server's stderr because a terminal broker refusal is
// the one failure a subscribe cannot report: the worker started and the
// refusal arrives later, so the line is the only place an operator learns the
// inbox stopped. It names the status and never the run token.
func (s *Server) inboxConsumerConfig(pollCtx context.Context, bound boundIdentity) feed.ConsumerConfig {
	inbox := s.inbox
	return feed.ConsumerConfig{
		Surface:        messaging.SurfaceMCP,
		RunID:          bound.result.RunID,
		Token:          bound.result.RunToken,
		BrokerURL:      bound.result.BrokerURL,
		AsksDir:        bound.asksDir,
		Log:            stderrOr(s.opts.Stderr),
		LogPrefix:      "mcp inbox",
		CancelInFlight: true,
		BeforePoll:     inbox.waitForDrain,
		OnRow:          inbox.record,
		OnBatchDone: func() {
			s.notifyInboxUpdated(pollCtx)
		},
	}
}

// notifyInboxUpdated wakes every subscriber to the inbox. A wake that cannot be
// delivered is dropped: the subscriber is gone or slow, the queue still holds
// the messages, and its next read reports them.
func (s *Server) notifyInboxUpdated(pollCtx context.Context) {
	notifyCtx, cancel := context.WithTimeout(pollCtx, inboxNotifyTimeout)
	defer cancel()
	_ = s.mcpServer.ResourceUpdated(notifyCtx, &mcp.ResourceUpdatedNotificationParams{URI: InboxResourceURI})
}

// handleResourceUnsubscribe forgets a session and stops the worker when it was
// the last subscriber. It never consults ctx: go-sdk defers this call until a
// subscriptions/listen context is already done, so an early return would leave
// the worker polling for a client that left.
func (s *Server) handleResourceUnsubscribe(_ context.Context, req *mcp.UnsubscribeRequest) error {
	if s.inbox == nil {
		return nil
	}
	uri := ""
	if req != nil && req.Params != nil {
		uri = req.Params.URI
	}
	if uri != InboxResourceURI {
		return nil
	}
	cancel, done := s.inbox.forgetSubscriber(req.Session)
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	return nil
}

// stopInbox ends the inbox worker and waits for it to release the consumer lock,
// so a closed server leaves no poller, pruner or flock behind. A permanent stop
// also forbids a later subscribe from starting a new worker.
func (s *Server) stopInbox(permanent bool) {
	inbox := s.inbox
	if inbox == nil {
		return
	}
	inbox.mu.Lock()
	if permanent {
		inbox.closing = true
	}
	inbox.legacySubscribers = make(map[*mcp.ServerSession]struct{})
	inbox.modernSubscribers = make(map[*mcp.ServerSession]struct{})
	inbox.wakeDrainersLocked()
	switch inbox.state {
	case inboxRunning:
		inbox.state = inboxStopping
		cancel, done := inbox.cancel, inbox.done
		inbox.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		if done != nil {
			<-done
		}
	case inboxStopping:
		// Another stop is already waiting for the same exit; joining it is
		// enough. A worker that exits during a permanent stop settles in
		// inboxClosed because closing is set.
		done := inbox.done
		inbox.mu.Unlock()
		if done != nil {
			<-done
		}
	default:
		if permanent {
			inbox.state = inboxClosed
		}
		inbox.mu.Unlock()
	}
}

// drain hands over everything queued and releases a worker paused for room.
func (i *mcpInbox) drain() []inboxMessage {
	i.mu.Lock()
	items := i.queue
	i.queue = nil
	i.queueBytes = 0
	i.wakeDrainersLocked()
	i.mu.Unlock()
	if items == nil {
		return []inboxMessage{}
	}
	return items
}

// record queues one delivered message for the next read. Every decoded row is
// accepted, including one that arrives while the worker is stopping: the broker
// has already drained it, so dropping it here would lose the only copy.
func (i *mcpInbox) record(m feed.Message) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.queue = append(i.queue, inboxMessage{
		ID:           m.ID,
		From:         m.From,
		Text:         m.Text,
		ExpectsReply: m.ExpectsReply,
		Answerable:   m.Answerable,
	})
	i.queueBytes += len(m.ID) + len(m.From) + len(m.Text)
	return nil
}

// waitForDrain is the worker's BeforePoll hook: it returns once the queue has
// room, or with the poll context's error when the worker is stopping.
func (i *mcpInbox) waitForDrain(ctx context.Context) error {
	i.mu.Lock()
	for len(i.queue) >= maxInboxMessages || i.queueBytes >= maxInboxBytes {
		drained := i.drainCh
		i.mu.Unlock()
		select {
		case <-drained:
		case <-ctx.Done():
			return ctx.Err()
		}
		i.mu.Lock()
	}
	i.mu.Unlock()
	return nil
}

// forgetSubscriber removes a session from both subscription styles and, when it
// was the last subscriber, begins the worker's stop. The handles it returns
// belong to the retiring worker and the caller uses them outside the lock, so
// cancelling and waiting never blocks a handler that is only joining the stop.
func (i *mcpInbox) forgetSubscriber(sess *mcp.ServerSession) (context.CancelFunc, chan struct{}) {
	i.mu.Lock()
	defer i.mu.Unlock()
	delete(i.legacySubscribers, sess)
	delete(i.modernSubscribers, sess)
	if len(i.legacySubscribers)+len(i.modernSubscribers) > 0 || i.state != inboxRunning {
		return nil, nil
	}
	i.wakeDrainersLocked()
	i.state = inboxStopping
	return i.cancel, i.done
}

// reconcileWorkerExit settles the state after the worker's poll loop returned.
// The worker ends when its context is cancelled, and also when the broker refuses
// its credentials or protocol outright; the second case leaves subscribers the
// SDK still believes in, so the maps are cleared and the next subscribe starts a
// clean worker instead of joining one that already ended.
func (i *mcpInbox) reconcileWorkerExit(done chan struct{}) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.done != done {
		return
	}
	i.cancel = nil
	i.done = nil
	if i.closing {
		i.state = inboxClosed
		return
	}
	i.state = inboxIdle
	i.legacySubscribers = make(map[*mcp.ServerSession]struct{})
	i.modernSubscribers = make(map[*mcp.ServerSession]struct{})
}

// wakeDrainersLocked releases a paused BeforePoll and arms a fresh channel for
// the next pause. Callers hold mu.
func (i *mcpInbox) wakeDrainersLocked() {
	close(i.drainCh)
	i.drainCh = make(chan struct{})
}
