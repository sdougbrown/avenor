// Package inprocess implements the messaging Transport over an askstore.Store.
// It performs no HTTP and reads no credentials: callers supply already
// authenticated identities and the generation they pinned.
package inprocess

import (
	"context"
	"sync"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/askstore"
)

// Transport is the in-process messaging transport.
type Transport struct {
	store *askstore.Store
	// resolve names the seam the transport ask, reply, receive, peer and
	// preflight operations delegate to. It is nil when the transport serves
	// them from the store alone.
	resolve  func() messaging.Seam
	ctx      context.Context
	cancel   context.CancelFunc
	inflight sync.WaitGroup
	mu       sync.Mutex
	closed   bool
}

// New builds a transport that serves messaging from store alone.
func New(store *askstore.Store) messaging.Transport {
	return newTransport(store, nil)
}

// NewRouted builds a transport over store whose ask, reply, receive, peer and
// preflight operations delegate to the seam resolve returns. Resolution
// happens per call, so a transport built before its host installs a seam still
// reaches it.
func NewRouted(store *askstore.Store, resolve func() messaging.Seam) messaging.Transport {
	return newTransport(store, resolve)
}

func newTransport(store *askstore.Store, resolve func() messaging.Seam) messaging.Transport {
	ctx, cancel := context.WithCancel(context.Background())
	return &Transport{store: store, resolve: resolve, ctx: ctx, cancel: cancel}
}

// routed returns the installed seam, or nil when this transport serves
// messaging from the store alone.
func (t *Transport) routed() messaging.Seam {
	if t.resolve == nil {
		return nil
	}
	return t.resolve()
}

func (t *Transport) Send(ctx context.Context, req messaging.SendRequest) (messaging.SendResult, error) {
	if err := t.open(req.Generation); err != nil {
		return messaging.SendResult{Outcome: messaging.OutcomeNotSent, MessageID: req.MessageID, Generation: req.Generation}, err
	}
	child, cancel, ok := t.child(ctx)
	if !ok {
		return messaging.SendResult{Outcome: messaging.OutcomeNotSent, MessageID: req.MessageID, Generation: req.Generation}, messaging.ErrTransportClosed
	}
	defer cancel()
	if m := t.routed(); m != nil {
		res, err := m.Send(child, req)
		return sendResult(res, err), err
	}
	res, err := t.store.Admit(child, askstore.AdmitRequest{
		Sender:     req.From,
		Target:     req.To.RunID,
		MessageID:  req.MessageID,
		Generation: req.Generation,
		Body:       req.Body,
		NoReply:    req.NoReply,
		Legacy:     req.Legacy,
	})
	out := sendResult(messaging.SendResult{Outcome: res.Outcome, MessageID: res.MessageID, Generation: res.Generation}, err)
	return out, err
}

// sendResult keeps the verdict a send already reached. Only a fence outcome a
// send cannot carry is re-reported as not-sent.
func sendResult(res messaging.SendResult, err error) messaging.SendResult {
	if err != nil && !messaging.IsSendOutcome(res.Outcome) {
		res.Outcome = messaging.OutcomeNotSent
	}
	return res
}

func (t *Transport) Wait(ctx context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	if err := t.open(req.Generation); err != nil {
		return messaging.WaitResult{Outcome: messaging.OutcomeEdgeAbsent}, err
	}
	child, cancel, ok := t.child(ctx)
	if !ok {
		return messaging.WaitResult{Outcome: messaging.OutcomePossiblyPending}, nil
	}
	defer cancel()
	if m := t.routed(); m != nil {
		res, err := m.Wait(child, req)
		return waitResult(res, err, t.isClosed())
	}
	res, err := t.store.Wait(child, t.key(req.Sender, req.MessageID, req.Generation), req.Timeout)
	return waitResult(res, err, t.isClosed())
}

// waitResult keeps the terminal result the store already returned. Only a wait
// without a terminal outcome is re-reported as possibly-pending when the
// transport closed while it was in flight.
func waitResult(res messaging.WaitResult, err error, closed bool) (messaging.WaitResult, error) {
	if closed && res.Outcome == messaging.OutcomePossiblyPending {
		return messaging.WaitResult{Outcome: messaging.OutcomePossiblyPending}, nil
	}
	return res, err
}

func (t *Transport) Reply(ctx context.Context, req messaging.ReplyRequest) (messaging.SendResult, error) {
	child, cancel, ok := t.child(ctx)
	if !ok {
		return messaging.SendResult{Outcome: messaging.OutcomeNotSent, MessageID: req.MessageID}, messaging.ErrTransportClosed
	}
	defer cancel()
	if m := t.routed(); m != nil {
		res, err := m.Reply(child, req)
		res.MessageID = req.MessageID
		res.Generation = t.store.Generation()
		return res, err
	}
	outcome, err := t.store.Reply(
		t.key(messaging.Identity{RunID: req.ToRunID}, req.MessageID, t.store.Generation()),
		req.From.RunID,
		[]byte(req.Body),
	)
	return messaging.SendResult{Outcome: outcome, MessageID: req.MessageID, Generation: t.store.Generation()}, err
}

func (t *Transport) Withdraw(ctx context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	if err := t.open(req.Generation); err != nil {
		return messaging.WithdrawResult{Outcome: messaging.OutcomeNotSent}, err
	}
	child, cancel, ok := t.child(ctx)
	if !ok {
		return messaging.WithdrawResult{Outcome: messaging.OutcomeNotSent}, messaging.ErrTransportClosed
	}
	defer cancel()
	if m := t.routed(); m != nil {
		return m.Withdraw(child, req)
	}
	return t.store.Withdraw(t.key(req.Sender, req.MessageID, req.Generation))
}

func (t *Transport) Receive(ctx context.Context, req messaging.ReceiveRequest) (messaging.ReceiveResult, error) {
	if err := t.open(req.Generation); err != nil {
		return messaging.ReceiveResult{}, err
	}
	child, cancel, ok := t.child(ctx)
	if !ok {
		return messaging.ReceiveResult{}, messaging.ErrTransportClosed
	}
	defer cancel()
	if m := t.routed(); m != nil {
		res, err := m.Receive(child, req)
		res.Generation = t.store.Generation()
		return res, err
	}
	msgs, err := t.store.Receive(req.Sender.RunID, messaging.ReceiveLimit(req.Limit))
	return messaging.ReceiveResult{Messages: msgs, Generation: t.store.Generation()}, err
}

func (t *Transport) Peers(ctx context.Context, req messaging.PeersRequest) (messaging.PeersResult, error) {
	if err := t.open(req.Generation); err != nil {
		return messaging.PeersResult{}, err
	}
	child, cancel, ok := t.child(ctx)
	if !ok {
		return messaging.PeersResult{}, messaging.ErrTransportClosed
	}
	defer cancel()
	if m := t.routed(); m != nil {
		res, err := m.Peers(child, req)
		res.Generation = t.store.Generation()
		return res, err
	}
	peers, err := t.store.Peers()
	return messaging.PeersResult{Peers: peers, Generation: t.store.Generation()}, err
}

func (t *Transport) Preflight(ctx context.Context) (messaging.PreflightResult, error) {
	if t.isClosed() {
		return messaging.PreflightResult{}, messaging.ErrTransportClosed
	}
	child, cancel, ok := t.child(ctx)
	if !ok {
		return messaging.PreflightResult{}, messaging.ErrTransportClosed
	}
	defer cancel()
	if m := t.routed(); m != nil {
		res, err := m.Preflight(child)
		res.Generation = t.store.Generation()
		return res, err
	}
	return messaging.PreflightResult{Protocol: messaging.ProtocolVersion, Generation: t.store.Generation()}, nil
}

// Close releases client-side resources, unblocks in-flight waits with a
// possibly-pending outcome and joins its watchers and admitted routed calls.
func (t *Transport) Close() error {
	t.mu.Lock()
	if !t.closed {
		t.closed = true
		t.cancel()
	}
	t.mu.Unlock()
	t.inflight.Wait()
	return nil
}

func (t *Transport) isClosed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

func (t *Transport) open(generation string) error {
	if t.isClosed() {
		return messaging.ErrTransportClosed
	}
	if generation == "" {
		return messaging.ErrGenerationMismatch
	}
	return nil
}

// child admits a context watcher and, for routed transports, the entire call.
// Its completion function must be deferred by the caller. Admission precedes
// delegate resolution and is locked against Close; resolution runs unlocked.
// Store-only calls join just their watcher so Close never waits on a store lock.
func (t *Transport) child(ctx context.Context) (context.Context, func(), bool) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, nil, false
	}
	joined := t.resolve != nil
	t.inflight.Add(1)
	if joined {
		t.inflight.Add(1)
	}
	t.mu.Unlock()
	child, cancel := context.WithCancel(ctx)
	go func() {
		defer t.inflight.Done()
		select {
		case <-t.ctx.Done():
			cancel()
		case <-child.Done():
		}
	}()
	return child, func() {
		cancel()
		if joined {
			t.inflight.Done()
		}
	}, true
}

func (t *Transport) key(sender messaging.Identity, messageID, generation string) messaging.FenceKey {
	return messaging.FenceKey{Sender: sender.RunID, MessageID: messageID, Generation: generation}
}

var _ messaging.Transport = (*Transport)(nil)
