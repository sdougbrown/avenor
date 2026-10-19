// Package inprocess implements the messaging Transport over an askstore.Store.
// It performs no HTTP and reads no credentials: callers supply already
// authenticated identities and the generation they pinned.
package inprocess

import (
	"context"
	"errors"
	"sync"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/askstore"
)

// Transport is the in-process messaging transport.
type Transport struct {
	store    *askstore.Store
	ctx      context.Context
	cancel   context.CancelFunc
	inflight sync.WaitGroup
	mu       sync.Mutex
	closed   bool
}

// New builds a transport over store.
func New(store *askstore.Store) messaging.Transport {
	ctx, cancel := context.WithCancel(context.Background())
	return &Transport{store: store, ctx: ctx, cancel: cancel}
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
	res, err := t.store.Admit(child, askstore.AdmitRequest{
		Sender:     req.From,
		Target:     req.To.RunID,
		MessageID:  req.MessageID,
		Generation: req.Generation,
	})
	out := messaging.SendResult{Outcome: res.Outcome, MessageID: res.MessageID, Generation: res.Generation}
	if err != nil && errors.Is(err, context.Canceled) {
		out.Outcome = messaging.OutcomeNotSent
	}
	return out, err
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
	if t.isClosed() {
		return messaging.SendResult{Outcome: messaging.OutcomeNotSent, MessageID: req.MessageID}, messaging.ErrTransportClosed
	}
	outcome, err := t.store.Reply(
		t.key(messaging.Identity{RunID: req.ToRunID}, req.MessageID, t.store.Generation()),
		req.From.RunID,
		[]byte(req.Body),
	)
	return messaging.SendResult{Outcome: outcome, MessageID: req.MessageID}, err
}

func (t *Transport) Withdraw(ctx context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	if err := t.open(req.Generation); err != nil {
		return messaging.WithdrawResult{Outcome: messaging.OutcomeNotSent}, err
	}
	return t.store.Withdraw(t.key(req.Sender, req.MessageID, req.Generation))
}

func (t *Transport) Receive(ctx context.Context, req messaging.ReceiveRequest) (messaging.ReceiveResult, error) {
	if err := t.open(req.Generation); err != nil {
		return messaging.ReceiveResult{}, err
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 32
	}
	msgs, err := t.store.Receive(req.Sender.RunID, limit)
	return messaging.ReceiveResult{Messages: msgs, Generation: t.store.Generation()}, err
}

func (t *Transport) Peers(ctx context.Context, req messaging.PeersRequest) (messaging.PeersResult, error) {
	if err := t.open(req.Generation); err != nil {
		return messaging.PeersResult{}, err
	}
	peers, err := t.store.Peers()
	return messaging.PeersResult{Peers: peers, Generation: t.store.Generation()}, err
}

func (t *Transport) Preflight(ctx context.Context) (messaging.PreflightResult, error) {
	if t.isClosed() {
		return messaging.PreflightResult{}, messaging.ErrTransportClosed
	}
	return messaging.PreflightResult{Protocol: messaging.ProtocolVersion, Generation: t.store.Generation()}, nil
}

// Close releases client-side resources, unblocks in-flight waits with a
// possibly-pending outcome and joins its goroutines.
func (t *Transport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.cancel()
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

// child returns a context cancelled by either ctx or transport shutdown, and
// registers the watcher so Close can join it. Registration happens under the
// transport lock, so Close can never observe an unregistered in-flight call.
func (t *Transport) child(ctx context.Context) (context.Context, context.CancelFunc, bool) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, nil, false
	}
	t.inflight.Add(1)
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
	return child, cancel, true
}

func (t *Transport) key(sender messaging.Identity, messageID, generation string) messaging.FenceKey {
	return messaging.FenceKey{Sender: sender.RunID, MessageID: messageID, Generation: generation}
}

var _ messaging.Transport = (*Transport)(nil)
