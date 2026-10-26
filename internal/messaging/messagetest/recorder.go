package messagetest

import (
	"context"
	"sync"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// RecordedSend is one send the recorder served, with the admission contract
// its caller asked for.
type RecordedSend struct {
	Request messaging.SendRequest
}

// Recorder is a messaging.Seam that records the operations it serves and
// forwards them to the seam it wraps, so a test can observe that a broker
// path reached the messaging seam. ReachableFunc, when set, answers Reachable
// instead of the wrapped seam.
type Recorder struct {
	Local         messaging.Seam
	ReachableFunc func(runID string) bool

	mu        sync.Mutex
	sends     []RecordedSend
	waits     []messaging.WaitRequest
	withdraws []messaging.WithdrawRequest
	replies   []messaging.ReplyRequest
	tells     []messaging.TellRequest
	peerCalls int
}

func (r *Recorder) Send(ctx context.Context, req messaging.SendRequest) (messaging.SendResult, error) {
	r.mu.Lock()
	r.sends = append(r.sends, RecordedSend{Request: req})
	r.mu.Unlock()
	return r.Local.Send(ctx, req)
}

func (r *Recorder) Wait(ctx context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	r.mu.Lock()
	r.waits = append(r.waits, req)
	r.mu.Unlock()
	return r.Local.Wait(ctx, req)
}

func (r *Recorder) Withdraw(ctx context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	r.mu.Lock()
	r.withdraws = append(r.withdraws, req)
	r.mu.Unlock()
	return r.Local.Withdraw(ctx, req)
}

func (r *Recorder) Reply(ctx context.Context, req messaging.ReplyRequest) (messaging.SendResult, error) {
	r.mu.Lock()
	r.replies = append(r.replies, req)
	r.mu.Unlock()
	return r.Local.Reply(ctx, req)
}

func (r *Recorder) Tell(ctx context.Context, req messaging.TellRequest) error {
	r.mu.Lock()
	r.tells = append(r.tells, req)
	r.mu.Unlock()
	return r.Local.Tell(ctx, req)
}

func (r *Recorder) Reachable(runID string) bool {
	if r.ReachableFunc != nil {
		return r.ReachableFunc(runID)
	}
	return r.Local.Reachable(runID)
}

func (r *Recorder) Peers(ctx context.Context, req messaging.PeersRequest) (messaging.PeersResult, error) {
	r.mu.Lock()
	r.peerCalls++
	r.mu.Unlock()
	return r.Local.Peers(ctx, req)
}

func (r *Recorder) Receive(ctx context.Context, req messaging.ReceiveRequest) (messaging.ReceiveResult, error) {
	return r.Local.Receive(ctx, req)
}

func (r *Recorder) Preflight(ctx context.Context) (messaging.PreflightResult, error) {
	return r.Local.Preflight(ctx)
}

func (r *Recorder) Close() error { return r.Local.Close() }

// Sends returns the sends the recorder served.
func (r *Recorder) Sends() []RecordedSend {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]RecordedSend(nil), r.sends...)
}

// Waits returns the waits the recorder served.
func (r *Recorder) Waits() []messaging.WaitRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]messaging.WaitRequest(nil), r.waits...)
}

// Withdraws returns the withdrawals the recorder served.
func (r *Recorder) Withdraws() []messaging.WithdrawRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]messaging.WithdrawRequest(nil), r.withdraws...)
}

// Replies returns the replies the recorder served.
func (r *Recorder) Replies() []messaging.ReplyRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]messaging.ReplyRequest(nil), r.replies...)
}

// Tells returns the tells the recorder served.
func (r *Recorder) Tells() []messaging.TellRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]messaging.TellRequest(nil), r.tells...)
}

// PeerCalls returns how many peer lookups the recorder served.
func (r *Recorder) PeerCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.peerCalls
}
