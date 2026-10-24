package askstore

import (
	"context"
	"encoding/json"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// Asker serves the ask operations from a store. It owns no queue and no state
// beyond the store, so the store's admission, wait, withdrawal and reply
// contract has one owner: a transport that serves messaging from the store
// alone and a composition that reaches other targets both reach it here.
type Asker struct {
	store *Store
}

// NewAsker builds the ask seam over store.
func NewAsker(store *Store) *Asker { return &Asker{store: store} }

// Send admits an ask into the store. legacy admits an unversioned message id
// without a generation, which only the broker's legacy local HTTP router does.
func (a *Asker) Send(ctx context.Context, req messaging.SendRequest, legacy bool) (messaging.SendResult, error) {
	res, err := a.store.Admit(ctx, AdmitRequest{
		Sender:     req.From,
		Target:     req.To.RunID,
		MessageID:  req.MessageID,
		Generation: req.Generation,
		Body:       req.Body,
		NoReply:    req.NoReply,
		Legacy:     legacy,
	})
	return messaging.SendResult{Outcome: res.Outcome, MessageID: res.MessageID, Generation: res.Generation}, err
}

// Wait blocks for the reply to an ask the store holds.
func (a *Asker) Wait(ctx context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	return a.store.Wait(ctx, a.key(req.Sender, req.MessageID, req.Generation), req.Timeout)
}

// Withdraw withdraws an ask the store holds.
func (a *Asker) Withdraw(_ context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	return a.store.Withdraw(a.key(req.Sender, req.MessageID, req.Generation))
}

// Reply completes the ask edge MessageID names for ToRunID. The reply carries
// no generation, so the edge is looked up in the store's current generation.
func (a *Asker) Reply(_ context.Context, req messaging.ReplyRequest) (messaging.Outcome, error) {
	// An empty body is no payload at all, which keeps the reply's stored
	// payload valid JSON on the wire.
	var payload json.RawMessage
	if req.Body != "" {
		payload = json.RawMessage(req.Body)
	}
	key := a.key(messaging.Identity{RunID: req.ToRunID}, req.MessageID, a.store.Generation())
	return a.store.Reply(key, req.From.RunID, payload)
}

// key is the store's fence key for one ask.
func (a *Asker) key(sender messaging.Identity, messageID, generation string) messaging.FenceKey {
	return messaging.FenceKey{Sender: sender.RunID, MessageID: messageID, Generation: generation}
}
