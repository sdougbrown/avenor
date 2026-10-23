package mcpserver

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/messaging"
)

// controlTransport is the messaging.Transport the Go host surface supplies to
// the shared router. Every method is one control verb; the owner connection
// always speaks as the supervisor session, so no request carries a second
// identity and the broker stamps the sender's current display name.
type controlTransport struct {
	client messageControlClient
}

func newControlTransport(client messageControlClient) *controlTransport {
	return &controlTransport{client: client}
}

func (t *controlTransport) Send(ctx context.Context, req messaging.SendRequest) (messaging.SendResult, error) {
	res, err := t.client.MessageSendContext(ctx, req)
	return res, transportError(err)
}

func (t *controlTransport) Wait(ctx context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	res, err := t.client.MessageWaitContext(ctx, req)
	return res, transportError(err)
}

func (t *controlTransport) Reply(ctx context.Context, req messaging.ReplyRequest) (messaging.SendResult, error) {
	res, err := t.client.MessageReplyContext(ctx, req)
	return res, transportError(err)
}

func (t *controlTransport) Withdraw(ctx context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	res, err := t.client.MessageWithdrawContext(ctx, req)
	return res, transportError(err)
}

func (t *controlTransport) Receive(ctx context.Context, req messaging.ReceiveRequest) (messaging.ReceiveResult, error) {
	res, err := t.client.MessageReceiveContext(ctx, req)
	return res, transportError(err)
}

func (t *controlTransport) Peers(ctx context.Context, req messaging.PeersRequest) (messaging.PeersResult, error) {
	res, err := t.client.MessagePeersContext(ctx, req)
	return res, transportError(err)
}

func (t *controlTransport) Preflight(ctx context.Context) (messaging.PreflightResult, error) {
	return t.client.MessagePreflightContext(ctx)
}

// transportError maps the control wire's typed MessageErrorData back to the
// #317 error class, so the ask client classifies an auth rejection or a fence
// refusal instead of treating every control failure as possibly-pending. An
// error with no wire code (an authority error) keeps its RPCError.
func transportError(err error) error {
	if err == nil {
		return nil
	}
	var rpcErr *client.RPCError
	if !errors.As(err, &rpcErr) || len(rpcErr.Data) == 0 {
		return err
	}
	var data control.MessageErrorData
	if json.Unmarshal(rpcErr.Data, &data) != nil || data.WireCode == "" {
		return err
	}
	if typed := messaging.WireErrorFor(data.WireCode); typed != nil {
		return typed
	}
	return err
}

// Close performs no I/O: the adapter dials nothing and the control client is
// owned by the server lifecycle.
func (t *controlTransport) Close() error { return nil }
