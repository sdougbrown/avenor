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
	client ControlClient
}

func newControlTransport(client ControlClient) *controlTransport {
	return &controlTransport{client: client}
}

func (t *controlTransport) Send(_ context.Context, req messaging.SendRequest) (messaging.SendResult, error) {
	res, err := t.client.MessageSend(req)
	return res, transportError(err)
}

func (t *controlTransport) Wait(_ context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	res, err := t.client.MessageWait(req)
	return res, transportError(err)
}

func (t *controlTransport) Reply(_ context.Context, req messaging.ReplyRequest) (messaging.SendResult, error) {
	res, err := t.client.MessageReply(req)
	return res, transportError(err)
}

func (t *controlTransport) Withdraw(_ context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	res, err := t.client.MessageWithdraw(req)
	return res, transportError(err)
}

func (t *controlTransport) Receive(_ context.Context, req messaging.ReceiveRequest) (messaging.ReceiveResult, error) {
	res, err := t.client.MessageReceive(req)
	return res, transportError(err)
}

func (t *controlTransport) Peers(_ context.Context, req messaging.PeersRequest) (messaging.PeersResult, error) {
	res, err := t.client.MessagePeers(req)
	return res, transportError(err)
}

func (t *controlTransport) Preflight(_ context.Context) (messaging.PreflightResult, error) {
	return t.client.MessagePreflight()
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
