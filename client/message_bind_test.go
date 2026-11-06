package client_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/control/controltest"
	"github.com/sdougbrown/avenor/internal/messaging"
)

type bindHandler struct {
	controltest.MessageHandler
	mu     sync.Mutex
	caller messaging.Caller
	req    messaging.MessageBindRequest
	result messaging.MessageBindResult
	fail   error
}

func (h *bindHandler) MessageBind(caller messaging.Caller, req messaging.MessageBindRequest) (messaging.MessageBindResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.caller = caller
	h.req = req
	return h.result, h.fail
}

func (h *bindHandler) set(result messaging.MessageBindResult, fail error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.result = result
	h.fail = fail
}

func (h *bindHandler) observed() (messaging.Caller, messaging.MessageBindRequest) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.caller, h.req
}

func bindServer(t *testing.T, h *bindHandler) *client.Client {
	t.Helper()
	socketPath := filepath.Join(t.TempDir(), "ctrl.sock")
	srv, owner := controltest.NewServer(control.NewState("run_test", "", 0))
	srv.SetStableHandler(h)
	if err := srv.Start(socketPath); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop() })

	c, err := client.Dial(socketPath)
	if err != nil {
		t.Fatalf("dial control socket: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if err := c.AuthenticateOwner(owner); err != nil {
		t.Fatalf("authenticate owner: %v", err)
	}
	return c
}

func TestMessageBind(t *testing.T) {
	t.Run("round-trips request and credentials", func(t *testing.T) {
		h := &bindHandler{}
		h.set(messaging.MessageBindResult{
			BrokerURL:  "https://broker.test",
			RunID:      "rt_1",
			RunToken:   "tok_1",
			Generation: "g1",
			Created:    true,
		}, nil)
		c := bindServer(t, h)

		got, err := c.MessageBindContext(context.Background(), messaging.MessageBindRequest{
			Host:      "pi",
			SessionID: "sess_1",
		})
		if err != nil {
			t.Fatalf("message bind: %v", err)
		}

		caller, req := h.observed()
		if req.Host != "pi" || req.SessionID != "sess_1" {
			t.Fatalf("handler saw request %+v, want host pi session sess_1", req)
		}
		if caller.Kind != messaging.CallerOwner || caller.RunID != messaging.SupervisorRunID {
			t.Fatalf("handler saw caller %+v, want owner/supervisor", caller)
		}

		want := messaging.MessageBindResult{
			BrokerURL:  "https://broker.test",
			RunID:      "rt_1",
			RunToken:   "tok_1",
			Generation: "g1",
			Created:    true,
		}
		if got != want {
			t.Fatalf("bind result = %+v, want %+v", got, want)
		}
	})

	t.Run("propagates handler errors", func(t *testing.T) {
		h := &bindHandler{}
		h.set(messaging.MessageBindResult{}, errors.New("bind refused: unknown host"))
		c := bindServer(t, h)

		_, err := c.MessageBindContext(context.Background(), messaging.MessageBindRequest{
			Host:      "unknown",
			SessionID: "sess_2",
		})
		if err == nil {
			t.Fatal("message bind = nil error, want the handler's refusal propagated")
		}
		var rpcErr *client.RPCError
		if !errors.As(err, &rpcErr) {
			t.Fatalf("bind error = %v, want an RPC error", err)
		}
		if rpcErr.Code != -32000 {
			t.Fatalf("rpc code = %d, want -32000", rpcErr.Code)
		}
		if rpcErr.Message != "bind refused: unknown host" {
			t.Fatalf("rpc message = %q, want the handler's error text", rpcErr.Message)
		}
	})
}
