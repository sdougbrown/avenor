package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/control/controltest"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
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

// bindErrorCode decodes the typed code a bind refusal carries in error.data.
func bindErrorCode(t *testing.T, err error) string {
	t.Helper()
	var rpcErr *client.RPCError
	if !errors.As(err, &rpcErr) {
		t.Fatalf("bind error = %v, want an RPC error", err)
	}
	var data struct {
		Code string `json:"code"`
	}
	if len(rpcErr.Data) == 0 || json.Unmarshal(rpcErr.Data, &data) != nil {
		t.Fatalf("bind error data = %s, want a typed code", rpcErr.Data)
	}
	return data.Code
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
		if code := bindErrorCode(t, err); code != "invalid-request" {
			t.Fatalf("untyped refusal code = %q, want invalid-request", code)
		}
	})

	t.Run("carries the handler's typed tool code", func(t *testing.T) {
		for _, tc := range []struct{ code, message string }{
			{"not_permitted", "message_bind requires the owner connection"},
			{"refused", "message bind capacity reached (256 bound conversations)"},
		} {
			h := &bindHandler{}
			h.set(messaging.MessageBindResult{}, &router.ToolError{Code: tc.code, Message: tc.message})
			c := bindServer(t, h)

			_, err := c.MessageBindContext(context.Background(), messaging.MessageBindRequest{
				Host:      "pi",
				SessionID: "sess_3",
			})
			var rpcErr *client.RPCError
			if !errors.As(err, &rpcErr) {
				t.Fatalf("code %s: bind error = %v, want an RPC error", tc.code, err)
			}
			if rpcErr.Code != -32000 {
				t.Fatalf("code %s: rpc code = %d, want -32000", tc.code, rpcErr.Code)
			}
			if rpcErr.Message != tc.message {
				t.Fatalf("code %s: rpc message = %q, want %q", tc.code, rpcErr.Message, tc.message)
			}
			if got := bindErrorCode(t, err); got != tc.code {
				t.Fatalf("code %s: error.data.code = %q", tc.code, got)
			}
		}
	})
}
