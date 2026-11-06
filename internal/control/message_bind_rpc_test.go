package control

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestMessageBindRPCDispatch pins the wire contract of the owner-only bind:
// a non-owner connection is refused, the owner's request reaches the stable
// handler as the supervisor caller with the host and session it supplied, the
// result crosses as the snake_case DTO, and a handler refusal surfaces as a
// server error without exposing the requested credential.
func TestMessageBindRPCDispatch(t *testing.T) {
	state := NewState("run_1", "", 0)
	s, cap := NewTestServer(state)
	handler := &mockStableHandler{
		bindResult: messaging.MessageBindResult{
			BrokerURL:  "http://127.0.0.1:7300",
			RunID:      "b3f2b90d-8f5e-4a6f-9a44-2f1b0d17c9ab",
			RunToken:   "run-token",
			Generation: "gen_7",
			Created:    true,
		},
	}
	s.SetStableHandler(handler)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	owner := dialOwner(t, s, cap)
	defer owner.Close()
	readonly := mustDial(t, path)
	defer readonly.Close()

	_ = writeReq(t, readonly, Request{JSONRPC: "2.0", ID: 1, Method: "message_bind", Params: json.RawMessage(`{"host":"opencode","session_id":"ses_1"}`)})
	if r := readResp(t, readonly); r.Error == nil || r.Error.Code != -32010 {
		t.Fatalf("message_bind non-owner = %+v", r)
	}

	_ = writeReq(t, owner, Request{JSONRPC: "2.0", ID: 2, Method: "message_bind", Params: json.RawMessage(`{"host":"opencode","session_id":"ses_1"}`)})
	r := readResp(t, owner)
	if r.Error != nil {
		t.Fatalf("message_bind: %+v", r.Error)
	}
	result, _ := r.Result.(map[string]any)
	if result["run_id"] != handler.bindResult.RunID || result["run_token"] != "run-token" ||
		result["broker_url"] != "http://127.0.0.1:7300" || result["generation"] != "gen_7" || result["created"] != true {
		t.Fatalf("message_bind result = %v", r.Result)
	}
	if handler.bindReq.Host != "opencode" || handler.bindReq.SessionID != "ses_1" {
		t.Fatalf("bind request = %+v", handler.bindReq)
	}
	if handler.bindCaller.Kind != messaging.CallerOwner || handler.bindCaller.RunID != messaging.SupervisorRunID {
		t.Fatalf("bind caller = %+v", handler.bindCaller)
	}

	handler.bindErr = errors.New("bound run refused: not adopted")
	_ = writeReq(t, owner, Request{JSONRPC: "2.0", ID: 3, Method: "message_bind", Params: json.RawMessage(`{"host":"opencode","session_id":"ses_2"}`)})
	r = readRespForID(t, owner, 3)
	if r.Error == nil || r.Error.Code != -32000 || r.Error.Message != "bound run refused: not adopted" {
		t.Fatalf("message_bind refusal = %+v", r)
	}
	if handler.bindReq.SessionID != "ses_2" {
		t.Fatalf("refused bind request = %+v", handler.bindReq)
	}
}
