package control

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestMessageBindLookupRPCDispatch pins the wire contract of the read: a
// non-owner connection is refused before the handler runs, the owner's request
// reaches the stable handler as the supervisor caller with the host and session
// it supplied, the result crosses as the same snake_case DTO the bind answers
// with, and a refusal surfaces as a server error.
func TestMessageBindLookupRPCDispatch(t *testing.T) {
	state := NewState("run_1", "", 0)
	s, cap := NewTestServer(state)
	handler := &mockStableHandler{
		lookupResult: messaging.MessageBindResult{
			BrokerURL:  "http://127.0.0.1:7300",
			RunID:      "b3f2b90d-8f5e-4a6f-9a44-2f1b0d17c9ab",
			RunToken:   "run-token",
			Generation: "gen_7",
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

	_ = writeReq(t, readonly, Request{JSONRPC: "2.0", ID: 1, Method: "message_bind_lookup", Params: json.RawMessage(`{"host":"poo","session_id":"ses_1"}`)})
	if r := readResp(t, readonly); r.Error == nil || r.Error.Code != -32010 {
		t.Fatalf("message_bind_lookup non-owner = %+v", r)
	}
	if handler.lookupReq.Host != "" {
		t.Fatalf("a non-owner lookup reached the handler: %+v", handler.lookupReq)
	}

	_ = writeReq(t, owner, Request{JSONRPC: "2.0", ID: 2, Method: "message_bind_lookup", Params: json.RawMessage(`{"host":"poo","session_id":"ses_1"}`)})
	r := readResp(t, owner)
	if r.Error != nil {
		t.Fatalf("message_bind_lookup: %+v", r.Error)
	}
	result, _ := r.Result.(map[string]any)
	if result["run_id"] != handler.lookupResult.RunID || result["run_token"] != "run-token" ||
		result["broker_url"] != "http://127.0.0.1:7300" || result["generation"] != "gen_7" || result["created"] != false {
		t.Fatalf("message_bind_lookup result = %v", r.Result)
	}
	if handler.lookupReq.Host != "poo" || handler.lookupReq.SessionID != "ses_1" {
		t.Fatalf("lookup request = %+v", handler.lookupReq)
	}
	if handler.lookupCaller.Kind != messaging.CallerOwner || handler.lookupCaller.RunID != messaging.SupervisorRunID {
		t.Fatalf("lookup caller = %+v", handler.lookupCaller)
	}

	handler.lookupErr = errors.New(`no live conversation binding for host "poo" session "ses_2"`)
	_ = writeReq(t, owner, Request{JSONRPC: "2.0", ID: 3, Method: "message_bind_lookup", Params: json.RawMessage(`{"host":"poo","session_id":"ses_2"}`)})
	r = readRespForID(t, owner, 3)
	if r.Error == nil || r.Error.Code != -32000 || r.Error.Message != `no live conversation binding for host "poo" session "ses_2"` {
		t.Fatalf("message_bind_lookup refusal = %+v", r)
	}
}
