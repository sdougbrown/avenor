package control

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

// recordingTransport records the transport calls the message_* handlers make.
type recordingTransport struct {
	calls    []string
	sendReq  messaging.SendRequest
	waitReq  messaging.WaitRequest
	replyReq messaging.ReplyRequest
	withReq  messaging.WithdrawRequest
	recvReq  messaging.ReceiveRequest
	peersReq messaging.PeersRequest
	waitErr  error
	waitRes  messaging.WaitResult
	sendErr  error
}

func (r *recordingTransport) Send(_ context.Context, req messaging.SendRequest) (messaging.SendResult, error) {
	r.calls = append(r.calls, "send")
	r.sendReq = req
	if r.sendErr != nil {
		return messaging.SendResult{Outcome: messaging.OutcomeNotSent, MessageID: req.MessageID, Generation: req.Generation}, r.sendErr
	}
	return messaging.SendResult{Outcome: messaging.OutcomeSent, MessageID: req.MessageID, Generation: req.Generation}, nil
}

func (r *recordingTransport) Wait(_ context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	r.calls = append(r.calls, "wait")
	r.waitReq = req
	return r.waitRes, r.waitErr
}

func (r *recordingTransport) Reply(_ context.Context, req messaging.ReplyRequest) (messaging.SendResult, error) {
	r.calls = append(r.calls, "reply")
	r.replyReq = req
	return messaging.SendResult{Outcome: messaging.OutcomeSent, MessageID: req.MessageID}, nil
}

func (r *recordingTransport) Withdraw(_ context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	r.calls = append(r.calls, "withdraw")
	r.withReq = req
	return messaging.WithdrawResult{Outcome: messaging.OutcomeWithdrawn, Fence: messaging.FenceAck{MessageID: req.MessageID, Terminal: true}}, nil
}

func (r *recordingTransport) Receive(_ context.Context, req messaging.ReceiveRequest) (messaging.ReceiveResult, error) {
	r.calls = append(r.calls, "receive")
	r.recvReq = req
	return messaging.ReceiveResult{Generation: req.Generation, Messages: []messaging.InboundMessage{{MessageID: "in_1", FromRunID: "rt_b"}}}, nil
}

func (r *recordingTransport) Peers(_ context.Context, req messaging.PeersRequest) (messaging.PeersResult, error) {
	r.calls = append(r.calls, "peers")
	r.peersReq = req
	return messaging.PeersResult{Generation: req.Generation, Peers: []messaging.Peer{{RunID: "rt_b", Session: json.RawMessage(`{"run_id":"rt_b"}`)}}}, nil
}

func (r *recordingTransport) Preflight(context.Context) (messaging.PreflightResult, error) {
	r.calls = append(r.calls, "preflight")
	return messaging.PreflightResult{Protocol: 1, Generation: "gen_1"}, nil
}

func (r *recordingTransport) Close() error { return nil }

func TestMessageRPCDispatch(t *testing.T) {
	state := NewState("run_1", "", 0)
	s, cap := NewTestServer(state)
	tr := &recordingTransport{}
	handler := &mockStableHandler{
		messageTransport: tr,
		resumeResult:     messaging.ResumeResult{RunID: "run_2", Label: "follow", RuntimeID: "rt_2", SessionID: "ses_2", SentinelFile: "/tmp/s", OnEvent: "/tmp/e"},
		renameResult:     messaging.RenameResult{RunID: "supervisor", Label: "main", PreviousLabel: "supervisor"},
		lastRecord:       broker.LastAgentMessage{RunID: "rt_2", Label: "peer", Text: "answer", StopReason: "done"},
		lastOK:           true,
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

	// The mutating message methods require the owner connection.
	_ = writeReq(t, readonly, Request{JSONRPC: "2.0", ID: 1, Method: "message_send", Params: json.RawMessage(`{"from_run_id":"supervisor","to_run_id":"rt_2","message_id":"m","generation":"g","body":"{}"}`)})
	if r := readResp(t, readonly); r.Error == nil || r.Error.Code != -32010 {
		t.Fatalf("message_send non-owner = %+v", r)
	}

	_ = writeReq(t, owner, Request{JSONRPC: "2.0", ID: 2, Method: "message_send", Params: json.RawMessage(`{"from_run_id":"rt_child","to_run_id":"rt_2","message_id":"m1","generation":"g","body":"{}","no_reply":true}`)})
	r := readResp(t, owner)
	if r.Error != nil {
		t.Fatalf("message_send: %+v", r.Error)
	}
	result, _ := r.Result.(map[string]any)
	if result["outcome"] != "sent" || result["message_id"] != "m1" {
		t.Fatalf("message_send result = %v", r.Result)
	}
	if tr.sendReq.From.RunID != "supervisor" || tr.sendReq.From.Sender != "" || tr.sendReq.To.RunID != "rt_2" || !tr.sendReq.NoReply {
		t.Fatalf("send request = %+v", tr.sendReq)
	}

	// A failed wait carries structured error data a client classifies without
	// parsing text.
	tr.waitErr = messaging.ErrTransportClosed
	tr.waitRes = messaging.WaitResult{Outcome: messaging.OutcomePossiblyPending}
	_ = writeReq(t, owner, Request{JSONRPC: "2.0", ID: 3, Method: "message_wait", Params: json.RawMessage(`{"sender_run_id":"supervisor","message_id":"m1","generation":"g","timeout_ms":5}`)})
	r = readResp(t, owner)
	if r.Error == nil || r.Error.Code != -32000 {
		t.Fatalf("message_wait error = %+v", r)
	}
	data, ok := r.Error.Data.(map[string]any)
	if !ok || data["wire_code"] != "transport-closed" || data["outcome"] != "possibly-pending" || data["message_id"] != "m1" {
		t.Fatalf("wait error data = %#v", r.Error.Data)
	}

	_ = writeReq(t, owner, Request{JSONRPC: "2.0", ID: 4, Method: "message_reply", Params: json.RawMessage(`{"from_run_id":"supervisor","to_run_id":"rt_2","message_id":"m1","body":"hi"}`)})
	if r := readResp(t, owner); r.Error != nil {
		t.Fatalf("message_reply: %+v", r.Error)
	}
	if tr.replyReq.From.RunID != "supervisor" || tr.replyReq.ToRunID != "rt_2" {
		t.Fatalf("reply request = %+v", tr.replyReq)
	}

	_ = writeReq(t, owner, Request{JSONRPC: "2.0", ID: 5, Method: "message_withdraw", Params: json.RawMessage(`{"sender_run_id":"supervisor","message_id":"m1","generation":"g"}`)})
	r = readResp(t, owner)
	result, _ = r.Result.(map[string]any)
	if r.Error != nil || result["outcome"] != "withdrawn" {
		t.Fatalf("message_withdraw result = %+v", r)
	}

	// The read-only message methods work on a credential-free dial.
	_ = writeReq(t, readonly, Request{JSONRPC: "2.0", ID: 6, Method: "message_receive", Params: json.RawMessage(`{"sender_run_id":"supervisor","generation":"g","limit":1}`)})
	r = readResp(t, readonly)
	result, _ = r.Result.(map[string]any)
	messages, _ := result["messages"].([]any)
	if r.Error != nil || len(messages) != 1 {
		t.Fatalf("message_receive result = %+v", r)
	}
	_ = writeReq(t, readonly, Request{JSONRPC: "2.0", ID: 7, Method: "message_peers", Params: json.RawMessage(`{"sender_run_id":"supervisor","generation":"g"}`)})
	if r := readResp(t, readonly); r.Error != nil {
		t.Fatalf("message_peers: %+v", r.Error)
	}
	_ = writeReq(t, readonly, Request{JSONRPC: "2.0", ID: 8, Method: "message_preflight"})
	r = readResp(t, readonly)
	result, _ = r.Result.(map[string]any)
	if r.Error != nil || result["protocol"] != float64(1) || result["generation"] != "gen_1" {
		t.Fatalf("message_preflight result = %+v", r)
	}
}

func TestResumeAndSessionRPCDispatch(t *testing.T) {
	state := NewState("run_1", "", 0)
	s, cap := NewTestServer(state)
	handler := &mockStableHandler{
		resumeResult: messaging.ResumeResult{RunID: "run_2", Label: "follow", RuntimeID: "rt_2", SessionID: "ses_2", SentinelFile: "/tmp/s", OnEvent: "/tmp/e"},
		renameResult: messaging.RenameResult{RunID: "supervisor", Label: "main", PreviousLabel: "supervisor"},
		lastRecord:   broker.LastAgentMessage{RunID: "rt_2", Label: "peer", Text: "answer", StopReason: "done", Truncated: true, At: 7},
		lastOK:       true,
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

	_ = writeReq(t, readonly, Request{JSONRPC: "2.0", ID: 1, Method: "resume", Params: json.RawMessage(`{"run_id":"run_2","target":"rt_1","message":"again"}`)})
	if r := readResp(t, readonly); r.Error == nil || r.Error.Code != -32010 {
		t.Fatalf("resume non-owner = %+v", r)
	}
	_ = writeReq(t, owner, Request{JSONRPC: "2.0", ID: 2, Method: "resume", Params: json.RawMessage(`{"run_id":"run_2","target":"rt_1","message":"again","label":"follow","idempotency_key":"k","sentinel_file":"/tmp/s","on_event":"/tmp/e"}`)})
	r := readResp(t, owner)
	result, _ := r.Result.(map[string]any)
	if r.Error != nil || result["run_id"] != "run_2" || result["runtime_id"] != "rt_2" || result["session_id"] != "ses_2" || result["sentinel_file"] != "/tmp/s" || result["on_event"] != "/tmp/e" {
		t.Fatalf("resume result = %+v", r)
	}

	_ = writeReq(t, readonly, Request{JSONRPC: "2.0", ID: 3, Method: "session_rename", Params: json.RawMessage(`{"session_id":"","name":"main"}`)})
	if r := readResp(t, readonly); r.Error == nil || r.Error.Code != -32010 {
		t.Fatalf("session_rename non-owner = %+v", r)
	}
	_ = writeReq(t, owner, Request{JSONRPC: "2.0", ID: 4, Method: "session_rename", Params: json.RawMessage(`{"session_id":"supervisor","name":"main"}`)})
	r = readResp(t, owner)
	result, _ = r.Result.(map[string]any)
	if r.Error != nil || result["label"] != "main" || result["previous_label"] != "supervisor" {
		t.Fatalf("session_rename result = %+v", r)
	}
	if handler.renameSessionID != "supervisor" || handler.renameName != "main" {
		t.Fatalf("forwarded rename = %q/%q", handler.renameSessionID, handler.renameName)
	}

	// session_last is read-only.
	_ = writeReq(t, readonly, Request{JSONRPC: "2.0", ID: 5, Method: "session_last", Params: json.RawMessage(`{"runtime_id":"rt_2"}`)})
	r = readResp(t, readonly)
	result, _ = r.Result.(map[string]any)
	if r.Error != nil || result["has_record"] != true || result["final_output"] != "answer" || result["final_output_truncated"] != true || result["at"] != float64(7) {
		t.Fatalf("session_last result = %+v", r)
	}
	if handler.lastRuntimeID != "rt_2" {
		t.Fatalf("session_last runtime id = %q", handler.lastRuntimeID)
	}
	handler.lastOK = false
	handler.lastRecord = broker.LastAgentMessage{RunID: "rt_2", Label: "peer"}
	_ = writeReq(t, readonly, Request{JSONRPC: "2.0", ID: 6, Method: "session_last", Params: json.RawMessage(`{"runtime_id":"rt_2"}`)})
	r = readResp(t, readonly)
	result, _ = r.Result.(map[string]any)
	if r.Error != nil || result["has_record"] != false || result["run_id"] != "rt_2" || result["final_output"] != "" {
		t.Fatalf("session_last no-record = %+v", r)
	}

	_ = writeReq(t, readonly, Request{JSONRPC: "2.0", ID: 7, Method: "session_last_set", Params: json.RawMessage(`{"runtime_id":"supervisor","final_output":"x"}`)})
	if r := readResp(t, readonly); r.Error == nil || r.Error.Code != -32010 {
		t.Fatalf("session_last_set non-owner = %+v", r)
	}
	_ = writeReq(t, owner, Request{JSONRPC: "2.0", ID: 8, Method: "session_last_set", Params: json.RawMessage(`{"runtime_id":"supervisor","final_output":"x","stop_reason":"done","final_output_truncated":true}`)})
	if r := readResp(t, owner); r.Error != nil {
		t.Fatalf("session_last_set: %+v", r.Error)
	}
	if handler.lastSetCalls != 1 {
		t.Fatalf("session_last_set calls = %d, want 1", handler.lastSetCalls)
	}
}

func TestMessageRPCTransportUnavailable(t *testing.T) {
	state := NewState("run_1", "", 0)
	s, cap := NewTestServer(state)
	s.SetStableHandler(&mockStableHandler{})
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()
	owner := dialOwner(t, s, cap)
	defer owner.Close()

	_ = writeReq(t, owner, Request{JSONRPC: "2.0", ID: 1, Method: "message_send", Params: json.RawMessage(`{"to_run_id":"rt_2","message_id":"m","generation":"g","body":"{}"}`)})
	r := readResp(t, owner)
	if r.Error == nil || r.Error.Code != -32000 {
		t.Fatalf("missing transport = %+v", r)
	}
	data, ok := r.Error.Data.(map[string]any)
	if !ok || data["wire_code"] != "invalid-request" {
		t.Fatalf("missing transport data = %#v", r.Error.Data)
	}
}
