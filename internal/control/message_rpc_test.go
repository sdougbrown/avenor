package control

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
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
	peersErr error
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
	if r.peersErr != nil {
		return messaging.PeersResult{Generation: req.Generation, Peers: []messaging.Peer{{RunID: "rt_b", Session: json.RawMessage(`{"run_id":"rt_b"}`)}}}, r.peersErr
	}
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

	// A send that fails on the transport still reports what the transport
	// knows: the outcome it reached and the message id it carried.
	tr.sendErr = messaging.ErrTransportClosed
	_ = writeReq(t, owner, Request{JSONRPC: "2.0", ID: 20, Method: "message_send", Params: json.RawMessage(`{"from_run_id":"rt_child","to_run_id":"rt_2","message_id":"m2","generation":"g","body":"{}","no_reply":true}`)})
	r = readRespForID(t, owner, 20)
	if r.Error == nil || r.Error.Code != -32000 {
		t.Fatalf("message_send transport error = %+v", r)
	}
	sendData, ok := r.Error.Data.(map[string]any)
	if !ok || sendData["wire_code"] != "transport-closed" || sendData["outcome"] != "not-sent" || sendData["message_id"] != "m2" {
		t.Fatalf("send error data = %#v", r.Error.Data)
	}
	tr.sendErr = nil

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

// TestMessagePeersKeepsAPartialListing pins that a peer listing the broker
// could not complete still returns the peers it did read, with the reason the
// listing was incomplete, so the caller keeps them instead of losing the
// whole set.
func TestMessagePeersKeepsAPartialListing(t *testing.T) {
	state := NewState("run_1", "", 0)
	s, cap := NewTestServer(state)
	tr := &recordingTransport{peersErr: fmt.Errorf("%w: the broker could not read every peer", messaging.ErrIncompleteListing)}
	handler := &mockStableHandler{messageTransport: tr}
	s.SetStableHandler(handler)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	owner := dialOwner(t, s, cap)
	defer owner.Close()

	_ = writeReq(t, owner, Request{JSONRPC: "2.0", ID: 1, Method: "message_peers", Params: json.RawMessage(`{"sender_run_id":"supervisor","generation":"g"}`)})
	r := readResp(t, owner)
	if r.Error != nil {
		t.Fatalf("message_peers: %+v", r.Error)
	}
	result, _ := r.Result.(map[string]any)
	peers, _ := result["peers"].([]any)
	if len(peers) != 1 {
		t.Fatalf("message_peers peers = %#v, want the peer the listing did read", result["peers"])
	}
	if result["peers_error"] == nil || result["peers_error"] == "" {
		t.Fatalf("message_peers result = %#v, want the incomplete listing reported", result)
	}
}

func TestResumeAndSessionRPCDispatch(t *testing.T) {	state := NewState("run_1", "", 0)
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

	// The rename and resume authorities' typed codes survive the wire, so a
	// surface classifies the failure without parsing text.
	handler.resumeErr = &router.ToolError{Code: "not_resumable", Message: "run is not resumable"}
	_ = writeReq(t, owner, Request{JSONRPC: "2.0", ID: 9, Method: "resume", Params: json.RawMessage(`{"run_id":"run_3","target":"rt_1","message":"again"}`)})
	r = readRespForID(t, owner, 9)
	if r.Error == nil {
		t.Fatalf("resume failure = %+v", r)
	}
	if data, _ := r.Error.Data.(map[string]any); data["code"] != "not_resumable" {
		t.Fatalf("resume error data = %#v", r.Error.Data)
	}

	handler.renameErr = &router.ToolError{Code: "invalid_name", Message: "bad name"}
	_ = writeReq(t, owner, Request{JSONRPC: "2.0", ID: 10, Method: "session_rename", Params: json.RawMessage(`{"session_id":"supervisor","name":"bad"}`)})
	r = readRespForID(t, owner, 10)
	if r.Error == nil {
		t.Fatalf("rename failure = %+v", r)
	}
	if data, _ := r.Error.Data.(map[string]any); data["code"] != "invalid_name" {
		t.Fatalf("rename error data = %#v", r.Error.Data)
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

// blockingWaitTransport parks message_wait until the test releases it, so the
// connection's ability to serve other requests is observable.
type blockingWaitTransport struct {
	recordingTransport
	entered     chan struct{}
	enteredOnce sync.Once
	release     chan struct{}
	// canceled is closed when the parked wait ends through its context
	// instead of the release, which is how a disconnect is observed.
	canceled chan struct{}
}

func (b *blockingWaitTransport) Wait(ctx context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	b.waitReq = req
	b.enteredOnce.Do(func() { close(b.entered) })
	select {
	case <-b.release:
		return messaging.WaitResult{Outcome: messaging.OutcomeTimedOut}, nil
	case <-ctx.Done():
		if b.canceled != nil {
			close(b.canceled)
		}
		return messaging.WaitResult{Outcome: messaging.OutcomePossiblyPending}, ctx.Err()
	}
}

// saturatedWaitTransport blocks every wait until release, so the connection's
// in-flight slot limit is observable.
type saturatedWaitTransport struct {
	recordingTransport
	entered chan struct{}
	release chan struct{}
}

func (b *saturatedWaitTransport) Wait(ctx context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	b.entered <- struct{}{}
	select {
	case <-b.release:
		return messaging.WaitResult{Outcome: messaging.OutcomeTimedOut}, nil
	case <-ctx.Done():
		return messaging.WaitResult{Outcome: messaging.OutcomePossiblyPending}, ctx.Err()
	}
}

// TestMessageWaitRefusesBeyondInflightLimit fills every blocking slot, pins the
// synchronous -32000 refusal for the next request, then releases the waits and
// checks the connection drains and serves again.
func TestMessageWaitRefusesBeyondInflightLimit(t *testing.T) {
	tr := &saturatedWaitTransport{
		entered: make(chan struct{}, maxInflightPerConn),
		release: make(chan struct{}),
	}
	state := NewState("run_1", "", 0)
	s, cap := NewTestServer(state)
	s.SetStableHandler(&mockStableHandler{messageTransport: tr})
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := dialOwner(t, s, cap)
	defer c.Close()

	for i := 1; i <= maxInflightPerConn; i++ {
		params := json.RawMessage(fmt.Sprintf(`{"message_id":"m%d","generation":"g"}`, i))
		_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: uint64(i), Method: "message_wait", Params: params})
	}
	for i := 0; i < maxInflightPerConn; i++ {
		select {
		case <-tr.entered:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d waits reached the transport", i)
		}
	}

	overflowID := uint64(maxInflightPerConn + 1)
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: overflowID, Method: "message_wait", Params: json.RawMessage(`{"message_id":"overflow","generation":"g"}`)})
	resp := readRespForID(t, c, overflowID)
	if resp.Error == nil || resp.Error.Code != -32000 || !strings.Contains(resp.Error.Message, "too many in-flight") {
		t.Fatalf("overflow wait = %+v, want -32000 slot refusal", resp)
	}

	close(tr.release)
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	scanner := bufio.NewScanner(c)
	drained := map[uint64]bool{}
	for len(drained) < maxInflightPerConn {
		if !scanner.Scan() {
			t.Fatalf("drained %d of %d waits: %v", len(drained), maxInflightPerConn, scanner.Err())
		}
		var r Response
		if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
			t.Fatalf("decode drained response: %v", err)
		}
		id, ok := r.ID.(float64)
		if !ok {
			t.Fatalf("drained response has no id: %+v", r)
		}
		if r.Error != nil {
			t.Fatalf("wait %v failed after release: %+v", id, r.Error)
		}
		drained[uint64(id)] = true
	}
	for i := 1; i <= maxInflightPerConn; i++ {
		if !drained[uint64(i)] {
			t.Fatalf("missing response for wait %d", i)
		}
	}

	// Every slot is free again, so the connection serves a fresh request.
	probeID := uint64(maxInflightPerConn + 2)
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: probeID, Method: "message_preflight"})
	if resp := readRespForID(t, c, probeID); resp.Error != nil {
		t.Fatalf("message_preflight after drain: %+v", resp.Error)
	}
}

// TestMessageWaitDoesNotBlockConnection pins the async message_wait shape: an
// outstanding wait must not stop the owner connection from serving a second
// request, because the MCP host reuses that connection for permission answers
// and cancellation.
func TestMessageWaitDoesNotBlockConnection(t *testing.T) {
	tr := &blockingWaitTransport{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	state := NewState("run_1", "", 0)
	s, cap := NewTestServer(state)
	s.SetStableHandler(&mockStableHandler{messageTransport: tr})
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := dialOwner(t, s, cap)
	defer c.Close()

	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "message_wait", Params: json.RawMessage(`{"sender_run_id":"supervisor","message_id":"m1","generation":"g","timeout_ms":5}`)})
	select {
	case <-tr.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("message_wait never reached the transport")
	}

	// A second request on the same connection must be served while the wait is
	// still outstanding.
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 2, Method: "message_preflight"})
	if resp := readRespForID(t, c, 2); resp.Error != nil {
		t.Fatalf("message_preflight while wait in flight: %+v", resp.Error)
	}

	close(tr.release)
	if resp := readRespForID(t, c, 1); resp.Error != nil {
		t.Fatalf("message_wait response after release: %+v", resp.Error)
	}
}

// TestMessageWaitClampsTimeout pins the server-side ceiling: a wire timeout
// above maxAskTimeoutMS reaches the transport as the ceiling, so the wait
// expires on the server with structured data instead of racing the ask store's
// own timeout.
func TestMessageWaitClampsTimeout(t *testing.T) {
	tr := &recordingTransport{waitRes: messaging.WaitResult{Outcome: messaging.OutcomeTimedOut}}
	state := NewState("run_1", "", 0)
	s, cap := NewTestServer(state)
	s.SetStableHandler(&mockStableHandler{messageTransport: tr})
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := dialOwner(t, s, cap)
	defer c.Close()

	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 2, Method: "message_wait", Params: json.RawMessage(`{"message_id":"m1","generation":"g","timeout_ms":100000000}`)})
	if resp := readRespForID(t, c, 2); resp.Error != nil {
		t.Fatalf("message_wait: %+v", resp.Error)
	}
	if want := time.Duration(maxAskTimeoutMS) * time.Millisecond; tr.waitReq.Timeout != want {
		t.Fatalf("transport timeout = %v, want the server ceiling %v", tr.waitReq.Timeout, want)
	}

	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 3, Method: "message_wait", Params: json.RawMessage(`{"message_id":"m1","generation":"g","timeout_ms":-5}`)})
	if resp := readRespForID(t, c, 3); resp.Error == nil || resp.Error.Code != -32602 {
		t.Fatalf("negative timeout = %+v, want -32602", resp)
	}
	if len(tr.calls) != 1 {
		t.Fatalf("negative timeout reached the transport: calls = %v", tr.calls)
	}
}

// TestMessageWaitDisconnectCancels pins the wait's lifetime: it is bound to the
// connection context, so a client that goes away must not leave a parked
// transport wait behind.
func TestMessageWaitDisconnectCancels(t *testing.T) {
	tr := &blockingWaitTransport{
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
		canceled: make(chan struct{}),
	}
	state := NewState("run_1", "", 0)
	s, cap := NewTestServer(state)
	s.SetStableHandler(&mockStableHandler{messageTransport: tr})
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := dialOwner(t, s, cap)
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 2, Method: "message_wait", Params: json.RawMessage(`{"message_id":"m1","generation":"g"}`)})
	select {
	case <-tr.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("message_wait never reached the transport")
	}

	c.Close()
	select {
	case <-tr.canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("the parked wait outlived the connection")
	}
}

// TestMessageWaitSlotRelease pins the per-connection slot accounting: every
// completed wait returns its slot on both the answer and the failure path, so a
// connection is never wedged by its own history.
func TestMessageWaitSlotRelease(t *testing.T) {
	for _, tc := range []struct {
		name    string
		waitErr error
	}{
		{name: "answered"},
		{name: "failed", waitErr: messaging.ErrTransportClosed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := &recordingTransport{
				waitRes: messaging.WaitResult{Outcome: messaging.OutcomeTimedOut},
				waitErr: tc.waitErr,
			}
			state := NewState("run_1", "", 0)
			s, cap := NewTestServer(state)
			s.SetStableHandler(&mockStableHandler{messageTransport: tr})
			path := testSocketPath(t)
			if err := s.Start(path); err != nil {
				t.Fatalf("start: %v", err)
			}
			defer s.Stop()

			c := dialOwner(t, s, cap)
			defer c.Close()

			// One more than the connection allows in flight: sequential waits
			// must never hit the slot refusal if each releases its slot.
			for i := 0; i <= maxInflightPerConn; i++ {
				id := uint64(i + 2)
				_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: id, Method: "message_wait", Params: json.RawMessage(`{"message_id":"m1","generation":"g","timeout_ms":1}`)})
				resp := readRespForID(t, c, id)
				if resp.Error != nil && strings.Contains(resp.Error.Message, "too many in-flight") {
					t.Fatalf("wait %d hit the slot refusal; a completed wait leaked its slot", i+1)
				}
				if tc.waitErr != nil && resp.Error == nil {
					t.Fatalf("wait %d = %+v, want the transport error", i+1, resp)
				}
			}
		})
	}
}

// blockingReceiveTransport parks message_receive until the test releases it, so
// the connection's ability to serve other requests is observable.
type blockingReceiveTransport struct {
	recordingTransport
	entered     chan struct{}
	enteredOnce sync.Once
	release     chan struct{}
}

func (b *blockingReceiveTransport) Receive(ctx context.Context, req messaging.ReceiveRequest) (messaging.ReceiveResult, error) {
	b.recvReq = req
	b.enteredOnce.Do(func() { close(b.entered) })
	select {
	case <-b.release:
		return messaging.ReceiveResult{Generation: req.Generation}, nil
	case <-ctx.Done():
		return messaging.ReceiveResult{}, ctx.Err()
	}
}

// TestMessageReceiveDoesNotBlockConnection pins the same async shape for
// message_receive that message_wait has: a receive that parks server-side must
// not stop the owner connection from serving a second request.
func TestMessageReceiveDoesNotBlockConnection(t *testing.T) {
	tr := &blockingReceiveTransport{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	state := NewState("run_1", "", 0)
	s, cap := NewTestServer(state)
	s.SetStableHandler(&mockStableHandler{messageTransport: tr})
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := dialOwner(t, s, cap)
	defer c.Close()

	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 2, Method: "message_receive", Params: json.RawMessage(`{"generation":"g"}`)})
	select {
	case <-tr.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("message_receive never reached the transport")
	}

	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 3, Method: "message_preflight"})
	if resp := readRespForID(t, c, 3); resp.Error != nil {
		t.Fatalf("message_preflight while a receive is in flight: %+v", resp.Error)
	}

	close(tr.release)
	if resp := readRespForID(t, c, 2); resp.Error != nil {
		t.Fatalf("message_receive response after release: %+v", resp.Error)
	}
}

// TestMessageReceiveTimeoutContract pins the receive timeout rules: a value
// above the server ceiling is clamped, and a negative one is refused before the
// transport is touched.
func TestMessageReceiveTimeoutContract(t *testing.T) {
	tr := &recordingTransport{}
	state := NewState("run_1", "", 0)
	s, cap := NewTestServer(state)
	s.SetStableHandler(&mockStableHandler{messageTransport: tr})
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := dialOwner(t, s, cap)
	defer c.Close()

	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 2, Method: "message_receive", Params: json.RawMessage(`{"generation":"g","timeout_ms":100000000}`)})
	if resp := readRespForID(t, c, 2); resp.Error != nil {
		t.Fatalf("message_receive: %+v", resp.Error)
	}
	if want := time.Duration(maxAskTimeoutMS) * time.Millisecond; tr.recvReq.Timeout != want {
		t.Fatalf("transport timeout = %v, want the server ceiling %v", tr.recvReq.Timeout, want)
	}

	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 3, Method: "message_receive", Params: json.RawMessage(`{"generation":"g","timeout_ms":-5}`)})
	if resp := readRespForID(t, c, 3); resp.Error == nil || resp.Error.Code != -32602 {
		t.Fatalf("negative timeout = %+v, want -32602", resp)
	}
	if len(tr.calls) != 1 {
		t.Fatalf("negative timeout reached the transport: calls = %v", tr.calls)
	}
}

// TestMessageRPCNonMessagingErrorData pins the authority verbs' error.data:
// outcome and wire_code describe an ask transport, so these verbs report the
// typed code and nothing that would claim a send happened.
func TestMessageRPCNonMessagingErrorData(t *testing.T) {
	state := NewState("run_1", "", 0)
	s, cap := NewTestServer(state)
	s.SetStableHandler(&mockStableHandler{
		resumeErr:  &router.ToolError{Code: "not_resumable", Message: "run is not resumable"},
		renameErr:  &router.ToolError{Code: "invalid_name", Message: "bad name"},
		lastErr:    &router.ToolError{Code: "unknown_target", Message: `no run matches "rt_ghost"`},
		lastSetErr: &router.ToolError{Code: "not_permitted", Message: "denied"},
	})
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	owner := dialOwner(t, s, cap)
	defer owner.Close()

	for _, tc := range []struct {
		id     uint64
		method string
		params string
		code   string
	}{
		{id: 2, method: "resume", params: `{"run_id":"run_3","target":"rt_1","message":"again"}`, code: "not_resumable"},
		{id: 3, method: "session_rename", params: `{"session_id":"supervisor","name":"main"}`, code: "invalid_name"},
		{id: 4, method: "session_last", params: `{"runtime_id":"rt_ghost"}`, code: "unknown_target"},
		{id: 5, method: "session_last_set", params: `{"runtime_id":"supervisor","final_output":"x"}`, code: "not_permitted"},
	} {
		_ = writeReq(t, owner, Request{JSONRPC: "2.0", ID: tc.id, Method: tc.method, Params: json.RawMessage(tc.params)})
		resp := readRespForID(t, owner, tc.id)
		if resp.Error == nil {
			t.Fatalf("%s: no error", tc.method)
		}
		data, ok := resp.Error.Data.(map[string]any)
		if !ok {
			t.Fatalf("%s error.data = %#v", tc.method, resp.Error.Data)
		}
		if data["code"] != tc.code {
			t.Errorf("%s code = %v, want %s", tc.method, data["code"], tc.code)
		}
		for _, field := range []string{"outcome", "wire_code", "message_id"} {
			if value, ok := data[field]; ok {
				t.Errorf("%s error.data carries %s=%v", tc.method, field, value)
			}
		}
	}
}
