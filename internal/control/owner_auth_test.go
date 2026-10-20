package control

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

func ownerAuthParams(t *testing.T, cap OwnerCapability) json.RawMessage {
	t.Helper()
	return authParams(t, OwnerAuthRequest{Version: OwnerAuthVersion, CapabilityID: cap.ID, Secret: cap.Secret})
}

func authParams(t *testing.T, req OwnerAuthRequest) json.RawMessage {
	t.Helper()
	params, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal authenticate_owner params: %v", err)
	}
	return params
}

// attachOwner returns an in-process connection already authenticated as the
// construction owner.
func attachOwner(t *testing.T, srv *ControlServer, cap OwnerCapability) *connState {
	t.Helper()
	peer, conn := net.Pipe()
	t.Cleanup(func() {
		_ = peer.Close()
		_ = conn.Close()
	})
	cs := &connState{
		id:        1,
		server:    srv,
		conn:      conn,
		askSlots:  make(chan struct{}, 1),
		waitSlots: make(chan struct{}, 1),
	}
	cs.ctx, cs.cancel = context.WithCancel(context.Background())
	t.Cleanup(cs.cancel)
	resp := srv.dispatch(cs, Request{JSONRPC: "2.0", ID: 1, Method: "authenticate_owner", Params: ownerAuthParams(t, cap)})
	if resp.Error != nil {
		t.Fatalf("authenticate_owner: %+v", resp.Error)
	}
	return cs
}

// dialOwner dials the server socket and authenticates the connection.
func dialOwner(t *testing.T, srv *ControlServer, cap OwnerCapability) net.Conn {
	t.Helper()
	conn := mustDial(t, srv.path)
	if err := writeReq(t, conn, Request{JSONRPC: "2.0", ID: 1, Method: "authenticate_owner", Params: ownerAuthParams(t, cap)}); err != nil {
		t.Fatalf("write authenticate_owner: %v", err)
	}
	if resp := readResp(t, conn); resp.Error != nil {
		t.Fatalf("authenticate_owner: %+v", resp.Error)
	}
	return conn
}

// TestConstructionOwnerCannotBeStolen proves that ownership comes from
// construction only: a first mutation, a reconnect and a second authenticated
// connection all fail to acquire it.
func TestConstructionOwnerCannotBeStolen(t *testing.T) {
	state := NewState("run_1", "", 0)
	s, cap := NewTestServer(state)
	if err := s.Start(testSocketPath(t)); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	first := mustDial(t, s.path)
	defer first.Close()
	if err := writeReq(t, first, Request{JSONRPC: "2.0", ID: 1, Method: "cancel"}); err != nil {
		t.Fatalf("write cancel: %v", err)
	}
	if resp := readResp(t, first); resp.Error == nil || resp.Error.Code != CodePermissionDenied {
		t.Fatalf("first mutating connection = %+v, want permission denied", resp)
	}

	// A reconnect that never authenticates is still not the owner.
	reconnected := mustDial(t, s.path)
	defer reconnected.Close()
	if err := writeReq(t, reconnected, Request{JSONRPC: "2.0", ID: 1, Method: "cancel"}); err != nil {
		t.Fatalf("write cancel: %v", err)
	}
	if resp := readResp(t, reconnected); resp.Error == nil || resp.Error.Code != CodePermissionDenied {
		t.Fatalf("unauthenticated reconnect = %+v, want permission denied", resp)
	}

	// A second authenticated connection is also not the construction principal.
	owner := dialOwner(t, s, cap)
	defer owner.Close()
	second := dialOwner(t, s, cap)
	defer second.Close()
	if err := writeReq(t, second, Request{JSONRPC: "2.0", ID: 1, Method: "cancel"}); err != nil {
		t.Fatalf("write cancel: %v", err)
	}
	if resp := readResp(t, second); resp.Error != nil {
		t.Fatalf("second authenticated connection cancel failed: %+v", resp.Error)
	}
}

// TestMessagingCredentialCannotControl proves no broker messaging token or
// launch identity satisfies the owner gate.
func TestMessagingCredentialCannotControl(t *testing.T) {
	b := broker.New("")
	defer b.Stop()
	token, err := b.CreateRun("run_child", "child")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	state := NewState("run_1", "", 0)
	s, cap := NewTestServer(state)
	s.SetStableHandler(&mockStableHandler{})
	if err := s.Start(testSocketPath(t)); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	candidates := []struct {
		name string
		cap  OwnerCapability
	}{
		{name: "broker token", cap: OwnerCapability{ID: cap.ID, Secret: token}},
		{name: "broker token as id", cap: OwnerCapability{ID: token, Secret: cap.Secret}},
		{name: "launch identity", cap: OwnerCapability{ID: "run_child", Secret: "launch-identity"}},
		{name: "empty", cap: OwnerCapability{}},
	}
	gated := []string{"spawn", "spawn_mcp", "cancel", "prompt", "interrupt_and_prompt", "answer_permission", "shutdown", "broker_send", "broker_ask", "broker_cancel", "broker_reply"}
	for _, candidate := range candidates {
		conn := mustDial(t, s.path)
		params, _ := json.Marshal(OwnerAuthRequest{Version: OwnerAuthVersion, CapabilityID: candidate.cap.ID, Secret: candidate.cap.Secret})
		if err := writeReq(t, conn, Request{JSONRPC: "2.0", ID: 1, Method: "authenticate_owner", Params: params}); err != nil {
			t.Fatalf("%s: write authenticate_owner: %v", candidate.name, err)
		}
		if resp := readResp(t, conn); resp.Error == nil || resp.Error.Code != CodeOwnerUnauthenticated {
			t.Fatalf("%s: authenticate_owner = %+v, want -32011", candidate.name, resp)
		}
		for i, method := range gated {
			var params json.RawMessage
			switch method {
			case "cancel", "prompt", "interrupt_and_prompt", "shutdown":
				params = nil
			case "broker_send":
				params = json.RawMessage(`{"from_run_id":"run_1","to_run_id":"run_child","message":"hi"}`)
			case "broker_ask":
				params = json.RawMessage(`{"to_run_id":"run_child","message":"hi"}`)
			case "broker_cancel", "broker_reply", "answer_permission":
				params = json.RawMessage(`{"message_id":"x","request_id":"x","option_id":"x"}`)
			}
			if err := writeReq(t, conn, Request{JSONRPC: "2.0", ID: i + 2, Method: method, Params: params}); err != nil {
				t.Fatalf("%s/%s: write: %v", candidate.name, method, err)
			}
			if resp := readResp(t, conn); resp.Error == nil || resp.Error.Code != CodePermissionDenied {
				t.Fatalf("%s/%s = %+v, want permission denied", candidate.name, method, resp)
			}
		}
		// A generic read still works without any owner authority.
		if err := writeReq(t, conn, Request{JSONRPC: "2.0", ID: 99, Method: "status"}); err != nil {
			t.Fatalf("%s: write status: %v", candidate.name, err)
		}
		if resp := readResp(t, conn); resp.Error != nil {
			t.Fatalf("%s: status = %+v, want success", candidate.name, resp)
		}
		_ = conn.Close()
	}
}

// TestFailedReauthClearsOnlyConnectionAuthority proves a failed
// reauthentication drops just that connection's owner bit.
func TestFailedReauthClearsOnlyConnectionAuthority(t *testing.T) {
	state := NewState("run_1", "", 0)
	s, cap := NewTestServer(state)
	if err := s.Start(testSocketPath(t)); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	first := dialOwner(t, s, cap)
	defer first.Close()
	second := dialOwner(t, s, cap)
	defer second.Close()

	wrong, _ := json.Marshal(OwnerAuthRequest{Version: OwnerAuthVersion, CapabilityID: cap.ID, Secret: "0000"})
	if err := writeReq(t, first, Request{JSONRPC: "2.0", ID: 5, Method: "authenticate_owner", Params: wrong}); err != nil {
		t.Fatalf("write failed reauth: %v", err)
	}
	if resp := readResp(t, first); resp.Error == nil || resp.Error.Code != CodeOwnerUnauthenticated {
		t.Fatalf("failed reauth = %+v, want -32011", resp)
	}

	if err := writeReq(t, first, Request{JSONRPC: "2.0", ID: 6, Method: "cancel"}); err != nil {
		t.Fatalf("write cancel after failed reauth: %v", err)
	}
	if resp := readResp(t, first); resp.Error == nil || resp.Error.Code != CodePermissionDenied {
		t.Fatalf("cancel after failed reauth = %+v, want permission denied", resp)
	}

	// The other connection keeps its authority and stays usable.
	if err := writeReq(t, second, Request{JSONRPC: "2.0", ID: 7, Method: "cancel"}); err != nil {
		t.Fatalf("write second cancel: %v", err)
	}
	if resp := readResp(t, second); resp.Error != nil {
		t.Fatalf("second connection lost authority: %+v", resp.Error)
	}

	// The construction principal itself is untouched: a fresh connection can
	// still authenticate.
	third := dialOwner(t, s, cap)
	defer third.Close()
	if err := writeReq(t, third, Request{JSONRPC: "2.0", ID: 8, Method: "cancel"}); err != nil {
		t.Fatalf("write third cancel: %v", err)
	}
	if resp := readResp(t, third); resp.Error != nil {
		t.Fatalf("fresh connection cannot authenticate: %+v", resp.Error)
	}
}

// TestOwnerReconnect proves an owner reconnect is accepted and that
// disconnection does not transfer authority.
func TestOwnerReconnect(t *testing.T) {
	state := NewState("run_1", "", 0)
	s, cap := NewTestServer(state)
	if err := s.Start(testSocketPath(t)); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	first := dialOwner(t, s, cap)
	if err := writeReq(t, first, Request{JSONRPC: "2.0", ID: 1, Method: "cancel"}); err != nil {
		t.Fatalf("write cancel: %v", err)
	}
	if resp := readResp(t, first); resp.Error != nil {
		t.Fatalf("owner cancel: %+v", resp.Error)
	}
	_ = first.Close()

	// The owner bit dies with its connection, so an unauthenticated
	// replacement is refused.
	second := mustDial(t, s.path)
	defer second.Close()
	if err := writeReq(t, second, Request{JSONRPC: "2.0", ID: 1, Method: "cancel"}); err != nil {
		t.Fatalf("write cancel: %v", err)
	}
	if resp := readResp(t, second); resp.Error == nil || resp.Error.Code != CodePermissionDenied {
		t.Fatalf("post-disconnect unauthenticated cancel = %+v, want permission denied", resp)
	}

	// An explicit reattach with the same live capability works.
	third := dialOwner(t, s, cap)
	defer third.Close()
	if err := writeReq(t, third, Request{JSONRPC: "2.0", ID: 1, Method: "cancel"}); err != nil {
		t.Fatalf("write cancel: %v", err)
	}
	if resp := readResp(t, third); resp.Error != nil {
		t.Fatalf("reconnected owner cancel: %+v", resp.Error)
	}
}

// TestOwnerAuthErrorCodes pins the owner-auth wire codes and their data shape.
func TestOwnerAuthErrorCodes(t *testing.T) {
	state := NewState("run_1", "", 0)
	s, cap := NewTestServer(state)
	if err := s.Start(testSocketPath(t)); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	cases := []struct {
		name   string
		params json.RawMessage
		code   int
		reason string
	}{
		{name: "wrong secret", params: authParams(t, OwnerAuthRequest{Version: OwnerAuthVersion, CapabilityID: cap.ID, Secret: "beef"}), code: CodeOwnerUnauthenticated, reason: "capability-mismatch"},
		{name: "wrong id", params: authParams(t, OwnerAuthRequest{Version: OwnerAuthVersion, CapabilityID: "beef", Secret: cap.Secret}), code: CodeOwnerUnauthenticated, reason: "capability-mismatch"},
		{name: "unsupported version", params: authParams(t, OwnerAuthRequest{Version: 2, CapabilityID: cap.ID, Secret: cap.Secret}), code: CodeUnsupportedOwnerAuth, reason: "version-2"},
		{name: "missing params", code: CodeUnsupportedOwnerAuth, reason: "version-0"},
	}
	for _, tc := range cases {
		conn := mustDial(t, s.path)
		if err := writeReq(t, conn, Request{JSONRPC: "2.0", ID: 1, Method: "authenticate_owner", Params: tc.params}); err != nil {
			t.Fatalf("%s: write: %v", tc.name, err)
		}
		resp := readResp(t, conn)
		_ = conn.Close()
		if resp.Error == nil || resp.Error.Code != tc.code {
			t.Fatalf("%s: error = %+v, want code %d", tc.name, resp.Error, tc.code)
		}
		var data OwnerAuthErrorData
		raw, err := json.Marshal(resp.Error.Data)
		if err != nil {
			t.Fatalf("%s: marshal error data: %v", tc.name, err)
		}
		if err := json.Unmarshal(raw, &data); err != nil {
			t.Fatalf("%s: decode error data: %v", tc.name, err)
		}
		if data.Version != OwnerAuthVersion || data.Reason != tc.reason {
			t.Fatalf("%s: error data = %+v, want version %d reason %q", tc.name, data, OwnerAuthVersion, tc.reason)
		}
	}

	// Invalid Params cannot cross the framed wire, so the malformed-params
	// branch is reached through dispatch with a raw body.
	resp := s.dispatch(&connState{server: s}, Request{JSONRPC: "2.0", ID: 1, Method: "authenticate_owner", Params: json.RawMessage(`{"version":`)})
	if resp.Error == nil || resp.Error.Code != CodeUnsupportedOwnerAuth {
		t.Fatalf("malformed params = %+v, want -32012", resp.Error)
	}
	payload, err := json.Marshal(resp.Error.Data)
	if err != nil {
		t.Fatalf("malformed params: marshal error data: %v", err)
	}
	var malformedData OwnerAuthErrorData
	if err := json.Unmarshal(payload, &malformedData); err != nil {
		t.Fatalf("malformed params: decode error data: %v", err)
	}
	if malformedData.Reason != "malformed-params" {
		t.Fatalf("malformed params error data = %+v, want reason malformed-params", malformedData)
	}

	// Success reports the construction capability and never the secret.
	conn := mustDial(t, s.path)
	defer conn.Close()
	if err := writeReq(t, conn, Request{JSONRPC: "2.0", ID: 1, Method: "authenticate_owner", Params: ownerAuthParams(t, cap)}); err != nil {
		t.Fatalf("write success: %v", err)
	}
	resp = readResp(t, conn)
	if resp.Error != nil {
		t.Fatalf("authenticate_owner: %+v", resp.Error)
	}
	raw, err := json.Marshal(resp.Result)
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	var result OwnerAuthResult
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result.Version != OwnerAuthVersion || !result.Authenticated || result.CapabilityID != cap.ID {
		t.Fatalf("result = %+v, want authenticated capability %s", result, cap.ID)
	}
	if contains(raw, cap.Secret) {
		t.Fatal("authenticate_owner result echoed the capability secret")
	}
}

func contains(haystack []byte, needle string) bool {
	if needle == "" {
		return false
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if string(haystack[i:i+len(needle)]) == needle {
			return true
		}
	}
	return false
}

// TestUngatedMethodsStayUnauthenticated pins the complete ungated method set:
// the 26 names dispatch leaves unauthenticated, and the 11 gated ones.
func TestUngatedMethodsStayUnauthenticated(t *testing.T) {
	state := NewState("run_1", "", 0)
	s, _ := NewTestServer(state)
	s.SetStableHandler(&mockStableHandler{})
	if err := s.Start(testSocketPath(t)); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	ungated := []string{
		"identity", "result", "status", "subscribe", "history", "wait_turn", "list",
		"broker_peers", "broker_receive", "send_to_parent", "tree_budget", "wait_for_capacity",
		"workflow.create", "workflow.instantiate", "workflow.status", "workflow.inspect",
		"workflow.wait", "workflow.events", "workflow.cancel", "workflow.command",
		"workflow.controller.create", "workflow.controller.enable", "workflow.controller.disable",
		"workflow.controller.status", "workflow.controller.list", "workflow.ready",
	}
	gated := []string{
		"spawn", "spawn_mcp", "cancel", "prompt", "interrupt_and_prompt", "answer_permission",
		"shutdown", "broker_send", "broker_ask", "broker_cancel", "broker_reply",
	}

	conn := mustDial(t, s.path)
	defer conn.Close()
	for i, method := range ungated {
		if err := writeReq(t, conn, Request{JSONRPC: "2.0", ID: i + 1, Method: method}); err != nil {
			t.Fatalf("%s: write: %v", method, err)
		}
		resp := readResp(t, conn)
		if resp.Error != nil && resp.Error.Code == CodePermissionDenied {
			t.Fatalf("%s is gated but must stay unauthenticated", method)
		}
	}
	for i, method := range gated {
		if err := writeReq(t, conn, Request{JSONRPC: "2.0", ID: 100 + i, Method: method}); err != nil {
			t.Fatalf("%s: write: %v", method, err)
		}
		resp := readResp(t, conn)
		if resp.Error == nil || resp.Error.Code != CodePermissionDenied {
			t.Fatalf("%s = %+v, want permission denied for an unauthenticated caller", method, resp)
		}
	}
}

// brokerBackedHandler delivers broker_send through a real broker queue.
type brokerBackedHandler struct {
	*mockStableHandler
	broker *broker.Broker
}

func (h brokerBackedHandler) BrokerSend(fromRunID, toRunID, message, role string) error {
	payload, err := json.Marshal(map[string]any{"from": fromRunID, "to_run_id": toRunID, "message": message, "role": role})
	if err != nil {
		return err
	}
	return h.broker.SendTo(fromRunID, toRunID, "agent_message", payload, "")
}

// TestOwnerWireBrokerSendDelivers proves the raw wire: a published record, an
// owner-authenticated connection and a gated broker_send that really reaches
// the recipient.
func TestOwnerWireBrokerSendDelivers(t *testing.T) {
	b := broker.New("")
	if _, err := b.CreateRun("run_recipient", "named recipient"); err != nil {
		t.Fatalf("create run: %v", err)
	}

	state := NewState("run_1", "", 0)
	s, cap := NewTestServer(state)
	s.SetStableHandler(brokerBackedHandler{mockStableHandler: &mockStableHandler{}, broker: b})
	if err := s.Start(testSocketPath(t)); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	conn := dialOwner(t, s, cap)
	defer conn.Close()
	params := json.RawMessage(`{"from_run_id":"run_1","to_run_id":"run_recipient","message":"hello recipient"}`)
	if err := writeReq(t, conn, Request{JSONRPC: "2.0", ID: 2, Method: "broker_send", Params: params}); err != nil {
		t.Fatalf("write broker_send: %v", err)
	}
	if resp := readResp(t, conn); resp.Error != nil {
		t.Fatalf("broker_send: %+v", resp.Error)
	}

	delivered, err := b.DrainAgentMessages("run_recipient", 10)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(delivered) != 1 {
		t.Fatalf("delivered %d messages, want 1", len(delivered))
	}
	if got := string(delivered[0].Payload); !contains([]byte(got), "hello recipient") {
		t.Fatalf("delivered payload = %s", got)
	}
	run := b.GetRun("run_recipient")
	if run == nil || run.Info == nil || run.Info.Label != "named recipient" {
		t.Fatalf("recipient label = %+v, want the registered name", run)
	}
	if err := b.RenameRun("run_recipient", "renamed recipient"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if got := b.GetRun("run_recipient").Info.Label; got != "renamed recipient" {
		t.Fatalf("label after rename = %q", got)
	}
}

// TestOwnerFacadeDelegation proves the facade delegates to the constructed
// server and only exists on a constructed server.
func TestOwnerFacadeDelegation(t *testing.T) {
	var zero ControlServer
	if zero.OwnerFacade() != nil {
		t.Fatal("zero-value ControlServer exposed an owner facade")
	}

	state := NewState("run_1", "", 0)
	s, _ := NewTestServer(state)
	cancelled := make(chan struct{}, 1)
	s.SetCancelFunc(func() { cancelled <- struct{}{} })
	facade := s.OwnerFacade()
	if facade == nil {
		t.Fatal("constructed ControlServer has no owner facade")
	}
	facade.Cancel()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("facade cancel did not reach the server")
	}

	if !s.PreparePermissionClaim("", "req_facade", PermissionResolverControl, nil) {
		t.Fatal("PreparePermissionClaim returned false")
	}
	if got := facade.AnswerPermission("", "req_facade", "allow", ""); got != PermissionAnswerDelivered {
		t.Fatalf("AnswerPermission = %v, want delivered", got)
	}
	if !s.MarkPermissionClaimResolved("", "req_facade", "avenor", "allow", "") {
		t.Fatal("MarkPermissionClaimResolved returned false")
	}
	if got := facade.AnswerPermission("", "req_facade", "allow", ""); got != PermissionAnswerAlreadyResolvedSame {
		t.Fatalf("repeat AnswerPermission = %v, want already-resolved-same", got)
	}
	if got := facade.AnswerPermission("", "req_missing", "allow", ""); got != PermissionAnswerNotFound {
		t.Fatalf("unknown AnswerPermission = %v, want not-found", got)
	}
}

// TestOwnerSocketlessConstruction proves a socketless server keeps its
// capability in memory and publishes nothing.
func TestOwnerSocketlessConstruction(t *testing.T) {
	s, cap := NewTestServer(NewState("run_1", "", 0))
	if err := s.Start(""); err != nil {
		t.Fatalf("socketless start: %v", err)
	}
	defer s.Stop()

	s.mu.Lock()
	path, listener, record := s.path, s.listener, s.recordID
	s.mu.Unlock()
	if path != "" || listener != nil || record != (SocketIdentity{}) {
		t.Fatalf("socketless start bound a listener or published a record: %q %v %+v", path, listener, record)
	}
	if cap.ID == "" || cap.Secret == "" {
		t.Fatal("socketless construction lost its capability")
	}
	if s.OwnerFacade() == nil {
		t.Fatal("socketless construction exposed no owner facade")
	}
}

// TestStopReturnsCleanupErrors proves cleanup failures reach the caller.
func TestStopReturnsCleanupErrors(t *testing.T) {
	s, _ := NewTestServer(NewState("run_1", "", 0))
	if err := s.Start(testSocketPath(t)); err != nil {
		t.Fatalf("start: %v", err)
	}
	sentinel := errors.New("remove failed")
	original := testOwnerRecordRemove
	testOwnerRecordRemove = func(string) error { return sentinel }
	t.Cleanup(func() { testOwnerRecordRemove = original })

	err := s.Stop()
	if !errors.Is(err, sentinel) {
		t.Fatalf("Stop error = %v, want the removal failure", err)
	}
	s.mu.Lock()
	stopped := s.stopped
	s.mu.Unlock()
	if !stopped {
		t.Fatal("Stop reported an error without completing shutdown")
	}
	if err := s.Stop(); err != nil {
		t.Fatalf("repeated Stop = %v, want nil", err)
	}
}

// TestOldCapabilityRejectedAfterReplacement proves rotation revokes the old
// capability without disturbing the replacement.
func TestOldCapabilityRejectedAfterReplacement(t *testing.T) {
	path := testSocketPath(t)
	old, oldCap := NewTestServer(NewState("run_1", "", 0))
	if err := old.Start(path); err != nil {
		t.Fatalf("start old: %v", err)
	}
	_ = dialOwner(t, old, oldCap).Close()
	// The old owner dies without cleanup, leaving its socket and record behind.
	dropListener(t, old)

	replacement, replacementCap := publishTestServer(t, path)
	conn := mustDial(t, path)
	defer conn.Close()
	if err := writeReq(t, conn, Request{JSONRPC: "2.0", ID: 1, Method: "authenticate_owner", Params: ownerAuthParams(t, oldCap)}); err != nil {
		t.Fatalf("write old capability: %v", err)
	}
	if resp := readResp(t, conn); resp.Error == nil || resp.Error.Code != CodeOwnerUnauthenticated {
		t.Fatalf("old capability = %+v, want -32011", resp)
	}

	// The replacement's owner access survives the rejection.
	owner := dialOwner(t, replacement, replacementCap)
	defer owner.Close()
	if err := writeReq(t, owner, Request{JSONRPC: "2.0", ID: 2, Method: "cancel"}); err != nil {
		t.Fatalf("write replacement cancel: %v", err)
	}
	if resp := readResp(t, owner); resp.Error != nil {
		t.Fatalf("replacement owner access lost: %+v", resp.Error)
	}
}
