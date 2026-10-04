package client

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func startTestServer(t *testing.T) (string, func()) {
	t.Helper()
	path := filepath.Join(os.TempDir(), "avc-client-"+time.Now().Format("150405.000000")+".sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	var mu sync.Mutex
	var seq int

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 4096)
				n, _ := c.Read(buf)
				var req Request
				_ = json.Unmarshal(buf[:n], &req)

				var resp Response
				switch req.Method {
				case "status":
					resp = Response{JSONRPC: "2.0", ID: req.ID}
					snap := map[string]any{"session_id": "ses_test", "phase": "working"}
					resp.Result, _ = json.Marshal(snap)
				case "identity":
					resp = Response{JSONRPC: "2.0", ID: req.ID}
					resp.Result, _ = json.Marshal(map[string]any{"token": "test-identity"})
				case "subscribe":
					resp = Response{JSONRPC: "2.0", ID: req.ID}
					resp.Result, _ = json.Marshal(map[string]any{"subscribed": true})
				case "list":
					resp = Response{JSONRPC: "2.0", ID: req.ID}
					list := []map[string]any{{"runtime_id": "rt_1", "status": "running"}}
					resp.Result, _ = json.Marshal(list)
				case "cancel":
					resp = Response{JSONRPC: "2.0", ID: req.ID, Result: json.RawMessage(`{"ok":true}`)}
				case "prompt":
					resp = Response{JSONRPC: "2.0", ID: req.ID, Result: json.RawMessage(`{"accepted":true}`)}
				case "answer_permission":
					resp = Response{JSONRPC: "2.0", ID: req.ID, Result: json.RawMessage(`{"accepted":true}`)}
				case "shutdown":
					resp = Response{JSONRPC: "2.0", ID: req.ID, Result: json.RawMessage(`{"shutting_down":true}`)}
				default:
					resp = Response{JSONRPC: "2.0", ID: req.ID, Error: &RespError{Code: -32601, Message: "method not found"}}
				}
				mu.Lock()
				seq++
				mu.Unlock()
				data, _ := json.Marshal(resp)
				data = append(data, '\n')
				c.Write(data)

				// After subscribe, send a test event notification.
				if req.Method == "subscribe" {
					ev := map[string]any{"event": "agent.status", "phase": "thinking"}
					notif := Notification{JSONRPC: "2.0", Method: "event"}
					notif.Params, _ = json.Marshal(ev)
					data, _ = json.Marshal(notif)
					data = append(data, '\n')
					c.Write(data)
				}
			}(conn)
		}
	}()

	return path, func() { ln.Close(); os.Remove(path) }
}

func TestClientStatus(t *testing.T) {
	path, cleanup := startTestServer(t)
	defer cleanup()

	c, err := Dial(path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	snap, err := c.Status("")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if snap["session_id"] != "ses_test" {
		t.Errorf("session_id = %v, want ses_test", snap["session_id"])
	}
}

func TestClientIdentity(t *testing.T) {
	path, cleanup := startTestServer(t)
	defer cleanup()

	c, err := Dial(path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	identity, err := c.Identity()
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	if identity != "test-identity" {
		t.Fatalf("identity = %q, want test-identity", identity)
	}
}

func TestClientCancel(t *testing.T) {
	path, cleanup := startTestServer(t)
	defer cleanup()

	c, err := Dial(path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if err := c.Cancel(""); err != nil {
		t.Fatalf("cancel: %v", err)
	}
}

func TestClientPrompt(t *testing.T) {
	path, cleanup := startTestServer(t)
	defer cleanup()

	c, err := Dial(path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if err := c.Prompt("", "do something"); err != nil {
		t.Fatalf("prompt: %v", err)
	}
}

func TestClientAnswerPermission(t *testing.T) {
	path, cleanup := startTestServer(t)
	defer cleanup()

	c, err := Dial(path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if err := c.AnswerPermission("", "req_1", "allow"); err != nil {
		t.Fatalf("answer_permission: %v", err)
	}
}

func TestClientList(t *testing.T) {
	path, cleanup := startTestServer(t)
	defer cleanup()

	c, err := Dial(path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	runtimes, err := c.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(runtimes) != 1 || runtimes[0]["runtime_id"] != "rt_1" {
		t.Errorf("runtimes = %v, want [rt_1]", runtimes)
	}
}

func TestClientShutdown(t *testing.T) {
	path, cleanup := startTestServer(t)
	defer cleanup()

	c, err := Dial(path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if err := c.Shutdown("graceful"); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func TestClientSendToParentValidation(t *testing.T) {
	c := &Client{}
	if err := c.SendToParent("", "msg"); err == nil {
		t.Fatal("expected error for missing runtime_id")
	}
	if err := c.SendToParent("rt_1", ""); err == nil {
		t.Fatal("expected error for missing message")
	}
	big := strings.Repeat("a", maxSendToParentMessageBytes+1)
	if err := c.SendToParent("rt_1", big); err == nil {
		t.Fatal("expected error for oversized message")
	}
}

func TestClientSubscribe(t *testing.T) {
	path, cleanup := startTestServer(t)
	defer cleanup()

	c, err := Dial(path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	// Must call subscribe before Events.
	if err := c.Call("subscribe", nil, nil); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	ch := c.Events()
	select {
	case ev := <-ch:
		if ev.Event != "agent.status" {
			t.Errorf("event = %q, want agent.status", ev.Event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event")
	}
}

// serveOneSubscribe reads the first request frame and, if it is the global
// subscribe call, replies with the control server's success result. It
// returns a channel that is closed once the first frame has been read (and
// the subscribe reply, when applicable, written). Tests that build a Client
// directly over net.Pipe need this so ensureSubscribed's request is consumed
// and answered before they drive events themselves.
func serveOneSubscribe(conn net.Conn) <-chan struct{} {
	served := make(chan struct{})
	go func() {
		defer close(served)
		line, err := bufio.NewReader(conn).ReadBytes('\n')
		if err != nil {
			return
		}
		var req Request
		if json.Unmarshal(line, &req) != nil || req.Method != "subscribe" {
			return
		}
		resp := Response{JSONRPC: "2.0", ID: req.ID, Result: json.RawMessage(`{"subscribed":true}`)}
		data, _ := json.Marshal(resp)
		_, _ = conn.Write(append(data, '\n'))
	}()
	return served
}

// waitForBufferLen busy-waits until len(c.eventCh) == n or timeout elapses.
// It returns false if the timeout is reached before the condition is met,
// causing the caller to fail the test with a clear message rather than
// letting a timing-sensitive sleep hide the hang.
func waitForBufferLen(t *testing.T, c *Client, n int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(c.eventCh) == n {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

// TestLaggedOrdering verifies that client.lagged arrives *before* the
// post-drop event — matching the server's subscriber.loop() guarantee.
func TestLaggedOrdering(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()

	// Small channel so we can provoke back-pressure without sending 256 events.
	c := &Client{
		conn:    clientConn,
		pending: map[int]chan Response{},
		eventCh: make(chan Event, 2),
	}

	sendEvent := func(name string) {
		n := Notification{JSONRPC: "2.0", Method: "event"}
		n.Params, _ = json.Marshal(map[string]any{"event": name})
		data, _ := json.Marshal(n)
		data = append(data, '\n')
		serverConn.Write(data)
	}

	servedSubscribe := serveOneSubscribe(serverConn)

	ch := c.Events() // starts readLoop and sends the subscribe request
	select {
	case <-servedSubscribe:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for subscribe to be answered")
	}

	// Fill the channel buffer without consuming, then send more to force drops.
	sendEvent("first")
	sendEvent("second")
	// Wait until readLoop has placed both events into the buffer before sending
	// more — this replaces the original time.Sleep(20ms) with a deterministic
	// check so CI CPU contention can't cause a false pass or hang.
	if !waitForBufferLen(t, c, 2, time.Second) {
		t.Fatal("timeout waiting for buffer to fill with first two events")
	}
	// third, fourth, fifth, and sixth arrive while the buffer is full — all dropped.
	sendEvent("third")
	sendEvent("fourth")
	sendEvent("fifth")
	sendEvent("sixth")
	// Drain to unblock the channel so readLoop can make progress.
	<-ch // "first"
	<-ch // "second"
	// Send the recovery event. readLoop will emit client.lagged *before* it
	// when the next real event arrives and c.dropped > 0 is detected.
	sendEvent("seventh")

	var got []Event
	deadline := time.After(2 * time.Second)
	for len(got) < 2 {
		select {
		case ev := <-ch:
			got = append(got, ev)
		case <-deadline:
			t.Fatalf("timeout; collected so far: %v", got)
		}
	}

	if got[0].Event != "client.lagged" {
		t.Errorf("got[0] = %q, want client.lagged (lag notification must precede recovery events)", got[0].Event)
	}

	// Validate the dropped_count payload. The event is constructed directly in
	// readLoop (not via JSON unmarshal), so dropped_count is stored as int.
	// We sent 6 events with channel capacity 2: "first" and "second" filled
	// the buffer, so "third" through "sixth" should have been dropped. In
	// practice the drain (above) may race with readLoop's processing of
	// "sixth" — if readLoop sees an empty channel while processing "sixth", it
	// successfully enqueues the lagged notification (count=3) instead of
	// dropping it. We therefore expect exactly 3 or 4 drops.
	if dc, ok := got[0].Raw["dropped_count"]; !ok {
		t.Errorf("got[0].Raw missing dropped_count key")
	} else {
		count, isInt := dc.(int)
		if !isInt {
			t.Errorf("dropped_count has wrong type %T (want int), value: %v", dc, dc)
		} else if count < 3 || count > 4 {
			t.Errorf("dropped_count = %d, want 3 or 4 (third–sixth dropped, sixth may have enqueued if drain raced)", count)
		}
	}

	if got[1].Event == "client.lagged" {
		t.Errorf("got[1] = client.lagged again; expected a real event after the lag notification")
	}

	serverConn.Close()
}

func TestClientResultReadsReplyBeyondScannerLimit(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	c := &Client{
		conn:    clientConn,
		pending: map[int]chan Response{},
		eventCh: make(chan Event, 1),
	}
	complete := strings.Repeat("é", 40_000) // 80 KiB UTF-8, beyond Scanner's 64 KiB default.
	go func() {
		line, err := bufio.NewReader(serverConn).ReadBytes('\n')
		if err != nil {
			return
		}
		var req Request
		if json.Unmarshal(line, &req) != nil {
			return
		}
		result, _ := json.Marshal(map[string]string{"final_output": complete})
		data, _ := json.Marshal(Response{JSONRPC: "2.0", ID: req.ID, Result: result})
		_, _ = serverConn.Write(append(data, '\n'))
	}()

	result, err := c.Result("rt_large")
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	if got, _ := result["final_output"].(string); got != complete {
		t.Fatalf("final_output length = %d, want %d", len(got), len(complete))
	}
}

func TestClientStatusWithRuntimeID(t *testing.T) {
	path, cleanup := startTestServer(t)
	defer cleanup()

	c, err := Dial(path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	_, err = c.Status("rt_1")
	if err != nil {
		t.Fatalf("status with runtime_id: %v", err)
	}
}

func TestClientCallAfterEventsHandlesImmediateResponse(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	c := &Client{
		conn:    clientConn,
		pending: map[int]chan Response{},
		eventCh: make(chan Event, 2),
	}
	go func() {
		// The first request is ensureSubscribed's subscribe call (sent by
		// Events()), the second is the status call under test.
		scanner := bufio.NewScanner(serverConn)
		for i := 0; i < 2 && scanner.Scan(); i++ {
			var req Request
			_ = json.Unmarshal(scanner.Bytes(), &req)
			resp := Response{JSONRPC: "2.0", ID: req.ID}
			if req.Method == "subscribe" {
				resp.Result, _ = json.Marshal(map[string]any{"subscribed": true})
			} else {
				resp.Result, _ = json.Marshal(map[string]any{"ok": true})
			}
			data, _ := json.Marshal(resp)
			data = append(data, '\n')
			_, _ = serverConn.Write(data)
		}
	}()

	_ = c.Events()

	var result map[string]any
	if err := c.Call("status", nil, &result); err != nil {
		t.Fatalf("Call after Events(): %v", err)
	}
	if result["ok"] != true {
		t.Fatalf("result = %#v, want ok=true", result)
	}
}

func TestClientCallReturnsErrorWhenConnectionCloses(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	c := &Client{
		conn:    clientConn,
		pending: map[int]chan Response{},
		eventCh: make(chan Event, 2),
	}

	go func() {
		buf := make([]byte, 1024)
		_, _ = serverConn.Read(buf)
		_ = serverConn.Close()
	}()

	if err := c.Call("status", nil, nil); err == nil {
		t.Fatal("Call returned nil after connection closed; want error")
	}
}

func TestSubscribeRuntimeFanoutIndependent(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	c := &Client{
		conn:        clientConn,
		pending:     map[int]chan Response{},
		eventCh:     make(chan Event, 8),
		runtimeSubs: map[string]map[chan Event]struct{}{},
	}
	serveOneSubscribe(serverConn)

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	ch1 := c.SubscribeRuntime(ctx1, "rt_1")
	ch2 := c.SubscribeRuntime(ctx2, "rt_2")

	sendEvent := func(runtimeID string) {
		n := Notification{JSONRPC: "2.0", Method: "event"}
		n.Params, _ = json.Marshal(map[string]any{"event": "session.end", "runtime_id": runtimeID})
		data, _ := json.Marshal(n)
		data = append(data, '\n')
		_, _ = serverConn.Write(data)
	}

	sendEvent("rt_1")
	sendEvent("rt_2")

	select {
	case ev := <-ch1:
		if ev.RuntimeID != "rt_1" {
			t.Fatalf("ch1 runtime_id = %q, want rt_1", ev.RuntimeID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for rt_1 event")
	}

	select {
	case ev := <-ch2:
		if ev.RuntimeID != "rt_2" {
			t.Fatalf("ch2 runtime_id = %q, want rt_2", ev.RuntimeID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for rt_2 event")
	}
}

func TestSubscribeRuntimeMatchesSessionID(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	c := &Client{
		conn:        clientConn,
		pending:     map[int]chan Response{},
		eventCh:     make(chan Event, 8),
		runtimeSubs: map[string]map[chan Event]struct{}{},
	}
	serveOneSubscribe(serverConn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := c.SubscribeRuntime(ctx, "ses_123")

	n := Notification{JSONRPC: "2.0", Method: "event"}
	n.Params, _ = json.Marshal(map[string]any{"event": "session.end", "session_id": "ses_123", "runtime_id": "rt_77"})
	data, _ := json.Marshal(n)
	data = append(data, '\n')
	_, _ = serverConn.Write(data)

	select {
	case ev := <-ch:
		if ev.SessionID != "ses_123" {
			t.Fatalf("session_id = %q, want ses_123", ev.SessionID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for session-id matched event")
	}
}

func TestSubscribeRuntimeIgnoresOtherRuntimeIDs(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	c := &Client{
		conn:        clientConn,
		pending:     map[int]chan Response{},
		eventCh:     make(chan Event, 8),
		runtimeSubs: map[string]map[chan Event]struct{}{},
	}
	serveOneSubscribe(serverConn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := c.SubscribeRuntime(ctx, "rt_1")

	n := Notification{JSONRPC: "2.0", Method: "event"}
	n.Params, _ = json.Marshal(map[string]any{"event": "session.end", "runtime_id": "rt_3"})
	data, _ := json.Marshal(n)
	data = append(data, '\n')
	_, _ = serverConn.Write(data)

	select {
	case ev := <-ch:
		t.Fatalf("unexpected event for wrong runtime id: %+v", ev)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestSubscribeRuntimeIgnoresOtherSessionIDs(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	c := &Client{
		conn:        clientConn,
		pending:     map[int]chan Response{},
		eventCh:     make(chan Event, 8),
		runtimeSubs: map[string]map[chan Event]struct{}{},
	}
	serveOneSubscribe(serverConn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := c.SubscribeRuntime(ctx, "ses_123")

	n := Notification{JSONRPC: "2.0", Method: "event"}
	n.Params, _ = json.Marshal(map[string]any{"event": "session.end", "session_id": "ses_other", "runtime_id": "rt_77"})
	data, _ := json.Marshal(n)
	data = append(data, '\n')
	_, _ = serverConn.Write(data)

	select {
	case ev := <-ch:
		t.Fatalf("unexpected event for wrong session id: %+v", ev)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestClientWorkflowCommandHelpers pins the typed workflow command helpers:
// each emits method "workflow.command" whose params carry the workflow id and
// a command object with the right op discriminator and the caller's fields.
func TestClientWorkflowCommandHelpers(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	c := &Client{
		conn:    clientConn,
		pending: map[int]chan Response{},
		eventCh: make(chan Event, 4),
	}

	got := make(chan map[string]any, 4)
	go func() {
		dec := json.NewDecoder(serverConn)
		for i := 0; i < 4; i++ {
			var raw map[string]any
			if err := dec.Decode(&raw); err != nil {
				return
			}
			resp := map[string]any{"jsonrpc": "2.0", "id": raw["id"], "result": map[string]any{"ok": true}}
			data, _ := json.Marshal(resp)
			data = append(data, '\n')
			serverConn.Write(data)
			got <- raw
		}
	}()

	cases := []struct {
		name    string
		op      string
		check   string
		wantVal any
		call    func() (map[string]any, error)
	}{
		{"complete", "complete", "node_id", "start",
			func() (map[string]any, error) {
				return c.WorkflowComplete("wf_1", map[string]any{"node_id": "start", "outcome": "done"})
			}},
		{"gate", "gate", "gate_id", "review",
			func() (map[string]any, error) {
				return c.WorkflowGate("wf_1", map[string]any{"gate_id": "review", "operation": "satisfy"})
			}},
		{"skip", "skip", "actor", "alice",
			func() (map[string]any, error) { return c.WorkflowSkip("wf_1", map[string]any{"actor": "alice"}) }},
		{"unblock", "unblock", "reason", "fixed",
			func() (map[string]any, error) { return c.WorkflowUnblock("wf_1", map[string]any{"reason": "fixed"}) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := tc.call()
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if res["ok"] != true {
				t.Fatalf("%s result = %#v, want ok=true", tc.name, res)
			}
			select {
			case raw := <-got:
				if raw["method"] != "workflow.command" {
					t.Fatalf("method = %v, want workflow.command", raw["method"])
				}
				params, _ := raw["params"].(map[string]any)
				if params == nil || params["workflow_id"] != "wf_1" {
					t.Fatalf("params = %v, want workflow_id wf_1", raw["params"])
				}
				command, _ := params["command"].(map[string]any)
				if command == nil {
					t.Fatalf("params.command missing: %v", params)
				}
				if command["op"] != tc.op {
					t.Errorf("command.op = %v, want %q", command["op"], tc.op)
				}
				if command[tc.check] != tc.wantVal {
					t.Errorf("command.%s = %v, want %v", tc.check, command[tc.check], tc.wantVal)
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("%s: timed out waiting for captured request", tc.name)
			}
		})
	}
}

func TestClientAnswerPermissionWithMessage(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()

	c := &Client{
		conn:    clientConn,
		pending: map[int]chan Response{},
		eventCh: make(chan Event, 1),
	}

	done := make(chan map[string]any, 1)
	go func() {
		dec := json.NewDecoder(serverConn)
		var raw map[string]any
		if err := dec.Decode(&raw); err != nil {
			done <- map[string]any{"error": err.Error()}
			return
		}
		// Write a success response so Call doesn't block.
		id := raw["id"]
		resp := map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"accepted": true}}
		respData, _ := json.Marshal(resp)
		respData = append(respData, '\n')
		serverConn.Write(respData)
		done <- raw
	}()

	if err := c.AnswerPermissionWithMessage("rt_1", "req_1", "allow", "write-in note"); err != nil {
		t.Fatalf("AnswerPermissionWithMessage: %v", err)
	}

	select {
	case cmd := <-done:
		if _, isErr := cmd["error"]; isErr {
			t.Fatalf("failed to read command: %v", cmd["error"])
		}
		params, _ := cmd["params"].(map[string]any)
		if params == nil {
			t.Fatalf("params missing from request: %v", cmd)
		}
		if params["message"] != "write-in note" {
			t.Errorf("message = %v, want 'write-in note'", params["message"])
		}
		if params["option_id"] != "allow" {
			t.Errorf("option_id = %v, want allow", cmd["option_id"])
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for command")
	}
}

func TestClientAnswerPermissionOmitsEmptyMessage(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()

	c := &Client{
		conn:    clientConn,
		pending: map[int]chan Response{},
		eventCh: make(chan Event, 1),
	}

	done := make(chan map[string]any, 1)
	go func() {
		dec := json.NewDecoder(serverConn)
		var raw map[string]any
		if err := dec.Decode(&raw); err != nil {
			done <- map[string]any{"error": err.Error()}
			return
		}
		id := raw["id"]
		resp := map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"accepted": true}}
		respData, _ := json.Marshal(resp)
		respData = append(respData, '\n')
		serverConn.Write(respData)
		done <- raw
	}()

	// Old API (no message) should not include "message" in the JSON.
	if err := c.AnswerPermission("rt_1", "req_1", "allow"); err != nil {
		t.Fatalf("AnswerPermission: %v", err)
	}

	select {
	case cmd := <-done:
		params, _ := cmd["params"].(map[string]any)
		if params == nil {
			t.Fatalf("params missing from request: %v", cmd)
		}
		if _, hasMessage := params["message"]; hasMessage {
			t.Error("message should not be present when using old API")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for command")
	}
}

func TestClientAnswerPermissionWithMessageOmitsEmpty(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()

	c := &Client{
		conn:    clientConn,
		pending: map[int]chan Response{},
		eventCh: make(chan Event, 1),
	}

	done := make(chan map[string]any, 1)
	go func() {
		dec := json.NewDecoder(serverConn)
		var raw map[string]any
		if err := dec.Decode(&raw); err != nil {
			done <- map[string]any{"error": err.Error()}
			return
		}
		id := raw["id"]
		resp := map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"accepted": true}}
		respData, _ := json.Marshal(resp)
		respData = append(respData, '\n')
		serverConn.Write(respData)
		done <- raw
	}()

	// AnswerPermissionWithMessage with empty message should also omit it.
	if err := c.AnswerPermissionWithMessage("rt_1", "req_1", "allow", ""); err != nil {
		t.Fatalf("AnswerPermissionWithMessage: %v", err)
	}

	select {
	case cmd := <-done:
		params, _ := cmd["params"].(map[string]any)
		if params == nil {
			t.Fatalf("params missing from request: %v", cmd)
		}
		if _, hasMessage := params["message"]; hasMessage {
			t.Error("message should not be present when empty")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for command")
	}
}

// TestWorkflowInstantiateParamsWire pins the wire contract: params are sent
// only when non-empty (omitted from the JSON when empty).
func TestWorkflowInstantiateParamsWire(t *testing.T) {
	call := func(metadata map[string]any, params map[string]string) map[string]any {
		t.Helper()
		serverConn, clientConn := net.Pipe()
		defer serverConn.Close()
		c := &Client{
			conn:    clientConn,
			pending: map[int]chan Response{},
			eventCh: make(chan Event, 1),
		}
		done := make(chan map[string]any, 1)
		go func() {
			dec := json.NewDecoder(serverConn)
			var raw map[string]any
			if err := dec.Decode(&raw); err != nil {
				done <- map[string]any{"error": err.Error()}
				return
			}
			id := raw["id"]
			resp := map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"workflow_id": "wf_1"}}
			respData, _ := json.Marshal(resp)
			respData = append(respData, '\n')
			serverConn.Write(respData)
			done <- raw
		}()
		if _, err := c.WorkflowInstantiate("tmpl_1", "1", metadata, params); err != nil {
			t.Fatalf("WorkflowInstantiate: %v", err)
		}
		select {
		case raw := <-done:
			return raw
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for command")
			return nil
		}
	}

	// Non-empty params are sent on the wire (nested under the JSON-RPC params object).
	raw := call(nil, map[string]string{"worktree": "avenor-issue-130"})
	outer, _ := raw["params"].(map[string]any)
	if outer == nil {
		t.Fatalf("params missing from request: %v", raw)
	}
	params, _ := outer["params"].(map[string]any)
	if params == nil {
		t.Fatalf("params missing from request: %v", raw)
	}
	if got := params["worktree"]; got != "avenor-issue-130" {
		t.Fatalf("params[worktree] = %v, want avenor-issue-130", got)
	}

	// Empty params are omitted from the wire.
	raw = call(nil, nil)
	outer, _ = raw["params"].(map[string]any)
	if outer == nil {
		t.Fatalf("params missing from request: %v", raw)
	}
	if _, hasParams := outer["params"]; hasParams {
		t.Fatalf("params should be omitted when empty: %v", raw)
	}
}

// TestCallErrorPreservesData pins the RPCError contract: structured error
// data (e.g. a failed broker ask's message_id) survives instead of being
// flattened into the message string.
func TestCallErrorPreservesData(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	c := &Client{
		conn:    clientConn,
		pending: map[int]chan Response{},
		eventCh: make(chan Event, 1),
	}
	go func() {
		line, err := bufio.NewReader(serverConn).ReadBytes('\n')
		if err != nil {
			return
		}
		var req Request
		if json.Unmarshal(line, &req) != nil {
			return
		}
		data, _ := json.Marshal(map[string]any{"message_id": "ask123", "pending": false})
		resp, _ := json.Marshal(Response{JSONRPC: "2.0", ID: req.ID, Error: &RespError{
			Code: -32000, Message: "wait for reply: context deadline exceeded", Data: data,
		}})
		_, _ = serverConn.Write(append(resp, '\n'))
	}()

	err := c.Call("broker_ask", map[string]any{"to_run_id": "rt_1", "message": "hi"}, nil)
	var rpcErr *RPCError
	if err == nil || !errors.As(err, &rpcErr) {
		t.Fatalf("error type = %T (%v), want *RPCError", err, err)
	}
	if rpcErr.Code != -32000 {
		t.Errorf("code = %d, want -32000", rpcErr.Code)
	}
	var payload map[string]any
	if unmarshalErr := json.Unmarshal(rpcErr.Data, &payload); unmarshalErr != nil {
		t.Fatalf("unmarshal data: %v", unmarshalErr)
	}
	if id, _ := payload["message_id"].(string); id != "ask123" {
		t.Errorf("message_id = %v, want ask123", payload["message_id"])
	}
	if pending, _ := payload["pending"].(bool); pending {
		t.Errorf("pending = %v, want false", payload["pending"])
	}
	if got := rpcErr.Error(); got != "rpc error [-32000]: wait for reply: context deadline exceeded" {
		t.Errorf("Error() = %q", got)
	}
}

// TestWaitTurnIndefiniteTimeoutPinsWireParams pins the indefinite-wait
// contract: a non-positive timeout must omit timeout_ms from the wire params
// (the server then waits indefinitely) and must not arm a client-side
// response timer — a settle delivered later still unblocks the wait.
func TestWaitTurnIndefiniteTimeoutPinsWireParams(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	c := &Client{
		conn:    clientConn,
		pending: map[int]chan Response{},
		eventCh: make(chan Event, 1),
	}

	type waitResult struct {
		result map[string]any
		err    error
	}
	done := make(chan waitResult, 1)
	go func() {
		result, err := c.WaitTurn("rt_1", 0)
		done <- waitResult{result, err}
	}()

	var raw map[string]any
	if err := json.NewDecoder(serverConn).Decode(&raw); err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if raw["method"] != "wait_turn" {
		t.Fatalf("method = %v, want wait_turn", raw["method"])
	}
	params, _ := raw["params"].(map[string]any)
	if params == nil {
		t.Fatalf("params missing: %v", raw["params"])
	}
	if params["runtime_id"] != "rt_1" {
		t.Errorf("runtime_id = %v, want rt_1", params["runtime_id"])
	}
	if _, ok := params["timeout_ms"]; ok {
		t.Errorf("params = %v; an indefinite wait must not carry timeout_ms", params)
	}

	// Deliver the settle late: the indefinite wait must still unblock.
	go func() {
		time.Sleep(50 * time.Millisecond)
		resp := map[string]any{"jsonrpc": "2.0", "id": raw["id"], "result": map[string]any{"stop_reason": "end_turn"}}
		data, _ := json.Marshal(resp)
		_, _ = serverConn.Write(append(data, '\n'))
	}()

	select {
	case wr := <-done:
		if wr.err != nil {
			t.Fatalf("WaitTurn: %v", wr.err)
		}
		if wr.result["stop_reason"] != "end_turn" {
			t.Errorf("stop_reason = %v, want end_turn", wr.result["stop_reason"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("indefinite wait never unblocked after the settle was delivered")
	}
}

func TestClientClosedOnServerDisconnect(t *testing.T) {
	path := filepath.Join(os.TempDir(), "avc-client-closed-"+time.Now().Format("150405.000000")+".sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { ln.Close(); os.Remove(path) }()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- conn
	}()

	c, err := Dial(path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	select {
	case conn, ok := <-accepted:
		if !ok {
			t.Fatal("server failed to accept the connection")
		}
		// The supervisor side drops the connection.
		conn.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("server never accepted the connection")
	}

	// readLoop observes the EOF and marks the client closed; wait on the
	// close channel with a deadline rather than polling.
	select {
	case <-c.ClosedChan():
	case <-time.After(5 * time.Second):
		t.Fatal("Closed() never became true after server disconnect")
	}
	if !c.Closed() {
		t.Fatal("ClosedChan closed but Closed() is false")
	}

	// A new Call after close must fail immediately (no 30s wait): the
	// immediate-error path means the error is returned synchronously.
	err = c.Call("status", nil, nil)
	if err == nil {
		t.Fatal("expected Call after close to fail")
	}
	if !strings.Contains(err.Error(), "connection closed") {
		t.Fatalf("expected connection-closed error, got: %v", err)
	}
}

// startErrorAndHangServer returns a stub supervisor that replies with a
// JSON-RPC error frame for the "rpc_error" method and accepts but never
// replies (keeping the connection open) for any other method.
func startErrorAndHangServer(t *testing.T) (string, func()) {
	t.Helper()
	path := filepath.Join(os.TempDir(), "avc-client-err-hang-"+time.Now().Format("150405.000000")+".sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	done := make(chan struct{})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				data, err := bufio.NewReader(c).ReadBytes('\n')
				if err != nil {
					return
				}
				var req Request
				if err := json.Unmarshal(data, &req); err != nil {
					return
				}
				if req.Method == "rpc_error" {
					resp := Response{JSONRPC: "2.0", ID: req.ID, Error: &RespError{Code: -32000, Message: "boom"}}
					respData, _ := json.Marshal(resp)
					_, _ = c.Write(append(respData, '\n'))
				}
				// Keep the connection open until cleanup so the client's readLoop
				// does not observe an EOF.
				<-done
			}(conn)
		}
	}()
	return path, func() { close(done); ln.Close(); os.Remove(path) }
}

func TestClientClosedFalseOnRPCErrorAndTimeout(t *testing.T) {
	path, cleanup := startErrorAndHangServer(t)
	defer cleanup()

	t.Run("rpc error does not close", func(t *testing.T) {
		c, err := Dial(path)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer c.Close()
		err = c.Call("rpc_error", nil, nil)
		if err == nil {
			t.Fatal("expected an RPC error")
		}
		var rpcErr *RPCError
		if !errors.As(err, &rpcErr) {
			t.Fatalf("error = %v (%T), want an *RPCError", err, err)
		}
		if c.Closed() {
			t.Fatal("Closed() = true after an RPC error, want false")
		}
	})

	t.Run("response timeout does not close", func(t *testing.T) {
		c, err := Dial(path)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer c.Close()
		// A short wait bounds the response wait so the test stays fast; the
		// timeout path must not mark the connection closed.
		err = c.call("hang", nil, nil, 200*time.Millisecond)
		if err == nil {
			t.Fatal("expected a timeout error")
		}
		if !strings.Contains(err.Error(), "timeout") {
			t.Fatalf("error = %v, want a timeout", err)
		}
		if c.Closed() {
			t.Fatal("Closed() = true after a response timeout, want false")
		}
	})
}
