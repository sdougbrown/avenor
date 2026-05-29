package client

import (
	"bufio"
	"context"
	"encoding/json"
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
		scan:    bufio.NewScanner(clientConn),
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

	ch := c.Events() // starts readLoop

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
		scan:    bufio.NewScanner(clientConn),
		pending: map[int]chan Response{},
		eventCh: make(chan Event, 2),
	}
	_ = c.Events()

	go func() {
		scanner := bufio.NewScanner(serverConn)
		if !scanner.Scan() {
			return
		}
		var req Request
		_ = json.Unmarshal(scanner.Bytes(), &req)
		resp := Response{JSONRPC: "2.0", ID: req.ID}
		resp.Result, _ = json.Marshal(map[string]any{"ok": true})
		data, _ := json.Marshal(resp)
		data = append(data, '\n')
		_, _ = serverConn.Write(data)
	}()

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
		scan:    bufio.NewScanner(clientConn),
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
		scan:        bufio.NewScanner(clientConn),
		pending:     map[int]chan Response{},
		eventCh:     make(chan Event, 8),
		runtimeSubs: map[string]map[chan Event]struct{}{},
	}

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
		scan:        bufio.NewScanner(clientConn),
		pending:     map[int]chan Response{},
		eventCh:     make(chan Event, 8),
		runtimeSubs: map[string]map[chan Event]struct{}{},
	}

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
		scan:        bufio.NewScanner(clientConn),
		pending:     map[int]chan Response{},
		eventCh:     make(chan Event, 8),
		runtimeSubs: map[string]map[chan Event]struct{}{},
	}

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
		scan:        bufio.NewScanner(clientConn),
		pending:     map[int]chan Response{},
		eventCh:     make(chan Event, 8),
		runtimeSubs: map[string]map[chan Event]struct{}{},
	}

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
