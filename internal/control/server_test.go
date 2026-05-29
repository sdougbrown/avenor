package control

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
)

func TestSocketLifecycleActiveListenerFails(t *testing.T) {
	state := NewState("run_1", "", 0)
	s1 := NewServer(state)
	path := testSocketPath(t)
	if err := s1.Start(path); err != nil {
		t.Fatalf("start first: %v", err)
	}
	defer s1.Stop()

	s2 := NewServer(state)
	if err := s2.Start(path); err == nil {
		t.Fatal("expected active listener failure")
	}
}

func TestOwnerRejectionForMutatingMethods(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c1 := mustDial(t, path)
	defer c1.Close()
	c2 := mustDial(t, path)
	defer c2.Close()

	_ = writeReq(t, c1, Request{JSONRPC: "2.0", ID: 1, Method: "cancel"})
	r1 := readResp(t, c1)
	if r1.Error != nil {
		t.Fatalf("owner cancel failed: %+v", r1.Error)
	}
	_ = writeReq(t, c2, Request{JSONRPC: "2.0", ID: 2, Method: "cancel"})
	r2 := readResp(t, c2)
	if r2.Error == nil || r2.Error.Code != -32010 {
		t.Fatalf("expected permission denied for cancel, got %+v", r2)
	}

	apParams, _ := json.Marshal(PermissionAnswer{RequestID: "req_1", OptionID: "allow"})
	_ = writeReq(t, c2, Request{JSONRPC: "2.0", ID: 3, Method: "answer_permission", Params: apParams})
	r3 := readResp(t, c2)
	if r3.Error == nil || r3.Error.Code != -32010 {
		t.Fatalf("expected permission denied for answer_permission, got %+v", r3)
	}
}

func TestOwnershipTransferOnDisconnect(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c1 := mustDial(t, path)
	_ = writeReq(t, c1, Request{JSONRPC: "2.0", ID: 1, Method: "cancel"})
	r1 := readResp(t, c1)
	if r1.Error != nil {
		t.Fatalf("owner cancel failed: %+v", r1.Error)
	}
	c1.Close()

	// Poll until owner is cleared (server processes disconnect).
	deadline := time.Now().Add(2 * time.Second)
	for {
		s.mu.Lock()
		owner := s.owner
		s.mu.Unlock()
		if owner == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("owner not cleared after close")
		}
		time.Sleep(5 * time.Millisecond)
	}

	c2 := mustDial(t, path)
	defer c2.Close()
	_ = writeReq(t, c2, Request{JSONRPC: "2.0", ID: 2, Method: "cancel"})
	r2 := readResp(t, c2)
	if r2.Error != nil {
		t.Fatalf("new owner cancel failed: %+v", r2.Error)
	}
	res, ok := r2.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type: %T", r2.Result)
	}
	if v, _ := res["ok"].(bool); !v {
		t.Fatal("expected ok=true")
	}
}

func TestSubscriberBackpressureLaggedEvent(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	client, srv := net.Pipe()
	defer client.Close()
	defer srv.Close()
	cs := &connState{server: s, conn: srv}
	sub := &subscriber{conn: cs, ch: make(chan events.Event, 1)}
	go sub.loop()

	for i := 0; i < 5; i++ {
		sub.enqueue(events.Event{Event: "e", Fields: map[string]any{"i": i}})
	}
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	scanner := bufio.NewScanner(client)
	var (
		foundLag     bool
		eventCount   int
		droppedCount float64
	)
	for scanner.Scan() {
		var n Notification
		if err := json.Unmarshal(scanner.Bytes(), &n); err != nil {
			continue
		}
		if n.Method == "event" {
			m, _ := n.Params.(map[string]any)
			ev, _ := m["event"].(string)
			if ev == "subscriber.lagged" {
				foundLag = true
				droppedCount, _ = m["dropped_count"].(float64)
			} else {
				eventCount++
			}
		}
	}
	if !foundLag {
		t.Fatal("expected subscriber.lagged notification")
	}
	if droppedCount <= 0 {
		t.Fatalf("expected dropped_count > 0, got %v", droppedCount)
	}
	if eventCount == 0 {
		t.Fatal("expected at least one event notification")
	}
}

func TestSubscribeAndStatus(t *testing.T) {
	state := NewState("run_1", "label", 2)
	s := NewServer(state)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := mustDial(t, path)
	defer c.Close()
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "subscribe"})
	r1 := readResp(t, c)
	if r1.Error != nil {
		t.Fatalf("subscribe error: %+v", r1.Error)
	}
	res1, ok := r1.Result.(map[string]any)
	if !ok {
		t.Fatalf("subscribe result type: %T", r1.Result)
	}
	if subscribed, _ := res1["subscribed"].(bool); !subscribed {
		t.Fatal("expected subscribed=true")
	}
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 2, Method: "status"})
	r := readResp(t, c)
	if r.Error != nil {
		t.Fatalf("status error: %+v", r.Error)
	}
	res, ok := r.Result.(map[string]any)
	if !ok {
		t.Fatalf("status result type: %T", r.Result)
	}
	if runID, _ := res["run_id"].(string); runID != "run_1" {
		t.Fatalf("run_id = %q, want run_1", runID)
	}
	if runLabel, _ := res["run_label"].(string); runLabel != "label" {
		t.Fatalf("run_label = %q, want label", runLabel)
	}
	mr, _ := res["max_retries"].(float64)
	if int(mr) != 2 {
		t.Fatalf("max_retries = %v, want 2", mr)
	}
}

func mustDial(t *testing.T, path string) net.Conn {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		c, err := net.Dial("unix", path)
		if err == nil {
			return c
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func writeReq(t *testing.T, c net.Conn, req Request) error {
	t.Helper()
	b, _ := json.Marshal(req)
	b = append(b, '\n')
	_, err := c.Write(b)
	return err
}

func readResp(t *testing.T, c net.Conn) Response {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	scanner := bufio.NewScanner(c)
	if !scanner.Scan() {
		t.Fatal("no response")
	}
	var r Response
	if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return r
}

func TestHTTPDebugStatusAndCancel(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	h, err := NewHTTPDebugServer("127.0.0.1:0", s)
	if err != nil {
		t.Fatalf("NewHTTPDebugServer: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	go func() { _ = h.server.Serve(ln) }()
	defer h.Stop(context.Background())

	client := &http.Client{Timeout: 2 * time.Second}
	token := h.token

	// GET /status without token — assert 401
	resp, err := client.Get("http://" + addr + "/status")
	if err != nil {
		t.Fatalf("GET /status (no token): %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET /status (no token): %d, want 401", resp.StatusCode)
	}

	// GET /status with token — assert 200 and run_id
	req, _ := http.NewRequest(http.MethodGet, "http://"+addr+"/status", http.NoBody)
	req.Header.Set("X-Avenor-Token", token)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("GET /status: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /status: %d, want 200", resp.StatusCode)
	}
	var status Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatalf("decode /status: %v", err)
	}
	if status.RunID != "run_1" {
		t.Fatalf("run_id = %q, want run_1", status.RunID)
	}

	// POST /cancel — assert 200 and ok:true
	req, _ = http.NewRequest(http.MethodPost, "http://"+addr+"/cancel", http.NoBody)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Avenor-Token", token)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("POST /cancel: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /cancel: %d, want 200", resp.StatusCode)
	}
	var cancelResult map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&cancelResult); err != nil {
		t.Fatalf("decode /cancel: %v", err)
	}
	if v, _ := cancelResult["ok"].(bool); !v {
		t.Fatalf("cancel ok = %v, want true", cancelResult["ok"])
	}

	// POST /answer-permission with no pending permission — assert 409
	req, _ = http.NewRequest(http.MethodPost, "http://"+addr+"/answer-permission", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Avenor-Token", token)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("POST /answer-permission: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("POST /answer-permission: %d, want 409", resp.StatusCode)
	}
}

func TestPromptDispatch(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := mustDial(t, path)
	defer c.Close()
	params, _ := json.Marshal(map[string]any{"text": "do something"})
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "prompt", Params: params})
	r := readResp(t, c)
	if r.Error != nil {
		t.Fatalf("prompt error: %+v", r.Error)
	}
	res, ok := r.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type: %T", r.Result)
	}
	if v, _ := res["accepted"].(bool); !v {
		t.Fatal("expected accepted=true")
	}
}

func TestInterruptAndPromptDispatch(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	ch := s.InterruptChan()
	c := mustDial(t, path)
	defer c.Close()
	params, _ := json.Marshal(map[string]any{"text": "new task"})
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "interrupt_and_prompt", Params: params})
	r := readResp(t, c)
	if r.Error != nil {
		t.Fatalf("interrupt_and_prompt error: %+v", r.Error)
	}
	res, ok := r.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type: %T", r.Result)
	}
	if v, _ := res["accepted"].(bool); !v {
		t.Fatal("expected accepted=true")
	}
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("expected InterruptChan to fire")
	}
}

func TestPromptRejectedForNonOwner(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c1 := mustDial(t, path)
	defer c1.Close()
	_ = writeReq(t, c1, Request{JSONRPC: "2.0", ID: 1, Method: "cancel"})
	r1 := readResp(t, c1)
	if r1.Error != nil {
		t.Fatalf("cancel error: %+v", r1.Error)
	}

	c2 := mustDial(t, path)
	defer c2.Close()
	params, _ := json.Marshal(map[string]any{"text": "hello"})
	_ = writeReq(t, c2, Request{JSONRPC: "2.0", ID: 2, Method: "prompt", Params: params})
	r2 := readResp(t, c2)
	if r2.Error == nil || r2.Error.Code != -32010 {
		t.Fatalf("expected permission_denied for prompt, got %+v", r2)
	}
}

func TestInterruptAndPromptRejectedForNonOwner(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c1 := mustDial(t, path)
	defer c1.Close()
	_ = writeReq(t, c1, Request{JSONRPC: "2.0", ID: 1, Method: "cancel"})
	r1 := readResp(t, c1)
	if r1.Error != nil {
		t.Fatalf("cancel error: %+v", r1.Error)
	}

	c2 := mustDial(t, path)
	defer c2.Close()
	params, _ := json.Marshal(map[string]any{"text": "new task"})
	_ = writeReq(t, c2, Request{JSONRPC: "2.0", ID: 2, Method: "interrupt_and_prompt", Params: params})
	r2 := readResp(t, c2)
	if r2.Error == nil || r2.Error.Code != -32010 {
		t.Fatalf("expected permission_denied for interrupt_and_prompt, got %+v", r2)
	}
}

func TestPromptMissingTextReturnsError(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := mustDial(t, path)
	defer c.Close()
	params, _ := json.Marshal(map[string]any{"text": ""})
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "prompt", Params: params})
	r := readResp(t, c)
	if r.Error == nil || r.Error.Code != -32602 {
		t.Fatalf("expected invalid params error, got %+v", r)
	}
}

func TestDequeuePromptOrder(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)

	s.QueuePrompt("prompt1")
	s.QueuePrompt("prompt2")

	if got := s.DequeuePrompt(); got != "prompt1" {
		t.Fatalf("first dequeue = %q, want prompt1", got)
	}
	if got := s.DequeuePrompt(); got != "prompt2" {
		t.Fatalf("second dequeue = %q, want prompt2", got)
	}
	if got := s.DequeuePrompt(); got != "" {
		t.Fatalf("third dequeue = %q, want empty", got)
	}
}

func testSocketPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(os.TempDir(), "avc-"+time.Now().Format("150405.000000")+".sock")
}

type mockStableHandler struct {
	spawnResult          any
	spawnErr             error
	listResult           any
	sendToParentCalled   int
	sendToParentMessages []string
}

func (m *mockStableHandler) Spawn(params json.RawMessage) (any, error) {
	return m.spawnResult, m.spawnErr
}

func (m *mockStableHandler) List() any {
	return m.listResult
}

func (m *mockStableHandler) Shutdown(mode string) error { return nil }

func (m *mockStableHandler) RuntimeStatus(runtimeID string) (any, error) {
	return map[string]any{"runtime_id": runtimeID, "status": "running"}, nil
}

func (m *mockStableHandler) RuntimeCancel(runtimeID string) error { return nil }

func (m *mockStableHandler) RuntimePrompt(runtimeID, text, requestID string) error { return nil }

func (m *mockStableHandler) RuntimeAnswerPermission(runtimeID, requestID, optionID string) error {
	return nil
}

func (m *mockStableHandler) RuntimeInterruptAndPrompt(runtimeID, text string, keepQueue bool) error {
	return nil
}

func (m *mockStableHandler) RuntimeSendToParent(runtimeID, message string) error {
	m.sendToParentCalled++
	m.sendToParentMessages = append(m.sendToParentMessages, message)
	return nil
}

func TestStableSpawnMethod(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	s.SetStableHandler(&mockStableHandler{
		spawnResult: map[string]any{"runtime_id": "rt_1", "session_id": "ses_x"},
	})
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := mustDial(t, path)
	defer c.Close()
	params, _ := json.Marshal(map[string]any{"prompt": "hello", "dir": "/tmp"})
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "spawn", Params: params})
	r := readResp(t, c)
	if r.Error != nil {
		t.Fatalf("spawn error: %+v", r.Error)
	}
	res, ok := r.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type: %T", r.Result)
	}
	if v, _ := res["runtime_id"].(string); v != "rt_1" {
		t.Errorf("runtime_id = %q, want rt_1", v)
	}
}

func TestStableListMethod(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	s.SetStableHandler(&mockStableHandler{
		listResult: []map[string]any{
			{"runtime_id": "rt_1", "status": "running"},
		},
	})
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := mustDial(t, path)
	defer c.Close()
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "list"})
	r := readResp(t, c)
	if r.Error != nil {
		t.Fatalf("list error: %+v", r.Error)
	}
	list, ok := r.Result.([]any)
	if !ok {
		t.Fatalf("result type: %T", r.Result)
	}
	if len(list) != 1 {
		t.Fatalf("len(list) = %d, want 1", len(list))
	}
}

func TestStableStatusWithRuntimeID(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	s.SetStableHandler(&mockStableHandler{})
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := mustDial(t, path)
	defer c.Close()
	params, _ := json.Marshal(map[string]any{"runtime_id": "rt_1"})
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "status", Params: params})
	r := readResp(t, c)
	if r.Error != nil {
		t.Fatalf("status error: %+v", r.Error)
	}
	res, ok := r.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type: %T", r.Result)
	}
	if v, _ := res["runtime_id"].(string); v != "rt_1" {
		t.Errorf("runtime_id = %q, want rt_1", v)
	}
}

func TestStableCancelWithRuntimeID(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	s.SetStableHandler(&mockStableHandler{})
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := mustDial(t, path)
	defer c.Close()
	params, _ := json.Marshal(map[string]any{"runtime_id": "rt_1"})
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "cancel", Params: params})
	r := readResp(t, c)
	if r.Error != nil {
		t.Fatalf("cancel error: %+v", r.Error)
	}
}

func TestStableSpawnRejectedWithoutHandler(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := mustDial(t, path)
	defer c.Close()
	params, _ := json.Marshal(map[string]any{"prompt": "hello", "dir": "/tmp"})
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "spawn", Params: params})
	r := readResp(t, c)
	if r.Error == nil || r.Error.Code != -32601 {
		t.Fatalf("expected -32601 method not found, got %+v", r.Error)
	}
}

func TestStableSendToParent(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	mock := &mockStableHandler{}
	s.SetStableHandler(mock)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := mustDial(t, path)
	defer c.Close()
	params, _ := json.Marshal(map[string]any{"runtime_id": "rt_1", "message": "help"})
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "send_to_parent", Params: params})
	resp := readResp(t, c)
	if resp.Error != nil {
		t.Fatalf("send_to_parent error: %+v", resp.Error)
	}
	if mock.sendToParentCalled != 1 {
		t.Errorf("sendToParentCalled = %d, want 1", mock.sendToParentCalled)
	}
	if len(mock.sendToParentMessages) != 1 || mock.sendToParentMessages[0] != "help" {
		t.Errorf("sendToParentMessages = %v, want [help]", mock.sendToParentMessages)
	}
}

func TestStableSendToParentMissingRuntimeID(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	mock := &mockStableHandler{}
	s.SetStableHandler(mock)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := mustDial(t, path)
	defer c.Close()
	params, _ := json.Marshal(map[string]any{"message": "help"})
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "send_to_parent", Params: params})
	resp := readResp(t, c)
	if resp.Error == nil {
		t.Fatal("expected error for missing runtime_id, got nil")
	}
	if mock.sendToParentCalled != 0 {
		t.Errorf("sendToParentCalled = %d, want 0 (should not be called)", mock.sendToParentCalled)
	}
}

func TestStableSendToParentMissingMessage(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	mock := &mockStableHandler{}
	s.SetStableHandler(mock)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := mustDial(t, path)
	defer c.Close()
	params, _ := json.Marshal(map[string]any{"runtime_id": "rt_1"})
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "send_to_parent", Params: params})
	resp := readResp(t, c)
	if resp.Error == nil {
		t.Fatal("expected error for missing message, got nil")
	}
	if mock.sendToParentCalled != 0 {
		t.Errorf("sendToParentCalled = %d, want 0 (should not be called)", mock.sendToParentCalled)
	}
}

func TestStableSendToParentMessageTooLarge(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	mock := &mockStableHandler{}
	s.SetStableHandler(mock)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := mustDial(t, path)
	defer c.Close()
	big := strings.Repeat("a", maxSendToParentMessageBytes+1)
	params, _ := json.Marshal(map[string]any{"runtime_id": "rt_1", "message": big})
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "send_to_parent", Params: params})
	resp := readResp(t, c)
	if resp.Error == nil {
		t.Fatal("expected error for oversized message, got nil")
	}
	if mock.sendToParentCalled != 0 {
		t.Errorf("sendToParentCalled = %d, want 0 (should not be called)", mock.sendToParentCalled)
	}
}
