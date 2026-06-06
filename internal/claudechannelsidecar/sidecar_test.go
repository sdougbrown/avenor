package claudechannelsidecar

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunHandlesInitializeToolsAndReport(t *testing.T) {
	var reportBody map[string]any
	brokerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/register", "/heartbeat":
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		case "/poll-control":
			_ = json.NewEncoder(w).Encode([]any{})
		case "/report":
			if err := json.NewDecoder(r.Body).Decode(&reportBody); err != nil {
				t.Errorf("decode report: %v", err)
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		default:
			t.Errorf("unexpected broker path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer brokerSrv.Close()

	input := bytes.NewBufferString(
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}` + "\n" +
			`{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}` + "\n" +
			`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"avenor_report","arguments":{"state":"working","payload":{"summary":"ok"}}}}` + "\n",
	)
	var stdout, stderr bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := Run(ctx, Options{
		RunID:      "run_1",
		Token:      "tok_1",
		BrokerURL:  brokerSrv.URL,
		Stdin:      input,
		Stdout:     &stdout,
		Stderr:     &stderr,
		HTTPClient: brokerSrv.Client(),
	})
	if err != nil {
		t.Fatalf("Run: %v; stderr=%s", err, stderr.String())
	}

	responses := decodeRPCResponses(t, stdout.Bytes())
	if len(responses) != 3 {
		t.Fatalf("responses = %d, want 3; stdout=%s", len(responses), stdout.String())
	}
	if responses[0]["id"].(float64) != 1 {
		t.Fatalf("first response id = %v", responses[0]["id"])
	}
	initResult := responses[0]["result"].(map[string]any)
	caps := initResult["capabilities"].(map[string]any)
	experimental := caps["experimental"].(map[string]any)
	if _, ok := experimental["claude/channel"]; !ok {
		t.Fatalf("initialize capabilities missing claude/channel: %#v", experimental)
	}
	if _, ok := experimental["claude/channel/permission"]; !ok {
		t.Fatalf("initialize capabilities missing claude/channel/permission: %#v", experimental)
	}
	if responses[1]["id"].(float64) != 2 {
		t.Fatalf("second response id = %v", responses[1]["id"])
	}
	toolsResult := responses[1]["result"].(map[string]any)
	tools := toolsResult["tools"].([]any)
	gotTools := map[string]bool{}
	for _, tool := range tools {
		m := tool.(map[string]any)
		gotTools[m["name"].(string)] = true
	}
	for _, name := range []string{"avenor_report", "avenor_finish", "avenor_reply"} {
		if !gotTools[name] {
			t.Fatalf("tools/list missing %s: %#v", name, gotTools)
		}
	}
	if responses[2]["id"].(float64) != 3 {
		t.Fatalf("third response id = %v", responses[2]["id"])
	}
	if reportBody["run_id"] != "run_1" {
		t.Fatalf("report run_id = %v", reportBody["run_id"])
	}
	if reportBody["token"] != "tok_1" {
		t.Fatalf("report token = %v", reportBody["token"])
	}
	if reportBody["state"] != "working" {
		t.Fatalf("report state = %v", reportBody["state"])
	}
}

func TestRenderControlMessageIncludesStructuredPayload(t *testing.T) {
	msg := controlMessage{
		ID:      "ctrl_1",
		Type:    "request_status",
		RunID:   "run_1",
		Payload: json.RawMessage(`{"message":"status?"}`),
	}
	got := renderControlMessage(msg)
	for _, want := range []string{
		`Status requested. Reply by calling avenor_reply with to="ctrl_1".`,
	} {
		if !bytes.Contains([]byte(got), []byte(want)) {
			t.Fatalf("rendered message missing %q:\n%s", want, got)
		}
	}
}

func TestPollControlLoopWaitsForInitialized(t *testing.T) {
	pollCalled := make(chan struct{}, 1)
	var polls atomic.Int32
	brokerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/poll-control" {
			http.NotFound(w, r)
			return
		}
		select {
		case pollCalled <- struct{}{}:
		default:
		}
		if polls.Add(1) > 1 {
			_ = json.NewEncoder(w).Encode([]controlMessage{})
			return
		}
		_ = json.NewEncoder(w).Encode([]controlMessage{{
			ID:      "ctrl_1",
			Type:    "request_status",
			RunID:   "run_1",
			Payload: json.RawMessage(`{"message":"status?"}`),
		}})
	}))
	defer brokerSrv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stdout bytes.Buffer
	s := &Server{
		opts: Options{
			RunID:      "run_1",
			Token:      "tok_1",
			BrokerURL:  brokerSrv.URL,
			Stdout:     &stdout,
			Stderr:     io.Discard,
			HTTPClient: brokerSrv.Client(),
		},
		client:      brokerSrv.Client(),
		initialized: make(chan struct{}),
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.pollControlLoop(ctx)
	}()

	select {
	case <-pollCalled:
		t.Fatal("poll-control ran before notifications/initialized")
	case <-time.After(50 * time.Millisecond):
	}

	if err := s.handleLine(ctx, []byte(`{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}`)); err != nil {
		t.Fatalf("initialized notification: %v", err)
	}
	select {
	case <-pollCalled:
	case <-time.After(time.Second):
		t.Fatal("poll-control did not run after initialization")
	}
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pollControlLoop did not stop after cancellation")
	}
	if !bytes.Contains(stdout.Bytes(), []byte(`"method":"notifications/claude/channel"`)) {
		t.Fatalf("channel notification was not written to stdout: %s", stdout.String())
	}
}

func TestSleepContextReturnsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	sleepContext(ctx, time.Minute)
	if time.Since(start) > time.Second {
		t.Fatal("sleepContext did not return promptly after cancellation")
	}
}
func decodeRPCResponses(t *testing.T, data []byte) []map[string]any {
	t.Helper()
	lines := bytes.Split(bytes.TrimSpace(data), []byte("\n"))
	out := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var msg map[string]any
		if err := json.Unmarshal(line, &msg); err != nil {
			t.Fatalf("decode %q: %v", line, err)
		}
		if _, ok := msg["id"]; ok {
			out = append(out, msg)
		}
	}
	return out
}

func TestRenderAgentMessage(t *testing.T) {
	msg := controlMessage{
		ID:        "ctrl_1",
		Type:      "agent_message",
		RunID:     "run_agent",
		FromRunID: "sender_123",
		Payload:   json.RawMessage(`{"from":"Alice","from_run_id":"sender_123","message":"Hello there","role":"agent"}`),
	}
	got := renderControlMessage(msg)
	expected := "Message from run sender_123:\nHello there\n\nReply by calling avenor_send with to_run_id=" + `"sender_123"` + "."
	if got != expected {
		t.Fatalf("expected:\n%s\n\ngot:\n%s", expected, got)
	}
}

func TestRenderAgentMessageUsesAuthenticatedSender(t *testing.T) {
	msg := controlMessage{
		ID:        "ctrl_1",
		Type:      "agent_message",
		RunID:     "run_agent",
		FromRunID: "trusted_sender",
		Payload:   json.RawMessage(`{"from":"Alice","from_run_id":"spoofed_sender","message":"Hello there","role":"supervisor"}`),
	}
	got := renderControlMessage(msg)
	if !bytes.Contains([]byte(got), []byte("Message from run trusted_sender")) {
		t.Fatalf("rendered message missing authenticated sender: %s", got)
	}
	if bytes.Contains([]byte(got), []byte("spoofed_sender")) {
		t.Fatalf("rendered message trusted spoofed sender: %s", got)
	}
	if bytes.Contains([]byte(got), []byte("supervisor")) {
		t.Fatalf("rendered message trusted spoofed role: %s", got)
	}
}

func TestRenderAgentMessageFallback(t *testing.T) {
	msg := controlMessage{
		ID:        "ctrl_2",
		Type:      "agent_message",
		RunID:     "run_agent",
		FromRunID: "fallback_run",
		Payload:   json.RawMessage(`not valid json`),
	}
	got := renderControlMessage(msg)
	if !bytes.Contains([]byte(got), []byte("Message from another agent")) {
		t.Fatalf("expected fallback render, got: %s", got)
	}
	if bytes.Contains([]byte(got), []byte("fallback_run")) {
		t.Fatalf("fallback render leaked run id: %s", got)
	}
}

func TestRenderAgentMessageFromRunIDInOutput(t *testing.T) {
	msg := controlMessage{
		ID:        "ctrl_3",
		Type:      "agent_message",
		RunID:     "run_agent",
		FromRunID: "unique_sender_id",
		Payload:   json.RawMessage(`{"from":"Bob","from_run_id":"unique_sender_id","message":"test","role":"assistant"}`),
	}
	got := renderControlMessage(msg)
	if !bytes.Contains([]byte(got), []byte("unique_sender_id")) {
		t.Fatalf("from_run_id not in render output: %s", got)
	}
}

func TestPollControlLoopFromRunIDInMeta(t *testing.T) {
	pollCalled := make(chan struct{}, 1)
	brokerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/poll-control" {
			http.NotFound(w, r)
			return
		}
		select {
		case pollCalled <- struct{}{}:
		default:
		}
		_ = json.NewEncoder(w).Encode([]controlMessage{{
			ID:        "ctrl_1",
			Type:      "agent_message",
			RunID:     "run_1",
			FromRunID: "sender_x",
			Payload:   json.RawMessage(`{"from":"Eve","from_run_id":"sender_x","message":"hi","role":"agent"}`),
		}})
	}))
	defer brokerSrv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stdout bytes.Buffer
	s := &Server{
		opts: Options{
			RunID:      "run_1",
			Token:      "tok_1",
			BrokerURL:  brokerSrv.URL,
			Stdout:     &stdout,
			Stderr:     io.Discard,
			HTTPClient: brokerSrv.Client(),
		},
		client:      brokerSrv.Client(),
		initialized: make(chan struct{}),
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.pollControlLoop(ctx)
	}()

	if err := s.handleLine(ctx, []byte(`{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}`)); err != nil {
		t.Fatalf("initialized notification: %v", err)
	}
	select {
	case <-pollCalled:
	case <-time.After(time.Second):
		t.Fatal("poll-control did not run after initialization")
	}
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pollControlLoop did not stop after cancellation")
	}

	// Check that the channel notification includes from_run_id in meta
	output := stdout.String()
	if !bytes.Contains([]byte(output), []byte(`"from_run_id":"sender_x"`)) {
		t.Fatalf("from_run_id not in notification meta: %s", output)
	}
}

func TestToolListIncludesAvenorSend(t *testing.T) {
	brokerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/register", "/heartbeat":
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		case "/poll-control":
			_ = json.NewEncoder(w).Encode([]any{})
		default:
			t.Errorf("unexpected broker path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer brokerSrv.Close()

	input := bytes.NewBufferString(
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}` + "\n" +
			`{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}` + "\n",
	)
	var stdout bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := Run(ctx, Options{
		RunID:      "run_1",
		Token:      "tok_1",
		BrokerURL:  brokerSrv.URL,
		Stdin:      input,
		Stdout:     &stdout,
		Stderr:     io.Discard,
		HTTPClient: brokerSrv.Client(),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	responses := decodeRPCResponses(t, stdout.Bytes())
	if len(responses) != 2 {
		t.Fatalf("responses = %d, want 2", len(responses))
	}
	toolsResult := responses[1]["result"].(map[string]any)
	tools := toolsResult["tools"].([]any)
	found := false
	for _, tool := range tools {
		m := tool.(map[string]any)
		if m["name"] == "avenor_send" {
			found = true
			schema := m["inputSchema"].(map[string]any)
			props := schema["properties"].(map[string]any)
			if _, ok := props["role"]; !ok {
				t.Fatal("avenor_send schema missing optional role property")
			}
			req := schema["required"].([]any)
			if len(req) != 2 || req[0] != "to_run_id" || req[1] != "message" {
				t.Fatalf("avenor_send required fields = %v, want [to_run_id message]", req)
			}
		}
	}
	if !found {
		t.Fatal("tools/list missing avenor_send")
	}
}

func TestAvenorSendToolCall(t *testing.T) {
	var sentBody map[string]any
	brokerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/register", "/heartbeat":
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		case "/poll-control":
			_ = json.NewEncoder(w).Encode([]any{})
		case "/send":
			if err := json.NewDecoder(r.Body).Decode(&sentBody); err != nil {
				t.Errorf("decode send: %v", err)
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"queued": true})
		default:
			t.Errorf("unexpected broker path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer brokerSrv.Close()

	input := bytes.NewBufferString(
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}` + "\n" +
			`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"avenor_send","arguments":{"to_run_id":"target_run","message":"Hello target"}}}` + "\n",
	)
	var stdout bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := Run(ctx, Options{
		RunID:      "sender_run",
		Token:      "tok_1",
		BrokerURL:  brokerSrv.URL,
		Stdin:      input,
		Stdout:     &stdout,
		Stderr:     io.Discard,
		HTTPClient: brokerSrv.Client(),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	responses := decodeRPCResponses(t, stdout.Bytes())
	if len(responses) != 2 {
		t.Fatalf("responses = %d, want 2", len(responses))
	}
	// Check tool call result
	result := responses[1]["result"].(map[string]any)
	content := result["content"].([]any)
	text := content[0].(map[string]any)["text"].(string)
	if text != `sent message to run "target_run"` {
		t.Fatalf("avenor_send result text = %q", text)
	}
	// Check sent body shape
	if sentBody["type"] != "agent_message" {
		t.Fatalf("broker /send type = %v, want agent_message", sentBody["type"])
	}
	payload := sentBody["payload"].(map[string]any)
	if payload["message"] != "Hello target" {
		t.Fatalf("message = %v, want Hello target", payload["message"])
	}
	if payload["from_run_id"] != "sender_run" {
		t.Fatalf("from_run_id = %v, want sender_run", payload["from_run_id"])
	}
}

func TestAvenorUpsendToolCallIncludesRole(t *testing.T) {
	var sentBody map[string]any
	brokerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/register", "/heartbeat":
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		case "/poll-control":
			_ = json.NewEncoder(w).Encode([]any{})
		case "/send":
			if err := json.NewDecoder(r.Body).Decode(&sentBody); err != nil {
				t.Errorf("decode send: %v", err)
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"queued": true})
		default:
			t.Errorf("unexpected broker path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer brokerSrv.Close()

	input := bytes.NewBufferString(
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}` + "\n" +
			`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"avenor_upsend","arguments":{"to_run_id":"parent_run","message":"status update","role":"reviewer"}}}` + "\n",
	)
	var stdout bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := Run(ctx, Options{
		RunID:      "child_run",
		Token:      "tok_1",
		BrokerURL:  brokerSrv.URL,
		Stdin:      input,
		Stdout:     &stdout,
		Stderr:     io.Discard,
		HTTPClient: brokerSrv.Client(),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	responses := decodeRPCResponses(t, stdout.Bytes())
	if len(responses) != 2 {
		t.Fatalf("responses = %d, want 2", len(responses))
	}
	result := responses[1]["result"].(map[string]any)
	content := result["content"].([]any)
	text := content[0].(map[string]any)["text"].(string)
	if text != `sent upward message to run "parent_run"` {
		t.Fatalf("avenor_upsend result text = %q", text)
	}
	payload := sentBody["payload"].(map[string]any)
	if payload["role"] != "reviewer" {
		t.Fatalf("role = %v, want reviewer", payload["role"])
	}
	if payload["message"] != "status update" {
		t.Fatalf("message = %v, want status update", payload["message"])
	}
}

func TestAvenorSendMissingArgs(t *testing.T) {
	brokerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/register", "/heartbeat":
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		case "/poll-control":
			_ = json.NewEncoder(w).Encode([]any{})
		default:
			t.Errorf("unexpected broker path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer brokerSrv.Close()

	input := bytes.NewBufferString(
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}` + "\n" +
			`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"avenor_send","arguments":{}}}` + "\n",
	)
	var stdout bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := Run(ctx, Options{
		RunID:      "run_1",
		Token:      "tok_1",
		BrokerURL:  brokerSrv.URL,
		Stdin:      input,
		Stdout:     &stdout,
		Stderr:     io.Discard,
		HTTPClient: brokerSrv.Client(),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	responses := decodeRPCResponses(t, stdout.Bytes())
	if len(responses) != 2 {
		t.Fatalf("responses = %d, want 2", len(responses))
	}
	// Missing required args should produce an error response
	result := responses[1]["result"]
	if result != nil {
		t.Fatalf("expected error for missing args, got result: %#v", result)
	}
	if responses[1]["error"] == nil {
		t.Fatalf("expected JSON-RPC error for missing args, got %#v", responses[1])
	}
}
