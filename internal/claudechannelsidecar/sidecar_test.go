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
	broker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	defer broker.Close()

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
		BrokerURL:  broker.URL,
		Stdin:      input,
		Stdout:     &stdout,
		Stderr:     &stderr,
		HTTPClient: broker.Client(),
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
	broker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	defer broker.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stdout bytes.Buffer
	s := &Server{
		opts: Options{
			RunID:      "run_1",
			Token:      "tok_1",
			BrokerURL:  broker.URL,
			Stdout:     &stdout,
			Stderr:     io.Discard,
			HTTPClient: broker.Client(),
		},
		client:      broker.Client(),
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
