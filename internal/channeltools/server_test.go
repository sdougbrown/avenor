package channeltools

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

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
			t.Fatalf("decode response: %v", err)
		}
		out = append(out, msg)
	}
	return out
}

func TestToolListIncludesAvenorUpsend(t *testing.T) {
	input := bytes.NewBufferString(
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}` + "\n" +
			`{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}` + "\n",
	)
	var stdout bytes.Buffer
	err := Run(context.Background(), Options{
		RunID:     "run_1",
		Token:     "tok_1",
		BrokerURL: "http://127.0.0.1:1",
		Stdin:     input,
		Stdout:    &stdout,
		Stderr:    io.Discard,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	responses := decodeRPCResponses(t, stdout.Bytes())
	tools := responses[1]["result"].(map[string]any)["tools"].([]any)
	foundSend := false
	foundUpsend := false
	for _, tool := range tools {
		m := tool.(map[string]any)
		switch m["name"] {
		case "avenor_send":
			foundSend = true
		case "avenor_upsend":
			foundUpsend = true
		}
	}
	if !foundSend || !foundUpsend {
		t.Fatalf("tools/list missing send tools: %#v", tools)
	}
}

func TestAvenorUpsendToolCallIncludesRole(t *testing.T) {
	var sentBody map[string]any
	brokerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/send":
			if err := json.NewDecoder(r.Body).Decode(&sentBody); err != nil {
				t.Errorf("decode send: %v", err)
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"queued": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer brokerSrv.Close()

	input := bytes.NewBufferString(
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}` + "\n" +
			`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"avenor_upsend","arguments":{"to_run_id":"parent_run","message":"status update","role":"reviewer"}}}` + "\n",
	)
	var stdout bytes.Buffer
	err := Run(context.Background(), Options{
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
	text := responses[1]["result"].(map[string]any)["content"].([]any)[0].(map[string]any)["text"].(string)
	if text != `sent upward message to run "parent_run"` {
		t.Fatalf("avenor_upsend result text = %q", text)
	}
	payload := sentBody["payload"].(map[string]any)
	if payload["message"] != "status update" {
		t.Fatalf("message = %v, want status update", payload["message"])
	}
	if payload["role"] != "reviewer" {
		t.Fatalf("role = %v, want reviewer", payload["role"])
	}
	if payload["from_run_id"] != "child_run" {
		t.Fatalf("from_run_id = %v, want child_run", payload["from_run_id"])
	}
	if sentBody["to_run_id"] != "parent_run" {
		t.Fatalf("to_run_id = %v, want parent_run", sentBody["to_run_id"])
	}
}

func TestAvenorSendMissingArgs(t *testing.T) {
	input := bytes.NewBufferString(
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}` + "\n" +
			`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"avenor_send","arguments":{}}}` + "\n",
	)
	var stdout bytes.Buffer
	err := Run(context.Background(), Options{
		RunID:     "run_1",
		Token:     "tok_1",
		BrokerURL: "http://127.0.0.1:1",
		Stdin:     input,
		Stdout:    &stdout,
		Stderr:    io.Discard,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	responses := decodeRPCResponses(t, stdout.Bytes())
	errResp, ok := responses[1]["error"].(map[string]any)
	if !ok {
		t.Fatalf("expected JSON-RPC error for missing args, got %#v", responses[1])
	}
	if errResp["message"] != "to_run_id and message are required" {
		t.Fatalf("error message = %v, want validation failure", errResp["message"])
	}
}

func TestAvenorSendBrokerFailure(t *testing.T) {
	brokerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer brokerSrv.Close()

	input := bytes.NewBufferString(
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}` + "\n" +
			`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"avenor_send","arguments":{"to_run_id":"target_run","message":"hello"}}}` + "\n",
	)
	var stdout bytes.Buffer
	err := Run(context.Background(), Options{
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
	errResp, ok := responses[1]["error"].(map[string]any)
	if !ok {
		t.Fatalf("expected JSON-RPC error for broker failure, got %#v", responses[1])
	}
	if errResp["message"] != "broker /send: 500 Internal Server Error: boom" {
		t.Fatalf("broker failure message = %v", errResp["message"])
	}
}
