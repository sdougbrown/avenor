package channeltools

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime/broker/brokertest"
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
	foundAsk := false
	foundPeers := false
	foundCancel := false
	for _, tool := range tools {
		m := tool.(map[string]any)
		if m["name"] == "avenor_send" {
			foundSend = true
		}
		if m["name"] == "avenor_ask" {
			foundAsk = true
		}
		if m["name"] == "avenor_peers" {
			foundPeers = true
		}
		if m["name"] == "avenor_cancel" {
			foundCancel = true
		}
	}
	if !foundSend || !foundAsk || !foundPeers || !foundCancel {
		t.Fatalf("tools/list missing send/ask/peers/cancel: %#v", tools)
	}
}

func TestAvenorUpsendToolCallIncludesRole(t *testing.T) {
	var sentBody map[string]any
	brokerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sessions":
			_ = json.NewEncoder(w).Encode(map[string]any{"messaging_protocol": 1, "generation": "gen-1"})
		case "/send":
			if err := json.NewDecoder(r.Body).Decode(&sentBody); err != nil {
				t.Errorf("decode send: %v", err)
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"queued": true, "outcome": "sent"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer brokerSrv.Close()

	input := bytes.NewBufferString(
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}` + "\n" +
			`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"avenor_send","arguments":{"to_run_id":"parent_run","message":"status update","role":"reviewer"}}}` + "\n",
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
	if text != `sent message to run "parent_run"` {
		t.Fatalf("avenor_send result text = %q", text)
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
	if sentBody["expects_reply"] != false {
		t.Fatalf("expects_reply = %v, want false for a fire-and-forget send", sentBody["expects_reply"])
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
	cases := []struct {
		name           string
		sessionsStatus int
		sessionsBody   string
		sendStatus     int
		sendBody       string
		want           string
	}{
		{
			name:           "target not available",
			sessionsStatus: http.StatusOK,
			sessionsBody:   `{"messaging_protocol":1,"generation":"gen-1"}`,
			sendStatus:     http.StatusNotFound,
			sendBody:       `{"error":"to run not found","code":"target-unavailable","outcome":"not-sent"}`,
			want:           "send to target_run: messaging: target unavailable",
		},
		{
			name:           "run token rejected",
			sessionsStatus: http.StatusUnauthorized,
			sessionsBody:   "unauthorized",
			sendStatus:     http.StatusUnauthorized,
			sendBody:       "unauthorized",
			want:           "send to target_run: messaging: credentials rejected: local broker rejected the run token",
		},
		{
			name:           "admission uncertain",
			sessionsStatus: http.StatusOK,
			sessionsBody:   `{"messaging_protocol":1,"generation":"gen-1"}`,
			sendStatus:     http.StatusInternalServerError,
			sendBody:       `{"error":"boom","outcome":"possibly-pending"}`,
			want:           "send to target_run: possibly-pending",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			brokerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/sessions":
					w.WriteHeader(tc.sessionsStatus)
					_, _ = io.WriteString(w, tc.sessionsBody)
				case "/send":
					w.WriteHeader(tc.sendStatus)
					_, _ = io.WriteString(w, tc.sendBody)
				default:
					http.NotFound(w, r)
				}
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
			if errResp["message"] != tc.want {
				t.Fatalf("broker failure message = %v, want %q", errResp["message"], tc.want)
			}
		})
	}
}

// TestChanneltoolsStandaloneLocalAsk drives avenor_ask end to end against a real
// local broker: the tool sends a versioned ask under its own sender name, the
// target answers over the broker, and the tool renders the reply.
func TestChanneltoolsStandaloneLocalAsk(t *testing.T) {
	brokertest.RunStandaloneLocalAsk(t, brokertest.StandaloneAsk{
		Sender:       "channel-child",
		ReplyMessage: "here is the status",
		WantText:     "Reply from parent_run:\nhere is the status",
		Run: func(ctx context.Context, in brokertest.StandaloneAskRun) error {
			return Run(ctx, Options{
				RunID:     in.RunID,
				Sender:    in.Sender,
				Token:     in.Token,
				BrokerURL: in.BrokerURL,
				Stdin:     in.Stdin,
				Stdout:    in.Stdout,
				Stderr:    io.Discard,
			})
		},
	})
}
