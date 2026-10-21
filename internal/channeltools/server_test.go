package channeltools

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
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

// runTool drives the server over stdio and returns the tool call response.
func runTool(t *testing.T, brokerURL string, client *http.Client, name string, args map[string]any) map[string]any {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 2, "method": "tools/call",
		"params": map[string]any{"name": name, "arguments": args},
	})
	if err != nil {
		t.Fatal(err)
	}
	input := bytes.NewBufferString(
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}` + "\n" + string(body) + "\n",
	)
	var stdout, stderr bytes.Buffer
	if err := Run(context.Background(), Options{
		RunID:      "child_run",
		Token:      "tok_1",
		BrokerURL:  brokerURL,
		Stdin:      input,
		Stdout:     &stdout,
		Stderr:     &stderr,
		HTTPClient: client,
	}); err != nil {
		t.Fatalf("Run: %v; stderr=%s", err, stderr.String())
	}
	responses := decodeRPCResponses(t, stdout.Bytes())
	if len(responses) != 2 {
		t.Fatalf("responses = %d, want 2; stdout=%s", len(responses), stdout.String())
	}
	return responses[1]
}

func toolContent(t *testing.T, response map[string]any) (string, map[string]any, bool) {
	t.Helper()
	result, ok := response["result"].(map[string]any)
	if !ok {
		t.Fatalf("response has no result: %#v", response)
	}
	content, _ := result["content"].([]any)
	text := ""
	if len(content) > 0 {
		first, _ := content[0].(map[string]any)
		text, _ = first["text"].(string)
	}
	structured, _ := result["structuredContent"].(map[string]any)
	isError, _ := result["isError"].(bool)
	return text, structured, isError
}

func TestToolsListIncludesMessageAndPeersOnly(t *testing.T) {
	input := bytes.NewBufferString(
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}` + "\n" +
			`{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}` + "\n",
	)
	var stdout bytes.Buffer
	if err := Run(context.Background(), Options{
		RunID: "run_1", Token: "tok_1", BrokerURL: "http://127.0.0.1:1",
		Stdin: input, Stdout: &stdout, Stderr: io.Discard,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	responses := decodeRPCResponses(t, stdout.Bytes())
	tools := responses[1]["result"].(map[string]any)["tools"].([]any)
	names := map[string]bool{}
	for _, tool := range tools {
		names[tool.(map[string]any)["name"].(string)] = true
	}
	for _, want := range []string{"avenor_message", "avenor_peers"} {
		if !names[want] {
			t.Fatalf("tools/list missing %s: %#v", want, names)
		}
	}
	if len(names) != 2 {
		t.Fatalf("tools/list advertises %d tools, want exactly message and peers: %#v", len(names), names)
	}
}

func TestChildReplyIsEdgeAbsentWithoutASend(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	brokerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		switch r.URL.Path {
		case "/sessions":
			_ = json.NewEncoder(w).Encode(map[string]any{"messaging_protocol": 1, "generation": "gen-1"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer brokerSrv.Close()

	response := runTool(t, brokerSrv.URL, brokerSrv.Client(), "avenor_message", map[string]any{
		"action": "reply", "reply_to": "msg_in", "message": "answer",
	})
	text, structured, isError := toolContent(t, response)
	if isError {
		t.Fatalf("reply reported isError: %s", text)
	}
	if structured["outcome"] != "edge-absent" {
		t.Fatalf("reply outcome = %v, want edge-absent", structured["outcome"])
	}
	if text == "" {
		t.Fatal("reply result has no text")
	}
	mu.Lock()
	defer mu.Unlock()
	for _, path := range paths {
		if path == "/send" {
			t.Fatalf("a reply with no inbound delivery reached /send: %v", paths)
		}
	}
}

func TestChildTellSendsNoReplyWithNoSender(t *testing.T) {
	var sentBody map[string]any
	brokerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sessions":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"messaging_protocol": 1,
				"generation":         "gen-1",
				"sessions": []any{
					map[string]any{"run_id": "child_run", "label": "child", "kind": "agent", "status": "running"},
					map[string]any{"run_id": "parent_run", "label": "parent", "kind": "supervisor", "status": "running"},
				},
			})
		case "/send":
			if err := json.NewDecoder(r.Body).Decode(&sentBody); err != nil {
				t.Errorf("decode send: %v", err)
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"outcome": "sent"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer brokerSrv.Close()

	response := runTool(t, brokerSrv.URL, brokerSrv.Client(), "avenor_message", map[string]any{
		"action": "tell", "target": "parent_run", "message": "status update",
	})
	text, structured, isError := toolContent(t, response)
	if isError {
		t.Fatalf("tell reported isError: %s", text)
	}
	if structured["outcome"] != "sent" {
		t.Fatalf("tell outcome = %v, want sent; text=%s", structured["outcome"], text)
	}
	if sentBody["expects_reply"] != false {
		t.Fatalf("expects_reply = %v, want false for a tell", sentBody["expects_reply"])
	}
	if sender, _ := sentBody["sender"].(string); sender != "" {
		t.Fatalf("sender = %q, want empty so the broker stamps the current display name", sender)
	}
	if sentBody["to_run_id"] != "parent_run" {
		t.Fatalf("to_run_id = %v", sentBody["to_run_id"])
	}
}

func TestChildPeersLastShapes(t *testing.T) {
	cases := []struct {
		name       string
		lastStatus int
		lastBody   map[string]any
		wantError  bool
		wantCode   string
		wantText   string
	}{
		{
			name:       "record with text",
			lastStatus: http.StatusOK,
			lastBody: map[string]any{
				"run_id": "rt_peer", "label": "mesh-a", "has_record": true,
				"final_output": "OK", "stop_reason": "end_turn",
			},
			wantText: lastRenderText(t, "header", map[string]string{"run_id": "rt_peer", "label": "mesh-a", "stop_reason": "end_turn"}) + "\n\nOK",
		},
		{
			name:       "record with empty text",
			lastStatus: http.StatusOK,
			lastBody: map[string]any{
				"run_id": "rt_peer", "label": "mesh-a", "has_record": true, "stop_reason": "cancelled",
			},
			wantText: lastRenderText(t, "no_text", map[string]string{"run_id": "rt_peer", "stop_reason": "cancelled"}),
		},
		{
			name:       "known run with no record",
			lastStatus: http.StatusOK,
			lastBody: map[string]any{
				"run_id": "rt_peer", "label": "mesh-a", "has_record": false,
			},
			wantText: lastRenderText(t, "empty", map[string]string{"run_id": "rt_peer"}),
		},
		{
			name:       "target unavailable",
			lastStatus: http.StatusNotFound,
			lastBody:   map[string]any{"error": "run not found: rt_peer", "code": "target-unavailable"},
			wantError:  true,
			wantCode:   "unknown_target",
			wantText:   "run not found: rt_peer",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotPath, gotTarget, gotRunID, gotToken string
			brokerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/sessions":
					_ = json.NewEncoder(w).Encode(map[string]any{
						"messaging_protocol": 1, "generation": "gen-1",
						"sessions": []any{map[string]any{"run_id": "rt_peer", "label": "mesh-a", "status": "running"}},
					})
				case "/last_message":
					gotPath = r.URL.Path
					gotTarget = r.URL.Query().Get("target")
					gotRunID = r.Header.Get("X-Avenor-Run-Id")
					gotToken = r.Header.Get("X-Avenor-Run-Token")
					w.WriteHeader(tc.lastStatus)
					_ = json.NewEncoder(w).Encode(tc.lastBody)
				default:
					http.NotFound(w, r)
				}
			}))
			defer brokerSrv.Close()

			response := runTool(t, brokerSrv.URL, brokerSrv.Client(), "avenor_peers", map[string]any{
				"action": "last", "target": "rt_peer",
			})
			text, structured, isError := toolContent(t, response)
			if gotPath != "/last_message" || gotTarget != "rt_peer" {
				t.Fatalf("last read %q target %q, want /last_message target rt_peer", gotPath, gotTarget)
			}
			if gotRunID != "child_run" || gotToken != "tok_1" {
				t.Fatalf("last credentials = %q/%q, want the caller's own", gotRunID, gotToken)
			}
			if text != tc.wantText {
				t.Fatalf("last text = %q, want %q", text, tc.wantText)
			}
			if isError != tc.wantError {
				t.Fatalf("isError = %v, want %v", isError, tc.wantError)
			}
			if tc.wantCode != "" && structured["code"] != tc.wantCode {
				t.Fatalf("code = %v, want %s", structured["code"], tc.wantCode)
			}
			if !tc.wantError {
				last, _ := structured["last"].(map[string]any)
				if last["runtime_id"] != "rt_peer" || last["label"] != "mesh-a" {
					t.Fatalf("last details = %#v", structured)
				}
			}
		})
	}
}

func TestChildPeersRenameReportsBothLabelsWithoutName(t *testing.T) {
	brokerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sessions":
			_ = json.NewEncoder(w).Encode(map[string]any{"messaging_protocol": 1, "generation": "gen-1"})
		case "/rename":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"run_id": "child_run", "label": "child-a-2", "previous_label": "child-a",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer brokerSrv.Close()

	response := runTool(t, brokerSrv.URL, brokerSrv.Client(), "avenor_peers", map[string]any{
		"action": "rename", "name": "child-a-2",
	})
	text, structured, isError := toolContent(t, response)
	if isError {
		t.Fatalf("rename reported isError: %s", text)
	}
	renamed, _ := structured["renamed"].(map[string]any)
	if renamed["label"] != "child-a-2" || renamed["previous_label"] != "child-a" {
		t.Fatalf("renamed details = %#v", structured)
	}
	if _, ok := structured["name"]; ok {
		t.Fatalf("rename result has a second name field: %#v", structured)
	}
	if !strings.Contains(text, "child-a-2") {
		t.Fatalf("rename text = %q", text)
	}
}

func TestChildPeersListNamesKind(t *testing.T) {
	brokerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sessions":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"messaging_protocol": 1, "generation": "gen-1",
				"sessions": []any{
					map[string]any{"run_id": "supervisor", "label": "main", "kind": "supervisor", "status": "running", "backend": "pi"},
					map[string]any{"run_id": "rt_7", "label": "child-a", "kind": "agent", "status": "running"},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer brokerSrv.Close()

	response := runTool(t, brokerSrv.URL, brokerSrv.Client(), "avenor_peers", map[string]any{"action": "list"})
	text, structured, _ := toolContent(t, response)
	if !strings.Contains(text, "main (supervisor) [supervisor, running, pi]") {
		t.Fatalf("list text does not name the supervisor kind: %q", text)
	}
	peers, _ := structured["peers"].([]any)
	if len(peers) != 2 {
		t.Fatalf("peers = %#v", structured["peers"])
	}
	first, _ := peers[0].(map[string]any)
	if first["kind"] != "supervisor" {
		t.Fatalf("peer kind = %v, want supervisor", first["kind"])
	}
}

// lastRenderText renders one peers `last` template from the shared contract
// fixture, so a surface test pins the renderer's use rather than the template
// text.
func lastRenderText(t *testing.T, key string, facts map[string]string) string {
	t.Helper()
	quoted := make(map[string]string, len(facts))
	for name, value := range facts {
		if name == "stop_reason" {
			quoted[name] = messaging.Sanitize(value)
			continue
		}
		quoted[name] = messaging.Quote(value)
	}
	return messaging.LastRender(key, quoted)
}
