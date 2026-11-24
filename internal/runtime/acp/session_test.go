package acp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestSessionOpenParams(t *testing.T) {
	got := sessionOpenParams("/repo", SessionOpenOptions{}, "sessionId", "ses_123")

	if got["cwd"] != "/repo" {
		t.Fatalf("cwd = %v, want /repo", got["cwd"])
	}
	if got["sessionId"] != "ses_123" {
		t.Fatalf("sessionId = %v, want ses_123", got["sessionId"])
	}
	servers, ok := got["mcpServers"].([]any)
	if !ok {
		t.Fatalf("mcpServers has type %T, want []any", got["mcpServers"])
	}
	if len(servers) != 0 {
		t.Fatalf("mcpServers len = %d, want 0", len(servers))
	}
}

func TestSessionOpenParamsWithMCPServersCopiesAndNormalizesNilSlices(t *testing.T) {
	origArgs := []string{"channel-tools", "--run-id", "run_1"}
	origEnv := []EnvVar{{Name: "FOO", Value: "BAR"}}
	input := SessionOpenOptions{
		MCPServers: []MCPServer{
			{
				Name:    "nil-slices",
				Command: "/usr/bin/avenor",
				Args:    nil,
				Env:     nil,
			},
			{
				Name:    "populated",
				Command: "/usr/bin/avenor",
				Args:    origArgs,
				Env:     origEnv,
			},
		},
	}

	got := sessionOpenParams("/repo", input, "sessionId", "ses_mcp")

	// Caller struct must not be mutated.
	if input.MCPServers[0].Args != nil {
		t.Fatalf("caller Args was mutated to non-nil: %#v", input.MCPServers[0].Args)
	}
	if input.MCPServers[0].Env != nil {
		t.Fatalf("caller Env was mutated to non-nil: %#v", input.MCPServers[0].Env)
	}

	servers, ok := got["mcpServers"].([]MCPServer)
	if !ok {
		t.Fatalf("mcpServers has type %T, want []MCPServer", got["mcpServers"])
	}
	if len(servers) != 2 {
		t.Fatalf("len(servers) = %d, want 2", len(servers))
	}
	if servers[0].Args == nil || len(servers[0].Args) != 0 {
		t.Fatalf("servers[0].Args = %#v, want non-nil empty slice", servers[0].Args)
	}
	if servers[0].Env == nil || len(servers[0].Env) != 0 {
		t.Fatalf("servers[0].Env = %#v, want non-nil empty slice", servers[0].Env)
	}

	// Mutating returned slices must not affect caller slices.
	servers[1].Args[0] = "mutated"
	servers[1].Env[0].Value = "mutated"
	if origArgs[0] != "channel-tools" {
		t.Fatalf("origArgs mutated: %#v", origArgs)
	}
	if origEnv[0].Value != "BAR" {
		t.Fatalf("origEnv mutated: %#v", origEnv)
	}

	// JSON serialization must emit [] for nil Args/Env, never null.
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	encoded := string(raw)
	if strings.Contains(encoded, `"args":null`) || strings.Contains(encoded, `"env":null`) {
		t.Fatalf("serialized null slice in JSON: %s", encoded)
	}
	if !strings.Contains(encoded, `"args":[]`) || !strings.Contains(encoded, `"env":[]`) {
		t.Fatalf("expected empty JSON arrays in %s", encoded)
	}
}

func TestClientSessionMethodsSendExpectedRPC(t *testing.T) {
	var methods []string
	var paramsList []map[string]any

	var client *Client
	stdin := &rpcFuncWriteCloser{
		fn: func(msg rpcEnvelope) {
			methods = append(methods, msg.Method)
			var p map[string]any
			if err := json.Unmarshal(msg.Params, &p); err != nil {
				t.Fatalf("unmarshal params: %v", err)
			}
			paramsList = append(paramsList, p)
			client.pendingMu.Lock()
			ch := client.pending[string(msg.ID)]
			client.pendingMu.Unlock()
			if ch != nil {
				ch <- rpcEnvelope{
					ID:     msg.ID,
					Result: json.RawMessage(`{"sessionId":"ses_new"}`),
				}
			}
		},
	}
	client = &Client{
		stdin:   stdin,
		dir:     "/workspace",
		done:    make(chan struct{}),
		pending: map[string]chan rpcEnvelope{},
	}

	ctx := context.Background()
	opts := SessionOpenOptions{
		MCPServers: []MCPServer{{Name: "avenor-channel-tools", Command: "/bin/avenor"}},
	}

	sNew, err := client.NewSessionWithOptions(ctx, opts)
	if err != nil {
		t.Fatalf("NewSessionWithOptions: %v", err)
	}
	if sNew.SessionID != "ses_new" {
		t.Fatalf("NewSessionWithOptions SessionID = %q, want ses_new", sNew.SessionID)
	}

	sLoad, err := client.LoadSession(ctx, "ses_load")
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if sLoad.SessionID != "ses_load" {
		t.Fatalf("LoadSession SessionID = %q, want ses_load", sLoad.SessionID)
	}

	sResumePlain, err := client.ResumeSession(ctx, "ses_resume_plain")
	if err != nil {
		t.Fatalf("ResumeSession: %v", err)
	}
	if sResumePlain.SessionID != "ses_resume_plain" {
		t.Fatalf("ResumeSession SessionID = %q, want ses_resume_plain", sResumePlain.SessionID)
	}

	sResumeOpts, err := client.ResumeSessionWithOptions(ctx, "ses_resume_opts", opts)
	if err != nil {
		t.Fatalf("ResumeSessionWithOptions: %v", err)
	}
	if sResumeOpts.SessionID != "ses_resume_opts" {
		t.Fatalf("ResumeSessionWithOptions SessionID = %q, want ses_resume_opts", sResumeOpts.SessionID)
	}

	wantMethods := []string{"session/new", "session/load", "session/resume", "session/resume"}
	if len(methods) != len(wantMethods) {
		t.Fatalf("methods = %v, want %v", methods, wantMethods)
	}
	for i, want := range wantMethods {
		if methods[i] != want {
			t.Fatalf("methods[%d] = %q, want %q", i, methods[i], want)
		}
	}
	if got := len(paramsList[0]["mcpServers"].([]any)); got != 1 {
		t.Fatalf("session/new mcpServers len = %d, want 1", got)
	}
	if got := len(paramsList[1]["mcpServers"].([]any)); got != 0 {
		t.Fatalf("session/load mcpServers len = %d, want 0", got)
	}
	if got := len(paramsList[2]["mcpServers"].([]any)); got != 0 {
		t.Fatalf("plain session/resume mcpServers len = %d, want 0", got)
	}
	if got := len(paramsList[3]["mcpServers"].([]any)); got != 1 {
		t.Fatalf("option session/resume mcpServers len = %d, want 1", got)
	}
}

type rpcFuncWriteCloser struct {
	fn func(rpcEnvelope)
}

func (w *rpcFuncWriteCloser) Write(p []byte) (int, error) {
	var msg rpcEnvelope
	if err := json.Unmarshal(p, &msg); err == nil {
		w.fn(msg)
	}
	return len(p), nil
}

func (*rpcFuncWriteCloser) Close() error { return nil }

func TestSetSessionModeParams(t *testing.T) {
	got := setSessionModeParams("ses_123", "jockey")

	if got["sessionId"] != "ses_123" {
		t.Fatalf("sessionId = %v, want ses_123", got["sessionId"])
	}
	if got["modeId"] != "jockey" {
		t.Fatalf("modeId = %v, want jockey", got["modeId"])
	}
}

func TestSetSessionModelParams(t *testing.T) {
	got := setSessionModelParams("ses_123", "github-copilot/gpt-5")

	if got["sessionId"] != "ses_123" {
		t.Fatalf("sessionId = %v, want ses_123", got["sessionId"])
	}
	if got["configId"] != "model" {
		t.Fatalf("configId = %v, want model", got["configId"])
	}
	if got["value"] != "github-copilot/gpt-5" {
		t.Fatalf("value = %v, want github-copilot/gpt-5", got["value"])
	}
}
