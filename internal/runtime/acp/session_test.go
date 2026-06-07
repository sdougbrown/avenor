package acp

import "testing"

func TestSessionOpenParams(t *testing.T) {
	got := sessionOpenParams("/repo", nil, "sessionId", "ses_123")

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

func TestSessionOpenParamsWithMCP(t *testing.T) {
	got := sessionOpenParams("/repo", map[string]any{"tool-a": map[string]any{"type": "local"}})

	mcp, ok := got["mcp"].(map[string]any)
	if !ok {
		t.Fatalf("mcp has type %T, want map[string]any", got["mcp"])
	}
	if len(mcp) != 1 {
		t.Fatalf("mcp len = %d, want 1", len(mcp))
	}
	server, ok := mcp["tool-a"].(map[string]any)
	if !ok {
		t.Fatalf("mcp[tool-a] has type %T, want map[string]any", mcp["tool-a"])
	}
	if server["type"] != "local" {
		t.Fatalf("server type = %v, want local", server["type"])
	}
}

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
