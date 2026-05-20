package acp

import "testing"

func TestSessionOpenParams(t *testing.T) {
	got := sessionOpenParams("/repo", "sessionId", "ses_123")

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