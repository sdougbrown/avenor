package opencodeacp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
)

type fakeSessionConfigurer struct {
	modelCalls []modelCall
	modeCalls  []modeCall
}

type modelCall struct {
	sessionID string
	modelID   string
}

type modeCall struct {
	sessionID string
	modeID    string
}

func (f *fakeSessionConfigurer) SetSessionModel(_ context.Context, sessionID, modelID string) error {
	f.modelCalls = append(f.modelCalls, modelCall{sessionID, modelID})
	return nil
}

func (f *fakeSessionConfigurer) SetSessionMode(_ context.Context, sessionID, modeID string) error {
	f.modeCalls = append(f.modeCalls, modeCall{sessionID, modeID})
	return nil
}

func TestConfigureSessionAgentAndModel(t *testing.T) {
	fake := &fakeSessionConfigurer{}

	err := configureSession(context.Background(), fake, "ses_123", runtime.StartOptions{
		Agent: "jockey",
		Model: "deepseek/deepseek-v4-pro",
	})
	if err != nil {
		t.Fatalf("configureSession: %v", err)
	}

	if len(fake.modelCalls) != 1 {
		t.Fatalf("expected 1 SetSessionModel call, got %d", len(fake.modelCalls))
	}
	if fake.modelCalls[0].sessionID != "ses_123" {
		t.Errorf("model sessionID = %q, want ses_123", fake.modelCalls[0].sessionID)
	}
	if fake.modelCalls[0].modelID != "deepseek/deepseek-v4-pro" {
		t.Errorf("modelID = %q, want deepseek/deepseek-v4-pro", fake.modelCalls[0].modelID)
	}

	if len(fake.modeCalls) != 1 {
		t.Fatalf("expected 1 SetSessionMode call, got %d", len(fake.modeCalls))
	}
	if fake.modeCalls[0].sessionID != "ses_123" {
		t.Errorf("mode sessionID = %q, want ses_123", fake.modeCalls[0].sessionID)
	}
	if fake.modeCalls[0].modeID != "jockey" {
		t.Errorf("modeID = %q, want jockey", fake.modeCalls[0].modeID)
	}
}

func TestConfigureSessionOnlyModel(t *testing.T) {
	fake := &fakeSessionConfigurer{}

	err := configureSession(context.Background(), fake, "ses_456", runtime.StartOptions{
		Model: "gpt-4",
	})
	if err != nil {
		t.Fatalf("configureSession: %v", err)
	}

	if len(fake.modelCalls) != 1 {
		t.Fatalf("expected 1 model call, got %d", len(fake.modelCalls))
	}
	if fake.modelCalls[0].modelID != "gpt-4" {
		t.Errorf("modelID = %q, want gpt-4", fake.modelCalls[0].modelID)
	}
	if len(fake.modeCalls) != 0 {
		t.Fatalf("expected 0 mode calls, got %d", len(fake.modeCalls))
	}
}

func TestConfigureSessionOnlyAgent(t *testing.T) {
	fake := &fakeSessionConfigurer{}

	err := configureSession(context.Background(), fake, "ses_789", runtime.StartOptions{
		Agent: "mule",
	})
	if err != nil {
		t.Fatalf("configureSession: %v", err)
	}

	if len(fake.modeCalls) != 1 {
		t.Fatalf("expected 1 mode call, got %d", len(fake.modeCalls))
	}
	if fake.modeCalls[0].modeID != "mule" {
		t.Errorf("modeID = %q, want mule", fake.modeCalls[0].modeID)
	}
	if len(fake.modelCalls) != 0 {
		t.Fatalf("expected 0 model calls, got %d", len(fake.modelCalls))
	}
}

func TestConfigureSessionEmpty(t *testing.T) {
	fake := &fakeSessionConfigurer{}

	err := configureSession(context.Background(), fake, "ses_empty", runtime.StartOptions{})
	if err != nil {
		t.Fatalf("configureSession: %v", err)
	}

	if len(fake.modelCalls) != 0 || len(fake.modeCalls) != 0 {
		t.Fatalf("expected no calls, got model=%d mode=%d", len(fake.modelCalls), len(fake.modeCalls))
	}
}

func TestConfigureSessionModelError(t *testing.T) {
	fake := &errorConfigurer{modelErr: errors.New("model failed")}

	err := configureSession(context.Background(), fake, "ses_err", runtime.StartOptions{
		Model: "bad-model",
		Agent: "jockey",
	})
	if err == nil || err.Error() != "model failed" {
		t.Fatalf("expected 'model failed' error, got %v", err)
	}
	if fake.modeCalls != 0 {
		t.Fatalf("SetSessionMode was called %d times after SetSessionModel error — should have returned early", fake.modeCalls)
	}
}

func TestConfigureSessionModeError(t *testing.T) {
	fake := &errorConfigurer{modeErr: errors.New("mode failed")}

	err := configureSession(context.Background(), fake, "ses_err", runtime.StartOptions{
		Agent: "bad-agent",
	})
	if err == nil || err.Error() != "mode failed" {
		t.Fatalf("expected 'mode failed' error, got %v", err)
	}
}

type errorConfigurer struct {
	modelErr  error
	modeErr   error
	modeCalls int
}

func (e *errorConfigurer) SetSessionModel(_ context.Context, _, _ string) error {
	return e.modelErr
}

func (e *errorConfigurer) SetSessionMode(_ context.Context, _, _ string) error {
	e.modeCalls++
	return e.modeErr
}

func TestBuildClientEnv(t *testing.T) {
	env, cleanup := buildClientEnv(runtime.StartOptions{}, "run_123", "tok_456", "127.0.0.1:9999")
	if cleanup != nil {
		defer func() { _ = cleanup() }()
	}
	if len(env) != 1 {
		t.Fatalf("env len = %d, want 1", len(env))
	}
	cfgPath, ok := env["OPENCODE_CONFIG"]
	if !ok {
		t.Fatalf("missing OPENCODE_CONFIG: %#v", env)
	}
	if cfgPath == "" {
		t.Fatal("OPENCODE_CONFIG is empty")
	}

	// Read the written config file
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config file: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode config: %v", err)
	}
	mcp, ok := payload["mcp"].(map[string]any)
	if !ok {
		t.Fatalf("payload mcp has type %T, want map[string]any", payload["mcp"])
	}
	server, ok := mcp["avenor-channel-tools"].(map[string]any)
	if !ok {
		t.Fatalf("server has type %T, want map[string]any", mcp["avenor-channel-tools"])
	}
	if server["type"] != "local" {
		t.Fatalf("server type = %v, want local", server["type"])
	}
	if server["enabled"] != true {
		t.Fatalf("server enabled = %v, want true", server["enabled"])
	}
	commandRaw, ok := server["command"].([]any)
	if !ok {
		t.Fatalf("server command has type %T, want []any", server["command"])
	}
	command := make([]string, 0, len(commandRaw))
	for _, item := range commandRaw {
		value, ok := item.(string)
		if !ok {
			t.Fatalf("server command item has type %T, want string", item)
		}
		command = append(command, value)
	}
	if len(command) != 8 {
		t.Fatalf("command len = %d, want 8", len(command))
	}
	if command[0] != mustExecutable(t) || command[1] != "channel-tools" || command[2] != "--run-id" || command[3] != "run_123" || command[4] != "--token" || command[5] != "tok_456" || command[6] != "--broker-url" {
		t.Fatalf("unexpected command: %#v", command)
	}
	if command[7] != "http://127.0.0.1:9999" {
		t.Fatalf("broker url arg = %q, want http://127.0.0.1:9999", command[7])
	}
	tools, ok := payload["tools"].(map[string]any)
	if !ok {
		t.Fatalf("payload tools has type %T, want map[string]any", payload["tools"])
	}
	if tools["avenor-channel-tools*"] != true {
		t.Fatalf("tools[avenor-channel-tools*] = %v, want true", tools["avenor-channel-tools*"])
	}
}

func TestMergeConfigPreservesExistingCustomConfig(t *testing.T) {
	t.Setenv("OPENCODE_CONFIG_CONTENT", `{"tools":{"existing*":true},"mcp":{"existing":{"type":"local","command":["echo","ok"]}}}`)
	env, cleanup := buildClientEnv(runtime.StartOptions{}, "run_123", "tok_456", "127.0.0.1:9999")
	if cleanup != nil {
		defer func() { _ = cleanup() }()
	}
	raw, err := os.ReadFile(env["OPENCODE_CONFIG"])
	if err != nil {
		t.Fatalf("read merged config file: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode merged config: %v", err)
	}
	tools := payload["tools"].(map[string]any)
	if tools["existing*"] != true {
		t.Fatalf("existing tool override lost: %#v", tools)
	}
	mcp := payload["mcp"].(map[string]any)
	if _, ok := mcp["existing"]; !ok {
		t.Fatalf("existing mcp entry lost: %#v", mcp)
	}
	if _, ok := mcp["avenor-channel-tools"]; !ok {
		t.Fatalf("avenor-channel-tools entry missing: %#v", mcp)
	}
}

func TestBuildClientEnvCleanupRemovesConfigFile(t *testing.T) {
	env, cleanup := buildClientEnv(runtime.StartOptions{}, "run_cleanup", "tok_456", "127.0.0.1:9999")
	if cleanup == nil {
		t.Fatal("cleanup func is nil")
	}
	path := env["OPENCODE_CONFIG"]
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("config file missing before cleanup: %v", err)
	}
	if err := cleanup(); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("config file still exists after cleanup: %v", err)
	}
}

func mustExecutable(t *testing.T) string {
	t.Helper()
	path, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	return path
}
