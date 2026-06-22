package opencodeacp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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

// --- subagent mode override tests ---

func TestReadAgentMode(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "opencode.json")
	cfg := `{
		"agent": {
			"horse": {"mode": "subagent", "model": "sparky/qwen-moe"},
			"jockey": {"mode": "primary", "model": "deepseek/deepseek-v4-flash"},
			"docs-writer": {"mode": "all"},
			"nomode": {"model": "sparky/qwen-moe"}
		}
	}`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	getenv := func(key string) string {
		if key == "OPENCODE_CONFIG" {
			return cfgPath
		}
		return ""
	}

	tests := []struct {
		agent string
		want  string
	}{
		{"horse", "subagent"},
		{"jockey", "primary"},
		{"docs-writer", "all"},
		{"nomode", ""},
		{"unknown", ""},
	}
	for _, tt := range tests {
		t.Run(tt.agent, func(t *testing.T) {
			got := readAgentMode(tt.agent, getenv)
			if got != tt.want {
				t.Errorf("readAgentMode(%q) = %q, want %q", tt.agent, got, tt.want)
			}
		})
	}
}

func TestReadAgentModeMissingFile(t *testing.T) {
	emptyDir := t.TempDir()
	getenv := func(key string) string {
		switch key {
		case "OPENCODE_CONFIG":
			return "/nonexistent/path/opencode.json"
		case "XDG_CONFIG_HOME":
			return emptyDir
		}
		return ""
	}
	if got := readAgentMode("horse", getenv); got != "" {
		t.Errorf("readAgentMode with missing file = %q, want empty", got)
	}
}

func TestReadAgentModeMalformedJSON(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "opencode.json")
	if err := os.WriteFile(cfgPath, []byte("{not valid json"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	emptyDir := t.TempDir()
	getenv := func(key string) string {
		switch key {
		case "OPENCODE_CONFIG":
			return cfgPath
		case "XDG_CONFIG_HOME":
			return emptyDir
		}
		return ""
	}
	if got := readAgentMode("horse", getenv); got != "" {
		t.Errorf("readAgentMode with malformed JSON = %q, want empty", got)
	}
}

func TestBuildClientEnvSubagentModeOverride(t *testing.T) {
	// Write a global opencode config with a subagent-mode agent.
	dir := t.TempDir()
	globalCfgPath := filepath.Join(dir, "opencode.json")
	globalCfg := `{
		"agent": {
			"horse": {
				"mode": "subagent",
				"model": "sparky/qwen-moe",
				"tools": {"webFetch": false, "github": false},
				"permission": {"bash": {"curl*": "deny"}}
			},
			"jockey": {"mode": "primary"}
		}
	}`
	if err := os.WriteFile(globalCfgPath, []byte(globalCfg), 0o600); err != nil {
		t.Fatalf("write global config: %v", err)
	}
	t.Setenv("OPENCODE_CONFIG", globalCfgPath)

	// Clear OPENCODE_CONFIG_CONTENT so it doesn't interfere.
	t.Setenv("OPENCODE_CONFIG_CONTENT", "")

	env, cleanup := buildClientEnv(runtime.StartOptions{Agent: "horse"}, "run_sub", "tok", "127.0.0.1:1")
	if cleanup != nil {
		defer func() { _ = cleanup() }()
	}
	if env == nil {
		t.Fatal("buildClientEnv returned nil env")
	}

	raw, err := os.ReadFile(env["OPENCODE_CONFIG"])
	if err != nil {
		t.Fatalf("read config file: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode config: %v", err)
	}

	agent, ok := payload["agent"].(map[string]any)
	if !ok {
		t.Fatalf("payload has no agent map: %#v", payload)
	}
	horse, ok := agent["horse"].(map[string]any)
	if !ok {
		t.Fatalf("agent has no horse entry: %#v", agent)
	}
	if horse["mode"] != "all" {
		t.Errorf("horse mode = %v, want \"all\" (overridden from subagent)", horse["mode"])
	}
}

func TestBuildClientEnvNoOverrideForPrimaryAgent(t *testing.T) {
	dir := t.TempDir()
	globalCfgPath := filepath.Join(dir, "opencode.json")
	globalCfg := `{
		"agent": {
			"jockey": {"mode": "primary"},
			"horse": {"mode": "subagent"}
		}
	}`
	if err := os.WriteFile(globalCfgPath, []byte(globalCfg), 0o600); err != nil {
		t.Fatalf("write global config: %v", err)
	}
	t.Setenv("OPENCODE_CONFIG", globalCfgPath)
	t.Setenv("OPENCODE_CONFIG_CONTENT", "")

	// Broker info is present so MCP is injected, but no mode override for jockey.
	env, cleanup := buildClientEnv(runtime.StartOptions{Agent: "jockey"}, "run_pri", "tok", "127.0.0.1:1")
	if cleanup != nil {
		defer func() { _ = cleanup() }()
	}
	if env == nil {
		t.Fatal("buildClientEnv returned nil env (expected MCP injection)")
	}

	raw, err := os.ReadFile(env["OPENCODE_CONFIG"])
	if err != nil {
		t.Fatalf("read config file: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode config: %v", err)
	}

	// The global config's agent map is merged in. Jockey must still be "primary".
	if agent, ok := payload["agent"].(map[string]any); ok {
		if jockey, ok := agent["jockey"].(map[string]any); ok {
			if jockey["mode"] != "primary" {
				t.Errorf("jockey mode = %v, want \"primary\" (not overridden)", jockey["mode"])
			}
		}
	}
}

func TestBuildClientEnvNoOverrideForAllModeAgent(t *testing.T) {
	dir := t.TempDir()
	globalCfgPath := filepath.Join(dir, "opencode.json")
	globalCfg := `{
		"agent": {
			"docs-writer": {"mode": "all"}
		}
	}`
	if err := os.WriteFile(globalCfgPath, []byte(globalCfg), 0o600); err != nil {
		t.Fatalf("write global config: %v", err)
	}
	t.Setenv("OPENCODE_CONFIG", globalCfgPath)
	t.Setenv("OPENCODE_CONFIG_CONTENT", "")

	env, cleanup := buildClientEnv(runtime.StartOptions{Agent: "docs-writer"}, "run_all", "tok", "127.0.0.1:1")
	if cleanup != nil {
		defer func() { _ = cleanup() }()
	}
	if env == nil {
		t.Fatal("buildClientEnv returned nil env")
	}

	raw, err := os.ReadFile(env["OPENCODE_CONFIG"])
	if err != nil {
		t.Fatalf("read config file: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode config: %v", err)
	}

	agent, ok := payload["agent"].(map[string]any)
	if !ok {
		return
	}
	if dw, ok := agent["docs-writer"].(map[string]any); ok {
		if dw["mode"] != nil && dw["mode"] != "all" {
			t.Errorf("docs-writer mode = %v, want \"all\" (not overridden)", dw["mode"])
		}
	}
}

func TestBuildClientEnvNoOverrideForUnknownAgent(t *testing.T) {
	dir := t.TempDir()
	globalCfgPath := filepath.Join(dir, "opencode.json")
	globalCfg := `{"agent": {"horse": {"mode": "subagent"}}}`
	if err := os.WriteFile(globalCfgPath, []byte(globalCfg), 0o600); err != nil {
		t.Fatalf("write global config: %v", err)
	}
	t.Setenv("OPENCODE_CONFIG", globalCfgPath)
	t.Setenv("OPENCODE_CONFIG_CONTENT", "")

	env, cleanup := buildClientEnv(runtime.StartOptions{Agent: "nonexistent"}, "run_unk", "tok", "127.0.0.1:1")
	if cleanup != nil {
		defer func() { _ = cleanup() }()
	}
	if env == nil {
		t.Fatal("buildClientEnv returned nil env")
	}

	raw, err := os.ReadFile(env["OPENCODE_CONFIG"])
	if err != nil {
		t.Fatalf("read config file: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode config: %v", err)
	}

	// An unknown agent should not produce any agent overrides.
	if agent, ok := payload["agent"].(map[string]any); ok {
		if _, exists := agent["nonexistent"]; exists {
			t.Errorf("unknown agent should not have an override: %#v", agent["nonexistent"])
		}
	}
}

// TestBuildClientEnvSubagentOverrideWithoutBroker verifies that the mode
// override is applied even when broker parameters are empty (the CLI path).
// This is the key scenario: avenor -agent horse with no broker should still
// get the config override so set_mode succeeds.
func TestBuildClientEnvSubagentOverrideWithoutBroker(t *testing.T) {
	dir := t.TempDir()
	globalCfgPath := filepath.Join(dir, "opencode.json")
	globalCfg := `{
		"agent": {
			"horse": {"mode": "subagent", "model": "sparky/qwen-moe"}
		}
	}`
	if err := os.WriteFile(globalCfgPath, []byte(globalCfg), 0o600); err != nil {
		t.Fatalf("write global config: %v", err)
	}
	t.Setenv("OPENCODE_CONFIG", globalCfgPath)
	t.Setenv("OPENCODE_CONFIG_CONTENT", "")

	// No broker info — empty runID, token, addr.
	env, cleanup := buildClientEnv(runtime.StartOptions{Agent: "horse"}, "", "", "")
	if cleanup != nil {
		defer func() { _ = cleanup() }()
	}
	if env == nil {
		t.Fatal("buildClientEnv returned nil env — mode override should still be written without a broker")
	}

	raw, err := os.ReadFile(env["OPENCODE_CONFIG"])
	if err != nil {
		t.Fatalf("read config file: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode config: %v", err)
	}

	agent, ok := payload["agent"].(map[string]any)
	if !ok {
		t.Fatalf("payload has no agent map: %#v", payload)
	}
	horse, ok := agent["horse"].(map[string]any)
	if !ok {
		t.Fatalf("agent has no horse entry: %#v", agent)
	}
	if horse["mode"] != "all" {
		t.Errorf("horse mode = %v, want \"all\" (overridden from subagent)", horse["mode"])
	}

	// No MCP server should be injected without a broker.
	if mcp, ok := payload["mcp"].(map[string]any); ok {
		if _, has := mcp["avenor-channel-tools"]; has {
			t.Error("avenor-channel-tools MCP server injected without broker info")
		}
	}
}

// TestBuildClientEnvNoOverrideWithoutBroker verifies that buildClientEnv
// returns nil (no temp file) when there's no broker and no subagent override
// needed. This prevents writing a redundant config file that just echoes the
// global config back to opencode.
func TestBuildClientEnvNoOverrideWithoutBroker(t *testing.T) {
	dir := t.TempDir()
	globalCfgPath := filepath.Join(dir, "opencode.json")
	globalCfg := `{
		"agent": {
			"jockey": {"mode": "primary"}
		}
	}`
	if err := os.WriteFile(globalCfgPath, []byte(globalCfg), 0o600); err != nil {
		t.Fatalf("write global config: %v", err)
	}
	t.Setenv("OPENCODE_CONFIG", globalCfgPath)
	t.Setenv("OPENCODE_CONFIG_CONTENT", "")

	// Primary agent, no broker — nothing to override.
	env, cleanup := buildClientEnv(runtime.StartOptions{Agent: "jockey"}, "", "", "")
	if cleanup != nil {
		defer func() { _ = cleanup() }()
		t.Error("cleanup should be nil when no overrides are needed")
	}
	if env != nil {
		t.Errorf("buildClientEnv should return nil env when no overrides needed, got: %#v", env)
	}
}
