package opencodeacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/acp"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
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
	env, cleanup := buildClientEnv(runtime.StartOptions{Label: "worker"}, "run_123", "tok_456", "127.0.0.1:9999")
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
	if len(command) != 10 {
		t.Fatalf("command len = %d, want 10", len(command))
	}
	if command[0] != mustExecutable(t) || command[1] != "channel-tools" || command[2] != "--run-id" || command[3] != "run_123" || command[4] != "--sender" || command[5] != "worker" || command[6] != "--token" || command[7] != "tok_456" || command[8] != "--broker-url" {
		t.Fatalf("unexpected command: %#v", command)
	}
	if command[9] != "http://127.0.0.1:9999" {
		t.Fatalf("broker url arg = %q, want http://127.0.0.1:9999", command[9])
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

// writeFakeACPServer writes a minimal ACP stdio server that answers
// initialize and then session/new, after copying its OPENCODE_CONFIG to record
// so a test can see which config the child actually read. A non-empty gate
// makes the server announce itself at ready and then block until the gate file
// exists, which pauses it before it reads its config.
func writeFakeACPServer(t *testing.T, dir, name, gate, ready, record, sessionID string) string {
	t.Helper()
	script := filepath.Join(dir, name)
	var body strings.Builder
	body.WriteString("#!/bin/sh\n")
	if ready != "" {
		fmt.Fprintf(&body, "touch %s\n", shellQuotePath(ready))
	}
	if gate != "" {
		fmt.Fprintf(&body, "while [ ! -f %s ]; do sleep 0.01; done\n", shellQuotePath(gate))
	}
	body.WriteString("if [ ! -r \"$OPENCODE_CONFIG\" ]; then echo 'missing OPENCODE_CONFIG' >&2; exit 3; fi\n")
	fmt.Fprintf(&body, "cp \"$OPENCODE_CONFIG\" %s\n", shellQuotePath(record))
	body.WriteString("n=0\n")
	body.WriteString("while IFS= read -r line; do\n")
	body.WriteString("  id=$(printf '%s' \"$line\" | sed -n 's/.*\"id\":\\([0-9][0-9]*\\).*/\\1/p')\n")
	body.WriteString("  [ -n \"$id\" ] || continue\n")
	body.WriteString("  n=$((n+1))\n")
	body.WriteString("  if [ \"$n\" -eq 1 ]; then\n")
	body.WriteString("    printf '{\"jsonrpc\":\"2.0\",\"id\":%s,\"result\":{\"protocolVersion\":1}}\\n' \"$id\"\n")
	fmt.Fprintf(&body, "  else\n    printf '{\"jsonrpc\":\"2.0\",\"id\":%%s,\"result\":{\"sessionId\":%q}}\\n' \"$id\"\n", sessionID)
	body.WriteString("  fi\ndone\n")
	if err := os.WriteFile(script, []byte(body.String()), 0o755); err != nil {
		t.Fatalf("write fake ACP server: %v", err)
	}
	return script
}

// waitForPath blocks until path exists. The fake child writes the marker, so
// coordination does not depend on a fixed delay.
func waitForPath(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestConcurrentProvidersKeepDistinctConfigFiles pins the per-launch config
// ownership fix: two providers that share a broker run ID get separate config
// files, and closing one leaves the other's file — and the config its child
// reads — intact.
func TestConcurrentProvidersKeepDistinctConfigFiles(t *testing.T) {
	dir := t.TempDir()
	globalCfgPath := filepath.Join(dir, "opencode.json")
	globalCfg := `{"agent": {"alpha": {"mode": "subagent"}, "beta": {"mode": "primary"}}}`
	if err := os.WriteFile(globalCfgPath, []byte(globalCfg), 0o600); err != nil {
		t.Fatalf("write global config: %v", err)
	}
	t.Setenv("OPENCODE_CONFIG", globalCfgPath)
	t.Setenv("OPENCODE_CONFIG_CONTENT", "")

	gate := filepath.Join(dir, "release-b")
	ready := filepath.Join(dir, "b-waiting")
	observedB := filepath.Join(dir, "b-child-observed.json")
	binA := writeFakeACPServer(t, dir, "acp-a.sh", "", "", filepath.Join(dir, "a-child-observed.json"), "ses_a")
	binB := writeFakeACPServer(t, dir, "acp-b.sh", gate, ready, observedB, "ses_b")

	sharedBroker := broker.New("")
	if err := sharedBroker.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	defer func() { _ = sharedBroker.Stop() }()

	var mu sync.Mutex
	paths := map[string][]string{}
	newProvider := func(bin, agent string) *acp.Provider {
		return acp.NewProvider(acp.ProviderConfig{
			BackendID: "opencode-acp",
			Bin:       bin,
			Opts:      runtime.StartOptions{Agent: agent},
			BuildClientEnv: func(opts runtime.StartOptions, runID, brokerToken, brokerAddr string) (map[string]string, func() error) {
				env, cleanup := buildClientEnv(opts, runID, brokerToken, brokerAddr)
				if env != nil {
					mu.Lock()
					paths[agent] = append(paths[agent], env["OPENCODE_CONFIG"])
					mu.Unlock()
				}
				return env, cleanup
			},
		}).(*acp.Provider)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	providerB := newProvider(binB, "beta")
	startedB := make(chan error, 1)
	go func() {
		_, err := providerB.Start(ctx, runtime.StartOptions{Broker: sharedBroker, RuntimeID: "rt_shared"})
		startedB <- err
	}()
	waitForPath(t, ready)

	providerA := newProvider(binA, "alpha")
	if _, err := providerA.Start(ctx, runtime.StartOptions{Broker: sharedBroker, RuntimeID: "rt_shared"}); err != nil {
		t.Fatalf("provider A start: %v", err)
	}
	// The stub exits on SIGTERM, so Close reports the terminated client.
	_ = providerA.Close()

	if err := os.WriteFile(gate, nil, 0o600); err != nil {
		t.Fatalf("release provider B: %v", err)
	}
	if err := <-startedB; err != nil {
		t.Fatalf("provider B start after provider A closed: %v", err)
	}
	defer func() { _ = providerB.Close() }()

	pathA, pathB := paths["alpha"][0], paths["beta"][0]
	if pathA == pathB {
		t.Fatalf("providers share config path %q", pathA)
	}
	if _, err := os.Stat(pathA); !os.IsNotExist(err) {
		t.Fatalf("provider A config still exists after its close: %v", err)
	}
	rawB, err := os.ReadFile(pathB)
	if err != nil {
		t.Fatalf("provider B config missing after provider A closed: %v", err)
	}
	observed, err := os.ReadFile(observedB)
	if err != nil {
		t.Fatalf("provider B child did not observe its config: %v", err)
	}
	if !bytes.Equal(observed, rawB) {
		t.Fatalf("provider B child observed %q, want its own config %q", observed, rawB)
	}

	// Provider A flips alpha to "all"; provider B must keep the global mode.
	if bytes.Contains(rawB, []byte(`"mode":"all"`)) {
		t.Errorf("provider B config carries provider A's override: %s", rawB)
	}
}
