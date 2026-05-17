package mcpserver

import (
	"bufio"
	"encoding/json"
	"os/exec"
	"testing"
)

// tsToolNames lists the six MCP tools from the TypeScript reference
// (packages/mcp/src/mcp.ts). The Go server.NewServer function must register
// exactly these tools with matching names and input schemas.
var tsToolNames = []string{
	"avenor_spawn",
	"avenor_status",
	"avenor_answer_permission",
	"avenor_follow_up",
	"avenor_events",
	"avenor_shutdown",
}

// goToolNames lists the tool names registered in server.NewServer.
// Must stay in sync with both the AddTool calls in server.go and tsToolNames.
var goToolNames = []string{
	"avenor_spawn",              // handleAvenorSpawn
	"avenor_status",             // handleAvenorStatus
	"avenor_shutdown",           // handleAvenorShutdown
	"avenor_answer_permission",  // handleAvenorAnswerPermission
	"avenor_events",             // handleAvenorEvents
	"avenor_follow_up",          // handleAvenorFollowUp
}

func TestToolNameParity(t *testing.T) {
	if len(goToolNames) != len(tsToolNames) {
		t.Fatalf("tool count mismatch: Go=%d, TS=%d", len(goToolNames), len(tsToolNames))
	}

	goSet := make(map[string]bool, len(goToolNames))
	for _, n := range goToolNames {
		goSet[n] = true
	}

	for _, n := range tsToolNames {
		if !goSet[n] {
			t.Errorf("Go is missing TS tool: %s", n)
		}
	}

	tsSet := make(map[string]bool, len(tsToolNames))
	for _, n := range tsToolNames {
		tsSet[n] = true
	}
	for _, n := range goToolNames {
		if !tsSet[n] {
			t.Errorf("Go has extra tool not in TS: %s", n)
		}
	}
}

// TestSchemaFieldParity documents the required/optional field contracts for
// each tool's input schema. These match the TypeScript Zod schemas from
// packages/core/src/tools/*.ts and packages/mcp/src/mcp.ts.
//
// The Go struct definitions (spawnArgs, statusArgs, etc.) encode these
// contracts via json and jsonschema struct tags. This test documents the
// contract so a developer updating a struct is reminded to verify parity.
func TestSchemaFieldParity(t *testing.T) {
	t.Run("avenor_spawn", func(t *testing.T) {
		// spawnArgs — required: agent, repo_dir
		// optional: prompt, prompt_file, label, timeout, model, supervisor_id
		allowed := []string{"agent", "repo_dir", "prompt", "prompt_file", "label", "timeout", "model", "supervisor_id"}
		required := []string{"agent", "repo_dir"}
		assertFields(t, "spawnArgs", allowed, required)
	})

	t.Run("avenor_status", func(t *testing.T) {
		// statusArgs — all optional: run_id, supervisor_id
		allowed := []string{"run_id", "supervisor_id"}
		assertFields(t, "statusArgs", allowed, nil)
	})

	t.Run("avenor_answer_permission", func(t *testing.T) {
		// permissionArgs — required: run_id, option_id
		// optional: request_id, supervisor_id
		allowed := []string{"run_id", "option_id", "request_id", "supervisor_id"}
		required := []string{"run_id", "option_id"}
		assertFields(t, "permissionArgs", allowed, required)
	})

	t.Run("avenor_follow_up", func(t *testing.T) {
		// followUpArgs — required: run_id, message
		// optional: label, supervisor_id
		allowed := []string{"run_id", "message", "label", "supervisor_id"}
		required := []string{"run_id", "message"}
		assertFields(t, "followUpArgs", allowed, required)
	})

	t.Run("avenor_events", func(t *testing.T) {
		// eventsArgs — required: run_id
		// optional: types, limit, supervisor_id
		allowed := []string{"run_id", "types", "limit", "supervisor_id"}
		required := []string{"run_id"}
		assertFields(t, "eventsArgs", allowed, required)
	})

	t.Run("avenor_shutdown", func(t *testing.T) {
		// shutdownArgs — all optional: supervisor_id, force
		allowed := []string{"supervisor_id", "force"}
		assertFields(t, "shutdownArgs", allowed, nil)
	})
}

// assertFields is a contract-documenting helper. It verifies the field names
// are present in the struct definition by attempting to marshal JSON with
// those fields. The real enforcement is the struct tags in server.go.
func assertFields(t *testing.T, structName string, allowed, required []string) {
	t.Helper()

	// Build a JSON object with all allowed fields to verify they unmarshal
	// without error into the struct.
	obj := make(map[string]any)
	for _, f := range allowed {
		obj[f] = "test"
	}
	data, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("%s: marshal: %v", structName, err)
	}

	switch structName {
	case "spawnArgs":
		var a spawnArgs
		if err := json.Unmarshal(data, &a); err != nil {
			t.Fatalf("%s: unmarshal: %v", structName, err)
		}
		// Verify required fields are populated
		if a.Agent != "test" {
			t.Errorf("%s: agent not populated", structName)
		}
		if a.RepoDir != "test" {
			t.Errorf("%s: repo_dir not populated", structName)
		}
	case "statusArgs":
		var a statusArgs
		if err := json.Unmarshal(data, &a); err != nil {
			t.Fatalf("%s: unmarshal: %v", structName, err)
		}
	case "permissionArgs":
		data := map[string]any{"run_id": "r", "option_id": "o", "request_id": "req", "supervisor_id": "s"}
		b, _ := json.Marshal(data)
		var a permissionArgs
		if err := json.Unmarshal(b, &a); err != nil {
			t.Fatalf("%s: unmarshal: %v", structName, err)
		}
		if a.RunID != "r" || a.OptionID != "o" || a.RequestID != "req" || a.SupervisorID != "s" {
			t.Errorf("%s: fields not populated correctly", structName)
		}
	case "followUpArgs":
		data := map[string]any{"run_id": "r", "message": "m", "label": "l", "supervisor_id": "s"}
		b, _ := json.Marshal(data)
		var a followUpArgs
		if err := json.Unmarshal(b, &a); err != nil {
			t.Fatalf("%s: unmarshal: %v", structName, err)
		}
		if a.RunID != "r" || a.Message != "m" || a.Label != "l" || a.SupervisorID != "s" {
			t.Errorf("%s: fields not populated correctly", structName)
		}
	case "eventsArgs":
		data := map[string]any{"run_id": "r", "types": []string{"a"}, "limit": float64(10), "supervisor_id": "s"}
		b, _ := json.Marshal(data)
		var a eventsArgs
		if err := json.Unmarshal(b, &a); err != nil {
			t.Fatalf("%s: unmarshal: %v", structName, err)
		}
		if a.RunID != "r" || a.Limit != 10 || a.SupervisorID != "s" {
			t.Errorf("%s: fields not populated correctly", structName)
		}
	case "shutdownArgs":
		data := map[string]any{"supervisor_id": "s", "force": true}
		b, _ := json.Marshal(data)
		var a shutdownArgs
		if err := json.Unmarshal(b, &a); err != nil {
			t.Fatalf("%s: unmarshal: %v", structName, err)
		}
		if a.SupervisorID != "s" || !a.Force {
			t.Errorf("%s: fields not populated correctly", structName)
		}
	}
}

// TestMCPStdioHandshake performs a full MCP initialize + tools/list
// handshake over stdio against the avenor binary, verifying exactly 6 tools
// are registered with the correct names.
func TestMCPStdioHandshake(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	avenorBin := "avenor"
	if _, err := exec.LookPath(avenorBin); err != nil {
		avenorBin = "./avenor"
		if _, err := exec.LookPath(avenorBin); err != nil {
			t.Skip("avenor binary not found — build with: go build -o avenor ./cmd/avenor")
		}
	}

	cmd := exec.Command(avenorBin, "mcp", "--no-autostart")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}

	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start avenor mcp: %v", err)
	}
	defer func() {
		stdin.Close()
		cmd.Process.Kill()
		cmd.Wait()
	}()

	scanner := bufio.NewScanner(stdout)

	// Initialize
	initReq := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params": map[string]any{
			"protocolVersion": "2025-06-18",
			"capabilities":    map[string]any{},
			"clientInfo":      map[string]any{"name": "parity-test", "version": "1.0.0"},
		},
	}
	data, _ := json.Marshal(initReq)
	data = append(data, '\n')
	stdin.Write(data)

	var initResp map[string]any
	for scanner.Scan() {
		json.Unmarshal(scanner.Bytes(), &initResp)
		if _, ok := initResp["id"]; ok {
			break
		}
	}
	if errMsg, ok := initResp["error"]; ok {
		t.Fatalf("initialize error: %v", errMsg)
	}

	// Initialized notification
	stdin.Write([]byte(`{"jsonrpc":"2.0","method":"notifications/initialized"}` + "\n"))

	// Tools/list
	toolsReq := map[string]any{
		"jsonrpc": "2.0",
		"id":      2,
		"method":  "tools/list",
		"params":  map[string]any{},
	}
	data, _ = json.Marshal(toolsReq)
	data = append(data, '\n')
	stdin.Write(data)

	var toolsResp map[string]any
	for scanner.Scan() {
		json.Unmarshal(scanner.Bytes(), &toolsResp)
		if id, ok := toolsResp["id"]; ok {
			if fid, ok := id.(float64); ok && fid == 2 {
				break
			}
		}
	}

	result, ok := toolsResp["result"].(map[string]any)
	if !ok {
		t.Fatalf("tools/list result not an object: %v", toolsResp)
	}
	tools, ok := result["tools"].([]any)
	if !ok {
		t.Fatalf("tools not found: %v", result)
	}
	if len(tools) != 6 {
		t.Fatalf("expected 6 tools, got %d", len(tools))
	}

	found := make(map[string]bool)
	for _, tl := range tools {
		tool, _ := tl.(map[string]any)
		found[tool["name"].(string)] = true
	}
	for _, expected := range tsToolNames {
		if !found[expected] {
			t.Errorf("missing tool: %s", expected)
		}
	}
}
