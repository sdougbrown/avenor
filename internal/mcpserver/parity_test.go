package mcpserver

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/spawnselection"
)

// tsToolNames lists the seven MCP tools from the TypeScript reference
// (packages/mcp/src/mcp.ts).
var tsToolNames = []string{
	"avenor_spawn",
	"avenor_status",
	"avenor_result",
	"avenor_answer_permission",
	"avenor_follow_up",
	"avenor_events",
	"avenor_shutdown",
}

func TestToolNameParity(t *testing.T) {
	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: &fakeClient{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Query registered tool names from the actual server instance.
	registeredNames := make(map[string]bool)
	for _, name := range s.RegisteredToolNames() {
		registeredNames[name] = true
	}

	if len(registeredNames) != len(tsToolNames) {
		t.Errorf("registered tool count %d != expected %d", len(registeredNames), len(tsToolNames))
	}

	for _, name := range tsToolNames {
		if !registeredNames[name] {
			t.Errorf("registered tools missing TS tool: %s", name)
		}
	}

	// Also verify no extra tools are registered that TS doesn't have
	tsSet := make(map[string]bool, len(tsToolNames))
	for _, n := range tsToolNames {
		tsSet[n] = true
	}
	for name := range registeredNames {
		if !tsSet[name] {
			t.Errorf("registered tool not in TS reference: %s", name)
		}
	}
}

// TestSchemaFieldParity verifies the required/optional field contracts for
// each Go MCP tool's input schema. The shared fields match the TypeScript
// reference; Go-only fields are intentionally covered by the Go expectations.
//
// The Go struct definitions (spawnArgs, statusArgs, etc.) encode these
// contracts via json and jsonschema struct tags. This test verifies both
// the property names and the required fields match the expected contracts.
func TestSchemaFieldParity(t *testing.T) {
	t.Run("avenor_spawn", func(t *testing.T) {
		// spawnArgs — repo_dir is required.
		// agent and model are independently optional; omitting both uses runtime defaults.
		// All remaining fields are optional.
		allowed := []string{"agent", "repo_dir", "prompt", "prompt_file", "label", "timeout", "model", "thinking", "backend", "roster_file", "roster_entry", "server_url", "supervisor_id", "auto_approve"}
		required := []string{"repo_dir"}
		assertFields(t, "spawnArgs", allowed, required)
	})

	t.Run("avenor_status", func(t *testing.T) {
		// statusArgs — all optional: run_id, view, wait_for, timeout, supervisor_id
		allowed := []string{"run_id", "view", "wait_for", "timeout", "supervisor_id"}
		assertFields(t, "statusArgs", allowed, nil)
	})

	t.Run("avenor_result", func(t *testing.T) {
		// resultArgs — required: run_id; optional: wait, timeout, supervisor_id
		allowed := []string{"run_id", "wait", "timeout", "supervisor_id"}
		required := []string{"run_id"}
		assertFields(t, "resultArgs", allowed, required)
	})

	t.Run("avenor_answer_permission", func(t *testing.T) {
		// permissionArgs — required: run_id, option_id
		// optional: request_id, supervisor_id, message
		allowed := []string{"run_id", "option_id", "request_id", "supervisor_id", "message"}
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

// assertFields verifies that the Go struct's JSON fields match the
// expected allowed/required fields. It builds JSON objects and unmarshals
// them into the struct to confirm field mapping.
func loadConformanceCases(t *testing.T) []spawnselection.ConformanceCase {
	t.Helper()
	cases, err := spawnselection.LoadConformanceCases("../../schemas/spawn_selection.conformance.json")
	if err != nil {
		t.Fatalf("load conformance fixture: %v", err)
	}
	if len(cases) == 0 {
		t.Fatal("conformance fixture has no cases")
	}
	return cases
}

func TestSpawnSelectorParity(t *testing.T) {
	// The Go MCP server is a strict raw boundary. Drive it through the same
	// shared conformance fixture (ValidateJSON rejects unknown selector keys)
	// as packages/core/src/spawn-selection.test.ts so both share one fixture.
	for _, c := range loadConformanceCases(t) {
		t.Run(c.Name, func(t *testing.T) {
			err := spawnselection.ValidateJSON(c.Input, c.RosterConfigured)
			if (err != nil) == c.Valid {
				t.Fatalf("ValidateJSON() error = %v, want valid=%v", err, c.Valid)
			}
		})
	}
}

// TestSpawnArgsRejectsUnknownKeys verifies the Go MCP spawn tool is a strict
// raw boundary: unknown/deferred/misspelled keys (including selector look-alikes)
// are rejected while every declared provider option remains accepted.
//
// Rejection cases are driven from the shared conformance fixture's strictOnly
// cases so the test stays in lockstep with the fixture. Strict-only cases may
// carry keys that are known spawnArgs provider options (e.g. thinking) — those
// are accepted at the raw boundary and only rejected later by selector
// validation. Cases with truly unknown keys (system, rosterFile) must be
// rejected by spawnArgs.UnmarshalJSON.
func TestSpawnArgsRejectsUnknownKeys(t *testing.T) {
	knownFields := knownSpawnArgsFieldSet()
	for _, c := range loadConformanceCases(t) {
		if !c.StrictOnly {
			continue
		}
		t.Run(c.Name, func(t *testing.T) {
			var args spawnArgs
			err := json.Unmarshal(c.Input, &args)

			// Determine whether the input contains only known spawnArgs fields.
			var raw map[string]json.RawMessage
			if e := json.Unmarshal(c.Input, &raw); e != nil {
				t.Errorf("parse input: %v", e)
				return
			}
			hasUnknown := false
			for key := range raw {
				if !knownFields[key] {
					hasUnknown = true
					break
				}
			}
			if hasUnknown && err == nil {
				t.Errorf("spawnArgs accepted unknown key in: %s", c.Input)
			}
			if !hasUnknown && err != nil {
				t.Errorf("spawnArgs rejected known-key input: %s: %v", c.Input, err)
			}
		})
	}

	// Mixed input: a known provider option coexisting with an unknown key must
	// still be rejected — the strict boundary does not relax for familiar keys.
	t.Run("known plus unknown key", func(t *testing.T) {
		var args spawnArgs
		data := `{"repo_dir":"/tmp/r","thinking":"high","system":"deferred"}`
		if err := json.Unmarshal([]byte(data), &args); err == nil {
			t.Errorf("spawnArgs accepted unknown key alongside known field: %s", data)
		}
	})

	// Declared provider-specific options must remain accepted — asserted
	// individually so a failure names the exact field.
	acceptedIndividual := []struct {
		name string
		data string
	}{
		{"repo_dir only", `{"repo_dir":"/tmp/r"}`},
		{"agent", `{"repo_dir":"/tmp/r","agent":"reviewer"}`},
		{"model", `{"repo_dir":"/tmp/r","model":"provider/model"}`},
		{"backend", `{"repo_dir":"/tmp/r","backend":"opencode-acp"}`},
		{"roster_file", `{"repo_dir":"/tmp/r","roster_file":"/repo/r.json"}`},
		{"roster_entry", `{"repo_dir":"/tmp/r","roster_entry":"planner"}`},
		{"prompt", `{"repo_dir":"/tmp/r","prompt":"hi"}`},
		{"prompt_file", `{"repo_dir":"/tmp/r","prompt_file":"/tmp/p"}`},
		{"label", `{"repo_dir":"/tmp/r","label":"l"}`},
		{"timeout", `{"repo_dir":"/tmp/r","timeout":"5m"}`},
		{"thinking", `{"repo_dir":"/tmp/r","thinking":"high"}`},
		{"server_url", `{"repo_dir":"/tmp/r","server_url":"http://x"}`},
		{"supervisor_id", `{"repo_dir":"/tmp/r","supervisor_id":"s"}`},
		{"auto_approve", `{"repo_dir":"/tmp/r","auto_approve":true}`},
		// Combined: all provider options together to verify no interactions
		// cause unexpected rejection.
		{"all provider options", `{"repo_dir":"/tmp/r","agent":"reviewer","model":"provider/model","backend":"opencode-acp","prompt":"hi","prompt_file":"/tmp/p","label":"l","timeout":"5m","thinking":"high","server_url":"http://x","supervisor_id":"s","auto_approve":true}`},
	}
	for _, tt := range acceptedIndividual {
		t.Run("accepts/"+tt.name, func(t *testing.T) {
			var args spawnArgs
			if err := json.Unmarshal([]byte(tt.data), &args); err != nil {
				t.Errorf("spawnArgs rejected %s: %v", tt.name, err)
			}
		})
	}

	// The go-sdk decodes raw tool arguments into the typed struct through the
	// same json.Unmarshaler contract exercised above (segmentio/encoding/json
	// and encoding/json both honor UnmarshalJSON), so the strict boundary holds
	// on the real wire path, not just under encoding/json.
}

// knownSpawnArgsFieldSet derives the accepted JSON field names from the
// spawnArgs struct tags so the set stays in sync with the struct definition.
func knownSpawnArgsFieldSet() map[string]bool {
	set := make(map[string]bool)
	t := reflect.TypeOf(spawnArgs{})
	for i := 0; i < t.NumField(); i++ {
		tag := t.Field(i).Tag.Get("json")
		name := strings.Split(tag, ",")[0]
		if name != "" && name != "-" {
			set[name] = true
		}
	}
	return set
}

func assertFields(t *testing.T, structName string, allowed, required []string) {
	t.Helper()

	// Build a JSON object with all allowed fields to verify they unmarshal
	// without error into the struct.
	obj := make(map[string]any)
	for _, f := range allowed {
		if f == "auto_approve" || f == "wait" {
			obj[f] = true
			continue
		}
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
		if a.RosterFile != "test" || a.RosterEntry != "test" {
			t.Errorf("%s: roster selector fields not populated", structName)
		}
		if a.RepoDir != "test" {
			t.Errorf("%s: repo_dir not populated", structName)
		}
		if !a.AutoApprove {
			t.Errorf("%s: auto_approve not populated", structName)
		}
	case "statusArgs":
		var a statusArgs
		if err := json.Unmarshal(data, &a); err != nil {
			t.Fatalf("%s: unmarshal: %v", structName, err)
		}
	case "resultArgs":
		var a resultArgs
		if err := json.Unmarshal(data, &a); err != nil {
			t.Fatalf("%s: unmarshal: %v", structName, err)
		}
		if a.RunID != "test" || a.Wait == nil || !*a.Wait {
			t.Errorf("%s: fields not populated correctly", structName)
		}
	case "permissionArgs":
		data := map[string]any{"run_id": "r", "option_id": "o", "request_id": "req", "supervisor_id": "s", "message": "typed"}
		b, _ := json.Marshal(data)
		var a permissionArgs
		if err := json.Unmarshal(b, &a); err != nil {
			t.Fatalf("%s: unmarshal: %v", structName, err)
		}
		if a.RunID != "r" || a.OptionID != "o" || a.RequestID != "req" || a.SupervisorID != "s" || a.Message != "typed" {
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

	// Verify required fields: send JSON without required fields and confirm
	// that the jsonschema tags mark them as required and optional fields as
	// optional. This protects the generated MCP input schema contract.
	assertSchemaTags(t, structName, allowed, required)
}

func assertSchemaTags(t *testing.T, structName string, allowed, required []string) {
	t.Helper()
	requiredSet := make(map[string]bool, len(required))
	for _, f := range required {
		requiredSet[f] = true
	}

	var typ reflect.Type
	switch structName {
	case "spawnArgs":
		typ = reflect.TypeOf(spawnArgs{})
	case "statusArgs":
		typ = reflect.TypeOf(statusArgs{})
	case "resultArgs":
		typ = reflect.TypeOf(resultArgs{})
	case "permissionArgs":
		typ = reflect.TypeOf(permissionArgs{})
	case "followUpArgs":
		typ = reflect.TypeOf(followUpArgs{})
	case "eventsArgs":
		typ = reflect.TypeOf(eventsArgs{})
	case "shutdownArgs":
		typ = reflect.TypeOf(shutdownArgs{})
	default:
		t.Fatalf("unknown struct: %s", structName)
	}

	seen := make(map[string]bool, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		jsonName := strings.Split(field.Tag.Get("json"), ",")[0]
		if jsonName == "" || jsonName == "-" {
			continue
		}
		seen[jsonName] = true
		schemaTag := field.Tag.Get("jsonschema")
		if requiredSet[jsonName] {
			if !strings.Contains(schemaTag, "required") {
				t.Errorf("%s.%s missing required jsonschema tag", structName, jsonName)
			}
			continue
		}
		if !strings.Contains(schemaTag, "optional") {
			t.Errorf("%s.%s missing optional jsonschema tag", structName, jsonName)
		}
	}
	for _, f := range allowed {
		if !seen[f] {
			t.Errorf("%s: expected field %q not present", structName, f)
		}
	}
	if len(seen) != len(allowed) {
		t.Errorf("%s: field count %d != expected %d", structName, len(seen), len(allowed))
	}
}

// TestMCPStdioHandshake verifies that a NewServer with a fake client
// registers exactly 7 tools with the correct names, matching the
// TypeScript MCP tool surface. Uses in-process server inspection
// instead of spawning a subprocess.
func TestPermissionArgsRejectsUnpairedSurrogateMessage(t *testing.T) {
	var args permissionArgs
	if err := json.Unmarshal([]byte(`{"run_id":"r","option_id":"o","message":"\uD800"}`), &args); err == nil {
		t.Fatal("permissionArgs accepted malformed write-in")
	}
}

func TestMCPStdioHandshake(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	s, err := NewServer(Options{
		Transport:     "stdio",
		NoAutostart:   true,
		ControlClient: &fakeClient{},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer s.Close()

	registeredNames := make(map[string]bool)
	for _, name := range s.RegisteredToolNames() {
		registeredNames[name] = true
	}

	if len(registeredNames) != 7 {
		t.Fatalf("expected 7 registered tools, got %d", len(registeredNames))
	}
	for _, expected := range tsToolNames {
		if !registeredNames[expected] {
			t.Errorf("missing tool: %s", expected)
		}
	}
}
