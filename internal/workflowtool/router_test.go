package workflowtool

import (
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
)

// conformanceCase is one language-neutral case from
// schemas/workflow_route.conformance.json.
type conformanceCase struct {
	Name   string          `json:"name"`
	Input  json.RawMessage `json:"input"`
	Schema bool            `json:"schema"`
	Expect struct {
		Action  string         `json:"action"`
		Call    string         `json:"call"`
		Args    map[string]any `json:"args"`
		Code    string         `json:"code"`
		Message string         `json:"message"`
	} `json:"expect"`
}

func loadConformanceCases(t *testing.T) []conformanceCase {
	t.Helper()
	data, err := os.ReadFile("../../schemas/workflow_route.conformance.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var fixture struct {
		Cases []conformanceCase `json:"cases"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	if len(fixture.Cases) == 0 {
		t.Fatal("fixture carries no case")
	}
	return fixture.Cases
}

// fakeClient records the one control call the router makes and returns a canned
// result, so a conformance case can assert the dispatch without a supervisor.
type fakeClient struct {
	calls  []fakeCall
	result map[string]any
	err    error
}

type fakeCall struct {
	method string
	args   map[string]any
}

func (f *fakeClient) record(method string, args map[string]any) (map[string]any, error) {
	f.calls = append(f.calls, fakeCall{method: method, args: args})
	if f.err != nil {
		return nil, f.err
	}
	if f.result == nil {
		return map[string]any{}, nil
	}
	return f.result, nil
}

func (f *fakeClient) WorkflowStatus(workflowID string) (map[string]any, error) {
	return f.record("status", map[string]any{"workflow_id": workflowID})
}

func (f *fakeClient) WorkflowWait(workflowID string, timeout time.Duration) (map[string]any, error) {
	return f.record("wait", map[string]any{"workflow_id": workflowID, "timeout_ms": timeout.Milliseconds()})
}

func (f *fakeClient) WorkflowInspect(workflowID string) (map[string]any, error) {
	return f.record("inspect", map[string]any{"workflow_id": workflowID})
}

func (f *fakeClient) WorkflowEvents(workflowID string, afterSeq int64, limit int) (map[string]any, error) {
	return f.record("events", map[string]any{"workflow_id": workflowID, "after_seq": afterSeq, "limit": limit})
}

func (f *fakeClient) WorkflowComplete(workflowID string, fields map[string]any) (map[string]any, error) {
	args := map[string]any{"workflow_id": workflowID}
	for key, value := range fields {
		args[key] = value
	}
	return f.record("complete", args)
}

func (f *fakeClient) WorkflowGate(workflowID string, fields map[string]any) (map[string]any, error) {
	args := map[string]any{"workflow_id": workflowID}
	for key, value := range fields {
		args[key] = value
	}
	return f.record("gate", args)
}

func (f *fakeClient) WorkflowCancel(workflowID, actor, reason string) (map[string]any, error) {
	return f.record("cancel", map[string]any{"workflow_id": workflowID, "actor": actor, "reason": reason})
}

func (f *fakeClient) WorkflowControllerStatus(controllerID string) (map[string]any, error) {
	return f.record("controller_status", map[string]any{"controller_id": controllerID})
}

func (f *fakeClient) WorkflowControllerList() (map[string]any, error) {
	return f.record("controller_list", map[string]any{})
}

func (f *fakeClient) WorkflowReady(controllerID string, limit int) (map[string]any, error) {
	return f.record("ready", map[string]any{"controller_id": controllerID, "limit": limit})
}

// TestConformanceCases drives every fixture case through the router and asserts
// the dispatch or the typed error. The same fixture drives the TypeScript
// router, so a divergence between the two languages fails here or there.
func TestConformanceCases(t *testing.T) {
	for _, c := range loadConformanceCases(t) {
		t.Run(c.Name, func(t *testing.T) {
			client := &fakeClient{}
			result, err := Execute(client, c.Input, Options{})
			if c.Expect.Code != "" {
				if err == nil {
					t.Fatalf("Execute accepted %s", c.Input)
				}
				toolErr, ok := err.(*ToolError)
				if !ok {
					t.Fatalf("error is %T, want *ToolError", err)
				}
				if toolErr.Code != c.Expect.Code {
					t.Fatalf("code = %q, want %q", toolErr.Code, c.Expect.Code)
				}
				if toolErr.Message != c.Expect.Message {
					t.Fatalf("message = %q, want %q", toolErr.Message, c.Expect.Message)
				}
				if len(client.calls) != 0 {
					t.Fatalf("a refused call reached the client: %#v", client.calls)
				}
				return
			}
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if result.Action != c.Expect.Action {
				t.Fatalf("action = %q, want %q", result.Action, c.Expect.Action)
			}
			if len(client.calls) != 1 {
				t.Fatalf("client calls = %d, want 1", len(client.calls))
			}
			got := client.calls[0]
			if got.method != c.Expect.Call {
				t.Fatalf("call = %q, want %q", got.method, c.Expect.Call)
			}
			if !sameJSON(t, got.args, c.Expect.Args) {
				t.Fatalf("args = %#v, want %#v", got.args, c.Expect.Args)
			}
		})
	}
}

// TestConformanceSchemaCases asserts that the JSON Schema rejects every case
// the fixture marks as a schema violation and accepts every valid case, so a
// type typo cannot make the schema reject the whole contract silently.
func TestConformanceSchemaCases(t *testing.T) {
	schema := InputSchema()
	checked := 0
	for _, c := range loadConformanceCases(t) {
		if !c.Schema {
			continue
		}
		checked++
		t.Run(c.Name, func(t *testing.T) {
			if err := validateAgainst(t, schema, c.Input); err == nil {
				t.Fatalf("schema accepted %s", c.Input)
			}
		})
	}
	if checked == 0 {
		t.Fatal("fixture marks no case as a schema violation")
	}
	valid := 0
	for _, c := range loadConformanceCases(t) {
		if c.Schema || c.Expect.Code != "" {
			continue
		}
		valid++
		t.Run("valid/"+c.Name, func(t *testing.T) {
			if err := validateAgainst(t, schema, c.Input); err != nil {
				t.Fatalf("schema rejected the valid input %s: %v", c.Input, err)
			}
		})
	}
	if valid == 0 {
		t.Fatal("fixture carries no valid case")
	}
}

// TestSchemaMatchesRouteTable proves each schema variant and its applicability
// entry admit the same set: same properties, same required keys, same declared
// types, enums and array item types.
func TestSchemaMatchesRouteTable(t *testing.T) {
	schema := InputSchema()
	variants, _ := schema["oneOf"].([]any)
	if len(variants) != len(workflowApplicability) {
		t.Fatalf("schema has %d variants, table has %d actions", len(variants), len(workflowApplicability))
	}
	for _, raw := range variants {
		variant, _ := raw.(map[string]any)
		properties, _ := variant["properties"].(map[string]any)
		action, _ := properties["action"].(map[string]any)["const"].(string)
		entry, ok := workflowApplicability[action]
		if !ok {
			t.Fatalf("schema variant %q has no applicability entry", action)
		}
		permitted := make([]string, 0, len(properties))
		for name := range properties {
			permitted = append(permitted, name)
		}
		sort.Strings(permitted)
		wantPermitted := append([]string{}, entry.Permitted...)
		sort.Strings(wantPermitted)
		if !reflect.DeepEqual(permitted, wantPermitted) {
			t.Fatalf("%s: schema permits %v, table permits %v", action, permitted, wantPermitted)
		}
		required, _ := variant["required"].([]any)
		gotRequired := make([]string, 0, len(required))
		for _, name := range required {
			gotRequired = append(gotRequired, name.(string))
		}
		sort.Strings(gotRequired)
		wantRequired := append([]string{}, entry.Required...)
		sort.Strings(wantRequired)
		if !reflect.DeepEqual(gotRequired, wantRequired) {
			t.Fatalf("%s: schema requires %v, table requires %v", action, gotRequired, wantRequired)
		}
		for name, rawSpec := range properties {
			spec, _ := rawSpec.(map[string]any)
			declared, _ := spec["type"].(string)
			if declared == "" {
				if _, ok := spec["const"]; ok {
					declared = "string"
				} else {
					declared = "json"
				}
			}
			if entry.Types[name] != declared {
				t.Fatalf("%s.%s: schema type %q, table type %q", action, name, declared, entry.Types[name])
			}
			if enum, ok := spec["enum"].([]any); ok {
				got := make([]string, 0, len(enum))
				for _, value := range enum {
					got = append(got, value.(string))
				}
				if !reflect.DeepEqual(got, entry.Enums[name]) {
					t.Fatalf("%s.%s: schema enum %v, table enum %v", action, name, got, entry.Enums[name])
				}
			}
			if items, ok := spec["items"].(map[string]any); ok {
				if entry.Constraints[name].ItemType != items["type"] {
					t.Fatalf("%s.%s: schema item type %v, table item type %q", action, name, items["type"], entry.Constraints[name].ItemType)
				}
			}
		}
	}
}

// TestWaitClamp proves the surface's wait budget marks a timed-out result as
// clamped and leaves a terminal one alone.
func TestWaitClamp(t *testing.T) {
	client := &fakeClient{result: map[string]any{"timed_out": true, "terminal": false}}
	result, err := Execute(client, json.RawMessage(`{"action":"wait","workflow_id":"wf-1","timeout":"5m"}`), Options{
		ClampWait: func(time.Duration) (time.Duration, bool) { return 30 * time.Second, true },
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Value["wait_clamped"] != true {
		t.Fatalf("wait_clamped = %v, want true", result.Value["wait_clamped"])
	}
	if got := client.calls[0].args["timeout_ms"]; got != int64(30000) {
		t.Fatalf("timeout_ms = %v, want 30000", got)
	}

	client = &fakeClient{result: map[string]any{"timed_out": false, "terminal": true}}
	result, err = Execute(client, json.RawMessage(`{"action":"wait","workflow_id":"wf-1"}`), Options{
		ClampWait: func(time.Duration) (time.Duration, bool) { return 30 * time.Second, true },
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, ok := result.Value["wait_clamped"]; ok {
		t.Fatalf("a terminal result was marked clamped: %#v", result.Value)
	}
}

// TestResultText proves the text content is the result object as indented JSON.
func TestResultText(t *testing.T) {
	result := Result{Action: "status", Value: map[string]any{"workflow_id": "wf-1"}}
	want := "{\n  \"workflow_id\": \"wf-1\"\n}"
	if got := result.Text(); got != want {
		t.Fatalf("Text() = %q, want %q", got, want)
	}
}

// TestRefusedError proves a control failure becomes the typed refused error
// rather than a raw transport error.
func TestRefusedError(t *testing.T) {
	client := &fakeClient{err: os.ErrPermission}
	_, err := Execute(client, json.RawMessage(`{"action":"status","workflow_id":"wf-1"}`), Options{})
	toolErr, ok := err.(*ToolError)
	if !ok {
		t.Fatalf("error is %T, want *ToolError", err)
	}
	if toolErr.Code != "refused" {
		t.Fatalf("code = %q, want refused", toolErr.Code)
	}
}

func validateAgainst(t *testing.T, schema map[string]any, input json.RawMessage) error {
	t.Helper()
	raw, err := json.Marshal(schema)
	if err != nil {
		t.Fatalf("encode schema: %v", err)
	}
	var compiled jsonschema.Schema
	if err := json.Unmarshal(raw, &compiled); err != nil {
		t.Fatalf("decode schema: %v", err)
	}
	resolved, err := compiled.Resolve(nil)
	if err != nil {
		t.Fatalf("resolve schema: %v", err)
	}
	var instance any
	if err := json.Unmarshal(input, &instance); err != nil {
		t.Fatalf("decode input: %v", err)
	}
	return resolved.Validate(instance)
}

// sameJSON compares two values by their JSON encoding, so an int64 recorded by
// the fake client matches the float64 a fixture decodes.
func sameJSON(t *testing.T, got, want any) bool {
	t.Helper()
	gotJSON, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal got: %v", err)
	}
	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal want: %v", err)
	}
	return string(gotJSON) == string(wantJSON)
}
