package workflowtool

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"reflect"
	"slices"
	"sort"
	"strings"
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

// loadFixtureApplicability reads the fixture's applicability map, which is the
// source of the generated table. Comparing the compiled table against it catches
// a fixture edit that was never regenerated.
func loadFixtureApplicability(t *testing.T) map[string]ActionApplicability {
	t.Helper()
	data, err := os.ReadFile("../../schemas/workflow_route.conformance.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var fixture struct {
		Applicability map[string]struct {
			Access    string            `json:"access"`
			Permitted []string          `json:"permitted"`
			Required  []string          `json:"required"`
			Types     map[string]string `json:"types"`
		} `json:"applicability"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	out := make(map[string]ActionApplicability, len(fixture.Applicability))
	for action, entry := range fixture.Applicability {
		out[action] = ActionApplicability{
			Access:    entry.Access,
			Permitted: entry.Permitted,
			Required:  entry.Required,
			Types:     entry.Types,
		}
	}
	return out
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
	schema := schemaWithExactNumbers(t)
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
			// Every declared limit must survive compilation into the table, so a
			// schema edit that adds a bound cannot drift from the router.
			constraint := entry.Constraints[name]
			if v, ok := spec["minLength"]; ok {
				if int(mustInt64(t, v)) != constraint.MinLength {
					t.Fatalf("%s.%s: schema minLength %v, table %d", action, name, v, constraint.MinLength)
				}
			} else if constraint.MinLength != 0 {
				t.Fatalf("%s.%s: table declares minLength %d but the schema declares none", action, name, constraint.MinLength)
			}
			if v, ok := spec["maxLength"]; ok {
				if int(mustInt64(t, v)) != constraint.MaxLength {
					t.Fatalf("%s.%s: schema maxLength %v, table %d", action, name, v, constraint.MaxLength)
				}
			} else if constraint.MaxLength != 0 {
				t.Fatalf("%s.%s: table declares maxLength %d but the schema declares none", action, name, constraint.MaxLength)
			}
			if v, ok := spec["pattern"]; ok {
				if v.(string) != constraint.Pattern {
					t.Fatalf("%s.%s: schema pattern %v, table %q", action, name, v, constraint.Pattern)
				}
			} else if constraint.Pattern != "" {
				t.Fatalf("%s.%s: table declares pattern %q but the schema declares none", action, name, constraint.Pattern)
			}
			if v, ok := spec["minimum"]; ok && v.(json.Number).String() != fmt.Sprint(constraint.Minimum) {
				t.Fatalf("%s.%s: schema minimum %v, table %d", action, name, v, constraint.Minimum)
			}
			if _, ok := spec["minimum"]; ok != constraint.HasMinimum {
				t.Fatalf("%s.%s: schema minimum presence %v, table %v", action, name, !constraint.HasMinimum, constraint.HasMinimum)
			}
			if v, ok := spec["maximum"]; ok && v.(json.Number).String() != fmt.Sprint(constraint.Maximum) {
				t.Fatalf("%s.%s: schema maximum %v, table %d", action, name, v, constraint.Maximum)
			}
			if _, ok := spec["maximum"]; ok != constraint.HasMaximum {
				t.Fatalf("%s.%s: schema maximum presence %v, table %v", action, name, !constraint.HasMaximum, constraint.HasMaximum)
			}
		}
	}

	// The fixture's applicability map is the source of the generated table, so a
	// fixture edit that was never regenerated must fail here rather than leave
	// the router validating the old contract.
	fixture := loadFixtureApplicability(t)
	if len(fixture) != len(workflowApplicability) {
		t.Fatalf("fixture declares %d actions, table has %d", len(fixture), len(workflowApplicability))
	}
	for action, want := range fixture {
		got, ok := workflowApplicability[action]
		if !ok {
			t.Fatalf("fixture action %q has no table entry", action)
		}
		if got.Access != want.Access {
			t.Fatalf("%s: fixture access %q, table access %q", action, want.Access, got.Access)
		}
		if !reflect.DeepEqual(got.Permitted, want.Permitted) {
			t.Fatalf("%s: fixture permits %v, table permits %v", action, want.Permitted, got.Permitted)
		}
		if !reflect.DeepEqual(got.Required, want.Required) {
			t.Fatalf("%s: fixture requires %v, table requires %v", action, want.Required, got.Required)
		}
		if !reflect.DeepEqual(got.Types, want.Types) {
			t.Fatalf("%s: fixture types %v, table types %v", action, want.Types, got.Types)
		}
	}
}

// TestCompleteKeepsLargeIntegersExact proves the JSON-typed fields reach the
// client as the bytes the caller sent. Decoding them into any would turn every
// number into a float64 and round an integer above 2^53 before dispatch.
func TestCompleteKeepsLargeIntegersExact(t *testing.T) {
	client := &fakeClient{}
	_, err := Execute(client, json.RawMessage(`{"action":"complete","workflow_id":"wf-1","node_id":"n1","activation_id":"a1","attempt_id":"at1","lease_id":"l1","owner_token":"tok","outcome":"success","outputs":[{"definition_id":"o1","value":9007199254740993}]}`), Options{})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	raw, err := json.Marshal(client.calls[0].args["outputs"])
	if err != nil {
		t.Fatalf("marshal outputs: %v", err)
	}
	if !strings.Contains(string(raw), "9007199254740993") {
		t.Fatalf("outputs lost the exact integer: %s", raw)
	}
}

// TestApplicabilityTableCarriesAccess proves the exported accessor returns each
// action's access level and hands out a copy. The credential mode is chosen from
// it, so a clone that dropped the field would make every read action look
// owner-gated.
func TestApplicabilityTableCarriesAccess(t *testing.T) {
	table := ApplicabilityTable()
	for action, want := range workflowApplicability {
		if got := table[action].Access; got != want.Access {
			t.Fatalf("%s: exported access %q, want %q", action, got, want.Access)
		}
	}
	if got := table["status"].Access; got != accessRead {
		t.Fatalf("status: exported access %q, want %q", got, accessRead)
	}
	table["status"] = ActionApplicability{Access: "tampered", Permitted: []string{"tampered"}}
	if got := workflowApplicability["status"].Access; got == "tampered" {
		t.Fatal("ApplicabilityTable handed out the live table")
	}
	if got := workflowApplicability["status"].Permitted; len(got) == 1 && got[0] == "tampered" {
		t.Fatal("ApplicabilityTable handed out a live Permitted slice")
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

	// A clamp that returns the requested duration unchanged must not mark the
	// result: only a budget the surface actually shortened is clamped.
	client = &fakeClient{result: map[string]any{"timed_out": true, "terminal": false}}
	result, err = Execute(client, json.RawMessage(`{"action":"wait","workflow_id":"wf-1","timeout":"5m"}`), Options{
		ClampWait: func(requested time.Duration) (time.Duration, bool) { return requested, false },
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, ok := result.Value["wait_clamped"]; ok {
		t.Fatalf("an unclamped timed-out result was marked clamped: %#v", result.Value)
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
	// Decode both sides with UseNumber so an int64 recorded by the fake client
	// matches the float64 a fixture decodes, a json.RawMessage matches the object
	// it carries, and an integer above 2^53 stays exact.
	normalize := func(value any) any {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var out any
		if err := decoder.Decode(&out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return out
	}
	return reflect.DeepEqual(normalize(got), normalize(want))
}

// TestSchemaFilesCompiled catches a stale schema.gen.go without running go
// generate: the compiled constant and the checked-in file decode identically.
func TestSchemaFilesCompiled(t *testing.T) {
	if got, want := InputSchema(), readSchemaFile(t, "../../schemas/workflow.input.json"); !reflect.DeepEqual(got, want) {
		t.Fatal("workflow.input.json and schema.gen.go disagree; run go generate ./internal/workflowtool/...")
	}
	if got, want := InputFlatSchema(), readSchemaFile(t, "../../schemas/workflow.input.flat.json"); !reflect.DeepEqual(got, want) {
		t.Fatal("workflow.input.flat.json and schema.gen.go disagree; run go generate ./internal/workflowtool/...")
	}
}

// TestFlatSchemaAcceptsEveryValidInput proves the flat schema MCP surfaces
// publish admits every input the router admits, the valid conformance cases'
// integer and object arguments included, so publishing it cannot turn a valid
// call into a client-side schema error.
func TestFlatSchemaAcceptsEveryValidInput(t *testing.T) {
	valid := 0
	for _, c := range loadConformanceCases(t) {
		if c.Schema || c.Expect.Code != "" {
			continue
		}
		valid++
		t.Run(c.Name, func(t *testing.T) {
			if err := validateAgainst(t, InputFlatSchema(), c.Input); err != nil {
				t.Fatalf("flat schema rejected the valid input %s: %v", c.Input, err)
			}
		})
	}
	if valid == 0 {
		t.Fatal("fixture carries no valid case")
	}
}

// TestFlatSchemaIsTheUnion pins the flat schema to the union it is compiled
// from: every action's properties, each keeping the type and limits its action
// declares, an action enum, no union keyword, and only a key every action
// requires. A generator change that drops a property, a limit or the required
// intersection fails here.
func TestFlatSchemaIsTheUnion(t *testing.T) {
	union, flat := InputSchema(), InputFlatSchema()
	for _, banned := range []string{"oneOf", "anyOf"} {
		if _, ok := flat[banned]; ok {
			t.Fatalf("flat schema carries %s", banned)
		}
	}
	if flat["type"] != "object" {
		t.Fatalf("flat type = %v, want object", flat["type"])
	}
	if additional, ok := flat["additionalProperties"].(bool); !ok || additional {
		t.Fatal("flat additionalProperties must be false")
	}
	flatProperties, ok := flat["properties"].(map[string]any)
	if !ok {
		t.Fatal("flat properties is not an object")
	}

	var actions []any
	flatRequired := []string{}
	declaring := map[string][]string{}
	declared := map[string][]map[string]any{}
	variants, _ := union["oneOf"].([]any)
	for i, rawVariant := range variants {
		variant, _ := rawVariant.(map[string]any)
		properties, _ := variant["properties"].(map[string]any)
		action, _ := properties["action"].(map[string]any)["const"].(string)
		actions = append(actions, action)
		required := stringList(variant["required"])
		if i == 0 {
			flatRequired = required
		} else {
			flatRequired = intersectStrings(flatRequired, required)
		}
		for name, rawProperty := range properties {
			property, _ := rawProperty.(map[string]any)
			if name == "action" {
				continue
			}
			declaring[name] = append(declaring[name], action)
			declared[name] = append(declared[name], withoutKey(property, "description"))
		}
	}
	for _, name := range flatRequired {
		if name != "action" {
			t.Fatalf("flat schema would require %q, which is not the action", name)
		}
	}
	if got := stringList(flat["required"]); !slices.Equal(got, flatRequired) {
		t.Fatalf("flat required = %v, the union's intersection = %v", got, flatRequired)
	}

	names := make([]string, 0, len(flatProperties))
	for name := range flatProperties {
		names = append(names, name)
	}
	sort.Strings(names)
	wantNames := make([]string, 0, len(declared)+1)
	for name := range declared {
		wantNames = append(wantNames, name)
	}
	wantNames = append(wantNames, "action")
	sort.Strings(wantNames)
	if !slices.Equal(names, wantNames) {
		t.Fatalf("flat properties = %v, the union's = %v", names, wantNames)
	}

	for name, declarations := range declared {
		flatProperty, _ := flatProperties[name].(map[string]any)
		got := withoutKey(flatProperty, "description")
		for i, want := range declarations {
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%s: flat declaration = %v, union declaration = %v", declaring[name][i], got, want)
			}
		}
	}
	actionProperty, _ := flatProperties["action"].(map[string]any)
	if !reflect.DeepEqual(actionProperty["enum"], actions) {
		t.Fatalf("flat action enum = %v, want %v", actionProperty["enum"], actions)
	}
}

// schemaWithExactNumbers decodes the checked-in union with json.Number, so a
// declared bound at the int64 limit survives the comparison.
func schemaWithExactNumbers(t *testing.T) map[string]any {
	t.Helper()
	data, err := os.ReadFile("../../schemas/workflow.input.json")
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var schema map[string]any
	if err := decoder.Decode(&schema); err != nil {
		t.Fatalf("decode schema: %v", err)
	}
	return schema
}

func readSchemaFile(t *testing.T, path string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	// Same decode rule as decodeSchema: an integer bound at the int64 limit
	// must survive as an exact number on both sides of the comparison.
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var schema map[string]any
	if err := decoder.Decode(&schema); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return schema
}

func mustInt64(t *testing.T, value any) int64 {
	t.Helper()
	number, ok := value.(json.Number)
	if !ok {
		t.Fatalf("constraint %v is not a JSON number", value)
	}
	out, err := number.Int64()
	if err != nil {
		t.Fatalf("constraint %v: %v", value, err)
	}
	return out
}

func stringList(value any) []string {
	raw, _ := value.([]any)
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		out = append(out, item.(string))
	}
	return out
}

func intersectStrings(first, second []string) []string {
	out := make([]string, 0, len(first))
	for _, name := range first {
		if slices.Contains(second, name) {
			out = append(out, name)
		}
	}
	return out
}

func withoutKey(property map[string]any, key string) map[string]any {
	out := maps.Clone(property)
	delete(out, key)
	return out
}
