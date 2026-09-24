package workflow

// instance_params_test.go covers instance parameters and templated
// concurrency keys: template-side declaration validation, both declared
// concurrency-key wire forms, instantiate-time param enforcement, and the
// activation-creation freeze of a templated key from the recorded params
// (identical on replay recovery, distinct across different params, shared
// across equal params).

import (
	"encoding/json"
	"strings"
	"testing"
)

// paramsTemplateJSON is a template declaring one required worktree param
// whose auto node resolves its concurrency key from it.
func paramsTemplateJSON(templateID string, concurrencyKey any) []byte {
	tmpl := map[string]any{
		"schema_version":   1,
		"template_id":      templateID,
		"template_version": "1",
		"entry_nodes":      []string{"start"},
		"params":           []any{map[string]any{"id": "worktree", "type": "string", "required": true}},
		"nodes": []any{
			map[string]any{
				"id":         "start",
				"action":     map[string]any{"type": "run", "prompt": "do the thing"},
				"assignment": map[string]any{"role": "worker", "roster_entry": "worker"},
				"dispatch": map[string]any{
					"mode":            "auto",
					"controller_id":   "c1",
					"concurrency_key": concurrencyKey,
				},
			},
		},
		"terminal_outcomes": []string{"done"},
	}
	data, err := json.Marshal(tmpl)
	if err != nil {
		panic(err)
	}
	return data
}

func TestValidateTemplateParamsDeclarations(t *testing.T) {
	tests := []struct {
		name    string
		params  []map[string]any
		wantErr string
	}{
		{
			name:   "valid required string param",
			params: []map[string]any{{"id": "worktree", "type": "string", "required": true}},
		},
		{
			name:    "path-unsafe id",
			params:  []map[string]any{{"id": "a/b", "type": "string"}},
			wantErr: "path-safe identifier",
		},
		{
			name:    "identifier starting with a digit",
			params:  []map[string]any{{"id": "9tree", "type": "string"}},
			wantErr: "path-safe identifier",
		},
		{
			name:    "duplicate declaration",
			params:  []map[string]any{{"id": "a", "type": "string"}, {"id": "a", "type": "string"}},
			wantErr: "twice",
		},
		{
			name:    "unsupported type",
			params:  []map[string]any{{"id": "a", "type": "number"}},
			wantErr: "invalid workflow template",
		},
		{
			name:   "optional param",
			params: []map[string]any{{"id": "region", "type": "string"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data, err := json.Marshal(map[string]any{
				"schema_version":    1,
				"template_id":       "params-decl",
				"template_version":  "1",
				"entry_nodes":       []string{"start"},
				"params":            tc.params,
				"nodes":             []any{map[string]any{"id": "start", "action": map[string]any{"type": "manual"}}},
				"terminal_outcomes": []string{"done"},
			})
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			err = ValidateTemplateJSON(data)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateTemplateJSON() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ValidateTemplateJSON() = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestValidateTemplateTemplatedKeyRequiresDeclaredParam(t *testing.T) {
	// The object form naming an undeclared param is rejected.
	data := paramsTemplateJSON("key-undecl", map[string]any{"prefix": "worktree:", "from_instance_param": "nope"})
	if err := ValidateTemplateJSON(data); err == nil || !strings.Contains(err.Error(), "undeclared instance param") {
		t.Fatalf("ValidateTemplateJSON() = %v, want undeclared instance param", err)
	}
	// The object form with a blank from_instance_param is rejected.
	data = paramsTemplateJSON("key-blank", map[string]any{"prefix": "worktree:"})
	if err := ValidateTemplateJSON(data); err == nil || !strings.Contains(err.Error(), "from_instance_param is required") {
		t.Fatalf("ValidateTemplateJSON() = %v, want from_instance_param is required", err)
	}
	// Declaring both forms on one node is rejected.
	data = paramsTemplateJSON("key-both", map[string]any{"prefix": "worktree:", "from_instance_param": "worktree"})
	var tmpl Template
	if err := json.Unmarshal(data, &tmpl); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	tmpl.Nodes[0].Dispatch.ConcurrencyKey = "worktree:static"
	if err := ValidateTemplate(tmpl); err == nil || !strings.Contains(err.Error(), "not both") {
		t.Fatalf("ValidateTemplate() = %v, want not both", err)
	}
	// A well-formed templated key validates.
	data = paramsTemplateJSON("key-ok", map[string]any{"prefix": "worktree:", "from_instance_param": "worktree"})
	if err := ValidateTemplateJSON(data); err != nil {
		t.Fatalf("ValidateTemplateJSON() = %v, want nil", err)
	}
}

func TestWorkflowInstantiateEnforcesParams(t *testing.T) {
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	if _, err := m.WorkflowCreate(paramsTemplateJSON("params-enforce", map[string]any{"prefix": "worktree:", "from_instance_param": "worktree"})); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	instantiate := func(params map[string]string) error {
		t.Helper()
		payload, err := json.Marshal(map[string]any{
			"template_id": "params-enforce", "template_version": "1", "params": params,
		})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		_, err = m.WorkflowInstantiate(payload)
		return err
	}
	if err := instantiate(nil); err == nil || !strings.Contains(err.Error(), "required param") {
		t.Fatalf("missing required param: %v, want required param", err)
	}
	if err := instantiate(map[string]string{"bogus": "x"}); err == nil || !strings.Contains(err.Error(), "not declared") {
		t.Fatalf("unknown param: %v, want not declared", err)
	}
	if err := instantiate(map[string]string{"worktree": ""}); err == nil || !strings.Contains(err.Error(), "must not be empty") {
		t.Fatalf("empty value: %v, want must not be empty", err)
	}
	if err := instantiate(map[string]string{"worktree": strings.Repeat("x", 257)}); err == nil || !strings.Contains(err.Error(), "256") {
		t.Fatalf("overlong value: %v, want 256 limit", err)
	}
	if err := instantiate(map[string]string{"worktree": "bad\x07value"}); err == nil || !strings.Contains(err.Error(), "control characters") {
		t.Fatalf("control char value: %v, want control characters", err)
	}
	if err := instantiate(map[string]string{"worktree": "avenor-issue-130"}); err != nil {
		t.Fatalf("valid params rejected: %v", err)
	}
}

// TestTemplatedKeyFreezesFromInstanceParams proves the effective concurrency
// key freezes from the recorded params at activation creation: two instances
// with different params resolve different keys, two with the same param share
// one, and recovery replays the identical key from the recorded params.
func TestTemplatedKeyFreezesFromInstanceParams(t *testing.T) {
	keyJSON := map[string]any{"prefix": "worktree:", "from_instance_param": "worktree"}
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	if _, err := m.WorkflowCreate(paramsTemplateJSON("key-freeze", keyJSON)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	instantiate := func(params map[string]string) WorkflowID {
		t.Helper()
		payload, err := json.Marshal(map[string]any{
			"template_id": "key-freeze", "template_version": "1", "params": params,
		})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		out, err := m.WorkflowInstantiate(payload)
		if err != nil {
			t.Fatalf("WorkflowInstantiate: %v", err)
		}
		return WorkflowID(out.(map[string]any)["workflow_id"].(string))
	}
	wfA := instantiate(map[string]string{"worktree": "avenor-issue-115"})
	wfB := instantiate(map[string]string{"worktree": "avenor-issue-130"})
	wfC := instantiate(map[string]string{"worktree": "avenor-issue-115"})

	activationKey := func(t *testing.T, wf WorkflowID) string {
		t.Helper()
		insp, err := m.WorkflowInspect(string(wf))
		if err != nil {
			t.Fatalf("WorkflowInspect: %v", err)
		}
		inst := insp.(map[string]any)["instance"].(WorkflowInstance)
		if len(inst.Activations) != 1 || inst.Activations[0].Dispatch == nil {
			t.Fatalf("instance %s has no dispatched activation: %+v", wf, inst.Activations)
		}
		if inst.Params["worktree"] == "" {
			t.Fatalf("instance %s recorded no worktree param: %+v", wf, inst.Params)
		}
		return inst.Activations[0].Dispatch.ConcurrencyKey
	}
	if got := activationKey(t, wfA); got != "worktree:avenor-issue-115" {
		t.Fatalf("instance A key = %q, want worktree:avenor-issue-115", got)
	}
	if got := activationKey(t, wfB); got != "worktree:avenor-issue-130" {
		t.Fatalf("instance B key = %q, want worktree:avenor-issue-130", got)
	}
	if activationKey(t, wfA) != activationKey(t, wfC) {
		t.Fatal("instances with the same worktree param resolved different keys")
	}

	// Recovery replays the identical key from the recorded params.
	recovered := NewManager(s)
	for _, wf := range []WorkflowID{wfA, wfB} {
		insp, err := recovered.WorkflowInspect(string(wf))
		if err != nil {
			t.Fatalf("recovered WorkflowInspect(%s): %v", wf, err)
		}
		inst := insp.(map[string]any)["instance"].(WorkflowInstance)
		if len(inst.Activations) != 1 || inst.Activations[0].Dispatch == nil {
			t.Fatalf("recovered instance %s has no dispatched activation", wf)
		}
		if inst.Activations[0].Dispatch.ConcurrencyKey != activationKey(t, wf) {
			t.Fatalf("recovered key %q differs from live key %q",
				inst.Activations[0].Dispatch.ConcurrencyKey, activationKey(t, wf))
		}
	}
}

// TestLegacyTemplateWithoutParamsUnchanged proves a template with a plain
// string key and no params instantiates exactly as before: no params are
// recorded and the activation carries the literal key.
func TestLegacyTemplateWithoutParamsUnchanged(t *testing.T) {
	data, err := json.Marshal(map[string]any{
		"schema_version":   1,
		"template_id":      "legacy-key",
		"template_version": "1",
		"entry_nodes":      []string{"start"},
		"nodes": []any{
			map[string]any{
				"id":     "start",
				"action": map[string]any{"type": "run", "prompt": "do the thing"},
				"dispatch": map[string]any{
					"mode": "auto", "controller_id": "c1", "concurrency_key": "worktree:static",
				},
			},
		},
		"terminal_outcomes": []string{"done"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	if _, err := m.WorkflowCreate(data); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	out, err := m.WorkflowInstantiate([]byte(`{"template_id":"legacy-key","template_version":"1"}`))
	if err != nil {
		t.Fatalf("WorkflowInstantiate: %v", err)
	}
	wf := out.(map[string]any)["workflow_id"].(string)
	insp, err := m.WorkflowInspect(wf)
	if err != nil {
		t.Fatalf("WorkflowInspect: %v", err)
	}
	inst := insp.(map[string]any)["instance"].(WorkflowInstance)
	if len(inst.Params) != 0 {
		t.Fatalf("legacy instance recorded params %+v, want none", inst.Params)
	}
	if len(inst.Activations) != 1 || inst.Activations[0].Dispatch == nil || inst.Activations[0].Dispatch.ConcurrencyKey != "worktree:static" {
		t.Fatalf("legacy activation key = %+v, want worktree:static", inst.Activations)
	}
}
