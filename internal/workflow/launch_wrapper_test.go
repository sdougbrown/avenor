package workflow

// launch_wrapper_test.go covers the named launch-wrapper declarations:
// template-side ingress validation (definitions, node selection, and the
// strict argv variants), registration through the manager, and the pure
// ResolveNodeLaunchWrapper resolver that dispatch will call.

import (
	"encoding/json"
	"strings"
	"testing"
)

// launchWrapperTemplateJSON builds a template with one required "worktree"
// param, the given launch-wrapper definitions (nil omits the field), and an
// optional node-level launch_wrapper selection.
func launchWrapperTemplateJSON(t *testing.T, templateID string, wrappers []any, nodeWrapper any) []byte {
	t.Helper()
	tmpl := map[string]any{
		"schema_version":   1,
		"template_id":      templateID,
		"template_version": "1",
		"entry_nodes":      []string{"start"},
		"params":           []any{map[string]any{"id": "worktree", "type": "string", "required": true}},
		"nodes": []any{
			map[string]any{
				"id":     "start",
				"action": map[string]any{"type": "run", "prompt": "do the thing"},
			},
		},
		"terminal_outcomes": []string{"done"},
	}
	if wrappers != nil {
		tmpl["launch_wrappers"] = wrappers
	}
	if nodeWrapper != nil {
		tmpl["nodes"].([]any)[0].(map[string]any)["launch_wrapper"] = nodeWrapper
	}
	data, err := json.Marshal(tmpl)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}

// validClaudeWrapper is a definition that exercises every declared arg form.
func validClaudeWrapper() map[string]any {
	return map[string]any{
		"id": "claude",
		"argv": []any{
			map[string]any{"type": "literal", "value": "/usr/local/bin/claude"},
			map[string]any{"type": "literal", "value": "--workdir flag with spaces and $dollar"},
			map[string]any{"type": "instance_param", "from_instance_param": "worktree"},
		},
	}
}

func TestValidateLaunchWrapperIngress(t *testing.T) {
	t.Parallel()
	valid := func(mutate func(w map[string]any)) map[string]any {
		w := validClaudeWrapper()
		if mutate != nil {
			mutate(w)
		}
		return w
	}
	argv := func(elements ...any) []any { return elements }
	literal := func(value string) map[string]any {
		return map[string]any{"type": "literal", "value": value}
	}
	param := func(name string) map[string]any {
		return map[string]any{"type": "instance_param", "from_instance_param": name}
	}

	tests := []struct {
		name        string
		wrappers    []any
		nodeWrapper any
		wantErr     string
	}{
		{
			name:        "valid literal and instance-param args with node selection",
			wrappers:    []any{valid(nil)},
			nodeWrapper: "claude",
		},
		{
			name: "duplicate definition IDs",
			wrappers: []any{
				valid(nil),
				valid(func(w map[string]any) { w["id"] = "claude" }),
			},
			wantErr: "launch_wrappers declares \"claude\" twice",
		},
		{
			name:     "blank definition ID",
			wrappers: []any{valid(func(w map[string]any) { w["id"] = "" })},
			wantErr:  "minLength at /launch_wrappers/0/id",
		},
		{
			name:     "whitespace-only definition ID",
			wrappers: []any{valid(func(w map[string]any) { w["id"] = " " })},
			wantErr:  "launch wrapper ID is required",
		},
		{
			name:     "empty argv",
			wrappers: []any{valid(func(w map[string]any) { w["argv"] = []any{} })},
			wantErr:  "minItems",
		},
		{
			name:        "node names unknown wrapper",
			wrappers:    []any{valid(nil)},
			nodeWrapper: "missing",
			wantErr:     "node \"start\" launch_wrapper names unknown launch wrapper \"missing\"",
		},
		{
			name:        "whitespace-only node selection is not a valid wrapper",
			wrappers:    []any{valid(nil)},
			nodeWrapper: " ",
			wantErr:     `node "start" launch_wrapper names unknown launch wrapper " "`,
		},
		{
			name:        "node selection without any definitions",
			nodeWrapper: "claude",
			wantErr:     "node \"start\" launch_wrapper names unknown launch wrapper \"claude\"",
		},
		{
			name:        "first argv element is an instance param",
			wrappers:    []any{valid(func(w map[string]any) { w["argv"] = argv(param("worktree")) })},
			nodeWrapper: "claude",
			wantErr:     "argv[0] must be a literal absolute executable path",
		},
		{
			name:        "first argv element is a relative literal",
			wrappers:    []any{valid(func(w map[string]any) { w["argv"] = argv(literal("bin/claude")) })},
			nodeWrapper: "claude",
			wantErr:     "argv[0] must be a literal absolute executable path",
		},
		{
			name:     "empty literal value",
			wrappers: []any{valid(func(w map[string]any) { w["argv"] = append(argv(), literal("")) })},
			wantErr:  "minLength at /launch_wrappers/0/argv/0/value",
		},
		{
			name:     "NUL inside a literal value",
			wrappers: []any{valid(func(w map[string]any) { w["argv"] = append(argv(), literal("a\x00b")) })},
			wantErr:  "must not contain NUL",
		},
		{
			name:     "undeclared instance param",
			wrappers: []any{valid(func(w map[string]any) { w["argv"] = append(argv(), param("nope")) })},
			wantErr:  "names undeclared instance param \"nope\"",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateTemplateJSON(launchWrapperTemplateJSON(t, "lw-ingress", tc.wrappers, tc.nodeWrapper))
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

// TestValidateLaunchWrapperArgIngress pins the strict argv element shapes at
// the JSON ingress: wrong, missing, and null discriminators, extraneous
// variant fields, and non-object elements are each rejected.
func TestValidateLaunchWrapperArgIngress(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		element any
		wantErr string
	}{
		{
			name:    "unknown discriminator",
			element: map[string]any{"type": "env", "value": "/x"},
			wantErr: "discriminator",
		},
		{
			name:    "missing discriminator",
			element: map[string]any{"value": "/x"},
			wantErr: "required at /launch_wrappers/0/argv/1/type",
		},
		{
			name:    "null discriminator",
			element: map[string]any{"type": nil, "value": "/x"},
			wantErr: "cannot be null",
		},
		{
			name:    "extraneous variant field on a literal",
			element: map[string]any{"type": "literal", "value": "/x", "from_instance_param": "worktree"},
			wantErr: "additionalProperties at /launch_wrappers/0/argv/1/from_instance_param",
		},
		{
			name:    "extraneous variant field on an instance param",
			element: map[string]any{"type": "instance_param", "from_instance_param": "worktree", "value": "/x"},
			wantErr: "additionalProperties at /launch_wrappers/0/argv/1/value",
		},
		{
			name:    "non-object element",
			element: "/x",
			wantErr: "type at /launch_wrappers/0/argv/1",
		},
		{
			name:    "non-string literal value",
			element: map[string]any{"type": "literal", "value": 7},
			wantErr: "type at /launch_wrappers/0/argv/1/value",
		},
		{
			name:    "null literal value",
			element: map[string]any{"type": "literal", "value": nil},
			wantErr: "cannot be null",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wrapper := validClaudeWrapper()
			wrapper["argv"] = []any{
				map[string]any{"type": "literal", "value": "/usr/local/bin/claude"},
				tc.element,
			}
			err := ValidateTemplateJSON(launchWrapperTemplateJSON(t, "lw-arg", []any{wrapper}, nil))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ValidateTemplateJSON() = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

// TestValidateLaunchWrapperRequiresRequiredParam proves a launch-wrapper arg
// can only name a required param: an unsupplied optional param would resolve
// to an empty argv element.
func TestValidateLaunchWrapperRequiresRequiredParam(t *testing.T) {
	t.Parallel()
	var tmpl map[string]any
	if err := json.Unmarshal(launchWrapperTemplateJSON(t, "lw-optional", []any{validClaudeWrapper()}, nil), &tmpl); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	tmpl["params"] = []any{map[string]any{"id": "worktree", "type": "string"}}
	data, err := json.Marshal(tmpl)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := ValidateTemplateJSON(data); err == nil || !strings.Contains(err.Error(), "must be declared required") {
		t.Fatalf("ValidateTemplateJSON() = %v, want the required-param rejection", err)
	}
}

// TestValidateLaunchWrapperUnsupportedNodeActions proves only run, loop, and
// team nodes may select a launch wrapper: child-workflow, manual, and
// external nodes are rejected.
func TestValidateLaunchWrapperUnsupportedNodeActions(t *testing.T) {
	t.Parallel()
	wrapper := func() map[string]any { return validClaudeWrapper() }
	tests := []struct {
		name   string
		action map[string]any
	}{
		{"child workflow node", map[string]any{"type": "workflow", "template_id": "child", "template_version": "1", "child_key": "child-key", "outcome_map": map[string]string{"done": "done"}}},
		{"manual node", map[string]any{"type": "manual", "instructions": "do it by hand"}},
		{"external node", map[string]any{"type": "external", "source": "webhook"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := launchWrapperTemplateJSON(t, "lw-action", []any{wrapper()}, "claude")
			var tmpl map[string]any
			if err := json.Unmarshal(data, &tmpl); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			tmpl["nodes"].([]any)[0].(map[string]any)["action"] = tc.action
			out, err := json.Marshal(tmpl)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if err := ValidateTemplateJSON(out); err == nil || !strings.Contains(err.Error(), "requires a run, loop, or team action") {
				t.Fatalf("ValidateTemplateJSON() = %v, want the unsupported-action rejection", err)
			}
		})
	}
}

func TestValidateLaunchWrapperInMemoryCrossFields(t *testing.T) {
	t.Parallel()
	absolute := "/usr/local/bin/claude"
	tests := []struct {
		name    string
		mutate  func(w *LaunchWrapperDefinition)
		wantErr string
	}{
		{
			name: "blank ID",
			mutate: func(w *LaunchWrapperDefinition) {
				w.ID = ""
			},
			wantErr: "launch wrapper ID is required",
		},
		{
			name: "whitespace-only ID",
			mutate: func(w *LaunchWrapperDefinition) {
				w.ID = " "
			},
			wantErr: "launch wrapper ID is required",
		},
		{
			name: "empty argv",
			mutate: func(w *LaunchWrapperDefinition) {
				w.Argv = nil
			},
			wantErr: "argv must not be empty",
		},
		{
			name: "empty literal value",
			mutate: func(w *LaunchWrapperDefinition) {
				w.Argv = []LaunchWrapperArg{{Type: LaunchWrapperArgKindLiteral, Value: "/usr/local/bin/claude"}, {Type: LaunchWrapperArgKindLiteral}}
			},
			wantErr: "literal value must not be empty",
		},
		{
			name: "wrong discriminator",
			mutate: func(w *LaunchWrapperDefinition) {
				w.Argv = []LaunchWrapperArg{{Type: LaunchWrapperArgKind("env"), Value: absolute}}
			},
			wantErr: `type "env" is not a launch_wrapper_arg type`,
		},
		{
			name: "missing discriminator",
			mutate: func(w *LaunchWrapperDefinition) {
				w.Argv = []LaunchWrapperArg{{Value: absolute}}
			},
			wantErr: `type "" is not a launch_wrapper_arg type`,
		},
		{
			name: "literal with the instance-param field also set",
			mutate: func(w *LaunchWrapperDefinition) {
				w.Argv = []LaunchWrapperArg{{Type: LaunchWrapperArgKindLiteral, Value: absolute, FromInstanceParam: "worktree"}}
			},
			wantErr: "must not declare from_instance_param",
		},
		{
			name: "instance param with the literal value also set",
			mutate: func(w *LaunchWrapperDefinition) {
				w.Argv = []LaunchWrapperArg{{Type: LaunchWrapperArgKindInstanceParam, FromInstanceParam: "worktree", Value: absolute}}
			},
			wantErr: "must not declare value",
		},
		{
			name: "first element is an instance param",
			mutate: func(w *LaunchWrapperDefinition) {
				w.Argv = []LaunchWrapperArg{{Type: LaunchWrapperArgKindInstanceParam, FromInstanceParam: "worktree"}}
			},
			wantErr: "argv[0] must be a literal absolute executable path",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			template := Template{
				SchemaVersion:    1,
				TemplateID:       "lw-memory",
				TemplateVersion:  "1",
				EntryNodes:       []NodeID{"start"},
				Nodes:            []NodeDefinition{{ID: "start", Action: Action{Kind: ActionRun, Run: &RunAction{Prompt: "p"}}, LaunchWrapper: "claude"}},
				TerminalOutcomes: []OutcomeName{"done"},
				Params:           []TemplateParam{{ID: "worktree", Type: "string", Required: true}},
				LaunchWrappers:   []LaunchWrapperDefinition{{ID: "claude", Argv: []LaunchWrapperArg{{Type: LaunchWrapperArgKindLiteral, Value: absolute}}}},
			}
			tc.mutate(&template.LaunchWrappers[0])
			err := ValidateTemplate(template)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ValidateTemplate() = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

// TestLaunchWrapperRegistrationAndInstantiate proves the manager path accepts
// a valid wrapper template and instantiates it with the required param.
func TestLaunchWrapperRegistrationAndInstantiate(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	data := launchWrapperTemplateJSON(t, "lw-register", []any{validClaudeWrapper()}, "claude")
	if _, err := m.WorkflowCreate(data); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"template_id": "lw-register", "template_version": "1",
		"params": map[string]string{"worktree": "/tmp/lw-worktree"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := m.WorkflowInstantiate(payload); err != nil {
		t.Fatalf("WorkflowInstantiate: %v", err)
	}
}

// TestLaunchWrapperRoundTripPreservesExactArguments proves literal strings
// containing spaces and $ survive the JSON round trip byte-for-byte.
func TestLaunchWrapperRoundTripPreservesExactArguments(t *testing.T) {
	t.Parallel()
	spaces := "--workdir flag with spaces and $dollar"
	data := launchWrapperTemplateJSON(t, "lw-roundtrip", []any{validClaudeWrapper()}, "claude")
	var template Template
	if err := json.Unmarshal(data, &template); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	wrapper := template.LaunchWrappers[0]
	if wrapper.ID != "claude" {
		t.Fatalf("wrapper ID = %q, want %q", wrapper.ID, "claude")
	}
	if len(wrapper.Argv) != 3 {
		t.Fatalf("argv length = %d, want 3", len(wrapper.Argv))
	}
	if wrapper.Argv[0] != (LaunchWrapperArg{Type: LaunchWrapperArgKindLiteral, Value: "/usr/local/bin/claude"}) {
		t.Fatalf("argv[0] = %+v", wrapper.Argv[0])
	}
	if wrapper.Argv[1].Value != spaces {
		t.Fatalf("argv[1].value = %q, want %q", wrapper.Argv[1].Value, spaces)
	}
	if wrapper.Argv[2] != (LaunchWrapperArg{Type: LaunchWrapperArgKindInstanceParam, FromInstanceParam: "worktree"}) {
		t.Fatalf("argv[2] = %+v", wrapper.Argv[2])
	}
	encoded, err := json.Marshal(template)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var round map[string]any
	if err := json.Unmarshal(encoded, &round); err != nil {
		t.Fatalf("unmarshal round trip: %v", err)
	}
	argv := round["launch_wrappers"].([]any)[0].(map[string]any)["argv"].([]any)
	if argv[1].(map[string]any)["value"] != spaces {
		t.Fatalf("round-tripped argv[1] = %v, want the exact %q", argv[1], spaces)
	}
}

// TestResolveNodeLaunchWrapper covers the pure resolver: selection, exact
// values, fresh slices, and fail-closed behavior on bad inputs.
func TestResolveNodeLaunchWrapper(t *testing.T) {
	t.Parallel()
	template := Template{
		LaunchWrappers: []LaunchWrapperDefinition{{
			ID: "claude",
			Argv: []LaunchWrapperArg{
				{Type: LaunchWrapperArgKindLiteral, Value: "/usr/local/bin/claude"},
				{Type: LaunchWrapperArgKindLiteral, Value: "--workdir with spaces and $dollar"},
				{Type: LaunchWrapperArgKindInstanceParam, FromInstanceParam: "worktree"},
			},
		}},
	}
	node := NodeDefinition{ID: "start", LaunchWrapper: "claude"}
	params := map[string]string{"worktree": "/tmp/wt"}

	resolved, err := ResolveNodeLaunchWrapper(template, node, params)
	if err != nil {
		t.Fatalf("ResolveNodeLaunchWrapper() = %v, want nil", err)
	}
	want := []string{"/usr/local/bin/claude", "--workdir with spaces and $dollar", "/tmp/wt"}
	if len(resolved) != len(want) {
		t.Fatalf("resolved = %q, want %q", resolved, want)
	}
	for i, element := range want {
		if resolved[i] != element {
			t.Fatalf("resolved[%d] = %q, want %q", i, resolved[i], element)
		}
	}

	// The result is a fresh slice: mutating it cannot corrupt the template.
	resolved[0] = "mutated"
	again, err := ResolveNodeLaunchWrapper(template, node, params)
	if err != nil {
		t.Fatalf("second ResolveNodeLaunchWrapper() = %v, want nil", err)
	}
	if again[0] != want[0] {
		t.Fatalf("template argv leaked through the resolver: argv[0] = %q, want %q", again[0], want[0])
	}

	// No selection resolves to nil.
	plain := NodeDefinition{ID: "start"}
	if resolved, err := ResolveNodeLaunchWrapper(template, plain, params); resolved != nil || err != nil {
		t.Fatalf("ResolveNodeLaunchWrapper(no selection) = %q, %v, want nil, nil", resolved, err)
	}

	failures := []struct {
		name    string
		node    NodeDefinition
		params  map[string]string
		wantErr string
	}{
		{"missing definition", NodeDefinition{ID: "start", LaunchWrapper: "missing"}, params, "unknown launch wrapper"},
		{"whitespace-only selection", NodeDefinition{ID: "start", LaunchWrapper: " "}, params, "unknown launch wrapper"},
		{"missing param", node, map[string]string{}, "instance param \"worktree\" is not supplied"},
		{"empty resolved value", node, map[string]string{"worktree": ""}, "resolved to an empty value"},
		{"NUL in resolved value", node, map[string]string{"worktree": "a\x00b"}, "must not contain NUL"},
	}
	for _, tc := range failures {
		t.Run(tc.name, func(t *testing.T) {
			resolved, err := ResolveNodeLaunchWrapper(template, tc.node, tc.params)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ResolveNodeLaunchWrapper() = %q, %v, want containing %q", resolved, err, tc.wantErr)
			}
			if resolved != nil {
				t.Fatalf("ResolveNodeLaunchWrapper() = %q, want nil argv on error", resolved)
			}
		})
	}

	// Fail closed on unvalidated argv, even when the caller skipped validation.
	unvalidated := []struct {
		name string
		argv []LaunchWrapperArg
	}{
		{"empty literal value", []LaunchWrapperArg{{Type: LaunchWrapperArgKindLiteral, Value: "/usr/local/bin/claude"}, {Type: LaunchWrapperArgKindLiteral}}},
		{"NUL in a literal value", []LaunchWrapperArg{{Type: LaunchWrapperArgKindLiteral, Value: "/usr/local/bin/claude"}, {Type: LaunchWrapperArgKindLiteral, Value: "a\x00b"}}},
		{"non-literal executable", []LaunchWrapperArg{{Type: LaunchWrapperArgKind("env"), Value: "/usr/local/bin/claude"}}},
		{"empty argv", nil},
		{"relative executable", []LaunchWrapperArg{{Type: LaunchWrapperArgKindLiteral, Value: "bin/wrapper"}}},
		{"instance-param executable", []LaunchWrapperArg{{Type: LaunchWrapperArgKindInstanceParam, FromInstanceParam: "worktree"}}},
	}
	for _, tc := range unvalidated {
		t.Run(tc.name, func(t *testing.T) {
			broken := Template{LaunchWrappers: []LaunchWrapperDefinition{{ID: "claude", Argv: tc.argv}}}
			if _, err := ResolveNodeLaunchWrapper(broken, node, params); err == nil {
				t.Fatalf("ResolveNodeLaunchWrapper(%s) = nil error, want the fail-closed rejection", tc.name)
			}
		})
	}
}
