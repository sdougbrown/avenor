package workflow

// launch_wrapper_test.go covers the launch-wrapper registry declarations:
// ingress validation of definitions and the strict argv variants, the typed
// in-memory backstop, registration through the manager, and the exact-value
// JSON round trip.

import (
	"encoding/json"
	"strings"
	"testing"
)

// launchWrapperTemplateJSON builds a template with the given launch-wrapper
// definitions (nil omits the field).
func launchWrapperTemplateJSON(t *testing.T, templateID string, wrappers []any) []byte {
	t.Helper()
	tmpl := map[string]any{
		"schema_version":   1,
		"template_id":      templateID,
		"template_version": "1",
		"entry_nodes":      []string{"start"},
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
	data, err := json.Marshal(tmpl)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}

// validClaudeWrapper is a definition that exercises the declared arg form,
// including a value with spaces and shell metacharacters.
func validClaudeWrapper() map[string]any {
	return map[string]any{
		"id": "claude",
		"argv": []any{
			map[string]any{"type": "literal", "value": "/usr/local/bin/claude"},
			map[string]any{"type": "literal", "value": "--workdir flag with spaces and $dollar"},
		},
	}
}

func TestValidateLaunchWrapperIngress(t *testing.T) {
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

	tests := []struct {
		name     string
		wrappers []any
		wantErr  string
	}{
		{
			name:     "valid literal args",
			wrappers: []any{valid(nil)},
		},
		{
			name:     "unused definition still validates",
			wrappers: []any{valid(nil)},
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
			name:     "empty argv",
			wrappers: []any{valid(func(w map[string]any) { w["argv"] = []any{} })},
			wantErr:  "minItems",
		},
		{
			name:     "first argv element is a relative literal",
			wrappers: []any{valid(func(w map[string]any) { w["argv"] = argv(literal("bin/claude")) })},
			wantErr:  "argv[0] must be a literal absolute executable path",
		},
		{
			name:     "empty literal value",
			wrappers: []any{valid(func(w map[string]any) { w["argv"] = append(argv(literal("/usr/local/bin/claude")), literal("")) })},
			wantErr:  "minLength at /launch_wrappers/0/argv/1/value",
		},
		{
			name:     "NUL inside a literal value",
			wrappers: []any{valid(func(w map[string]any) { w["argv"] = append(argv(literal("/usr/local/bin/claude")), literal("a\x00b")) })},
			wantErr:  "must not contain NUL",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateTemplateJSON(launchWrapperTemplateJSON(t, "lw-ingress", tc.wrappers))
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
			err := ValidateTemplateJSON(launchWrapperTemplateJSON(t, "lw-arg", []any{wrapper}))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ValidateTemplateJSON() = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

// TestValidateLaunchWrapperInMemoryCrossFields pins the typed backstop on
// in-memory structs: a wrong or missing discriminator, an empty argv, empty
// literal values, and a blank ID are rejected even without going through JSON.
func TestValidateLaunchWrapperInMemoryCrossFields(t *testing.T) {
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
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			template := Template{
				SchemaVersion:    1,
				TemplateID:       "lw-memory",
				TemplateVersion:  "1",
				EntryNodes:       []NodeID{"start"},
				Nodes:            []NodeDefinition{{ID: "start", Action: Action{Kind: ActionRun, Run: &RunAction{Prompt: "p"}}}},
				TerminalOutcomes: []OutcomeName{"done"},
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
// a valid wrapper template and instantiates it.
func TestLaunchWrapperRegistrationAndInstantiate(t *testing.T) {
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	data := launchWrapperTemplateJSON(t, "lw-register", []any{validClaudeWrapper()})
	if _, err := m.WorkflowCreate(data); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"template_id": "lw-register", "template_version": "1",
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
	spaces := "--workdir flag with spaces and $dollar"
	data := launchWrapperTemplateJSON(t, "lw-roundtrip", []any{validClaudeWrapper()})
	var template Template
	if err := json.Unmarshal(data, &template); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	wrapper := template.LaunchWrappers[0]
	if wrapper.ID != "claude" {
		t.Fatalf("wrapper ID = %q, want %q", wrapper.ID, "claude")
	}
	if len(wrapper.Argv) != 2 {
		t.Fatalf("argv length = %d, want 2", len(wrapper.Argv))
	}
	if wrapper.Argv[0] != (LaunchWrapperArg{Type: LaunchWrapperArgKindLiteral, Value: "/usr/local/bin/claude"}) {
		t.Fatalf("argv[0] = %+v", wrapper.Argv[0])
	}
	if wrapper.Argv[1].Value != spaces {
		t.Fatalf("argv[1].value = %q, want %q", wrapper.Argv[1].Value, spaces)
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
