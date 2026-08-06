package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

const validTemplateJSON = `{
  "schema_version": 1,
  "template_id": "software-factory-work",
  "template_version": "1.0.0",
  "metadata": {"title": "Software factory work"},
  "entry_nodes": ["intake"],
  "nodes": [
    {"id": "intake", "name": "Intake", "action": {"type": "manual"}},
    {"id": "execute", "dependencies": ["intake"], "action": {"type": "loop", "loop_file": "execute.json"}}
  ],
  "terminal_outcomes": ["clean", "abandoned"],
  "default_lease_policy": {"ttl_seconds": 900},
  "default_retry_policy": {"max_attempts": 2, "exhaustion": "block"},
  "composition_limits": {"max_depth": 4, "max_children": 32}
}`

func TestValidateTemplateJSON(t *testing.T) {
	if err := ValidateTemplateJSON([]byte(validTemplateJSON)); err != nil {
		t.Fatalf("ValidateTemplateJSON() error = %v", err)
	}

	largeNumber := mutateTemplate(t, func(template map[string]any) {
		template["metadata"] = map[string]any{"large_number": json.Number("1e400")}
	})
	if err := ValidateTemplateJSON(largeNumber); err != nil {
		t.Fatalf("ValidateTemplateJSON() rejected unrestricted metadata number: %v", err)
	}
}

func TestValidateTemplateJSONRejectsRequiredFields(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(map[string]any)
		wantErr string
	}{
		{name: "missing schema version", mutate: deleteField("schema_version"), wantErr: "schema_version is required"},
		{name: "missing template id", mutate: deleteField("template_id"), wantErr: "template_id is required"},
		{name: "blank template id", mutate: setField("template_id", "  "), wantErr: "template_id is required"},
		{name: "missing template version", mutate: deleteField("template_version"), wantErr: "template_version is required"},
		{name: "blank template version", mutate: setField("template_version", "\t"), wantErr: "template_version is required"},
		{name: "missing entry nodes", mutate: deleteField("entry_nodes"), wantErr: "entry_nodes is required"},
		{name: "empty entry nodes", mutate: setField("entry_nodes", []any{}), wantErr: "entry_nodes is required"},
		{name: "blank entry node", mutate: setField("entry_nodes", []any{" "}), wantErr: "entry_nodes[0] is empty"},
		{name: "missing nodes", mutate: deleteField("nodes"), wantErr: "nodes is required"},
		{name: "empty nodes", mutate: setField("nodes", []any{}), wantErr: "nodes is required"},
		{name: "missing terminal outcomes", mutate: deleteField("terminal_outcomes"), wantErr: "terminal_outcomes is required"},
		{name: "empty terminal outcomes", mutate: setField("terminal_outcomes", []any{}), wantErr: "terminal_outcomes is required"},
		{name: "blank terminal outcome", mutate: setField("terminal_outcomes", []any{"\n"}), wantErr: "terminal_outcomes[0] is empty"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertTemplateError(t, mutateTemplate(t, test.mutate), test.wantErr)
		})
	}
}

func TestValidateTemplateJSONRejectsInvalidNodes(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(map[string]any)
		wantErr string
	}{
		{
			name: "missing node id",
			mutate: mutateFirstNode(func(node map[string]any) {
				delete(node, "id")
			}),
			wantErr: "nodes[0].id is required",
		},
		{
			name: "blank node id",
			mutate: mutateFirstNode(func(node map[string]any) {
				node["id"] = " "
			}),
			wantErr: "nodes[0].id is required",
		},
		{
			name: "missing action",
			mutate: mutateFirstNode(func(node map[string]any) {
				delete(node, "action")
			}),
			wantErr: `node "intake": action is required`,
		},
		{
			name: "null action",
			mutate: mutateFirstNode(func(node map[string]any) {
				node["action"] = nil
			}),
			wantErr: `field "action" cannot be null`,
		},
		{
			name: "non-object action",
			mutate: mutateFirstNode(func(node map[string]any) {
				node["action"] = "manual"
			}),
			wantErr: `workflow action:`,
		},
		{
			name: "missing action type",
			mutate: mutateFirstNode(func(node map[string]any) {
				node["action"] = map[string]any{}
			}),
			wantErr: `workflow action.type is required`,
		},
		{
			name: "blank action type",
			mutate: mutateFirstNode(func(node map[string]any) {
				node["action"] = map[string]any{"type": "  "}
			}),
			wantErr: `workflow action.type is required`,
		},
		{
			name: "non-string action type",
			mutate: mutateFirstNode(func(node map[string]any) {
				node["action"] = map[string]any{"type": 1}
			}),
			wantErr: `workflow action:`,
		},
		{
			name: "invalid action discriminator",
			mutate: mutateFirstNode(func(node map[string]any) {
				node["action"] = map[string]any{"type": "shell"}
			}),
			wantErr: `unsupported workflow action "shell"`,
		},
		{
			name: "unknown node field",
			mutate: mutateFirstNode(func(node map[string]any) {
				node["misspelled"] = true
			}),
			wantErr: `unknown field "misspelled"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertTemplateError(t, mutateTemplate(t, test.mutate), test.wantErr)
		})
	}
}

func TestValidateTemplateJSONRejectsIncompleteActionVariants(t *testing.T) {
	tests := []struct {
		name    string
		action  map[string]any
		wantErr string
	}{
		{name: "run", action: map[string]any{"type": "run"}, wantErr: "exactly one of prompt or prompt_file"},
		{name: "loop", action: map[string]any{"type": "loop"}, wantErr: "requires loop_file"},
		{name: "team", action: map[string]any{"type": "team", "team_file": " "}, wantErr: "requires team_file"},
		{name: "external", action: map[string]any{"type": "external"}, wantErr: "requires source"},
		{name: "workflow", action: map[string]any{"type": "workflow", "template_id": "child", "template_version": "1", "child_key": "child"}, wantErr: "requires outcome_map"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := mutateTemplate(t, mutateFirstNode(func(node map[string]any) {
				node["action"] = test.action
			}))
			assertTemplateError(t, input, test.wantErr)
		})
	}
}

func TestValidateTemplateJSONRejectsAmbiguousOrUnboundedJSON(t *testing.T) {
	t.Run("unknown top-level field", func(t *testing.T) {
		assertTemplateError(t, mutateTemplate(t, setField("misspelled", true)), `unknown field "misspelled"`)
	})
	t.Run("explicit null optional field", func(t *testing.T) {
		assertTemplateError(t, mutateTemplate(t, setField("metadata", nil)), `field "metadata" cannot be null`)
	})
	t.Run("explicit null node field", func(t *testing.T) {
		input := mutateTemplate(t, mutateFirstNode(func(node map[string]any) { node["name"] = nil }))
		assertTemplateError(t, input, `field "name" cannot be null`)
	})
	t.Run("explicit null typed slice element", func(t *testing.T) {
		input := mutateTemplate(t, mutateFirstNode(func(node map[string]any) { node["outputs"] = []any{nil} }))
		assertTemplateError(t, input, `nodes[0].outputs[0]: value cannot be null`)
	})
	t.Run("unsupported schema version", func(t *testing.T) {
		assertTemplateError(t, mutateTemplate(t, setField("schema_version", 2)), "schema_version must be the JSON integer 1")
	})
	t.Run("schema version must compare exactly", func(t *testing.T) {
		input := strings.Replace(validTemplateJSON, `"schema_version": 1`, `"schema_version": 1.0000000000000000000000000001`, 1)
		assertTemplateError(t, []byte(input), "schema_version must be the JSON integer 1")
	})
	t.Run("quoted schema version", func(t *testing.T) {
		input := strings.Replace(validTemplateJSON, `"schema_version": 1`, `"schema_version": "1"`, 1)
		assertTemplateError(t, []byte(input), "schema_version must be the JSON integer 1")
	})
	t.Run("schema version exponent is bounded", func(t *testing.T) {
		input := strings.Replace(validTemplateJSON, `"schema_version": 1`, `"schema_version": 1e2000000000`, 1)
		assertTemplateError(t, []byte(input), "schema_version must be the JSON integer 1")
	})
	t.Run("schema version uses canonical integer syntax", func(t *testing.T) {
		input := strings.Replace(validTemplateJSON, `"schema_version": 1`, `"schema_version": 1e0`, 1)
		assertTemplateError(t, []byte(input), "schema_version must be the JSON integer 1")
	})
	t.Run("multiple values", func(t *testing.T) {
		assertTemplateError(t, []byte(validTemplateJSON+` {"large":"value"}`), "multiple JSON values")
	})
	t.Run("duplicate top-level key", func(t *testing.T) {
		input := strings.Replace(validTemplateJSON, `"schema_version": 1`, `"schema_version": 1, "schema_version": 1`, 1)
		assertTemplateError(t, []byte(input), `duplicate object key "schema_version"`)
	})
	t.Run("duplicate nested discriminator", func(t *testing.T) {
		input := strings.Replace(validTemplateJSON, `"type": "manual"`, `"type": "manual", "type": "run"`, 1)
		assertTemplateError(t, []byte(input), `duplicate object key "type"`)
	})
	t.Run("case-folded top-level key", func(t *testing.T) {
		input := strings.Replace(validTemplateJSON, `"schema_version": 1`, `"Schema_Version": 2, "schema_version": 1`, 1)
		assertTemplateError(t, []byte(input), `non-canonical field "Schema_Version"`)
	})
	t.Run("case-folded node field", func(t *testing.T) {
		input := strings.Replace(validTemplateJSON, `"id": "intake"`, `"ID": "intake"`, 1)
		assertTemplateError(t, []byte(input), `non-canonical field "ID"`)
	})
	t.Run("case-folded nested policy field", func(t *testing.T) {
		input := mutateTemplate(t, setField("default_lease_policy", map[string]any{"TTL_SECONDS": 900}))
		assertTemplateError(t, input, `non-canonical field "TTL_SECONDS"; use "ttl_seconds"`)
	})
	t.Run("case-folded nested output field", func(t *testing.T) {
		input := mutateTemplate(t, mutateFirstNode(func(node map[string]any) {
			node["outputs"] = []any{map[string]any{"id": "result", "Name": "Result", "type": "string"}}
		}))
		assertTemplateError(t, input, `non-canonical field "Name"; use "name"`)
	})
	t.Run("case-folded nested discriminator", func(t *testing.T) {
		input := strings.Replace(validTemplateJSON, `"type": "manual"`, `"Type": "run", "type": "manual"`, 1)
		assertTemplateError(t, []byte(input), `non-canonical field "Type"`)
	})
	t.Run("byte limit", func(t *testing.T) {
		assertTemplateError(t, bytes.Repeat([]byte{' '}, maxTemplateBytes+1), "template exceeds")
	})
	t.Run("nesting limit", func(t *testing.T) {
		input := strings.Repeat("[", maxJSONDepth+1) + "0" + strings.Repeat("]", maxJSONDepth+1)
		assertTemplateError(t, []byte(input), "JSON nesting exceeds")
	})
	t.Run("collection limit", func(t *testing.T) {
		var input strings.Builder
		input.WriteByte('[')
		for index := 0; index <= maxContainerItems; index++ {
			if index > 0 {
				input.WriteByte(',')
			}
			input.WriteByte('0')
		}
		input.WriteByte(']')
		assertTemplateError(t, []byte(input.String()), "JSON array exceeds")
	})
}

func TestJSONPreflightAcceptsExactLimits(t *testing.T) {
	t.Run("byte limit", func(t *testing.T) {
		prefix := []byte(`{"metadata":{"padding":"`)
		suffix := []byte(`"}}`)
		input := append(prefix, bytes.Repeat([]byte{'x'}, maxTemplateBytes-len(prefix)-len(suffix))...)
		input = append(input, suffix...)
		var template Template
		if err := decodeStrict(input, &template); err != nil {
			t.Fatalf("decodeStrict() rejected exact byte limit: %v", err)
		}
	})
	t.Run("nesting limit", func(t *testing.T) {
		input := strings.Repeat("[", maxJSONDepth) + "0" + strings.Repeat("]", maxJSONDepth)
		if err := preflightJSON([]byte(input)); err != nil {
			t.Fatalf("preflightJSON() rejected exact nesting limit: %v", err)
		}
	})
	t.Run("array item limit", func(t *testing.T) {
		if err := preflightJSON(numberArray(maxContainerItems)); err != nil {
			t.Fatalf("preflightJSON() rejected exact array item limit: %v", err)
		}
	})
	t.Run("object member limit", func(t *testing.T) {
		var input strings.Builder
		input.WriteByte('{')
		for index := 0; index < maxContainerItems; index++ {
			if index > 0 {
				input.WriteByte(',')
			}
			fmt.Fprintf(&input, `"k%d":0`, index)
		}
		input.WriteByte('}')
		if err := preflightJSON([]byte(input.String())); err != nil {
			t.Fatalf("preflightJSON() rejected exact object member limit: %v", err)
		}
	})
}

func TestGeneratedWorkflowSchemaAPI(t *testing.T) {
	version := 1.0
	templateID := "example"
	templateVersion := "1"
	availability := Check(
		WorkflowFields{
			SchemaVersion:    &version,
			TemplateId:       &templateID,
			TemplateVersion:  &templateVersion,
			EntryNodes:       []string{"start"},
			Nodes:            []string{"start"},
			TerminalOutcomes: []string{"done"},
		},
		WorkflowConditions{},
		WorkflowFields{},
	)
	for name, status := range map[string]FieldStatus{
		"schema_version":    availability.SchemaVersion,
		"template_id":       availability.TemplateId,
		"template_version":  availability.TemplateVersion,
		"entry_nodes":       availability.EntryNodes,
		"nodes":             availability.Nodes,
		"terminal_outcomes": availability.TerminalOutcomes,
	} {
		if !status.Required || !status.Satisfied || !status.Fair {
			t.Errorf("generated Check() status for %s = %+v, want required, satisfied, and fair", name, status)
		}
	}

	badVersion := 2.0
	availability = Check(
		WorkflowFields{SchemaVersion: &badVersion},
		WorkflowConditions{},
		WorkflowFields{},
	)
	if availability.SchemaVersion.Fair || availability.SchemaVersion.Reason == nil {
		t.Fatalf("generated Check() accepted schema version 2: %+v", availability.SchemaVersion)
	}
}

func mutateTemplate(t *testing.T, mutate func(map[string]any)) []byte {
	t.Helper()
	var template map[string]any
	if err := json.Unmarshal([]byte(validTemplateJSON), &template); err != nil {
		t.Fatalf("decode valid template fixture: %v", err)
	}
	mutate(template)
	data, err := json.Marshal(template)
	if err != nil {
		t.Fatalf("encode mutated template fixture: %v", err)
	}
	return data
}

func deleteField(name string) func(map[string]any) {
	return func(template map[string]any) { delete(template, name) }
}

func setField(name string, value any) func(map[string]any) {
	return func(template map[string]any) { template[name] = value }
}

func mutateFirstNode(mutate func(map[string]any)) func(map[string]any) {
	return func(template map[string]any) {
		nodes := template["nodes"].([]any)
		mutate(nodes[0].(map[string]any))
	}
}

func assertTemplateError(t *testing.T, input []byte, want string) {
	t.Helper()
	err := ValidateTemplateJSON(input)
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("ValidateTemplateJSON() error = %v, want containing %q\ninput: %s", err, want, boundedTestInput(input))
	}
}

func numberArray(items int) []byte {
	var input strings.Builder
	input.WriteByte('[')
	for index := 0; index < items; index++ {
		if index > 0 {
			input.WriteByte(',')
		}
		input.WriteByte('0')
	}
	input.WriteByte(']')
	return []byte(input.String())
}

func boundedTestInput(input []byte) string {
	const limit = 512
	if len(input) <= limit {
		return string(input)
	}
	return fmt.Sprintf("%s... (%d bytes)", input[:limit], len(input))
}
