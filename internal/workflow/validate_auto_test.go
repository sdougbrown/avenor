package workflow

import (
	"strings"
	"testing"
)

// TestValidateAutoProviderNodeRejectsExplicitCompletion pins the dispatch
// rule that keeps supervisor-side auto-completion decidable: an auto run,
// loop, or team node must not declare completion.kind explicit, because no
// worker handoff exists for auto-dispatched provider nodes — the supervisor
// completes them from the declared contract. Manual nodes and auto external
// nodes (whose park path consumes an explicit completion legitimately) are
// unaffected.
func TestValidateAutoProviderNodeRejectsExplicitCompletion(t *testing.T) {
	explicit := func(node map[string]any) {
		node["completion"] = map[string]any{"kind": "explicit"}
	}
	auto := func(node map[string]any) {
		node["dispatch"] = map[string]any{"mode": "auto", "controller_id": "c1"}
	}
	tests := []struct {
		name    string
		mutate  func(map[string]any)
		wantErr string
	}{
		{
			name: "auto run node",
			mutate: func(template map[string]any) {
				node := template["nodes"].([]any)[1].(map[string]any)
				node["action"] = map[string]any{"type": "run", "prompt": "work"}
				auto(node)
				explicit(node)
			},
			wantErr: `must not declare completion.kind "explicit"`,
		},
		{
			name: "auto loop node",
			mutate: func(template map[string]any) {
				node := template["nodes"].([]any)[1].(map[string]any)
				auto(node)
				explicit(node)
			},
			wantErr: `must not declare completion.kind "explicit"`,
		},
		{
			name: "auto team node",
			mutate: func(template map[string]any) {
				node := template["nodes"].([]any)[1].(map[string]any)
				node["action"] = map[string]any{"type": "team", "team_file": "t.json"}
				auto(node)
				explicit(node)
			},
			wantErr: `must not declare completion.kind "explicit"`,
		},
		{
			name: "manual node keeps explicit",
			mutate: func(template map[string]any) {
				explicit(template["nodes"].([]any)[0].(map[string]any))
			},
		},
		{
			name: "auto provider node without explicit keeps files",
			mutate: func(template map[string]any) {
				node := template["nodes"].([]any)[1].(map[string]any)
				node["action"] = map[string]any{"type": "run", "prompt": "work"}
				auto(node)
				node["completion"] = map[string]any{"kind": "files", "artifacts": []any{map[string]any{"path": "out.md"}}}
			},
		},
		{
			name: "auto external node keeps explicit",
			mutate: func(template map[string]any) {
				// Give intake the outputs the gate binding references.
				template["nodes"].([]any)[0].(map[string]any)["outputs"] = []any{
					map[string]any{"id": "issue", "name": "Issue", "type": "string"},
					map[string]any{"id": "base_sha", "name": "Base SHA", "type": "string"},
					map[string]any{"id": "pr_number", "name": "PR number", "type": "number"},
				}
				node := template["nodes"].([]any)[1].(map[string]any)
				node["action"] = map[string]any{"type": "external", "source": "github", "subject_type": "pull_request"}
				auto(node)
				node["dispatch"].(map[string]any)["success_outcome"] = "clean"
				delete(node, "outputs")
				node["branches"] = map[string]any{"clean": "intake"}
				node["completion"] = map[string]any{"kind": "explicit"}
				node["gates"] = []any{map[string]any{
					"id": "ci", "type": "external", "required": true, "subject_type": "pull_request",
					"adapter_id": "circleci-pipeline",
					"subject_binding": map[string]any{
						"type":         "pull_request",
						"repository":   map[string]any{"from_node_output": map[string]any{"node_id": "intake", "output_id": "issue"}},
						"pull_request": map[string]any{"from_node_output": map[string]any{"node_id": "intake", "output_id": "pr_number"}},
						"revision":     map[string]any{"from_node_output": map[string]any{"node_id": "intake", "output_id": "base_sha"}},
					},
				}}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := mutateTemplate(t, test.mutate)
			err := ValidateTemplateJSON(data)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateTemplateJSON() rejected a valid template: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("ValidateTemplateJSON() error = %v, want containing %q", err, test.wantErr)
			}
		})
	}
}

// TestValidateAutoProviderNodeRejectsChangedFromBase pins that an auto
// provider node cannot declare a git contract the supervisor can never
// evaluate: no base commit is recorded, so changed_from_base would exhaust
// every attempt. Manual nodes and other git requirements are unaffected.
func TestValidateAutoProviderNodeRejectsChangedFromBase(t *testing.T) {
	gitContract := func(node map[string]any, req map[string]any) {
		node["action"] = map[string]any{"type": "run", "prompt": "work"}
		node["completion"] = map[string]any{"kind": "git", "git": req}
	}
	tests := []struct {
		name    string
		mutate  func(map[string]any)
		wantErr string
	}{
		{
			name: "auto run node with changed_from_base",
			mutate: func(template map[string]any) {
				node := template["nodes"].([]any)[1].(map[string]any)
				gitContract(node, map[string]any{"changed_from_base": true})
				node["dispatch"] = map[string]any{"mode": "auto", "controller_id": "c1"}
			},
			wantErr: "must not declare completion.git.changed_from_base",
		},
		{
			name: "auto run node with clean only",
			mutate: func(template map[string]any) {
				node := template["nodes"].([]any)[1].(map[string]any)
				gitContract(node, map[string]any{"clean": true})
				node["dispatch"] = map[string]any{"mode": "auto", "controller_id": "c1"}
			},
		},
		{
			name: "manual run node keeps changed_from_base",
			mutate: func(template map[string]any) {
				node := template["nodes"].([]any)[1].(map[string]any)
				gitContract(node, map[string]any{"changed_from_base": true})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateTemplateJSON(mutateTemplate(t, test.mutate))
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateTemplateJSON() rejected a valid template: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("ValidateTemplateJSON() error = %v, want containing %q", err, test.wantErr)
			}
		})
	}
}
