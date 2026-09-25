package workflow

// Tests for bound-gate declaration validation (adapter inputs, subject
// bindings, result routing) and the auto-dispatch eligibility of zero-output
// external nodes.

import (
	"encoding/json"
	"testing"
)

// boundGateTemplateJSON is the minimal valid template with a publication node
// that records the durable subject outputs and an auto external review node
// whose required gate binds them.
const boundGateTemplateJSON = `{
  "schema_version": 1,
  "template_id": "bound-gates",
  "template_version": "1.0.0",
  "entry_nodes": ["publication"],
  "nodes": [
    {
      "id": "publication",
      "action": {"type": "manual"},
      "outputs": [
        {"id": "repository", "name": "Repository", "type": "string", "required": true},
        {"id": "pr_number", "name": "PR number", "type": "number", "required": true},
        {"id": "pr_head", "name": "PR head SHA", "type": "string", "required": true},
        {"id": "is_draft", "name": "Is draft", "type": "boolean"},
        {"id": "report", "name": "Report", "type": "json"}
      ],
      "outcomes": [{"name": "published", "target_node_id": "review"}]
    },
    {
      "id": "review",
      "dependencies": ["publication"],
      "action": {"type": "external", "source": "github"},
      "dispatch": {"mode": "auto", "controller_id": "ctl", "success_outcome": "clean"},
      "branches": {"clean": "merge", "failed": "publication"},
      "gates": [{
        "id": "pr-review",
        "type": "external",
        "required": true,
        "adapter_id": "gh-review",
        "inputs": {
          "pull_number": {"from_node_output": {"node_id": "publication", "output_id": "pr_number"}},
          "head_sha": {"from_node_output": {"node_id": "publication", "output_id": "pr_head"}},
          "level": "high",
          "strict": true
        },
        "subject_binding": {
          "type": "pull_request",
          "repository": {"from_node_output": {"node_id": "publication", "output_id": "repository"}},
          "pull_request": {"from_node_output": {"node_id": "publication", "output_id": "pr_number"}},
          "revision": {"from_node_output": {"node_id": "publication", "output_id": "pr_head"}}
        },
        "result_outcomes": {"changes_requested": "failed"}
      }]
    },
    {"id": "merge", "dependencies": ["review"], "action": {"type": "manual"}}
  ],
  "terminal_outcomes": ["done"]
}`

func boundGateMutation(mutate func(template map[string]any)) []byte {
	return mutateBoundTemplate(boundGateTemplateJSON, mutate)
}

func mutateBoundTemplate(fixture string, mutate func(map[string]any)) []byte {
	var template map[string]any
	if err := json.Unmarshal([]byte(fixture), &template); err != nil {
		panic("decode fixture: " + err.Error())
	}
	mutate(template)
	data, err := json.Marshal(template)
	if err != nil {
		panic("encode mutated fixture: " + err.Error())
	}
	return data
}

func boundGateNode(template map[string]any, nodeID string) map[string]any {
	nodes := template["nodes"].([]any)
	for _, raw := range nodes {
		node := raw.(map[string]any)
		if node["id"] == nodeID {
			return node
		}
	}
	return nil
}

// boundGateFixtureWithoutRequiredHead drops the required flag from the
// pr_head output definition, so completing the publication without it leaves
// the review gate's pinned subject and head_sha input unresolved.
func boundGateFixtureWithoutRequiredHead() []byte {
	return mutateBoundTemplate(boundGateTemplateJSON, func(template map[string]any) {
		publication := boundGateNode(template, "publication")
		outputs := publication["outputs"].([]any)
		for _, raw := range outputs {
			def := raw.(map[string]any)
			if def["id"] == "pr_head" {
				delete(def, "required")
			}
		}
	})
}

func TestBoundGateTemplateValidation(t *testing.T) {
	if err := ValidateTemplateJSON([]byte(boundGateTemplateJSON)); err != nil {
		t.Fatalf("ValidateTemplateJSON() error = %v", err)
	}

	invalid := []struct {
		name   string
		mutate func(map[string]any)
		want   string
	}{
		{"adapter_id on human gate", func(t map[string]any) {
			node := boundGateNode(t, "merge")
			node["gates"] = []any{map[string]any{"id": "auth", "type": "human", "required": true, "adapter_id": "gh"}}
		}, "adapter_id is forbidden"},
		{"inputs on human gate", func(t map[string]any) {
			node := boundGateNode(t, "merge")
			node["gates"] = []any{map[string]any{"id": "auth", "type": "human", "required": true, "inputs": map[string]any{"x": 1}}}
		}, "inputs are forbidden"},
		{"adapter_id on machine gate", func(t map[string]any) {
			node := boundGateNode(t, "merge")
			node["gates"] = []any{map[string]any{"id": "ci", "type": "machine", "required": true, "adapter_id": "ci"}}
		}, "adapter_id is forbidden"},
		{"inputs on machine gate", func(t map[string]any) {
			node := boundGateNode(t, "merge")
			node["gates"] = []any{map[string]any{"id": "ci", "type": "machine", "required": true, "inputs": map[string]any{"x": 1}}}
		}, "inputs are forbidden"},
		{"subject_binding on machine gate", func(t map[string]any) {
			node := boundGateNode(t, "merge")
			node["gates"] = []any{map[string]any{
				"id": "ci", "type": "machine", "required": true,
				"subject_binding": map[string]any{
					"type":         "pull_request",
					"repository":   map[string]any{"from_node_output": map[string]any{"node_id": "publication", "output_id": "repository"}},
					"pull_request": map[string]any{"from_node_output": map[string]any{"node_id": "publication", "output_id": "pr_number"}},
					"revision":     map[string]any{"from_node_output": map[string]any{"node_id": "publication", "output_id": "pr_head"}},
				},
			}}
		}, "subject_binding is forbidden on machine gates"},
		{"subject_binding unsupported type", func(t map[string]any) {
			gates := boundGateNode(t, "review")["gates"].([]any)
			gate := gates[0].(map[string]any)
			binding := gate["subject_binding"].(map[string]any)
			binding["type"] = "pipeline"
		}, "const at /nodes/1/gates/0/subject_binding/type"},
		{"subject_binding missing revision", func(t map[string]any) {
			gates := boundGateNode(t, "review")["gates"].([]any)
			gate := gates[0].(map[string]any)
			binding := gate["subject_binding"].(map[string]any)
			delete(binding, "revision")
		}, "revision"},
		{"subject_binding non-ancestor node", func(t map[string]any) {
			gates := boundGateNode(t, "review")["gates"].([]any)
			gate := gates[0].(map[string]any)
			binding := gate["subject_binding"].(map[string]any)
			binding["revision"] = map[string]any{"from_node_output": map[string]any{"node_id": "merge", "output_id": "pr_head"}}
		}, "not a transitive dependency"},
		{"subject_binding undeclared output", func(t map[string]any) {
			gates := boundGateNode(t, "review")["gates"].([]any)
			gate := gates[0].(map[string]any)
			binding := gate["subject_binding"].(map[string]any)
			binding["revision"] = map[string]any{"from_node_output": map[string]any{"node_id": "publication", "output_id": "nope"}}
		}, "undeclared output"},
		{"subject_binding repository type mismatch", func(t map[string]any) {
			gates := boundGateNode(t, "review")["gates"].([]any)
			gate := gates[0].(map[string]any)
			binding := gate["subject_binding"].(map[string]any)
			binding["repository"] = map[string]any{"from_node_output": map[string]any{"node_id": "publication", "output_id": "pr_number"}}
		}, `requires a "string" output`},
		{"subject_binding pull_request type mismatch", func(t map[string]any) {
			gates := boundGateNode(t, "review")["gates"].([]any)
			gate := gates[0].(map[string]any)
			binding := gate["subject_binding"].(map[string]any)
			binding["pull_request"] = map[string]any{"from_node_output": map[string]any{"node_id": "publication", "output_id": "pr_head"}}
		}, `requires a "number" output`},
		{"subject_binding mixed source nodes", func(t map[string]any) {
			nodes := t["nodes"].([]any)
			nodes = append(nodes, map[string]any{
				"id": "publication2", "action": map[string]any{"type": "manual"},
				"outputs": []any{map[string]any{"id": "repo2", "name": "Repo2", "type": "string", "required": true}},
			})
			t["nodes"] = nodes
			review := boundGateNode(t, "review")
			review["dependencies"] = []any{"publication", "publication2"}
			gates := review["gates"].([]any)
			gate := gates[0].(map[string]any)
			binding := gate["subject_binding"].(map[string]any)
			binding["revision"] = map[string]any{"from_node_output": map[string]any{"node_id": "publication2", "output_id": "repo2"}}
		}, "one source node"},
		{"input ref non-ancestor node", func(t map[string]any) {
			gates := boundGateNode(t, "review")["gates"].([]any)
			gate := gates[0].(map[string]any)
			inputs := gate["inputs"].(map[string]any)
			inputs["far"] = map[string]any{"from_node_output": map[string]any{"node_id": "merge", "output_id": "pr_head"}}
		}, "not a transitive dependency"},
		{"input ref undeclared output", func(t map[string]any) {
			gates := boundGateNode(t, "review")["gates"].([]any)
			gate := gates[0].(map[string]any)
			inputs := gate["inputs"].(map[string]any)
			inputs["missing"] = map[string]any{"from_node_output": map[string]any{"node_id": "publication", "output_id": "nope"}}
		}, "undeclared output"},
		{"input ref non-primitive output", func(t map[string]any) {
			gates := boundGateNode(t, "review")["gates"].([]any)
			gate := gates[0].(map[string]any)
			inputs := gate["inputs"].(map[string]any)
			inputs["report"] = map[string]any{"from_node_output": map[string]any{"node_id": "publication", "output_id": "report"}}
		}, "non-primitive output"},
		{"input literal null", func(t map[string]any) {
			gates := boundGateNode(t, "review")["gates"].([]any)
			gate := gates[0].(map[string]any)
			inputs := gate["inputs"].(map[string]any)
			inputs["level"] = nil
		}, "cannot be null"},
		{"input literal object", func(t map[string]any) {
			gates := boundGateNode(t, "review")["gates"].([]any)
			gate := gates[0].(map[string]any)
			inputs := gate["inputs"].(map[string]any)
			inputs["level"] = map[string]any{"nested": true}
		}, "unknown field"},
		{"input literal unsafe number", func(t map[string]any) {
			gates := boundGateNode(t, "review")["gates"].([]any)
			gate := gates[0].(map[string]any)
			inputs := gate["inputs"].(map[string]any)
			inputs["level"] = json.Number("1e400")
		}, "finite number"},
		{"result_outcomes on unbound gate", func(t map[string]any) {
			node := boundGateNode(t, "merge")
			node["gates"] = []any{map[string]any{"id": "auth", "type": "human", "required": true, "result_outcomes": map[string]any{"failed": "clean"}}}
		}, "bound external gates"},
		{"result_outcomes passed key", func(t map[string]any) {
			gates := boundGateNode(t, "review")["gates"].([]any)
			gate := gates[0].(map[string]any)
			outcomes := gate["result_outcomes"].(map[string]any)
			outcomes["passed"] = "clean"
		}, "passed is excluded"},
		{"result_outcomes unknown result", func(t map[string]any) {
			gates := boundGateNode(t, "review")["gates"].([]any)
			gate := gates[0].(map[string]any)
			outcomes := gate["result_outcomes"].(map[string]any)
			outcomes["pending"] = "clean"
		}, "not a routable result"},
		{"result_outcomes branchless outcome", func(t map[string]any) {
			gates := boundGateNode(t, "review")["gates"].([]any)
			gate := gates[0].(map[string]any)
			outcomes := gate["result_outcomes"].(map[string]any)
			outcomes["changes_requested"] = "done"
		}, "declared node outcome with a branch"},
		{"result_outcomes undeclared outcome", func(t map[string]any) {
			gates := boundGateNode(t, "review")["gates"].([]any)
			gate := gates[0].(map[string]any)
			outcomes := gate["result_outcomes"].(map[string]any)
			outcomes["changes_requested"] = "nowhere"
		}, "declared node outcome with a branch"},
	}
	for _, tc := range invalid {
		tc := tc
		t.Run("invalid/"+tc.name, func(t *testing.T) {
			assertTemplateError(t, boundGateMutation(tc.mutate), tc.want)
		})
	}
}

func TestAutoExternalDispatchEligibility(t *testing.T) {
	valid := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"eligible bound external node", func(t map[string]any) {}},
		{"unbound optional external gate", func(t map[string]any) {
			gates := boundGateNode(t, "review")["gates"].([]any)
			gates = append(gates, map[string]any{"id": "extra", "type": "external"})
			boundGateNode(t, "review")["gates"] = gates
		}},
		{"success_outcome via outcomes entry", func(t map[string]any) {
			// The success outcome is declared through a non-terminal
			// node-outcomes entry with a target instead of a branches entry.
			review := boundGateNode(t, "review")
			delete(review["branches"].(map[string]any), "clean")
			review["outcomes"] = []any{map[string]any{"name": "clean", "target_node_id": "merge"}}
		}},
	}
	for _, tc := range valid {
		tc := tc
		t.Run("valid/"+tc.name, func(t *testing.T) {
			if err := ValidateTemplateJSON(boundGateMutation(tc.mutate)); err != nil {
				t.Fatalf("ValidateTemplateJSON() error = %v", err)
			}
		})
	}

	invalid := []struct {
		name   string
		mutate func(map[string]any)
		want   string
	}{
		{"external node with outputs", func(t map[string]any) {
			boundGateNode(t, "review")["outputs"] = []any{map[string]any{"id": "x", "name": "X", "type": "string"}}
		}, "must declare no outputs"},
		{"missing success_outcome", func(t map[string]any) {
			delete(boundGateNode(t, "review")["dispatch"].(map[string]any), "success_outcome")
		}, "must declare dispatch.success_outcome"},
		{"undeclared success_outcome", func(t map[string]any) {
			boundGateNode(t, "review")["dispatch"].(map[string]any)["success_outcome"] = "nowhere"
		}, "declared node outcome with a branch"},
		{"success_outcome without branch", func(t map[string]any) {
			boundGateNode(t, "review")["branches"].(map[string]any)["clean"] = "merge"
			delete(boundGateNode(t, "review")["branches"].(map[string]any), "clean")
		}, "declared node outcome with a branch"},
		{"terminal outcomes success_outcome", func(t map[string]any) {
			// A terminal outcomes entry declares no branch, so it cannot
			// serve as the dispatch success outcome.
			review := boundGateNode(t, "review")
			delete(review["branches"].(map[string]any), "clean")
			review["outcomes"] = []any{map[string]any{"name": "clean", "terminal": true}}
		}, "declared node outcome with a branch"},
		{"outcomes success_outcome without target", func(t map[string]any) {
			// A non-terminal outcomes entry without a target_node_id declares
			// no branch either.
			review := boundGateNode(t, "review")
			delete(review["branches"].(map[string]any), "clean")
			review["outcomes"] = []any{map[string]any{"name": "clean"}}
		}, "declared node outcome with a branch"},
		{"no required external gate", func(t map[string]any) {
			gates := boundGateNode(t, "review")["gates"].([]any)
			gates[0].(map[string]any)["required"] = false
		}, "at least one required external gate"},
		{"human gate on auto external node", func(t map[string]any) {
			gates := boundGateNode(t, "review")["gates"].([]any)
			gates = append(gates, map[string]any{"id": "auth", "type": "human", "required": true})
			boundGateNode(t, "review")["gates"] = gates
		}, "no human or machine gates"},
		{"machine gate on auto external node", func(t map[string]any) {
			gates := boundGateNode(t, "review")["gates"].([]any)
			gates = append(gates, map[string]any{"id": "ci", "type": "machine", "required": true})
			boundGateNode(t, "review")["gates"] = gates
		}, "no human or machine gates"},
		{"required gate without binding", func(t map[string]any) {
			gates := boundGateNode(t, "review")["gates"].([]any)
			gates = append(gates, map[string]any{"id": "lint", "type": "external", "required": true})
			boundGateNode(t, "review")["gates"] = gates
		}, "subject_binding on every required gate"},
		{"auto on workflow node", func(t map[string]any) {
			boundGateNode(t, "merge")["dispatch"] = map[string]any{"mode": "auto", "controller_id": "ctl"}
		}, "run, loop, and team"},
		{"success_outcome on run node", func(t map[string]any) {
			boundGateNode(t, "merge")["dispatch"] = map[string]any{"mode": "auto", "controller_id": "ctl", "success_outcome": "clean"}
			boundGateNode(t, "merge")["action"] = map[string]any{"type": "run", "prompt": "go"}
		}, "success_outcome is forbidden"},
		{"success_outcome on manual external node", func(t map[string]any) {
			node := boundGateNode(t, "review")
			node["dispatch"] = map[string]any{"mode": "manual", "success_outcome": "clean"}
		}, `forbidden for "manual" nodes`},
	}
	for _, tc := range invalid {
		tc := tc
		t.Run("invalid/"+tc.name, func(t *testing.T) {
			assertTemplateError(t, boundGateMutation(tc.mutate), tc.want)
		})
	}
}
