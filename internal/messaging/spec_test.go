package messaging

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"
)

func TestRepairTemplates(t *testing.T) {
	cases := []struct {
		code  string
		facts map[string]string
		want  string
	}{
		{
			code:  "wrong_action_for_state",
			facts: map[string]string{"run_id": `"run-9"`, "status": "done", "repair": `use action "resume" to continue it`},
			want:  `run "run-9" is done; use action "resume" to continue it`,
		},
		{
			code:  "target_not_terminal",
			facts: map[string]string{"run_id": `"rt_9"`, "status": "running"},
			want:  `run "rt_9" is running; wait for it to finish before resuming`,
		},
		{
			code:  "not_resumable",
			facts: map[string]string{"run_id": `"rt_9"`, "status": "timeout"},
			want:  `run "rt_9" is not resumable (status: timeout); resume only continues done, failed, or blocked runs`,
		},
		{
			code:  "unknown_target",
			facts: map[string]string{"target": `"ghost"`},
			want:  `no run matches "ghost"`,
		},
		{
			code:  "ambiguous_name",
			facts: map[string]string{"target": `"mesh"`, "count": "2", "candidates": `"rt_1", "rt_2"`},
			want:  `"mesh" matches 2 runs: "rt_1", "rt_2"`,
		},
		{
			code:  "invalid_request",
			facts: map[string]string{"action": `"tell"`, "detail": `unknown property "bogus"`},
			want:  `invalid input for action "tell": unknown property "bogus"`,
		},
	}
	covered := map[string]bool{}
	for _, c := range cases {
		t.Run(c.code, func(t *testing.T) {
			if got := Repair(c.code, c.facts); got != c.want {
				t.Fatalf("Repair = %q, want %q", got, c.want)
			}
			covered[c.code] = true
		})
	}
	for code := range repairsTemplates {
		if !covered[code] {
			t.Fatalf("repair template %q is unexercised", code)
		}
	}
	if got := Repair("unknown_target", map[string]string{}); got != "no run matches {target}" {
		t.Fatalf("unfilled placeholder = %q, want the template with the placeholder intact", got)
	}
}

func TestLastRenderTemplates(t *testing.T) {
	facts := map[string]string{"run_id": `"rt_7"`, "label": `"mesh-a"`, "stop_reason": "end_turn"}
	cases := map[string]string{
		"header":           `**Last message from "mesh-a"** (run "rt_7", stop reason: end_turn)`,
		"empty":            `run "rt_7" has no completed turn yet`,
		"no_text":          `run "rt_7" finished (stop reason: end_turn) with no final text`,
		"truncated_suffix": " (truncated)",
	}
	for key, want := range cases {
		if got := LastRender(key, facts); got != want {
			t.Fatalf("LastRender(%q) = %q, want %q", key, got, want)
		}
	}
	if got := LastRender("empty", map[string]string{}); got != "run {run_id} has no completed turn yet" {
		t.Fatalf("unfilled placeholder = %q", got)
	}
}

// TestOutcomeRenderTotal makes the text total: every matrix pair has a
// non-empty template, every template uses only the fixed fact vocabulary, and
// every template is exercised by at least one conformance case, so a new
// outcome cannot ship without text and a template cannot ship untested.
func TestOutcomeRenderTotal(t *testing.T) {
	facts := regexp.MustCompile(`\{([a-z_]+)\}`)
	vocabulary := map[string]bool{
		"message_id": true, "target": true, "reply": true,
		"run_id": true, "label": true, "previous_label": true,
	}
	spec := LoadOutcomeSpec()
	known := map[string]bool{}
	for _, row := range spec.Messaging {
		known[string(row.Outcome)] = true
	}
	for _, row := range spec.Tool {
		known[string(row.Outcome)] = true
	}
	if len(spec.PeersLast) == 0 {
		t.Fatal("no peers_last templates")
	}

	cases, err := LoadConformanceCases("../../schemas/message_route.conformance.json")
	if err != nil {
		t.Fatalf("load fixture: %v", err)
	}
	peersCases, err := LoadPeersConformanceCases("../../schemas/message_route.conformance.json")
	if err != nil {
		t.Fatalf("load peers fixture: %v", err)
	}
	exercised := map[string]bool{}
	for _, c := range cases {
		if c.Expect.Outcome == "" {
			continue
		}
		var input struct {
			Action string `json:"action"`
		}
		if err := json.Unmarshal(c.Input, &input); err != nil {
			t.Fatalf("decode %s: %v", c.Name, err)
		}
		exercised[input.Action+":"+c.Expect.Outcome] = true
	}
	for _, c := range peersCases {
		if c.Expect.Outcome == "" {
			continue
		}
		var input struct {
			Action string `json:"action"`
		}
		if err := json.Unmarshal(c.Input, &input); err != nil {
			t.Fatalf("decode %s: %v", c.Name, err)
		}
		exercised["peers_"+input.Action+":"+c.Expect.Outcome] = true
	}

	matrix := matrixFixture(t)
	if len(matrix) == 0 {
		t.Fatal("outcome fixture declares no matrix")
	}
	for action, outcomes := range matrix {
		for _, outcome := range outcomes {
			key := action + ":" + outcome
			if !known[outcome] {
				t.Fatalf("matrix pair %q names an outcome that is not declared", key)
			}
			template := outcomeRenderTemplates[key]
			if template == "" {
				t.Fatalf("matrix pair %q has no render template", key)
			}
			for _, match := range facts.FindAllStringSubmatch(template, -1) {
				if !vocabulary[match[1]] {
					t.Fatalf("matrix pair %q uses the unknown fact {%s}", key, match[1])
				}
			}
			if !exercised[key] {
				t.Fatalf("matrix pair %q has no conformance case", key)
			}
		}
	}
	for key := range outcomeRenderTemplates {
		action, _, _ := strings.Cut(key, ":")
		if matrix[action] == nil {
			t.Fatalf("render template %q belongs to no matrix row", key)
		}
	}
	for _, key := range []string{"header", "empty", "no_text", "truncated_suffix"} {
		if spec.PeersLast[key] == "" {
			t.Fatalf("peers_last template %q is missing", key)
		}
	}
}

func matrixFixture(t *testing.T) map[string][]string {
	t.Helper()
	var fixture struct {
		Matrix map[string][]string `json:"matrix"`
	}
	if err := decodeFixture("../../schemas/message.outcome.json", &fixture); err != nil {
		t.Fatalf("decode outcome fixture: %v", err)
	}
	return fixture.Matrix
}

// TestApplicabilityTypesAreDeclared keeps the per-action type table total: every
// permitted property must declare a wire type the routers and the schema can
// both enforce.
func TestApplicabilityTypesAreDeclared(t *testing.T) {
	for _, table := range []map[string]ActionApplicability{ApplicabilityTable(), PeersApplicabilityTable()} {
		for action, entry := range table {
			for _, name := range entry.Permitted {
				switch entry.Types[name] {
				case "string", "integer", "object":
				default:
					t.Fatalf("%s: property %q declares type %q", action, name, entry.Types[name])
				}
			}
		}
	}
}
