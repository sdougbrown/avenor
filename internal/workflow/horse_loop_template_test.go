package workflow

// horse_loop_template_test.go proves the shipped horse-loop work template
// (templates/horse-loop/work.json) validates through the manager boundary and
// that its decision points stay consistent with the kernel's completion
// semantics: the advisor's manual decision resolves both outcomes (proceed
// routes to correction, abandoned terminates the workflow), and the
// correction run node keeps a single-outcome vocabulary — run nodes have no
// terminal marker to select among multiple outcomes, so a replan branch can
// only ever live on the review gate.

import (
	"encoding/json"
	"os"
	"testing"
)

// nodeByID finds a node definition by ID in the decoded template.
func nodeByID(tmpl Template, id NodeID) *NodeDefinition {
	for i := range tmpl.Nodes {
		if tmpl.Nodes[i].ID == id {
			return &tmpl.Nodes[i]
		}
	}
	return nil
}

// horseLoopTemplatePath is the repo-relative path to the shipped horse-loop
// work template, resolved from this package's directory.
const horseLoopTemplatePath = "../../templates/horse-loop/work.json"

// loadHorseLoopTemplate reads and strictly validates the shipped template,
// returning the decoded Template for structural assertions.
func loadHorseLoopTemplate(t *testing.T) Template {
	t.Helper()
	data, err := os.ReadFile(horseLoopTemplatePath)
	if err != nil {
		t.Fatalf("read template: %v", err)
	}
	if err := ValidateTemplateJSON(data); err != nil {
		t.Fatalf("ValidateTemplateJSON: %v", err)
	}
	var tmpl Template
	if err := json.Unmarshal(data, &tmpl); err != nil {
		t.Fatalf("unmarshal template: %v", err)
	}
	return tmpl
}

// TestHorseLoopTemplateIsValid asserts the shipped template passes the full
// validation boundary (Profile structure, strict decode, graph rules).
func TestHorseLoopTemplateIsValid(t *testing.T) {
	tmpl := loadHorseLoopTemplate(t)
	if tmpl.TemplateID != "horse-loop-work" {
		t.Fatalf("template_id = %q, want horse-loop-work", tmpl.TemplateID)
	}
	if len(tmpl.EntryNodes) != 1 || tmpl.EntryNodes[0] != "intake" {
		t.Fatalf("entry_nodes = %v, want [intake]", tmpl.EntryNodes)
	}
}

// TestHorseLoopAdvisorOutcomesResolve asserts the advisor checkpoint can both
// route back into the loop and terminate the workflow: `proceed` follows the
// declared branch to correction, `abandoned` resolves as a declared terminal
// outcome so the human decision is never a dead end.
func TestHorseLoopAdvisorOutcomesResolve(t *testing.T) {
	tmpl := loadHorseLoopTemplate(t)
	node := nodeByID(tmpl, "advisor")
	if node == nil {
		t.Fatalf("advisor node not found")
	}
	target, terminal, declared := resolveOutcome(&tmpl, node, "proceed")
	if !declared || terminal || target != "correction" {
		t.Fatalf("resolveOutcome(advisor, proceed) = (%q, %t, %t), want correction, false, true", target, terminal, declared)
	}
	target, terminal, declared = resolveOutcome(&tmpl, node, "abandoned")
	if !declared || !terminal || target != "" {
		t.Fatalf("resolveOutcome(advisor, abandoned) = (%q, %t, %t), want empty target, terminal, declared", target, terminal, declared)
	}
}

// TestHorseLoopCorrectionSingleOutcomeVocabulary asserts the correction run
// node declares exactly one candidate outcome (`fixed`). An auto run node's
// completion needs a single-candidate vocabulary or a terminal marker to
// select an outcome; run attempts carry no marker, so widening this node's
// vocabulary (e.g. with replan) would leave every attempt contract-unmet.
func TestHorseLoopCorrectionSingleOutcomeVocabulary(t *testing.T) {
	tmpl := loadHorseLoopTemplate(t)
	node := nodeByID(tmpl, "correction")
	if node == nil {
		t.Fatalf("correction node not found")
	}
	vocabulary := declaredOutcomeVocabulary(&tmpl, node)
	if len(vocabulary) != 1 || vocabulary[0] != "fixed" {
		t.Fatalf("correction outcome vocabulary = %v, want [fixed]", vocabulary)
	}
}
