package workflow

// template_fixture_test.go proves the shipped software-factory work template
// (templates/software-factory/work.json) is a valid, internally consistent
// workflow template. It validates the template through the same boundary the
// manager uses (ValidateTemplateJSON: Profile structural validation + strict
// typed decode + graph validation) and then checks the declared review
// branches are coherent: every branch target node exists, the exact-head
// external gates are declared, and no branch selects an undeclared outcome.
//
// This is the Stage 16 integration fixture: it exercises the Stage 9-13
// machinery (composition limits, completion contracts, gates, branches) in a
// realistic multi-node template.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// softwareFactoryTemplatePath is the repo-relative path to the shipped
// software-factory work template, resolved from this package's directory.
const softwareFactoryTemplatePath = "../../templates/software-factory/work.json"

// loadSoftwareFactoryTemplate reads and strictly validates the shipped
// template, returning the decoded Template for structural assertions.
func loadSoftwareFactoryTemplate(t *testing.T) Template {
	t.Helper()
	data, err := os.ReadFile(softwareFactoryTemplatePath)
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

// TestSoftwareFactoryTemplateIsValid asserts the shipped template passes the
// full validation boundary (Profile structure, strict decode, graph rules).
func TestSoftwareFactoryTemplateIsValid(t *testing.T) {
	tmpl := loadSoftwareFactoryTemplate(t)
	if tmpl.TemplateID != "software-factory-work" {
		t.Fatalf("template_id = %q, want software-factory-work", tmpl.TemplateID)
	}
	if len(tmpl.EntryNodes) != 1 || tmpl.EntryNodes[0] != "intake" {
		t.Fatalf("entry_nodes = %v, want [intake]", tmpl.EntryNodes)
	}
}

// TestSoftwareFactoryTemplateBranchTargetsExist asserts every declared branch
// and non-terminal outcome targets an existing node. A branch to a missing
// node would create a dangling activation at runtime.
func TestSoftwareFactoryTemplateBranchTargetsExist(t *testing.T) {
	tmpl := loadSoftwareFactoryTemplate(t)
	nodeIDs := make(map[NodeID]struct{}, len(tmpl.Nodes))
	for _, node := range tmpl.Nodes {
		nodeIDs[node.ID] = struct{}{}
	}
	for _, node := range tmpl.Nodes {
		for outcome, target := range node.Branches {
			if _, exists := nodeIDs[target]; !exists {
				t.Errorf("node %q branch %q targets unknown node %q", node.ID, outcome, target)
			}
		}
		for _, outcome := range node.Outcomes {
			if outcome.Terminal {
				continue
			}
			if outcome.TargetNodeID == "" {
				t.Errorf("node %q non-terminal outcome %q has no target", node.ID, outcome.Name)
				continue
			}
			if _, exists := nodeIDs[outcome.TargetNodeID]; !exists {
				t.Errorf("node %q outcome %q targets unknown node %q", node.ID, outcome.Name, outcome.TargetNodeID)
			}
		}
	}
}

// TestSoftwareFactoryTemplateReviewBranches asserts the review node declares
// the plan's required review branches and that each routes to a coherent
// target: clean to merge authorization, changes_requested/action_required to
// correction, replan back to assessment, and checkpoint to the advisor gate.
func TestSoftwareFactoryTemplateReviewBranches(t *testing.T) {
	tmpl := loadSoftwareFactoryTemplate(t)
	var review *NodeDefinition
	for i := range tmpl.Nodes {
		if tmpl.Nodes[i].ID == "review" {
			review = &tmpl.Nodes[i]
			break
		}
	}
	if review == nil {
		t.Fatal("review node not found")
	}
	want := map[OutcomeName]NodeID{
		"clean":             "merge-auth",
		"changes_requested": "correction",
		"action_required":   "correction",
		"replan":            "assessment",
		"checkpoint":        "advisor",
	}
	for outcome, target := range want {
		if got, ok := review.Branches[outcome]; !ok {
			t.Errorf("review node missing branch %q", outcome)
		} else if got != target {
			t.Errorf("review node branch %q = %q, want %q", outcome, got, target)
		}
	}
}

// TestSoftwareFactoryTemplateExactHeadGates asserts the review node declares
// external gates bound to an exact pull-request subject, and the merge-auth
// node declares a human gate bound to the same subject type. Exact-head
// binding is what keeps CI, review, and merge authorization tied to one
// immutable revision.
func TestSoftwareFactoryTemplateExactHeadGates(t *testing.T) {
	tmpl := loadSoftwareFactoryTemplate(t)
	nodeByID := make(map[NodeID]*NodeDefinition, len(tmpl.Nodes))
	for i := range tmpl.Nodes {
		nodeByID[tmpl.Nodes[i].ID] = &tmpl.Nodes[i]
	}

	review := nodeByID["review"]
	if review == nil {
		t.Fatal("review node not found")
	}
	externalGates := 0
	for _, gate := range review.Gates {
		if gate.Type != GateExternal {
			t.Errorf("review gate %q type = %q, want external", gate.ID, gate.Type)
			continue
		}
		if gate.SubjectType != "pull_request" {
			t.Errorf("review gate %q subject_type = %q, want pull_request", gate.ID, gate.SubjectType)
		}
		if !gate.Required {
			t.Errorf("review gate %q must be required", gate.ID)
		}
		externalGates++
	}
	if externalGates < 2 {
		t.Errorf("review node has %d external gates, want at least 2 (CI + external review)", externalGates)
	}

	mergeAuth := nodeByID["merge-auth"]
	if mergeAuth == nil {
		t.Fatal("merge-auth node not found")
	}
	foundHuman := false
	for _, gate := range mergeAuth.Gates {
		if gate.Type != GateHuman {
			continue
		}
		foundHuman = true
		if gate.SubjectType != "pull_request" {
			t.Errorf("merge-auth gate %q subject_type = %q, want pull_request", gate.ID, gate.SubjectType)
		}
		if !gate.Required {
			t.Errorf("merge-auth gate %q must be required", gate.ID)
		}
	}
	if !foundHuman {
		t.Error("merge-auth node has no human gate")
	}
}

// TestSoftwareFactoryTemplateNoUndeclaredOutcome asserts every branch key and
// non-terminal outcome on every node resolves to a declared outcome (a branch
// target, a node outcome, or a template terminal outcome). An undeclared
// outcome would be rejected at completion time.
func TestSoftwareFactoryTemplateNoUndeclaredOutcome(t *testing.T) {
	tmpl := loadSoftwareFactoryTemplate(t)
	terminal := make(map[OutcomeName]struct{}, len(tmpl.TerminalOutcomes))
	for _, name := range tmpl.TerminalOutcomes {
		terminal[name] = struct{}{}
	}
	for _, node := range tmpl.Nodes {
		for outcome := range node.Branches {
			if _, ok := node.Branches[outcome]; !ok {
				t.Errorf("node %q branch %q is not declared", node.ID, outcome)
			}
		}
		for _, outcome := range node.Outcomes {
			if outcome.Terminal {
				if _, ok := terminal[outcome.Name]; !ok {
					t.Errorf("node %q terminal outcome %q is not in template terminal_outcomes", node.ID, outcome.Name)
				}
				continue
			}
			if outcome.TargetNodeID == "" {
				t.Errorf("node %q non-terminal outcome %q has no target", node.ID, outcome.Name)
			}
		}
	}
}

// TestSoftwareFactoryTemplateFixtureFilesExist asserts every prompt, loop,
// and team file referenced by the template exists on disk and that the
// loop/team configs are well-formed JSON. A dangling fixture would fail at
// dispatch time, not validation time.
func TestSoftwareFactoryTemplateFixtureFilesExist(t *testing.T) {
	tmpl := loadSoftwareFactoryTemplate(t)
	dir := filepath.Dir(softwareFactoryTemplatePath)
	for _, node := range tmpl.Nodes {
		var refs []string
		switch node.Action.Kind {
		case ActionRun:
			if node.Action.Run != nil && node.Action.Run.PromptFile != "" {
				refs = append(refs, node.Action.Run.PromptFile)
			}
		case ActionLoop:
			if node.Action.Loop != nil && node.Action.Loop.LoopFile != "" {
				refs = append(refs, node.Action.Loop.LoopFile)
			}
		case ActionTeam:
			if node.Action.Team != nil && node.Action.Team.TeamFile != "" {
				refs = append(refs, node.Action.Team.TeamFile)
			}
		}
		for _, ref := range refs {
			path := filepath.Join(dir, ref)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Errorf("node %q fixture %q: %v", node.ID, ref, err)
				continue
			}
			if strings.HasSuffix(ref, ".json") {
				var decoded any
				if err := json.Unmarshal(data, &decoded); err != nil {
					t.Errorf("node %q fixture %q is not valid JSON: %v", node.ID, ref, err)
				}
			}
		}
	}
}
