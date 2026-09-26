package workflow

// factory_template_test.go audits the shipped software-factory templates
// (templates/software-factory/work.json and stack.json) against the Stage 8
// factory contract: automatic dispatch is explicit and scoped to the
// software-factory controller, worktree-writing nodes share one concurrency
// key, later-pipeline work outranks earlier intake, hardening and merge
// authorization stay manual, the external review node is auto-parkable with
// bound trusted-adapter gates, and the stack parent composes pinned review
// unit children through the landed composition kernel.

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
)

const (
	factoryWorkTemplatePath  = "../../templates/software-factory/work.json"
	factoryStackTemplatePath = "../../templates/software-factory/stack.json"

	// factoryControllerID is the controller every auto factory node declares.
	factoryControllerID = "software-factory"
	// factoryWorktreeParam is the instance param every keyed factory node
	// resolves its concurrency key from, with the shared prefix.
	factoryWorktreeParam     = "worktree"
	factoryWorktreeKeyPrefix = "worktree:"
)

// loadFactoryTemplate reads and strictly validates a shipped factory
// template, returning the decoded Template.
func loadFactoryTemplate(t *testing.T, path string) Template {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if err := ValidateTemplateJSON(data); err != nil {
		t.Fatalf("ValidateTemplateJSON(%s): %v", path, err)
	}
	var tmpl Template
	if err := json.Unmarshal(data, &tmpl); err != nil {
		t.Fatalf("unmarshal %s: %v", path, err)
	}
	return tmpl
}

// factoryNodeByID indexes a template's nodes by id.
func factoryNodeByID(tmpl Template) map[NodeID]*NodeDefinition {
	byID := make(map[NodeID]*NodeDefinition, len(tmpl.Nodes))
	for i := range tmpl.Nodes {
		byID[tmpl.Nodes[i].ID] = &tmpl.Nodes[i]
	}
	return byID
}

// TestSoftwareFactoryTemplateAutoDispatchIsExplicit asserts every provider
// node intended for automatic dispatch declares mode auto under the
// software-factory controller with the templated worktree key resolved from
// the instance's worktree param, that hardening keeps manual dispatch as its
// provider-backed checkpoint, and that the human nodes (intake, merge-auth,
// advisor) never request automatic dispatch.
func TestSoftwareFactoryTemplateAutoDispatchIsExplicit(t *testing.T) {
	tmpl := loadFactoryTemplate(t, factoryWorkTemplatePath)

	// The template declares exactly one required instance param: worktree.
	if len(tmpl.Params) != 1 {
		t.Fatalf("template params = %+v, want exactly one declared param", tmpl.Params)
	}
	if tmpl.Params[0].ID != factoryWorktreeParam || tmpl.Params[0].Type != "string" || !tmpl.Params[0].Required {
		t.Fatalf("template param = %+v, want required string %q", tmpl.Params[0], factoryWorktreeParam)
	}

	// The audit classifies every node: these dispatch automatically, these
	// stay manual, and anything unclassified fails the audit.
	autoNodes := map[NodeID]bool{
		"assessment": true, "draft-plan": true, "execution": true,
		"verification": true, "publication": true, "review": true,
		"correction": true, "reverify": true, "reconciliation": true,
	}
	manualNodes := map[NodeID]bool{
		"intake": true, "hardening": true, "merge-auth": true, "advisor": true,
	}
	wantAuto := autoNodes
	for _, node := range tmpl.Nodes {
		policy := node.Dispatch
		if manualNodes[node.ID] {
			if policy != nil && policy.IsAuto() {
				t.Errorf("node %q must not dispatch automatically", node.ID)
			}
			if policy != nil && policy.ControllerID != "" {
				t.Errorf("manual node %q declares controller_id %q", node.ID, policy.ControllerID)
			}
			continue
		}
		if !wantAuto[node.ID] {
			t.Errorf("node %q is neither classified auto nor manual in this audit", node.ID)
			continue
		}
		if policy == nil || !policy.IsAuto() {
			t.Errorf("node %q must declare dispatch.mode auto", node.ID)
			continue
		}
		if policy.ControllerID != factoryControllerID {
			t.Errorf("node %q controller_id = %q, want %q", node.ID, policy.ControllerID, factoryControllerID)
		}
		if policy.Priority == nil {
			t.Errorf("node %q must declare an explicit priority", node.ID)
		}
		if policy.ConcurrencyKeyParams == nil {
			t.Errorf("node %q must declare the templated worktree concurrency key", node.ID)
			continue
		}
		if policy.ConcurrencyKey != "" {
			t.Errorf("node %q declares a plain concurrency key %q alongside the templated form", node.ID, policy.ConcurrencyKey)
		}
		if policy.ConcurrencyKeyParams.FromInstanceParam != factoryWorktreeParam || policy.ConcurrencyKeyParams.Prefix != factoryWorktreeKeyPrefix {
			t.Errorf("node %q concurrency key = {%q, %q}, want prefix %q from param %q",
				node.ID, policy.ConcurrencyKeyParams.Prefix, policy.ConcurrencyKeyParams.FromInstanceParam,
				factoryWorktreeKeyPrefix, factoryWorktreeParam)
		}
	}
}

// TestSoftwareFactoryTemplateLaterPipelineOutranksIntake asserts the
// declared priorities rank later-pipeline work above earlier intake so a
// controller with limited in-flight budget finishes published work before
// starting new assessments.
func TestSoftwareFactoryTemplateLaterPipelineOutranksIntake(t *testing.T) {
	tmpl := loadFactoryTemplate(t, factoryWorkTemplatePath)
	nodes := factoryNodeByID(tmpl)
	priority := func(id NodeID) int {
		node := nodes[id]
		if node == nil || node.Dispatch == nil || node.Dispatch.Priority == nil {
			t.Fatalf("node %q has no explicit priority", id)
		}
		return *node.Dispatch.Priority
	}
	for _, later := range []NodeID{"publication", "review", "reverify"} {
		if priority(later) <= priority("assessment") {
			t.Errorf("node %q priority %d must outrank assessment priority %d", later, priority(later), priority("assessment"))
		}
	}
	if priority("correction") <= priority("publication") {
		t.Errorf("correction priority %d must outrank publication priority %d", priority("correction"), priority("publication"))
	}
}

// TestSoftwareFactoryTemplatePublicationOutputsPinSubject asserts publication
// declares the repository/pr_number/pr_head outputs the review gates and the
// merge-authorization gate bind to.
func TestSoftwareFactoryTemplatePublicationOutputsPinSubject(t *testing.T) {
	tmpl := loadFactoryTemplate(t, factoryWorkTemplatePath)
	nodes := factoryNodeByID(tmpl)
	want := map[OutputID]OutputType{
		"repository": OutputString,
		"pr_number":  OutputNumber,
		"pr_head":    OutputString,
	}
	for id, typ := range want {
		found := false
		for _, out := range nodes["publication"].Outputs {
			if out.ID == id {
				found = true
				if out.Type != typ {
					t.Errorf("publication output %q type = %q, want %q", id, out.Type, typ)
				}
				if !out.Required {
					t.Errorf("publication output %q must be required", id)
				}
			}
		}
		if !found {
			t.Errorf("publication does not declare output %q", id)
		}
	}
	for _, out := range nodes["review"].Outputs {
		t.Errorf("review node must declare no outputs, found %q", out.ID)
	}
}

// TestSoftwareFactoryTemplateExternalReviewIsAutoParkableAndBound asserts the
// review node is eligible for kernel-local parking: auto dispatch with the
// declared clean success outcome, two required external gates bound to
// trusted adapters, adapter inputs and exact-head subjects pinned from
// publication's outputs, and result_outcomes routing every advisory result
// onto a declared branch.
func TestSoftwareFactoryTemplateExternalReviewIsAutoParkableAndBound(t *testing.T) {
	tmpl := loadFactoryTemplate(t, factoryWorkTemplatePath)
	review := factoryNodeByID(tmpl)["review"]

	if review.Dispatch == nil || !review.Dispatch.IsAuto() {
		t.Fatal("review node must dispatch automatically")
	}
	if review.Dispatch.SuccessOutcome != "clean" {
		t.Errorf("review success_outcome = %q, want clean", review.Dispatch.SuccessOutcome)
	}
	if target := review.Branches["clean"]; target != "merge-auth" {
		t.Errorf("review clean branch = %q, want merge-auth", target)
	}

	wantAdapters := map[GateID]string{
		"ci":             "circleci-pipeline",
		"review-verdict": "github-pr-review",
	}
	if len(review.Gates) != len(wantAdapters) {
		t.Fatalf("review gates = %d, want %d", len(review.Gates), len(wantAdapters))
	}
	for _, gate := range review.Gates {
		wantID, ok := wantAdapters[gate.ID]
		if !ok {
			t.Errorf("unexpected review gate %q", gate.ID)
			continue
		}
		if gate.Type != GateExternal || !gate.Required {
			t.Errorf("gate %q must be a required external gate", gate.ID)
		}
		if gate.AdapterID != wantID {
			t.Errorf("gate %q adapter_id = %q, want %q", gate.ID, gate.AdapterID, wantID)
		}
		if gate.SubjectBinding == nil {
			t.Errorf("gate %q has no subject_binding", gate.ID)
			continue
		}
		binding := gate.SubjectBinding
		for _, ref := range []*SubjectOutputRef{binding.Repository, binding.PullRequest, binding.Revision} {
			if ref == nil || ref.FromNodeOutput.NodeID != "publication" {
				t.Errorf("gate %q subject binding must pin every subject field from publication", gate.ID)
				break
			}
		}
		if len(gate.Inputs) != 3 {
			t.Errorf("gate %q inputs = %d, want 3 pinned adapter inputs", gate.ID, len(gate.Inputs))
		}
		for result, want := range map[GateResultName]OutcomeName{
			GateResultChangesRequested: "changes_requested",
			GateResultActionRequired:   "action_required",
			GateResultFailed:           "replan",
		} {
			if got := gate.ResultOutcomes[result]; got != want {
				t.Errorf("gate %q result_outcomes[%q] = %q, want %q", gate.ID, result, got, want)
			}
		}
	}
}

// TestSoftwareFactoryTemplateMergeAuthStaysManualAndBound asserts merge
// authorization remains a manual human gate whose decisions are bound to the
// exact published subject — the remote-human bridge path must never become
// automatic.
func TestSoftwareFactoryTemplateMergeAuthStaysManualAndBound(t *testing.T) {
	tmpl := loadFactoryTemplate(t, factoryWorkTemplatePath)
	mergeAuth := factoryNodeByID(tmpl)["merge-auth"]

	if mergeAuth.Dispatch != nil && mergeAuth.Dispatch.IsAuto() {
		t.Error("merge-auth must never dispatch automatically")
	}
	if mergeAuth.Action.Kind != ActionManual {
		t.Errorf("merge-auth action kind = %q, want manual", mergeAuth.Action.Kind)
	}
	found := false
	for _, gate := range mergeAuth.Gates {
		if gate.Type != GateHuman {
			t.Errorf("merge-auth gate %q type = %q, want human", gate.ID, gate.Type)
			continue
		}
		found = true
		if gate.SubjectBinding == nil {
			t.Errorf("merge-auth gate %q has no subject_binding", gate.ID)
			continue
		}
		binding := gate.SubjectBinding
		for _, ref := range []*SubjectOutputRef{binding.Repository, binding.PullRequest, binding.Revision} {
			if ref == nil || ref.FromNodeOutput.NodeID != "publication" {
				t.Errorf("merge-auth gate %q must pin its subject from publication", gate.ID)
				break
			}
		}
	}
	if !found {
		t.Error("merge-auth declares no human gate")
	}
}

// TestSoftwareFactoryStackTemplateComposesPinnedWorkChildren asserts the
// stack parent validates, pins the current work template version on every
// workflow action, binds each child's typed handoff from the planning node's
// declared outputs, and builds a two-child composition manifest through the
// landed composition kernel.
func TestSoftwareFactoryStackTemplateComposesPinnedWorkChildren(t *testing.T) {
	stack := loadFactoryTemplate(t, factoryStackTemplatePath)
	work := loadFactoryTemplate(t, factoryWorkTemplatePath)

	for _, node := range stack.Nodes {
		if node.Action.Kind != ActionWorkflow {
			continue
		}
		action := node.Action.Workflow
		if action.TemplateID != work.TemplateID || action.TemplateVersion != work.TemplateVersion {
			t.Errorf("node %q pins %s@%s, want the shipped %s@%s",
				node.ID, action.TemplateID, action.TemplateVersion, work.TemplateID, work.TemplateVersion)
		}
		if action.ChildKey == "" {
			t.Errorf("node %q declares no child_key", node.ID)
		}
		if len(action.Params) != 1 || action.Params[0].Param != "worktree" || action.Params[0].Value == "" || action.Params[0].FromInstanceParam != "" {
			t.Errorf("node %q must pass the child worktree param as an explicit literal, got %+v", node.ID, action.Params)
		}
		for _, binding := range action.InputBindings {
			if binding.From == nil || binding.From.NodeID != "plan-stack" {
				t.Errorf("node %q input binding must hand off from plan-stack", node.ID)
			}
		}
		for childOutcome, parentOutcome := range action.OutcomeMap {
			if _, ok := map[OutcomeName]bool{"merged": true, "abandoned": true}[childOutcome]; !ok {
				t.Errorf("node %q outcome_map routes undeclared child outcome %q", node.ID, childOutcome)
			}
			if parentOutcome != "done" && parentOutcome != "abandoned" && parentOutcome != "unit_merged" {
				t.Errorf("node %q outcome_map maps %q to undeclared parent outcome %q", node.ID, childOutcome, parentOutcome)
			}
		}
	}

	resolver := func(templateID TemplateID, templateVersion TemplateVersion) (Template, error) {
		if templateID == work.TemplateID && templateVersion == work.TemplateVersion {
			return work, nil
		}
		return Template{}, errTestUnresolvable
	}
	comp, instances, err := BuildComposition("wf-stack-parent", stack, resolver)
	if err != nil {
		t.Fatalf("BuildComposition: %v", err)
	}
	if len(comp.Children) != 2 {
		t.Fatalf("composition children = %d, want 2 review units", len(comp.Children))
	}
	if instances != 3 {
		t.Errorf("composition instances = %d, want 3 (parent + 2 children)", instances)
	}
	for i, child := range comp.Children {
		wantNode := NodeID("review-unit-1")
		wantKey := "unit-1"
		if i == 1 {
			wantNode, wantKey = "review-unit-2", "unit-2"
		}
		wantID := DeriveChildWorkflowID("wf-stack-parent", wantNode, wantKey)
		if child.ChildWorkflowID != wantID {
			t.Errorf("child %d workflow id = %q, want %q", i, child.ChildWorkflowID, wantID)
		}
	}
}

// errTestUnresolvable is returned by the stack test's resolver for any pin
// other than the shipped work template.
var errTestUnresolvable = errors.New("unresolvable template pin")
