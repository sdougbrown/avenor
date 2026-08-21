package workflow

import (
	"strings"
	"testing"
)

func TestValidateDeclaredOutputs(t *testing.T) {
	node := &NodeDefinition{
		ID: "build",
		Outputs: []OutputDefinition{
			{ID: "summary", Name: "Summary", Type: OutputString, Required: true},
			{ID: "artifacts", Name: "Artifacts", Type: OutputJSON},
		},
	}

	valid := []OutputValue{
		{ID: "ov1", DefinitionID: "summary"},
		{ID: "ov2", DefinitionID: "artifacts"},
	}
	if err := validateDeclaredOutputs(node, valid); err != nil {
		t.Fatalf("valid completion: %v", err)
	}

	optionalOnly := []OutputValue{{ID: "ov2", DefinitionID: "artifacts"}}
	if err := validateDeclaredOutputs(node, optionalOnly); err == nil {
		t.Fatal("missing required output: expected error, got nil")
	} else if !strings.Contains(err.Error(), "summary") {
		t.Fatalf("missing required output: error should mention the output id, got %v", err)
	}

	undeclared := []OutputValue{{ID: "ov3", DefinitionID: "notes"}}
	if err := validateDeclaredOutputs(node, undeclared); err == nil {
		t.Fatal("undeclared output: expected error, got nil")
	} else if !strings.Contains(err.Error(), "notes") {
		t.Fatalf("undeclared output: error should mention the output id, got %v", err)
	}

	duplicate := []OutputValue{
		{ID: "ov1", DefinitionID: "summary"},
		{ID: "ov4", DefinitionID: "summary"},
	}
	if err := validateDeclaredOutputs(node, duplicate); err == nil {
		t.Fatal("duplicate output: expected error, got nil")
	} else if !strings.Contains(err.Error(), "summary") {
		t.Fatalf("duplicate output: error should mention the output id, got %v", err)
	}
}

func TestResolveOutcome(t *testing.T) {
	tmpl := &Template{TemplateID: "tpl", TerminalOutcomes: []OutcomeName{"blocked"}}
	node := &NodeDefinition{
		ID:       "build",
		Branches: map[OutcomeName]NodeID{"rebuild": "build"},
		Outcomes: []OutcomeDefinition{
			{Name: "next", TargetNodeID: "test"},
			{Name: "done", Terminal: true},
		},
		Checkpoint: &CheckpointDefinition{Path: "cp", ExitOutcomes: []OutcomeName{"corrected"}},
	}

	tests := []struct {
		outcome  OutcomeName
		target   NodeID
		terminal bool
		declared bool
	}{
		{outcome: "rebuild", target: "build", terminal: false, declared: true},
		{outcome: "next", target: "test", terminal: false, declared: true},
		{outcome: "done", target: "", terminal: true, declared: true},
		{outcome: "blocked", target: "", terminal: true, declared: true},
		{outcome: "corrected", target: "", terminal: false, declared: true},
		{outcome: "", target: "", terminal: false, declared: false},
		{outcome: "bogus", target: "", terminal: false, declared: false},
	}
	for _, tc := range tests {
		target, terminal, declared := resolveOutcome(tmpl, node, tc.outcome)
		if target != tc.target || terminal != tc.terminal || declared != tc.declared {
			t.Fatalf("resolveOutcome(%q) = (%q, %v, %v), want (%q, %v, %v)",
				tc.outcome, target, terminal, declared, tc.target, tc.terminal, tc.declared)
		}
	}
}

func TestValidateDeclaredOutcome(t *testing.T) {
	other := &NodeDefinition{
		ID:       "test",
		Outcomes: []OutcomeDefinition{{Name: "passed", TargetNodeID: "ship"}},
	}
	tmpl := &Template{TemplateID: "tpl", TerminalOutcomes: []OutcomeName{"blocked"}}
	build := &NodeDefinition{
		ID:       "build",
		Branches: map[OutcomeName]NodeID{"rebuild": "build"},
	}

	if err := validateDeclaredOutcome(tmpl, build, "rebuild"); err != nil {
		t.Fatalf("branch outcome: %v", err)
	}
	if err := validateDeclaredOutcome(tmpl, other, "passed"); err != nil {
		t.Fatalf("targeted outcome: %v", err)
	}
	if err := validateDeclaredOutcome(tmpl, build, "blocked"); err != nil {
		t.Fatalf("template terminal outcome: %v", err)
	}

	// The empty outcome is rejected.
	if err := validateDeclaredOutcome(tmpl, build, ""); err == nil {
		t.Fatal("empty outcome: expected error, got nil")
	}
	// "passed" belongs to the other node, not build.
	if err := validateDeclaredOutcome(tmpl, build, "passed"); err == nil {
		t.Fatal("other node's outcome: expected error, got nil")
	} else if !strings.Contains(err.Error(), `node "build"`) || !strings.Contains(err.Error(), `outcome "passed"`) {
		t.Fatalf("other node's outcome: error should name node and outcome, got %v", err)
	}
}

func TestCompletionRequiresTerminal(t *testing.T) {
	tests := []struct {
		name     string
		contract *CompletionContract
		want     bool
	}{
		{name: "nil", contract: nil, want: false},
		{name: "explicit", contract: &CompletionContract{Kind: CompletionExplicit}, want: false},
		{name: "files", contract: &CompletionContract{Kind: CompletionFiles}, want: true},
		{name: "git", contract: &CompletionContract{Kind: CompletionGit}, want: true},
		{name: "unknown", contract: &CompletionContract{Kind: "mystery"}, want: false},
	}
	for _, tc := range tests {
		node := &NodeDefinition{ID: "n", Completion: tc.contract}
		if got := completionRequiresTerminal(node); got != tc.want {
			t.Fatalf("%s: completionRequiresTerminal = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestAttemptHasTerminalFact(t *testing.T) {
	var nilAttempt *Attempt
	if attemptHasTerminalFact(nilAttempt) {
		t.Fatal("nil attempt: expected false")
	}

	final := []AttemptStatus{AttemptSucceeded, AttemptFailed, AttemptCanceled, AttemptTimedOut, AttemptPanicked}
	for _, status := range final {
		if !attemptHasTerminalFact(&Attempt{Status: status}) {
			t.Fatalf("status %q: expected true", status)
		}
	}

	nonFinal := []AttemptStatus{AttemptStarting, AttemptRunning}
	for _, status := range nonFinal {
		if attemptHasTerminalFact(&Attempt{Status: status}) {
			t.Fatalf("status %q: expected false", status)
		}
	}
}

func TestUnsatisfiedRequiredGates(t *testing.T) {
	act := &Activation{ID: "act1", NodeID: "build"}
	defs := []GateDefinition{
		{ID: "g-pass", Required: true},
		{ID: "g-waive", Required: true},
		{ID: "g-extra", Required: false},
	}

	// All required gates satisfied: one passed, one waived.
	allSatisfied := &WorkflowInstance{Gates: []GateInstance{
		{ID: "gi1", GateID: "g-pass", ActivationID: "act1", Status: GatePassed},
		{ID: "gi2", GateID: "g-waive", ActivationID: "act1", Status: GateWaived},
	}}
	if got := unsatisfiedRequiredGates(allSatisfied, defs, act); len(got) != 0 {
		t.Fatalf("all satisfied: expected empty, got %v", got)
	}

	// g-waive missing (and its non-passing instance must not satisfy).
	oneMissing := &WorkflowInstance{Gates: []GateInstance{
		{ID: "gi1", GateID: "g-pass", ActivationID: "act1", Status: GatePassed},
		{ID: "gi2", GateID: "g-waive", ActivationID: "act1", Status: GateFailed},
	}}
	if got := unsatisfiedRequiredGates(oneMissing, defs, act); len(got) != 1 || got[0] != "g-waive" {
		t.Fatalf("one missing: expected [g-waive], got %v", got)
	}

	// Definition order is preserved when multiple are missing.
	none := &WorkflowInstance{}
	if got := unsatisfiedRequiredGates(none, defs, act); len(got) != 2 || got[0] != "g-pass" || got[1] != "g-waive" {
		t.Fatalf("none satisfied: expected [g-pass g-waive], got %v", got)
	}

	// A gate for a different activation does not satisfy.
	otherActivation := &WorkflowInstance{Gates: []GateInstance{
		{ID: "gi1", GateID: "g-pass", ActivationID: "act2", Status: GatePassed},
	}}
	if got := unsatisfiedRequiredGates(otherActivation, defs, act); len(got) != 2 {
		t.Fatalf("other activation: expected [g-pass g-waive], got %v", got)
	}
}
