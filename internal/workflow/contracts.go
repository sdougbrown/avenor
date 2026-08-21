package workflow

import "fmt"

// contracts.go holds pure, template-aware machine contract evaluators for
// Stage 11 (safe machine contract evaluation). These are read-only helpers the
// later phase's atomic workflow.complete command and reducer will call; they
// take only definitions and instance state, and mutate nothing.

// validateDeclaredOutputs checks the supplied output values against the
// node's declared output definitions: no undeclared or duplicate definition
// ids, and every required definition present.
func validateDeclaredOutputs(node *NodeDefinition, outputs []OutputValue) error {
	declared := make(map[OutputID]bool, len(node.Outputs))
	required := make(map[OutputID]bool)
	for _, def := range node.Outputs {
		declared[def.ID] = true
		if def.Required {
			required[def.ID] = true
		}
	}
	seen := make(map[OutputID]bool, len(outputs))
	for _, output := range outputs {
		if !declared[output.DefinitionID] {
			return fmt.Errorf("workflow output %q is not declared on node %q", output.DefinitionID, node.ID)
		}
		if seen[output.DefinitionID] {
			return fmt.Errorf("workflow output %q is declared multiple times on node %q", output.DefinitionID, node.ID)
		}
		seen[output.DefinitionID] = true
	}
	for id := range required {
		if !seen[id] {
			return fmt.Errorf("required workflow output %q is missing on node %q", id, node.ID)
		}
	}
	return nil
}

// resolveOutcome resolves an outcome name against the node and template
// declarations. Declared sources are checked in order: node branches, node
// outcomes, template terminal outcomes, then checkpoint exit outcomes.
// Checkpoint exits are declared but do not by themselves complete the
// workflow; routing for them is decided by a later phase.
func resolveOutcome(tmpl *Template, node *NodeDefinition, outcome OutcomeName) (target NodeID, terminal bool, declared bool) {
	if outcome == "" {
		return "", false, false
	}
	if target, ok := node.Branches[outcome]; ok {
		return target, false, true
	}
	for _, def := range node.Outcomes {
		if def.Name != outcome {
			continue
		}
		if def.TargetNodeID != "" {
			return def.TargetNodeID, def.Terminal, true
		}
		return "", def.Terminal, true
	}
	for _, name := range tmpl.TerminalOutcomes {
		if name == outcome {
			return "", true, true
		}
	}
	if node.Checkpoint != nil {
		for _, name := range node.Checkpoint.ExitOutcomes {
			if name == outcome {
				return "", false, true
			}
		}
	}
	return "", false, false
}

// validateDeclaredOutcome rejects any outcome the node and template do not
// declare.
func validateDeclaredOutcome(tmpl *Template, node *NodeDefinition, outcome OutcomeName) error {
	_, _, declared := resolveOutcome(tmpl, node, outcome)
	if !declared {
		return fmt.Errorf("workflow node %q outcome %q is not declared (undeclared outcomes are rejected)", node.ID, outcome)
	}
	return nil
}

// completionRequiresTerminal reports whether the node's completion contract
// observes the attempt's terminal output/state and must wait for the
// termination fact. Explicit handoff contracts may precede termination.
func completionRequiresTerminal(node *NodeDefinition) bool {
	if node.Completion == nil {
		return false
	}
	switch node.Completion.Kind {
	case CompletionFiles, CompletionGit:
		return true
	default:
		return false
	}
}

// attemptHasTerminalFact reports whether the attempt carries a final status
// recorded by the runtime.
func attemptHasTerminalFact(at *Attempt) bool {
	if at == nil {
		return false
	}
	switch at.Status {
	case AttemptSucceeded, AttemptFailed, AttemptCanceled, AttemptTimedOut, AttemptPanicked:
		return true
	default:
		return false
	}
}

// unsatisfiedRequiredGates returns the ids of every required gate definition
// with no passed or waived gate instance for the activation, in definition
// order. An empty slice means all required gates are satisfied.
func unsatisfiedRequiredGates(inst *WorkflowInstance, defs []GateDefinition, act *Activation) []GateID {
	satisfied := make(map[GateID]bool)
	for _, gi := range inst.Gates {
		if gi.ActivationID != act.ID {
			continue
		}
		if gi.Status != GatePassed && gi.Status != GateWaived {
			continue
		}
		satisfied[gi.GateID] = true
		satisfied[GateID(gi.ID)] = true
	}
	var missing []GateID
	for _, def := range defs {
		if def.Required && !satisfied[def.ID] {
			missing = append(missing, def.ID)
		}
	}
	return missing
}
