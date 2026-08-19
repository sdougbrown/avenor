package workflow

import (
	"fmt"
	"strings"
)

// ValidateGraph enforces the typed, cross-node graph rules that the closed
// Profile vocabulary cannot express. It is pure: it reads only template and
// never touches I/O. It assumes the structural (field-presence, action-variant,
// non-blank-id) checks have already run or will run; this function owns graph
// shape: node reference integrity, duplicate containment, the ordinary
// acyclic dependency graph, bounded loop well-formedness, composition-limit
// bounds, and workflow-action input-binding reference legality.
//
// The Branches map of a node is itself the declaration of its branch outcomes:
// branch keys do not need to be listed in the node's Outcomes, and
// TerminalOutcomes is a template-global vocabulary whose entries need not be
// redeclared as Terminal on any node.
//
// ValidateGraph deliberately terminates at the first violation so the message
// pinpoints the earliest structural break, mirroring the sibling ValidateTemplate
// walk's fail-fast contract.
func ValidateGraph(template Template) error {
	nodeIndex := make(map[NodeID]int, len(template.Nodes))
	for index, node := range template.Nodes {
		if _, exists := nodeIndex[node.ID]; exists {
			return fmt.Errorf("invalid workflow template: duplicate node id %q", node.ID)
		}
		nodeIndex[node.ID] = index
	}

	// Entry node containment (duplicates, then existence).
	if err := rejectDuplicateEntryNodes(template.EntryNodes); err != nil {
		return err
	}
	for _, id := range template.EntryNodes {
		if _, exists := nodeIndex[id]; !exists {
			return fmt.Errorf("invalid workflow template: entry node %q does not exist", id)
		}
	}

	// Per-node reference integrity and declared-vocabulary construction.
	outputSets := make(map[NodeID]map[OutputID]struct{}, len(template.Nodes))
	for _, node := range template.Nodes {
		outcomes := make(map[OutcomeName]struct{}, len(node.Outcomes))
		outputs := make(map[OutputID]struct{}, len(node.Outputs))

		deps := make(map[NodeID]struct{}, len(node.Dependencies))
		for _, dep := range node.Dependencies {
			if _, dup := deps[dep]; dup {
				return fmt.Errorf("invalid workflow template: node %q has duplicate dependency %q", node.ID, dep)
			}
			deps[dep] = struct{}{}
			if _, exists := nodeIndex[dep]; !exists {
				return fmt.Errorf("invalid workflow template: node %q depends on unknown node %q", node.ID, dep)
			}
		}

		for _, outcome := range node.Outcomes {
			if _, dup := outcomes[outcome.Name]; dup {
				return fmt.Errorf("invalid workflow template: node %q has duplicate outcome %q", node.ID, outcome.Name)
			}
			outcomes[outcome.Name] = struct{}{}
			target := strings.TrimSpace(string(outcome.TargetNodeID))
			switch {
			case outcome.Terminal && target != "":
				return fmt.Errorf("invalid workflow template: node %q outcome %q is terminal and must not declare target node %q", node.ID, outcome.Name, outcome.TargetNodeID)
			case !outcome.Terminal && target == "":
				return fmt.Errorf("invalid workflow template: node %q outcome %q is non-terminal and must declare a target node", node.ID, outcome.Name)
			case !outcome.Terminal:
				if _, exists := nodeIndex[outcome.TargetNodeID]; !exists {
					return fmt.Errorf("invalid workflow template: node %q outcome %q targets unknown node %q", node.ID, outcome.Name, outcome.TargetNodeID)
				}
			}
		}

		for branchOutcome, targetID := range node.Branches {
			if strings.TrimSpace(string(branchOutcome)) == "" {
				return fmt.Errorf("invalid workflow template: node %q has a blank branch outcome", node.ID)
			}
			if _, exists := nodeIndex[targetID]; !exists {
				return fmt.Errorf("invalid workflow template: node %q branch outcome %q targets unknown node %q", node.ID, branchOutcome, targetID)
			}
		}

		for _, output := range node.Outputs {
			outputs[output.ID] = struct{}{}
		}

		outputSets[node.ID] = outputs
	}

	// Ordinary dependency graph must be acyclic, self-dependency included and
	// loop body nodes included: cycles are legal only through explicit bounded
	// loops.
	if err := detectDependencyCycles(template.Nodes, nodeIndex); err != nil {
		return err
	}

	// Terminal outcome vocabulary: validate the template-global TerminalOutcomes
	// list for non-empty, non-blank entries, and uniqueness.
	if err := validateTerminalOutcomes(template); err != nil {
		return err
	}
	terminalSet := make(map[OutcomeName]struct{}, len(template.TerminalOutcomes))
	for _, name := range template.TerminalOutcomes {
		terminalSet[name] = struct{}{}
	}

	if err := validateBoundedLoops(template, nodeIndex, terminalSet); err != nil {
		return err
	}

	if err := validateCompositionLimits(template); err != nil {
		return err
	}

	if err := validateWorkflowActions(template, nodeIndex, outputSets); err != nil {
		return err
	}

	return nil
}

// rejectDuplicateEntryNodes rejects repeated IDs in template.EntryNodes.
func rejectDuplicateEntryNodes(entryNodes []NodeID) error {
	seen := make(map[NodeID]struct{}, len(entryNodes))
	for _, id := range entryNodes {
		if _, dup := seen[id]; dup {
			return fmt.Errorf("invalid workflow template: duplicate entry node %q", id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

// detectDependencyCycles runs iterative DFS over the ordinary dependency edges
// and reports the first back-edge it finds. Edges point from a node to each of
// its dependencies, so a node still being visited when an edge reaches it is a
// cycle (and reaches itself directly in the self-dependency case).
func detectDependencyCycles(nodes []NodeDefinition, nodeIndex map[NodeID]int) error {
	const (
		unvisited = 0
		visiting  = 1
		visited   = 2
	)
	state := make([]int, len(nodes))
	for start := range nodes {
		if state[start] != unvisited {
			continue
		}
		type frame struct {
			node    int
			depNext int
		}
		stack := []frame{{node: start}}
		state[start] = visiting
		for len(stack) > 0 {
			top := &stack[len(stack)-1]
			node := nodes[top.node]
			if top.depNext < len(node.Dependencies) {
				dep := node.Dependencies[top.depNext]
				top.depNext++
				depIndex, exists := nodeIndex[dep]
				if !exists {
					// Existence is reported by ValidateGraph's reference check.
					continue
				}
				switch state[depIndex] {
				case unvisited:
					state[depIndex] = visiting
					stack = append(stack, frame{node: depIndex})
				case visiting:
					return fmt.Errorf("invalid workflow template: dependency cycle detected involving node %q", nodes[depIndex].ID)
				}
				continue
			}
			state[top.node] = visited
			stack = stack[:len(stack)-1]
		}
	}
	return nil
}

// validateTerminalOutcomes enforces template.TerminalOutcomes is non-empty and
// free of blank or duplicated entries. TerminalOutcomes is the template-global
// declaration; entries need not be redeclared as Terminal on any node.
func validateTerminalOutcomes(template Template) error {
	if len(template.TerminalOutcomes) == 0 {
		return fmt.Errorf("invalid workflow template: terminal_outcomes must not be empty")
	}
	seen := make(map[OutcomeName]struct{}, len(template.TerminalOutcomes))
	for _, name := range template.TerminalOutcomes {
		if strings.TrimSpace(string(name)) == "" {
			return fmt.Errorf("invalid workflow template: terminal outcome entry is empty")
		}
		if _, dup := seen[name]; dup {
			return fmt.Errorf("invalid workflow template: duplicate terminal outcome %q", name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

// validateBoundedLoops checks each bounded loop's identity, body containment,
// iteration bound, and exit-outcome legality.
func validateBoundedLoops(template Template, nodeIndex map[NodeID]int, terminalSet map[OutcomeName]struct{}) error {
	loopIDs := make(map[LoopID]struct{}, len(template.BoundedLoops))
	for index, loop := range template.BoundedLoops {
		if strings.TrimSpace(string(loop.ID)) == "" {
			return fmt.Errorf("invalid workflow template: bounded_loops[%d].id is required", index)
		}
		if _, dup := loopIDs[loop.ID]; dup {
			return fmt.Errorf("invalid workflow template: duplicate bounded loop id %q", loop.ID)
		}
		loopIDs[loop.ID] = struct{}{}

		if loop.MaximumRuns <= 0 {
			return fmt.Errorf("invalid workflow template: bounded loop %q must declare maximum_runs > 0", loop.ID)
		}
		if len(loop.BodyNodes) == 0 {
			return fmt.Errorf("invalid workflow template: bounded loop %q must declare at least one body node", loop.ID)
		}

		body := make(map[NodeID]struct{}, len(loop.BodyNodes))
		for _, bodyID := range loop.BodyNodes {
			if _, exists := nodeIndex[bodyID]; !exists {
				return fmt.Errorf("invalid workflow template: bounded loop %q body node %q does not exist", loop.ID, bodyID)
			}
			if _, dup := body[bodyID]; dup {
				return fmt.Errorf("invalid workflow template: bounded loop %q has duplicate body node %q", loop.ID, bodyID)
			}
			body[bodyID] = struct{}{}
		}

		if _, inBody := body[loop.EntryNodeID]; !inBody {
			return fmt.Errorf("invalid workflow template: bounded loop %q entry node %q is not a body node", loop.ID, loop.EntryNodeID)
		}
		if _, inBody := body[loop.CheckpointID]; !inBody {
			return fmt.Errorf("invalid workflow template: bounded loop %q checkpoint node %q is not a body node", loop.ID, loop.CheckpointID)
		}

		for _, exit := range loop.ExitOutcomes {
			if _, isTerminal := terminalSet[exit]; !isTerminal {
				return fmt.Errorf("invalid workflow template: bounded loop %q exit outcome %q is not a terminal outcome", loop.ID, exit)
			}
		}
	}
	return nil
}

// validateCompositionLimits checks, when composition limits are configured,
// that both bounds are at least 1. Limits are optional — a template without
// them (e.g. an open leaf) is valid — but configured-but-invalid bounds are
// rejected unconditionally.
func validateCompositionLimits(template Template) error {
	limits := template.CompositionLimits
	if limits == nil {
		return nil
	}
	if limits.MaximumDepth < 1 {
		return fmt.Errorf("invalid workflow template: composition max_depth must be >= 1")
	}
	if limits.MaximumChildren < 1 {
		return fmt.Errorf("invalid workflow template: composition max_children must be >= 1")
	}
	return nil
}

// validateWorkflowActions checks, for every workflow-action node, the input
// binding From references when present: the From node must exist, and must
// declare the referenced output. Both are decidable purely locally from this
// template's node set, so they are enforced unconditionally — even for nodes
// without composition limits (the open-leaf case). Parent-output declaredness
// on output_bindings and parent-outcome declaredness on outcome_map are
// binding/outcome declaredness (Stage 9 scope, not locally decidable) and are
// deliberately not checked here.
func validateWorkflowActions(
	template Template,
	nodeIndex map[NodeID]int,
	outputSets map[NodeID]map[OutputID]struct{},
) error {
	for _, node := range template.Nodes {
		if node.Action.Kind != ActionWorkflow || node.Action.Workflow == nil {
			continue
		}
		action := node.Action.Workflow

		for bindingIndex, binding := range action.InputBindings {
			from := binding.From
			if from == nil {
				continue
			}
			if _, exists := nodeIndex[from.NodeID]; !exists {
				return fmt.Errorf("invalid workflow template: node %q input_bindings[%d] references unknown node %q", node.ID, bindingIndex, from.NodeID)
			}
			if _, declared := outputSets[from.NodeID][from.OutputID]; !declared {
				return fmt.Errorf("invalid workflow template: node %q input_bindings[%d] references undeclared output %q on node %q", node.ID, bindingIndex, from.OutputID, from.NodeID)
			}
		}
	}
	return nil
}

// declaredTerminalOutcomes returns the template-global terminal-outcome
// vocabulary, deduplicated in first-appearance order. The global
// TerminalOutcomes list is itself the declaration; nodes do not need to
// redeclare Terminal==true for an outcome to be terminal.
func declaredTerminalOutcomes(template Template) []OutcomeName {
	var names []OutcomeName
	seen := make(map[OutcomeName]struct{}, len(template.TerminalOutcomes))
	for _, name := range template.TerminalOutcomes {
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	return names
}
