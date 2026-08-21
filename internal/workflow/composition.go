package workflow

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// Default composition bounds applied when a template that composes child
// workflows omits composition_limits. They exist so an unbounded (or hostile)
// template can never explode fan-out or nest without limit; sane compositions
// sit far below all three. The depth bound is generous (8 levels) because
// nesting beyond a handful of levels is a design smell, not a need; the
// fan-out bound (16 children per template) keeps a single level from
// materializing an unbounded number of siblings. The per-template bounds do
// not bound the whole tree — the defaults alone permit up to ~16^8 nodes —
// so DefaultMaximumCompositionInstances caps the total number of instances
// (root plus every composed descendant) that one workflow.instantiate call
// may materialize, keeping the worst case a bounded few thousand store writes.
const (
	DefaultMaximumCompositionDepth     = 8
	DefaultMaximumCompositionChildren  = 16
	DefaultMaximumCompositionInstances = 4096
)

// TemplateResolver resolves a pinned child template. The manager provides the
// store-backed implementation; tests supply in-memory resolvers so the
// composition rules are unit-testable without a store.
type TemplateResolver func(templateID TemplateID, templateVersion TemplateVersion) (Template, error)

// Composition is the validated composition manifest for one template: one
// entry per workflow-action node, in node order. It is the pure, store-free
// output of BuildComposition; the manager converts entries into durable
// ChildReference records and materializes the child instances.
type Composition struct {
	Children []CompositionChild
}

// CompositionChild identifies one workflow-action node and the child instance
// it composes. Template is the resolved pinned child template (used by the
// manager to instantiate the child recursively); it is not part of the
// durable record.
type CompositionChild struct {
	NodeID          NodeID
	ChildWorkflowID WorkflowID
	Template        Template
}

// effectiveCompositionLimits returns the template's composition limits, or
// the defaults when the template omits them. Graph validation already rejects
// configured limits below 1, so the result is always >= 1 on each bound.
func effectiveCompositionLimits(template Template) CompositionLimits {
	if template.CompositionLimits != nil {
		return *template.CompositionLimits
	}
	return CompositionLimits{
		MaximumDepth:    DefaultMaximumCompositionDepth,
		MaximumChildren: DefaultMaximumCompositionChildren,
	}
}

// workflowActionNodes returns the template's workflow-action nodes in node
// order.
func workflowActionNodes(template Template) []NodeDefinition {
	var nodes []NodeDefinition
	for _, node := range template.Nodes {
		if node.Action.Kind == ActionWorkflow && node.Action.Workflow != nil {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// DeriveChildWorkflowID deterministically derives the child workflow ID for a
// workflow-action node from the parent's identity. Re-instantiation or replay
// of the same parent therefore always targets the same child instance, which
// is what makes child creation idempotent: a second attempt finds the child
// under its derived ID and resumes it instead of creating a duplicate.
func DeriveChildWorkflowID(parent WorkflowID, nodeID NodeID, childKey string) WorkflowID {
	sum := sha256.Sum256([]byte(string(parent) + "\x00" + string(nodeID) + "\x00" + childKey))
	return WorkflowID("wfchild_" + hex.EncodeToString(sum[:16]))
}

// BuildComposition validates the compose prerequisites of root (pinned
// versions resolve, no composition cycles, depth and fan-out within the
// effective limits, total instance count within the global cap, and all
// output/outcome bindings declared) and returns the composition manifest —
// one entry per workflow-action node of root, in node order — plus the total
// number of instances the tree would materialize (root plus every composed
// descendant). It is pure: the only I/O it performs is through resolve, and
// parent is only used to derive the deterministic child workflow IDs. A
// template with no workflow-action nodes composes nothing and yields an empty
// manifest without consulting the resolver.
func BuildComposition(parent WorkflowID, root Template, resolve TemplateResolver) (Composition, int, error) {
	comp := Composition{}
	_, instances, err := composeTemplate(parent, root, []TemplateID{root.TemplateID}, resolve, &comp)
	if err != nil {
		return Composition{}, 0, err
	}
	return comp, instances, nil
}

// composeTemplate validates one template's compose prerequisites and, when
// comp is non-nil, records its own workflow-action children on comp
// (descendants are validated but not recorded: they materialize when the
// child is itself instantiated). It returns the template's composition depth
// and the total number of instances in its subtree (itself plus every
// composed descendant), rejecting when the subtree exceeds
// DefaultMaximumCompositionInstances.
//
// Depth semantics: a template's composition depth is 0 when it composes no
// children, otherwise 1 plus the maximum depth of its composed children. A
// template is rejected when its depth exceeds its effective max_depth — i.e.
// max_depth is the number of child levels it may nest below itself (a
// max_depth of 1 permits composing leaf children only). Each template is
// checked against its own limits (or the defaults), and the fan-out bound
// applies to the count of that template's workflow-action nodes.
func composeTemplate(
	parent WorkflowID,
	template Template,
	ancestors []TemplateID,
	resolve TemplateResolver,
	comp *Composition,
) (int, int, error) {
	limits := effectiveCompositionLimits(template)
	nodes := workflowActionNodes(template)
	if len(nodes) > limits.MaximumChildren {
		return 0, 0, fmt.Errorf(
			"composition: template %q declares %d workflow actions, exceeding max_children %d",
			template.TemplateID, len(nodes), limits.MaximumChildren,
		)
	}
	depth := 0
	instances := 1
	for _, node := range nodes {
		action := node.Action.Workflow

		child, err := resolve(action.TemplateID, action.TemplateVersion)
		if err != nil {
			return 0, 0, fmt.Errorf("composition: node %q: %w", node.ID, err)
		}

		if index := indexTemplateID(ancestors, child.TemplateID); index >= 0 {
			chain := append(append([]TemplateID{}, ancestors[index:]...), child.TemplateID)
			return 0, 0, fmt.Errorf(
				"composition: composition cycle: template %s@%s composes an ancestor already in the chain (%s)",
				child.TemplateID, child.TemplateVersion, formatTemplateChain(chain),
			)
		}

		if err := validateCompositionBindings(node, template, child); err != nil {
			return 0, 0, err
		}

		childID := DeriveChildWorkflowID(parent, node.ID, action.ChildKey)
		chain := append(append([]TemplateID{}, ancestors...), child.TemplateID)
		childDepth, childInstances, err := composeTemplate(childID, child, chain, resolve, nil)
		if err != nil {
			return 0, 0, err
		}
		instances += childInstances
		if instances > DefaultMaximumCompositionInstances {
			return 0, 0, fmt.Errorf(
				"composition: template %q materializes %d instances in its subtree, exceeding the maximum of %d",
				template.TemplateID, instances, DefaultMaximumCompositionInstances,
			)
		}
		if d := 1 + childDepth; d > limits.MaximumDepth {
			return 0, 0, fmt.Errorf(
				"composition: template %q composes to depth %d, exceeding max_depth %d",
				template.TemplateID, d, limits.MaximumDepth,
			)
		}
		if d := 1 + childDepth; d > depth {
			depth = d
		}
		if comp != nil {
			comp.Children = append(comp.Children, CompositionChild{
				NodeID:          node.ID,
				ChildWorkflowID: childID,
				Template:        child,
			})
		}
	}
	return depth, instances, nil
}

func indexTemplateID(ids []TemplateID, id TemplateID) int {
	for i, candidate := range ids {
		if candidate == id {
			return i
		}
	}
	return -1
}

func formatTemplateChain(chain []TemplateID) string {
	parts := make([]string, len(chain))
	for i, id := range chain {
		parts[i] = string(id)
	}
	return strings.Join(parts, " -> ")
}

// validateCompositionBindings enforces, for one workflow-action node, that
// every declared binding names something its two contracts declare. Child
// output declaredness is checked against the child template's node output
// set; parent output declaredness against the parent node's declared
// outputs. Outcome-map keys must be outcomes the child template can produce
// (its terminal outcomes or any node's outcomes/branches), and values must be
// outcomes the parent node may produce (its own outcomes/branches or the
// parent template's terminal outcomes). Input-binding From references are
// already enforced locally by ValidateGraph.
func validateCompositionBindings(node NodeDefinition, parent Template, child Template) error {
	action := node.Action.Workflow
	childOutputs := templateOutputIDs(child)
	nodeOutputs := nodeOutputIDs(node)

	for i, binding := range action.OutputBindings {
		if _, declared := childOutputs[OutputID(binding.ChildOutput)]; !declared {
			return fmt.Errorf(
				"composition: node %q output_bindings[%d]: child_output %q is not a declared output of child template %s@%s",
				node.ID, i, binding.ChildOutput, child.TemplateID, child.TemplateVersion,
			)
		}
		if _, declared := nodeOutputs[OutputID(binding.ParentOutput)]; !declared {
			return fmt.Errorf(
				"composition: node %q output_bindings[%d]: parent_output %q is not a declared output of node %q",
				node.ID, i, binding.ParentOutput, node.ID,
			)
		}
	}

	childOutcomes := templateOutcomeNames(child)
	nodeOutcomes := nodeOutcomeNames(node)
	terminal := make(map[OutcomeName]struct{}, len(parent.TerminalOutcomes))
	for _, name := range parent.TerminalOutcomes {
		terminal[name] = struct{}{}
	}
	// Iterate outcome-map keys in sorted order so the reported error is
	// stable for a map with several undeclared keys.
	outcomeKeys := make([]OutcomeName, 0, len(action.OutcomeMap))
	for key := range action.OutcomeMap {
		outcomeKeys = append(outcomeKeys, key)
	}
	sort.Slice(outcomeKeys, func(i, j int) bool { return outcomeKeys[i] < outcomeKeys[j] })
	for _, childOutcome := range outcomeKeys {
		parentOutcome := action.OutcomeMap[childOutcome]
		if _, declared := childOutcomes[childOutcome]; !declared {
			return fmt.Errorf(
				"composition: node %q outcome_map: child outcome %q is not a declared outcome of child template %s@%s",
				node.ID, childOutcome, child.TemplateID, child.TemplateVersion,
			)
		}
		if _, declared := nodeOutcomes[parentOutcome]; !declared {
			if _, declared := terminal[parentOutcome]; !declared {
				return fmt.Errorf(
					"composition: node %q outcome_map: parent outcome %q is not an outcome of node %q or a terminal outcome of template %q",
					node.ID, parentOutcome, node.ID, parent.TemplateID,
				)
			}
		}
	}
	return nil
}

// templateOutputIDs returns every output ID declared on any node of template.
func templateOutputIDs(template Template) map[OutputID]struct{} {
	ids := make(map[OutputID]struct{})
	for i := range template.Nodes {
		for _, output := range template.Nodes[i].Outputs {
			ids[output.ID] = struct{}{}
		}
	}
	return ids
}

// nodeOutputIDs returns the output IDs declared on node.
func nodeOutputIDs(node NodeDefinition) map[OutputID]struct{} {
	ids := make(map[OutputID]struct{}, len(node.Outputs))
	for _, output := range node.Outputs {
		ids[output.ID] = struct{}{}
	}
	return ids
}

// templateOutcomeNames returns every outcome the template can produce: its
// terminal outcomes plus the outcomes and branch keys declared on its nodes.
func templateOutcomeNames(template Template) map[OutcomeName]struct{} {
	names := make(map[OutcomeName]struct{}, len(template.TerminalOutcomes))
	for _, name := range template.TerminalOutcomes {
		names[name] = struct{}{}
	}
	for i := range template.Nodes {
		for outcome := range nodeOutcomeNames(template.Nodes[i]) {
			names[outcome] = struct{}{}
		}
	}
	return names
}

// nodeOutcomeNames returns the outcomes node may produce: its declared
// outcomes plus its branch keys.
func nodeOutcomeNames(node NodeDefinition) map[OutcomeName]struct{} {
	names := make(map[OutcomeName]struct{}, len(node.Outcomes)+len(node.Branches))
	for _, outcome := range node.Outcomes {
		names[outcome.Name] = struct{}{}
	}
	for outcome := range node.Branches {
		names[outcome] = struct{}{}
	}
	return names
}
