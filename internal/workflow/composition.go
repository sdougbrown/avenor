package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"
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

// ---------------------------------------------------------------------------
// Composition executor (kernel-local, no provider admission)
// ---------------------------------------------------------------------------

// defaultChildWait bounds the composition executor's synchronous wait for the
// child workflow's terminal status. A timeout breaks nothing durable: the
// parent activation is left in awaiting_child and the (later-stage) supervisor
// resume continues the wait from that state.
const defaultChildWait = 30 * time.Minute

// defaultChildPollInterval and maxChildPollInterval bound the cadence of the
// child-status poll. Each non-terminal poll doubles the delay up to the cap,
// so a slow child stops pinning the poller to the cost of repeatedly replaying
// its (growing) event log.
const (
	defaultChildPollInterval = 100 * time.Millisecond
	maxChildPollInterval     = time.Second
)

// workflowExecutor is the kernel-local executor for workflow actions. It
// composes the already-created child workflow into the parent node without
// provider admission: it durably attaches the child (moving the parent
// activation to awaiting_child), waits for the child workflow to reach a
// terminal status, maps the child's terminal outcome through the parent
// node's declared outcome_map, selects the bound child outputs as
// identity-only references, and resolves the parent node through the
// child-outcome command. No child state is copied into the parent.
type workflowExecutor struct {
	manager      *Manager
	pollInterval time.Duration
	maxWait      time.Duration
}

// Dispatch attaches the child, waits for the child's terminal status, and
// resolves the parent node with the mapped outcome. It is synchronous like
// the run/loop/team executors: the parent activation is running (attempt
// recorded by the manager's start), moves to awaiting_child on the durable
// attach, and is satisfied — or branched, or the workflow completed — when
// the child's terminal outcome is recorded.
func (e *workflowExecutor) Dispatch(ctx context.Context, ec ExecutorContext) error {
	childID, err := e.childWorkflowID(ec)
	if err != nil {
		return err
	}
	// (a) Durable attach: the parent activation moves to awaiting_child and
	// claims its composition-manifest child reference. A duplicate attach for
	// the same attempt (re-dispatch/resume) is a no-op.
	if err := e.attachChild(ec); err != nil {
		return err
	}
	// (b) Bounded poll for the child's terminal status. A child that is
	// already terminal at attach time resolves on the first poll without a
	// sleep.
	childSnap, err := e.awaitChildTerminal(ctx, ec, childID)
	if err != nil {
		return err
	}
	if !isTerminalStatus(childSnap.Instance.Status) {
		// The parent activation left awaiting_child while the child was
		// still running: its resolution was (or is) handled elsewhere, so
		// stop without resolving again.
		return nil
	}
	// (c)+(d) Map the outcome through the declared contract, select the bound
	// child outputs, resolve the branch target, and issue the child-outcome
	// command once.
	return e.resolveChildOutcome(ec, childID, childSnap)
}

// childWorkflowID resolves the child instance for this dispatch, preferring
// the durable composition-manifest reference and falling back to the
// deterministic derived ID (the two agree by construction).
func (e *workflowExecutor) childWorkflowID(ec ExecutorContext) (WorkflowID, error) {
	action := ec.Action.Workflow
	if action == nil {
		return "", errors.New("workflow executor requires a workflow action")
	}
	snap, exists, err := e.manager.store.loadCurrent(ec.WorkflowID)
	if err != nil {
		return "", err
	}
	if !exists {
		return "", fmt.Errorf("parent workflow %s not found", ec.WorkflowID)
	}
	if ref := findChildReference(&snap.Instance, ec.NodeID); ref != nil {
		return ref.WorkflowID, nil
	}
	return DeriveChildWorkflowID(ec.WorkflowID, ec.NodeID, action.ChildKey), nil
}

// attachChild durably attaches the child (CommandChildAttach -> awaiting_child),
// retrying on optimistic-concurrency conflicts the way the other manager
// command paths do. A re-dispatch of the same attempt that already attached
// finds the idempotency key and resumes.
func (e *workflowExecutor) attachChild(ec ExecutorContext) error {
	const maxAttempts = 4
	for attempt := 0; attempt < maxAttempts; attempt++ {
		snap, exists, err := e.manager.store.loadCurrent(ec.WorkflowID)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("parent workflow %s not found", ec.WorkflowID)
		}
		_, err = e.manager.store.ApplyCommand(ec.WorkflowID, Command{
			ID:               NewCommandID(),
			Kind:             CommandChildAttach,
			ExpectedRevision: snap.Instance.Revision,
			IdempotencyKey:   "child-attach-" + string(ec.AttemptID),
			Identity:         ExecutionIdentity{WorkflowID: ec.WorkflowID, NodeID: ec.NodeID, ActivationID: ec.ActivationID, AttemptID: ec.AttemptID},
			LeaseID:          ec.LeaseID,
		})
		switch {
		case err == nil:
			return nil
		case errors.Is(err, errDuplicateIdempotency):
			// Already attached by a prior dispatch of this attempt.
			return nil
		case errors.Is(err, errRevisionMismatch):
			// A concurrent command advanced the instance; re-read and retry.
			continue
		default:
			return err
		}
	}
	return fmt.Errorf("attach child for node %s: revision kept moving under concurrent commands", ec.NodeID)
}

// awaitChildTerminal polls the child instance until its status is terminal,
// until the parent activation leaves awaiting_child, or until the
// context/bound ends, and returns the child's latest snapshot. A terminal
// child is returned as-is; a non-terminal child is returned only when the
// parent has already moved on (so no further waiting is warranted).
func (e *workflowExecutor) awaitChildTerminal(ctx context.Context, ec ExecutorContext, childID WorkflowID) (Snapshot, error) {
	delay := e.pollInterval
	if delay <= 0 {
		delay = defaultChildPollInterval
	}
	maxWait := e.maxWait
	if maxWait <= 0 {
		maxWait = defaultChildWait
	}
	deadline := time.Now().Add(maxWait)
	for {
		childSnap, exists, err := e.manager.store.loadCurrent(childID)
		if err != nil {
			return Snapshot{}, err
		}
		if !exists {
			// Children are created durably at instantiation; absence is a
			// corruption signal, not a wait condition.
			return Snapshot{}, fmt.Errorf("child workflow %s not found", childID)
		}
		if isTerminalStatus(childSnap.Instance.Status) {
			return childSnap, nil
		}
		// Parent-side check: if the activation has left awaiting_child
		// (its outcome was already resolved), stop polling instead of
		// replaying the child's growing log until the bound.
		if e.parentLeftAwaitingChild(ec) {
			return childSnap, nil
		}
		if time.Now().After(deadline) {
			return Snapshot{}, fmt.Errorf(
				"child workflow %s not terminal after %s; parent remains awaiting_child for resume", childID, maxWait)
		}
		select {
		case <-ctx.Done():
			return Snapshot{}, ctx.Err()
		case <-time.After(delay):
		}
		// Back off between polls so a slow child does not pin the poller to
		// repeated full replays of its (growing) event log.
		if delay < maxChildPollInterval {
			delay *= 2
			if delay > maxChildPollInterval {
				delay = maxChildPollInterval
			}
		}
	}
}

// parentLeftAwaitingChild reports whether the parent activation for this
// dispatch is no longer awaiting its child (already resolved, canceled, or
// absent). A parent that has moved on makes further waiting moot.
func (e *workflowExecutor) parentLeftAwaitingChild(ec ExecutorContext) bool {
	snap, exists, err := e.manager.store.loadCurrent(ec.WorkflowID)
	if err != nil || !exists {
		return true
	}
	act, err := findActivation(&snap.Instance, ec.NodeID, ec.ActivationID)
	if err != nil {
		return true
	}
	return act == nil || act.Status != ActivationAwaitingChild
}

// resolveChildOutcome maps the child's terminal outcome through the node's
// declared outcome_map, validates the mapping against the declared contract,
// selects the bound child outputs, resolves the branch target, and issues the
// child-outcome command exactly once (idempotent per attempt).
func (e *workflowExecutor) resolveChildOutcome(ec ExecutorContext, childID WorkflowID, childSnap Snapshot) error {
	m := e.manager
	action := ec.Action.Workflow

	// The child's terminal outcome maps only through the declared
	// outcome_map; an unmapped (or absent) outcome is never mapped to a
	// fabricated parent outcome.
	childOutcome := childSnap.Instance.TerminalOutcome
	parentOutcome, mapped := action.OutcomeMap[childOutcome]
	if !mapped || parentOutcome == "" {
		return fmt.Errorf(
			"workflow executor: child %s terminal outcome %q is not mapped by node %s's outcome_map",
			childID, childOutcome, ec.NodeID)
	}

	snap, exists, err := m.store.loadCurrent(ec.WorkflowID)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("parent workflow %s not found", ec.WorkflowID)
	}
	tmpl, err := m.templateFor(&snap)
	if err != nil {
		return err
	}
	node, err := findNode(tmpl, ec.NodeID)
	if err != nil {
		return err
	}
	// Defense in depth: composition validation at instantiation already
	// enforces the contract, but the executor never maps an undeclared
	// parent outcome.
	if !parentOutcomeDeclared(*tmpl, node, parentOutcome) {
		return fmt.Errorf(
			"workflow executor: mapped parent outcome %q is not declared by node %s or template %s",
			parentOutcome, ec.NodeID, tmpl.TemplateID)
	}
	outputs := selectChildOutputs(&childSnap, action)

	var payload json.RawMessage
	if target := resolveParentBranchTarget(*tmpl, node, parentOutcome); target != "" {
		t := Transition{
			ActivationID: ec.ActivationID,
			Outcome:      parentOutcome,
			TargetNodeID: target,
			CreatedAt:    nowUTC(),
		}
		data, err := json.Marshal(t)
		if err != nil {
			return err
		}
		payload = data
	}

	const maxAttempts = 4
	for attempt := 0; attempt < maxAttempts; attempt++ {
		snap, exists, err := m.store.loadCurrent(ec.WorkflowID)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("parent workflow %s not found", ec.WorkflowID)
		}
		act, err := findActivation(&snap.Instance, ec.NodeID, ec.ActivationID)
		if err != nil {
			return err
		}
		if act != nil {
			switch act.Status {
			case ActivationAwaitingChild:
				// Expected; proceed to issue the outcome.
			case ActivationSatisfied:
				// Already resolved by a prior dispatch. In a crash window the
				// child_outcome may have landed before the attempt termination
				// was recorded, leaving the kernel attempt stuck "starting"
				// forever; recording it here (idempotent per attempt) closes
				// that window. The reducer would reject the outcome, so the
				// re-dispatch itself is a no-op.
				if terr := m.RecordAttemptTerminated(ec.WorkflowID, ec.NodeID, ec.ActivationID, ec.AttemptID, ec.LeaseID, AttemptSucceeded); terr != nil {
					log.Printf("workflow %s: record attempt %s termination on re-dispatch: %v", ec.WorkflowID, ec.AttemptID, terr)
				}
				return nil
			default:
				return fmt.Errorf(
					"cannot resolve child outcome: activation for node %s is in status %q", ec.NodeID, act.Status)
			}
		}
		_, err = m.store.ApplyCommand(ec.WorkflowID, Command{
			ID:               NewCommandID(),
			Kind:             CommandChildOutcome,
			ExpectedRevision: snap.Instance.Revision,
			IdempotencyKey:   "child-outcome-" + string(ec.AttemptID),
			Identity:         ExecutionIdentity{WorkflowID: ec.WorkflowID, NodeID: ec.NodeID, ActivationID: ec.ActivationID, AttemptID: ec.AttemptID},
			LeaseID:          ec.LeaseID,
			Outcome:          parentOutcome,
			ChildOutputs:     outputs,
			Payload:          payload,
		})
		if err == nil {
			// The kernel-owned composition attempt terminates with the
			// resolution; a succeeded termination is a fact record and never
			// changes activation status (already satisfied).
			_ = m.RecordAttemptTerminated(ec.WorkflowID, ec.NodeID, ec.ActivationID, ec.AttemptID, ec.LeaseID, AttemptSucceeded)
			return nil
		}
		if errors.Is(err, errDuplicateIdempotency) {
			// Outcome already recorded for this attempt.
			return nil
		}
		if !errors.Is(err, errRevisionMismatch) {
			return err
		}
		// Optimistic-concurrency conflict; re-read and retry.
	}
	return fmt.Errorf("resolve child outcome for node %s: revision kept moving under concurrent commands", ec.NodeID)
}

// parentOutcomeDeclared reports whether outcome is one the parent node may
// produce: a node outcome/branch key or a terminal outcome of the parent
// template (the same contract composition validation enforces).
func parentOutcomeDeclared(parent Template, node *NodeDefinition, outcome OutcomeName) bool {
	if _, declared := nodeOutcomeNames(*node)[outcome]; declared {
		return true
	}
	for _, terminal := range parent.TerminalOutcomes {
		if terminal == outcome {
			return true
		}
	}
	return false
}

// resolveParentBranchTarget maps a resolved parent outcome to its declared
// branch target: the node's branch for the outcome, or the target declared on
// a non-terminal node outcome. A terminal outcome (terminal-marked node
// outcome, or the template's terminal outcomes) has no target and completes
// the workflow.
func resolveParentBranchTarget(parent Template, node *NodeDefinition, outcome OutcomeName) NodeID {
	if target, ok := node.Branches[outcome]; ok {
		return target
	}
	for i := range node.Outcomes {
		if node.Outcomes[i].Name == outcome {
			if node.Outcomes[i].Terminal {
				return ""
			}
			return node.Outcomes[i].TargetNodeID
		}
	}
	return ""
}

// selectChildOutputs builds the OutputReference list for the declared
// output_bindings: for each binding, the highest-revision child output value
// matching the bound child output id (by definition id) is referenced with
// full child identity. Only identity is referenced — no output value or
// other child state is copied into the parent.
func selectChildOutputs(child *Snapshot, action *WorkflowAction) []OutputReference {
	if len(action.OutputBindings) == 0 {
		return nil
	}
	nodeByActivation := make(map[ActivationID]NodeID, len(child.Instance.Activations))
	for i := range child.Instance.Activations {
		nodeByActivation[child.Instance.Activations[i].ID] = child.Instance.Activations[i].NodeID
	}
	latest := make(map[OutputID]OutputValue, len(child.Instance.Outputs))
	for i := range child.Instance.Outputs {
		ov := child.Instance.Outputs[i]
		if cur, ok := latest[ov.DefinitionID]; !ok || ov.Revision > cur.Revision {
			latest[ov.DefinitionID] = ov
		}
	}
	var refs []OutputReference
	for _, binding := range action.OutputBindings {
		ov, ok := latest[OutputID(binding.ChildOutput)]
		if !ok {
			// The bound child output was never produced; leave it absent.
			continue
		}
		refs = append(refs, OutputReference{
			WorkflowID:   child.Instance.WorkflowID,
			NodeID:       nodeByActivation[ov.ActivationID],
			ActivationID: ov.ActivationID,
			OutputID:     OutputID(binding.ChildOutput),
			Revision:     ov.Revision,
		})
	}
	return refs
}
