package workflow

// gate_bindings.go implements the template-side validation and the
// command-time resolution of bound gate declarations. A bound gate carries
// late-bound adapter inputs (primitive literals or from_node_output
// references) and, optionally, a subject_binding whose three output
// references resolve into one immutable Subject. Template validation owns
// the static rules (declared outputs, transitive dependency paths, output
// types, same-source-node subjects); the kernel pins the causal references
// on every activation whose node declares bound gates, resolved at command
// time in event building. The reducer only copies what the event carries.

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
)

// Typed gate-binding failures. Callers match with errors.Is; each failure
// leaves the store unchanged (the command never lands).
var (
	// ErrAmbiguousBinding reports a fan-in: the causal provenance chain of a
	// new activation exposes more than one completed source activation for a
	// required reference, so no unique pin exists.
	ErrAmbiguousBinding = errors.New("ambiguous gate binding")
	// ErrSubjectMismatch reports a supplied subject that does not equal the
	// gate's pinned subject on type, repository, pull request number, or
	// revision.
	ErrSubjectMismatch = errors.New("gate subject mismatch")
	// ErrSubjectUnresolved reports a bound gate whose pinned subject has not
	// resolved yet, so no decision can be attributed.
	ErrSubjectUnresolved = errors.New("gate subject unresolved")
	// ErrPollReplayConflict reports a re-reported poll whose response hash
	// differs from the hash recorded for the same poll ID.
	ErrPollReplayConflict = errors.New("poll replay conflict")
)

// maxSafeOutputFloat is the inclusive host-integer bound for primitive
// numbers that feed adapter inputs or pull_request subjects: the JavaScript
// safe-integer range.
const maxSafeOutputFloat = 9007199254740991

// bindingTemplateResolve is the template-aware lookup installed by the
// manager, mirroring the other template resolvers: event building needs the
// target node's gate bindings when it pins resolved gate inputs and
// subjects. Load errors disable pinning for that template (legacy snapshots
// without templates keep today's behavior).
var bindingTemplateResolve func(templateID TemplateID, templateVersion TemplateVersion) (*Template, error)

// childSnapshotResolve loads one child workflow's current snapshot during
// event building so subject references that route through a workflow-action
// node can read the pinned child output value. Load failures leave the
// subject unresolved instead of failing the command.
var childSnapshotResolve func(workflowID WorkflowID) (Snapshot, bool, error)

// SetBindingTemplateResolver wires the template lookup used at command time
// to pin bound gate inputs and subjects onto transition-created activations.
// It is owned by the manager (which can resolve the versioned template).
func SetBindingTemplateResolver(fn func(templateID TemplateID, templateVersion TemplateVersion) (*Template, error)) {
	bindingTemplateResolve = fn
}

// SetChildSnapshotResolver wires the child-workflow snapshot lookup used at
// command time to resolve subject values that flow through child output
// bindings. It is owned by the manager.
func SetChildSnapshotResolver(fn func(workflowID WorkflowID) (Snapshot, bool, error)) {
	childSnapshotResolve = fn
}

// ---------------------------------------------------------------------------
// Template-side validation
// ---------------------------------------------------------------------------

// validateGateBindings enforces the bound-gate declaration rules the
// structural schema vocabulary cannot express: adapter inputs and
// subject_binding placement per gate type, transitive dependency paths,
// declared output existence and primitive types, the same-source-node rule
// for subject references, and the result_outcomes routing contract.
func validateGateBindings(template Template, nodeIndex map[NodeID]int, node NodeDefinition) error {
	outputTypes := make(map[NodeID]map[OutputID]OutputType, len(template.Nodes))
	for _, n := range template.Nodes {
		types := make(map[OutputID]OutputType, len(n.Outputs))
		for _, def := range n.Outputs {
			types[def.ID] = def.Type
		}
		outputTypes[n.ID] = types
	}
	ancestors := transitiveDependencies(template, nodeIndex, node)
	for _, gate := range node.Gates {
		if gate.Type != GateExternal {
			if gate.AdapterID != "" {
				return fmt.Errorf("invalid workflow template: node %q gate %q: adapter_id is forbidden on %s gates", node.ID, gate.ID, gate.Type)
			}
			if len(gate.Inputs) > 0 {
				return fmt.Errorf("invalid workflow template: node %q gate %q: inputs are forbidden on %s gates", node.ID, gate.ID, gate.Type)
			}
			if gate.Type == GateMachine && gate.SubjectBinding != nil {
				return fmt.Errorf("invalid workflow template: node %q gate %q: subject_binding is forbidden on machine gates", node.ID, gate.ID)
			}
		}
		if len(gate.ResultOutcomes) > 0 {
			if gate.Type != GateExternal || gate.SubjectBinding == nil {
				return fmt.Errorf("invalid workflow template: node %q gate %q: result_outcomes are only declared on bound external gates", node.ID, gate.ID)
			}
			for result, outcome := range gate.ResultOutcomes {
				switch result {
				case GateResultFailed, GateResultActionRequired, GateResultChangesRequested:
				default:
					return fmt.Errorf("invalid workflow template: node %q gate %q: result_outcomes key %q is not a routable result (passed is excluded)", node.ID, gate.ID, result)
				}
				if target, _, declared := resolveOutcome(&template, &node, outcome); !declared || target == "" {
					return fmt.Errorf("invalid workflow template: node %q gate %q: result_outcomes %q must name a declared node outcome with a branch", node.ID, gate.ID, outcome)
				}
			}
		}
		if binding := gate.SubjectBinding; binding != nil {
			if err := validateSubjectBinding(node, gate, binding, ancestors, outputTypes); err != nil {
				return err
			}
		}
		for name, value := range gate.Inputs {
			if value.FromNodeOutput == nil {
				if err := validatePrimitiveLiteral(value.Literal, fmt.Sprintf("node %q gate %q input %q", node.ID, gate.ID, name)); err != nil {
					return fmt.Errorf("invalid workflow template: %w", err)
				}
				continue
			}
			ref := value.FromNodeOutput
			if !ancestors[ref.NodeID] {
				return fmt.Errorf("invalid workflow template: node %q gate %q input %q references node %q, which is not a transitive dependency", node.ID, gate.ID, name, ref.NodeID)
			}
			outputType, declared := outputTypes[ref.NodeID][ref.OutputID]
			if !declared {
				return fmt.Errorf("invalid workflow template: node %q gate %q input %q references undeclared output %q on node %q", node.ID, gate.ID, name, ref.OutputID, ref.NodeID)
			}
			switch outputType {
			case OutputString, OutputNumber, OutputBoolean:
			default:
				return fmt.Errorf("invalid workflow template: node %q gate %q input %q references non-primitive output %q (type %q) on node %q", node.ID, gate.ID, name, ref.OutputID, outputType, ref.NodeID)
			}
		}
	}
	return nil
}

// validateSubjectBinding checks one gate's subject_binding: only the
// pull_request subject type exists, every reference must name a declared
// output of the gate node's own primitive type, and all three references
// must resolve from the same source node.
func validateSubjectBinding(node NodeDefinition, gate GateDefinition, binding *SubjectBinding, ancestors map[NodeID]bool, outputTypes map[NodeID]map[OutputID]OutputType) error {
	if binding.Type != "pull_request" {
		return fmt.Errorf("invalid workflow template: node %q gate %q: subject_binding type %q is not supported (only \"pull_request\")", node.ID, gate.ID, binding.Type)
	}
	refs := map[string]*TemplateOutputReference{
		"repository":   subjectBindingRef(binding.Repository),
		"pull_request": subjectBindingRef(binding.PullRequest),
		"revision":     subjectBindingRef(binding.Revision),
	}
	expected := map[string]OutputType{
		"repository":   OutputString,
		"pull_request": OutputNumber,
		"revision":     OutputString,
	}
	source := NodeID("")
	for _, field := range []string{"repository", "pull_request", "revision"} {
		ref := refs[field]
		if ref == nil || ref.NodeID == "" || ref.OutputID == "" {
			return fmt.Errorf("invalid workflow template: node %q gate %q: subject_binding.%s requires node_id and output_id", node.ID, gate.ID, field)
		}
		if !ancestors[ref.NodeID] {
			return fmt.Errorf("invalid workflow template: node %q gate %q: subject_binding.%s references node %q, which is not a transitive dependency", node.ID, gate.ID, field, ref.NodeID)
		}
		outputType, declared := outputTypes[ref.NodeID][ref.OutputID]
		if !declared {
			return fmt.Errorf("invalid workflow template: node %q gate %q: subject_binding.%s references undeclared output %q on node %q", node.ID, gate.ID, field, ref.OutputID, ref.NodeID)
		}
		if outputType != expected[field] {
			return fmt.Errorf("invalid workflow template: node %q gate %q: subject_binding.%s requires a %q output but %q on node %q is %q", node.ID, gate.ID, field, expected[field], ref.OutputID, ref.NodeID, outputType)
		}
		if source == "" {
			source = ref.NodeID
		} else if source != ref.NodeID {
			return fmt.Errorf("invalid workflow template: node %q gate %q: subject_binding references must resolve from one source node (%q and %q differ)", node.ID, gate.ID, source, ref.NodeID)
		}
	}
	return nil
}

// subjectBindingRef unwraps a subject_binding output reference; a missing
// reference yields an empty placeholder that the field checks reject.
func subjectBindingRef(ref *SubjectOutputRef) *TemplateOutputReference {
	if ref == nil {
		return &TemplateOutputReference{}
	}
	out := ref.FromNodeOutput
	return &out
}

// transitiveDependencies returns every node reachable from node through its
// dependency edges (the ancestors a bound reference may name).
func transitiveDependencies(template Template, nodeIndex map[NodeID]int, node NodeDefinition) map[NodeID]bool {
	seen := make(map[NodeID]bool)
	stack := append([]NodeID(nil), node.Dependencies...)
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[id] {
			continue
		}
		seen[id] = true
		index, exists := nodeIndex[id]
		if !exists {
			continue
		}
		stack = append(stack, template.Nodes[index].Dependencies...)
	}
	return seen
}

// validatePrimitiveLiteral checks that a raw JSON literal is a strict
// primitive (string, boolean, or finite number within the safe integer
// range). null, objects, arrays, and unsafe numbers are rejected.
func validatePrimitiveLiteral(raw json.RawMessage, what string) error {
	trimmed := trimmedJSON(raw)
	if len(trimmed) == 0 {
		return fmt.Errorf("%s requires a primitive literal (string, number, or boolean)", what)
	}
	switch trimmed[0] {
	case '"':
		return nil
	case 't', 'f':
		return nil
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		return validateSafeJSONNumber(trimmed, false, what)
	case 'n':
		return fmt.Errorf("%s cannot be null", what)
	}
	return fmt.Errorf("%s must be a primitive literal (string, number, or boolean)", what)
}

// validateSafeJSONNumber checks a raw JSON number literal: finite, and —
// when required — integral within the safe integer range.
func validateSafeJSONNumber(raw []byte, requireIntegral bool, what string) error {
	value, err := strconv.ParseFloat(string(raw), 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return fmt.Errorf("%s is not a finite number", what)
	}
	if math.Abs(value) > maxSafeOutputFloat {
		return fmt.Errorf("%s exceeds the safe integer range", what)
	}
	if requireIntegral && value != math.Trunc(value) {
		return fmt.Errorf("%s must be an integral number", what)
	}
	return nil
}

// trimmedJSON returns the raw value without surrounding whitespace.
func trimmedJSON(raw json.RawMessage) []byte {
	start := 0
	for start < len(raw) && (raw[start] == ' ' || raw[start] == '\t' || raw[start] == '\n' || raw[start] == '\r') {
		start++
	}
	end := len(raw)
	for end > start && (raw[end-1] == ' ' || raw[end-1] == '\t' || raw[end-1] == '\n' || raw[end-1] == '\r') {
		end--
	}
	if start >= end {
		return nil
	}
	return raw[start:end]
}

// ---------------------------------------------------------------------------
// Command-time resolution (event building)
// ---------------------------------------------------------------------------

// attachGateBindings pins the target node's bound gate inputs and subject
// onto a transition event at command time. It resolves every declared
// reference along the causal provenance chain of the causing activation —
// never from the latest workflow-global output value — and records the
// result on the event; the reducer only copies what the event carries. A
// fan-in (more than one completed source activation at the nearest causal
// distance) fails the whole command with ErrAmbiguousBinding so nothing is
// appended. Templates without bound gates resolve nothing.
func attachGateBindings(state Snapshot, event *Event, targetNodeID NodeID, causedBy ActivationID) error {
	event.CausedBy = append([]ActivationID(nil), causedBy)
	if bindingTemplateResolve == nil {
		return nil
	}
	tmpl, err := bindingTemplateResolve(state.Instance.TemplateID, state.Instance.TemplateVersion)
	if err != nil || tmpl == nil {
		// A missing template disables pinning; the activation behaves like a
		// legacy unbound one.
		return nil
	}
	node, err := findNode(tmpl, targetNodeID)
	if err != nil || !nodeDeclaresBoundGates(node) {
		return nil
	}
	resolved, err := resolveGates(state, tmpl, node, causedBy)
	if err != nil {
		return err
	}
	event.ResolvedGates = resolved
	return nil
}

// nodeDeclaresBoundGates reports whether any gate on the node carries a
// subject binding or a from_node_output input reference.
func nodeDeclaresBoundGates(node *NodeDefinition) bool {
	for _, gate := range node.Gates {
		if gate.SubjectBinding != nil || len(gate.Inputs) > 0 {
			return true
		}
	}
	return false
}

// resolveGates pins every bound gate of node for the activation a transition
// creates. References resolve by walking the causal provenance chain from
// the causing activation back to the nearest completed activation of the
// source node; a fan-in (two or more at the same nearest distance) is
// rejected; a missing source output is recorded as unresolved.
func resolveGates(state Snapshot, tmpl *Template, node *NodeDefinition, causedBy ActivationID) (map[GateID]ResolvedGate, error) {
	byID := make(map[ActivationID]*Activation, len(state.Instance.Activations))
	for i := range state.Instance.Activations {
		byID[state.Instance.Activations[i].ID] = &state.Instance.Activations[i]
	}
	resolved := make(map[GateID]ResolvedGate, len(node.Gates))
	for _, gate := range node.Gates {
		if gate.SubjectBinding == nil && len(gate.Inputs) == 0 {
			continue
		}
		gate := gate
		record := ResolvedGate{Inputs: make(map[string]ResolvedGateInput, len(gate.Inputs))}
		for name, value := range gate.Inputs {
			if value.FromNodeOutput == nil {
				record.Inputs[name] = ResolvedGateInput{Literal: append(json.RawMessage(nil), value.Literal...)}
				continue
			}
			input, err := resolveReferencedInput(state, tmpl, byID, causedBy, value.FromNodeOutput)
			if err != nil {
				return nil, err
			}
			if input.Reference == nil && len(input.Literal) == 0 {
				record.Unresolved = append(record.Unresolved, name)
			}
			record.Inputs[name] = input
		}
		if binding := gate.SubjectBinding; binding != nil {
			subject, unresolved, err := resolvePinnedSubject(state, tmpl, byID, causedBy, binding)
			if err != nil {
				return nil, err
			}
			record.Subject = subject
			record.Unresolved = append(record.Unresolved, unresolved...)
		}
		if len(record.Unresolved) == 0 && record.Subject != nil {
			record.Unresolved = nil
		}
		resolved[gate.ID] = record
	}
	if len(resolved) == 0 {
		return nil, nil
	}
	return resolved, nil
}

// resolveReferencedInput pins one from_node_output input reference against
// the causal chain. A nil result (with no error) records the reference as
// unresolved.
func resolveReferencedInput(state Snapshot, tmpl *Template, byID map[ActivationID]*Activation, causedBy ActivationID, ref *TemplateOutputReference) (ResolvedGateInput, error) {
	source, err := nearestCompletedSource(byID, causedBy, ref.NodeID)
	if err != nil {
		return ResolvedGateInput{}, err
	}
	if source == nil {
		return ResolvedGateInput{}, nil
	}
	pinned, _, err := pinOutputReference(state, tmpl, source, ref.NodeID, ref.OutputID)
	if err != nil {
		return ResolvedGateInput{}, err
	}
	if pinned == nil {
		return ResolvedGateInput{}, nil
	}
	return ResolvedGateInput{Reference: pinned}, nil
}

// resolvePinnedSubject builds the immutable subject of a bound gate from the
// causal source activation: repository and revision from string outputs and
// the pull request number from an integral number output. Any missing
// reference leaves the subject nil and reports the field under unresolved.
func resolvePinnedSubject(state Snapshot, tmpl *Template, byID map[ActivationID]*Activation, causedBy ActivationID, binding *SubjectBinding) (*Subject, []string, error) {
	fields := []struct {
		name string
		ref  *TemplateOutputReference
	}{
		{"subject.repository", subjectBindingRef(binding.Repository)},
		{"subject.pull_request", subjectBindingRef(binding.PullRequest)},
		{"subject.revision", subjectBindingRef(binding.Revision)},
	}
	var (
		subject     Subject
		unresolved  []string
		numbers     []json.RawMessage
		haveSubject = true
	)
	for _, field := range fields {
		source, err := nearestCompletedSource(byID, causedBy, field.ref.NodeID)
		if err != nil {
			return nil, nil, err
		}
		if source == nil {
			haveSubject = false
			unresolved = append(unresolved, field.name)
			continue
		}
		pinned, value, err := pinOutputReference(state, tmpl, source, field.ref.NodeID, field.ref.OutputID)
		if err != nil {
			return nil, nil, err
		}
		if pinned == nil {
			haveSubject = false
			unresolved = append(unresolved, field.name)
			continue
		}
		numbers = append(numbers, value)
	}
	if !haveSubject {
		return nil, unresolved, nil
	}
	subject.Type = binding.Type
	for i, field := range fields {
		switch field.name {
		case "subject.repository", "subject.revision":
			var text string
			if err := json.Unmarshal(numbers[i], &text); err != nil {
				haveSubject = false
				unresolved = append(unresolved, field.name)
				continue
			}
			if field.name == "subject.repository" {
				subject.Repository = text
			} else {
				subject.Revision = text
			}
		case "subject.pull_request":
			var number float64
			if err := json.Unmarshal(numbers[i], &number); err != nil || number != math.Trunc(number) || math.Abs(number) > maxSafeOutputFloat {
				haveSubject = false
				unresolved = append(unresolved, field.name)
				continue
			}
			subject.PullRequest = int(number)
		}
	}
	if !haveSubject {
		return nil, unresolved, nil
	}
	return &subject, nil, nil
}

// nearestCompletedSource walks the causal provenance chain from the causing
// activation and returns the nearest completed activation of sourceNode. A
// fan-in — two or more completed source activations at the same nearest
// causal distance — returns ErrAmbiguousBinding. No source yields nil.
func nearestCompletedSource(byID map[ActivationID]*Activation, causedBy ActivationID, sourceNode NodeID) (*Activation, error) {
	root, exists := byID[causedBy]
	if !exists {
		return nil, nil
	}
	visited := map[ActivationID]bool{causedBy: true}
	level := []*Activation{root}
	for len(level) > 0 {
		var found []*Activation
		var next []*Activation
		for _, act := range level {
			if act.NodeID == sourceNode && act.Status == ActivationSatisfied {
				found = append(found, act)
			}
			for _, parent := range act.CausedBy {
				if visited[parent] {
					continue
				}
				visited[parent] = true
				if p, ok := byID[parent]; ok {
					next = append(next, p)
				}
			}
		}
		if len(found) == 1 {
			return found[0], nil
		}
		if len(found) > 1 {
			return nil, fmt.Errorf("%w: node %q has %d completed causal source activations", ErrAmbiguousBinding, sourceNode, len(found))
		}
		level = next
	}
	return nil, nil
}

// pinOutputReference resolves the pinned OutputReference (and raw value) for
// one output of a completed source activation. A workflow-action source
// resolves through its declared output_bindings and the durable child
// reference; the pinned reference then carries the child workflow identity.
// A nil result means the source completed without recording the output.
func pinOutputReference(state Snapshot, tmpl *Template, source *Activation, sourceNode NodeID, outputID OutputID) (*OutputReference, json.RawMessage, error) {
	node, err := findNode(tmpl, sourceNode)
	if err != nil {
		return nil, nil, err
	}
	if node.Action.Kind == ActionWorkflow && node.Action.Workflow != nil {
		childOutput := OutputID("")
		for _, binding := range node.Action.Workflow.OutputBindings {
			if OutputID(binding.ParentOutput) == outputID {
				childOutput = OutputID(binding.ChildOutput)
				break
			}
		}
		if childOutput == "" {
			return nil, nil, nil
		}
		ref := findChildReference(&state.Instance, sourceNode)
		if ref == nil || ref.ParentActivation != source.ID {
			return nil, nil, nil
		}
		for _, out := range ref.Outputs {
			if out.OutputID != childOutput {
				continue
			}
			pinned := out
			if childSnapshotResolve != nil {
				if childSnap, ok, err := childSnapshotResolve(out.WorkflowID); err == nil && ok {
					for _, childOut := range childSnap.Instance.Outputs {
						if childOut.ActivationID == out.ActivationID && childOut.DefinitionID == out.OutputID {
							return &pinned, childOut.Value, nil
						}
					}
				}
			}
			return &pinned, nil, nil
		}
		return nil, nil, nil
	}
	for _, out := range state.Instance.Outputs {
		if out.ActivationID == source.ID && out.DefinitionID == outputID {
			return &OutputReference{
				WorkflowID:   state.Instance.WorkflowID,
				NodeID:       sourceNode,
				ActivationID: source.ID,
				OutputID:     outputID,
				Revision:     out.Revision,
			}, out.Value, nil
		}
	}
	return nil, nil, nil
}
