package workflow

// Policies supplies the template-aware lookups the reducer and the
// command-time gate pinning consult. The reducer cannot see templates
// itself, so a nil Policies reads as no template knowledge: completions
// always satisfy, any failure exhausts immediately, every activation reads
// as manual, and bound-gate subjects that route through child workflows
// stay unresolved. A template is versioned, so every lookup carries the
// template version.
type Policies interface {
	// CompletionGates returns the node's declared gates.
	CompletionGates(templateID TemplateID, templateVersion TemplateVersion, nodeID NodeID) []GateDefinition
	// RetryPolicy returns the node's retry policy, falling back to the
	// template default, or nil when none applies.
	RetryPolicy(templateID TemplateID, templateVersion TemplateVersion, nodeID NodeID) *RetryPolicy
	// DispatchPolicy returns the node's dispatch policy, or nil when the
	// node declares none.
	DispatchPolicy(templateID TemplateID, templateVersion TemplateVersion, nodeID NodeID) *DispatchPolicy
	// Template loads the versioned template for command-time gate binding,
	// which reads the target node's gate bindings to pin resolved inputs and
	// subjects. A load error disables pinning for that template (legacy
	// snapshots without templates keep the unbound behavior).
	Template(templateID TemplateID, templateVersion TemplateVersion) (*Template, error)
	// ChildSnapshot loads one child workflow's current snapshot so subject
	// values that flow through a workflow-action node's child output bindings
	// can be read from the pinned child output. Load failures leave the
	// subject unresolved instead of failing the command.
	ChildSnapshot(workflowID WorkflowID) (Snapshot, bool, error)
}

// templatePolicies resolves policies from the versioned templates held by a
// store. Load errors read as no policy for that node.
type templatePolicies struct {
	store *Store
}

func (p templatePolicies) node(templateID TemplateID, templateVersion TemplateVersion, nodeID NodeID) (*Template, *NodeDefinition) {
	tmpl, err := p.store.LoadTemplate(templateID, templateVersion)
	if err != nil {
		return nil, nil
	}
	node, err := findNode(&tmpl, nodeID)
	if err != nil {
		return nil, nil
	}
	return &tmpl, node
}

func (p templatePolicies) Template(templateID TemplateID, templateVersion TemplateVersion) (*Template, error) {
	tmpl, err := p.store.LoadTemplate(templateID, templateVersion)
	if err != nil {
		return nil, err
	}
	return &tmpl, nil
}

func (p templatePolicies) CompletionGates(templateID TemplateID, templateVersion TemplateVersion, nodeID NodeID) []GateDefinition {
	_, node := p.node(templateID, templateVersion, nodeID)
	if node == nil {
		return nil
	}
	return node.Gates
}

func (p templatePolicies) RetryPolicy(templateID TemplateID, templateVersion TemplateVersion, nodeID NodeID) *RetryPolicy {
	tmpl, node := p.node(templateID, templateVersion, nodeID)
	if node == nil {
		return nil
	}
	if node.RetryPolicy != nil {
		return node.RetryPolicy
	}
	return tmpl.DefaultRetry
}

func (p templatePolicies) DispatchPolicy(templateID TemplateID, templateVersion TemplateVersion, nodeID NodeID) *DispatchPolicy {
	_, node := p.node(templateID, templateVersion, nodeID)
	if node == nil || node.Dispatch == nil {
		return nil
	}
	// Stamp the action kind onto the resolved copy so activations carry the
	// dispatched node's kind without a template reload.
	effective := node.Dispatch.effective()
	effective.ActionKind = node.Action.Kind
	return &effective
}

// ChildSnapshot loads one child workflow's current snapshot from the store.
// The child read takes the child instance's flock while the parent command
// holds the parent's; no code path takes them in the reverse order.
func (p templatePolicies) ChildSnapshot(workflowID WorkflowID) (Snapshot, bool, error) {
	return p.store.loadCurrent(workflowID)
}

// withLegacyDefaults fills the identity and timestamp fields that legacy
// events do not carry, so the rest of the reducer only copies event fields.
// A missing field gets a freshly minted ID or the wall-clock time. Replay of
// a legacy event is therefore not reproducible. A later event that refers to
// a minted ID resolves only against a materialized snapshot that already
// holds that ID, never against a from-zero rebuild. Events written by the
// command boundary carry every field and replay deterministically.
func withLegacyDefaults(event Event) Event {
	if event.At.IsZero() {
		event.At = nowUTC()
	}
	switch event.Kind {
	case EventInstantiated:
		if event.Instantiated == nil {
			return event
		}
		rec := *event.Instantiated
		if rec.InstanceID == "" {
			rec.InstanceID = NewInstanceID()
		}
		event.Instantiated = &rec
		if event.Identity.WorkflowID == "" {
			event.Identity.WorkflowID = NewWorkflowID()
		}
		event.ActivationIDs = padActivationIDs(event.ActivationIDs, len(rec.EntryNodes))
	case EventTransition, EventRerouted:
		event.ActivationIDs = padActivationIDs(event.ActivationIDs, 1)
	case EventStarted:
		if event.AttemptID == "" {
			event.AttemptID = NewAttemptID()
		}
	}
	return event
}

// padActivationIDs returns ids extended with freshly minted IDs up to n.
func padActivationIDs(ids []ActivationID, n int) []ActivationID {
	if len(ids) >= n {
		return ids
	}
	return append(append([]ActivationID(nil), ids...), newActivationIDs(n-len(ids))...)
}
