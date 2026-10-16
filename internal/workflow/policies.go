package workflow

// Policies supplies the template-aware lookups the reducer consults while
// folding events. The reducer cannot see templates itself, so a nil Policies
// reads as no template knowledge: completions always satisfy, any failure
// exhausts immediately, and every activation reads as manual. A template is
// versioned, so every lookup carries the template version.
type Policies interface {
	// CompletionGates returns the node's declared gates.
	CompletionGates(templateID TemplateID, templateVersion TemplateVersion, nodeID NodeID) []GateDefinition
	// RetryPolicy returns the node's retry policy, falling back to the
	// template default, or nil when none applies.
	RetryPolicy(templateID TemplateID, templateVersion TemplateVersion, nodeID NodeID) *RetryPolicy
	// DispatchPolicy returns the node's dispatch policy, or nil when the
	// node declares none.
	DispatchPolicy(templateID TemplateID, templateVersion TemplateVersion, nodeID NodeID) *DispatchPolicy
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

// withLegacyDefaults fills the identity and timestamp fields that events
// written before the command boundary minted them do not carry, so the rest
// of the reducer only ever copies event fields. A legacy event lacking a
// field gets a freshly minted ID or the wall-clock time; replay of such an
// event is therefore not reproducible, and a later event that refers to a
// minted ID resolves only against a snapshot that already holds it (the
// materialized snapshot, never a from-zero rebuild). Events written by the
// command boundary always carry every field and replay deterministically.
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
