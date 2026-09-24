package workflow

// park_external.go implements kernel-local parking of auto external
// activations. An eligible zero-output external node under an auto dispatch
// policy never runs an attempt: once its required bound gates all carry
// resolved pins, the controller parks the activation in awaiting_gate under
// the node's declared success_outcome. Parking consumes no admission, records
// no provider attempt, and lands no gate result — only the activation's own
// status transition.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

// ErrUnresolvedBinding reports an auto external activation whose required
// gate bindings have not fully resolved (a missing causal source output).
// The activation stays ready so a later transition can resolve the pins.
var ErrUnresolvedBinding = errors.New("gate binding unresolved")

// ParkExternalRequest asks the manager to park one eligible auto external
// activation. The caller must hold the controller leader lease and validate
// it (WithLeader) around the call.
type ParkExternalRequest struct {
	WorkflowID       WorkflowID
	NodeID           NodeID
	ActivationID     ActivationID
	ExpectedRevision int64
	ControllerID     string
	LeaderLeaseID    string
}

// ParkedGateSeed describes one required external gate of a parked activation
// that a controller must poll: its adapter ID and the hash of the pinned
// subject at park time. The subject hash keys the controller's poll cursor
// and poll IDs; a new publication head yields a different hash.
type ParkedGateSeed struct {
	GateID      GateID `json:"gate_id"`
	AdapterID   string `json:"adapter_id"`
	SubjectHash string `json:"subject_hash"`
}

// ParkExternalResult reports the recorded park.
type ParkExternalResult struct {
	WorkflowID     WorkflowID
	NodeID         NodeID
	ActivationID   ActivationID
	Revision       int64
	SuccessOutcome OutcomeName
	Gates          []ParkedGateSeed
}

// readyCandidateKind classifies a candidate activation for the controller
// policy: auto external nodes park kernel-locally, everything else is
// provider-backed.
func readyCandidateKind(policy DispatchPolicy) ReadyCandidateKind {
	if policy.ActionKind == ActionExternal {
		return ReadyCandidateExternalPark
	}
	return ReadyCandidateProvider
}

// SubjectHash derives the stable subject hash used in poll cursor keys and
// poll IDs: the SHA-256 of the subject's canonical JSON encoding. Identical
// subjects hash identically; a changed revision hashes differently.
func SubjectHash(subject *Subject) string {
	data, err := json.Marshal(subject)
	if err != nil {
		// Subject fields are plain scalars; marshaling cannot fail.
		panic(fmt.Sprintf("subject hash: %v", err))
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// externalGateSeeds collects the required external gates of node from the
// activation's pinned ResolvedGates. Every required gate must carry a fully
// resolved pin (subject present, no unresolved references); otherwise
// ErrUnresolvedBinding is returned and nothing is recorded.
func externalGateSeeds(act *Activation, node *NodeDefinition) ([]ParkedGateSeed, error) {
	var seeds []ParkedGateSeed
	for _, gate := range node.Gates {
		if gate.Type != GateExternal || !gate.Required {
			continue
		}
		resolved, ok := act.ResolvedGates[gate.ID]
		if !ok || resolved.Subject == nil || len(resolved.Unresolved) > 0 {
			return nil, fmt.Errorf("%w: gate %q on activation %s has unresolved bindings", ErrUnresolvedBinding, gate.ID, act.ID)
		}
		seeds = append(seeds, ParkedGateSeed{
			GateID:      gate.ID,
			AdapterID:   gate.AdapterID,
			SubjectHash: SubjectHash(resolved.Subject),
		})
	}
	return seeds, nil
}

// ParkExternal parks one eligible auto external activation into awaiting_gate
// under the node's declared success_outcome. It revalidates the candidate
// under the workflow store's lock (the caller must already hold and have
// validated the controller leader lease): expected revision, claimable
// status, auto policy matching the controller, and fully resolved gate
// bindings. A competing manual claim or a stale revision is a benign
// ErrStaleCandidate; unresolved bindings are a typed ErrUnresolvedBinding
// that leaves the activation ready. No gate result, provider attempt, or
// admission is recorded.
func (m *Manager) ParkExternal(req ParkExternalRequest) (ParkExternalResult, error) {
	snap, exists, err := m.store.loadCurrent(req.WorkflowID)
	if err != nil {
		return ParkExternalResult{}, err
	}
	if !exists {
		return ParkExternalResult{}, fmt.Errorf("workflow not found: %s", req.WorkflowID)
	}
	stale := func() (ParkExternalResult, error) {
		return ParkExternalResult{}, ErrStaleCandidate
	}
	if _, err := findActivation(&snap.Instance, req.NodeID, req.ActivationID); err != nil {
		return ParkExternalResult{}, err
	}
	tmpl, err := m.templateFor(&snap)
	if err != nil {
		return ParkExternalResult{}, err
	}
	node, err := findNode(tmpl, req.NodeID)
	if err != nil {
		return ParkExternalResult{}, err
	}
	if !isAutoExternalNode(node) {
		return stale()
	}
	idemKey := "park-external-" + string(req.ActivationID)
	const maxParkAttempts = 4
	for attempt := 0; attempt < maxParkAttempts; attempt++ {
		fresh, exists, err := m.store.loadCurrent(req.WorkflowID)
		if err != nil {
			return ParkExternalResult{}, err
		}
		if !exists {
			return ParkExternalResult{}, fmt.Errorf("workflow not found: %s", req.WorkflowID)
		}
		fa, err := findActivation(&fresh.Instance, req.NodeID, req.ActivationID)
		if err != nil {
			return ParkExternalResult{}, err
		}
		if fa == nil {
			return ParkExternalResult{}, fmt.Errorf("activation not found for node %q", req.NodeID)
		}
		if _, done := fresh.Idempotency[idemKey]; done {
			// An idempotent re-park: the same command already committed (it
			// may have landed just before a crash). A still-parked activation
			// reports the same seeds; anything else is stale.
			if fa.Status != ActivationAwaitingGate {
				return stale()
			}
			reseeded, err := externalGateSeeds(fa, node)
			if err != nil {
				return ParkExternalResult{}, err
			}
			return ParkExternalResult{
				WorkflowID:     req.WorkflowID,
				NodeID:         req.NodeID,
				ActivationID:   req.ActivationID,
				Revision:       fresh.Instance.Revision,
				SuccessOutcome: fa.Dispatch.SuccessOutcome,
				Gates:          reseeded,
			}, nil
		}
		if fresh.Instance.Revision != req.ExpectedRevision {
			return stale()
		}
		if !claimableActivationStatus(fa.Status) || fa.ActiveLease != nil {
			return stale()
		}
		if fa.Dispatch == nil || !fa.Dispatch.IsAuto() || fa.Dispatch.ControllerID != req.ControllerID {
			return stale()
		}
		if fa.Dispatch.ActionKind != ActionExternal || fa.Dispatch.SuccessOutcome == "" {
			return stale()
		}
		seeds, err := externalGateSeeds(fa, node)
		if err != nil {
			return ParkExternalResult{}, err
		}
		if _, err := m.store.ApplyCommand(req.WorkflowID, Command{
			ID:               NewCommandID(),
			Kind:             CommandParkExternal,
			ExpectedRevision: fresh.Instance.Revision,
			IdempotencyKey:   idemKey,
			Identity:         ExecutionIdentity{WorkflowID: req.WorkflowID, NodeID: req.NodeID, ActivationID: req.ActivationID},
			Actor:            req.ControllerID,
			Outcome:          fa.Dispatch.SuccessOutcome,
		}); err != nil {
			if errors.Is(err, errDuplicateIdempotency) {
				// A concurrent twin of this park committed first; the
				// revalidation above reports the committed state on the
				// caller's next attempt.
				return ParkExternalResult{}, ErrStaleCandidate
			}
			if errors.Is(err, errRevisionMismatch) {
				// Another command on the same workflow committed between the
				// read above and this commit; retry revalidates fresh state.
				continue
			}
			return ParkExternalResult{}, err
		}
		return ParkExternalResult{
			WorkflowID:     req.WorkflowID,
			NodeID:         req.NodeID,
			ActivationID:   req.ActivationID,
			Revision:       fresh.Instance.Revision + 1,
			SuccessOutcome: fa.Dispatch.SuccessOutcome,
			Gates:          seeds,
		}, nil
	}
	return ParkExternalResult{}, fmt.Errorf("park external %s/%s: revision kept moving under concurrent commands", req.WorkflowID, req.NodeID)
}

// ExternalGatePollState is the poll-time view of one bound external gate on a
// parked activation: the resolved adapter inputs and the pinned subject.
type ExternalGatePollState struct {
	GateID    GateID                     `json:"gate_id"`
	AdapterID string                     `json:"adapter_id"`
	Inputs    map[string]json.RawMessage `json:"inputs"`
	Subject   *Subject                   `json:"subject,omitempty"`
}

// ExternalGateState returns the pinned poll state of one external gate on an
// activation: the adapter ID, the concrete input values (literals and
// resolved output references), and the pinned subject. A gate whose pins are
// not fully resolved returns an error; callers treat that as a stale cursor.
func (m *Manager) ExternalGateState(wf WorkflowID, nodeID NodeID, actID ActivationID, gateID GateID) (ExternalGatePollState, error) {
	snap, exists, err := m.store.loadCurrent(wf)
	if err != nil {
		return ExternalGatePollState{}, err
	}
	if !exists {
		return ExternalGatePollState{}, fmt.Errorf("workflow not found: %s", wf)
	}
	act, err := findActivation(&snap.Instance, nodeID, actID)
	if err != nil {
		return ExternalGatePollState{}, err
	}
	if act == nil {
		return ExternalGatePollState{}, fmt.Errorf("activation not found for node %q", nodeID)
	}
	resolved, ok := act.ResolvedGates[gateID]
	if !ok || resolved.Subject == nil || len(resolved.Unresolved) > 0 {
		return ExternalGatePollState{}, fmt.Errorf("%w: gate %q on activation %s", ErrUnresolvedBinding, gateID, actID)
	}
	tmpl, err := m.templateFor(&snap)
	if err != nil {
		return ExternalGatePollState{}, err
	}
	node, err := findNode(tmpl, nodeID)
	if err != nil {
		return ExternalGatePollState{}, err
	}
	def := gateDefinitionByID(node, gateID)
	if def == nil {
		return ExternalGatePollState{}, fmt.Errorf("gate %q is not declared on node %q", gateID, nodeID)
	}
	inputs := make(map[string]json.RawMessage, len(resolved.Inputs))
	for name, input := range resolved.Inputs {
		if len(input.Literal) > 0 {
			inputs[name] = append(json.RawMessage(nil), input.Literal...)
			continue
		}
		if input.Reference == nil {
			return ExternalGatePollState{}, fmt.Errorf("%w: gate %q input %q has no pin", ErrUnresolvedBinding, gateID, name)
		}
		value, err := m.outputValue(input.Reference)
		if err != nil {
			return ExternalGatePollState{}, err
		}
		if value == nil {
			return ExternalGatePollState{}, fmt.Errorf("%w: gate %q input %q references output %q with no recorded value", ErrUnresolvedBinding, gateID, name, input.Reference.OutputID)
		}
		inputs[name] = value
	}
	return ExternalGatePollState{
		GateID:    gateID,
		AdapterID: def.AdapterID,
		Inputs:    inputs,
		Subject:   resolved.Subject,
	}, nil
}

// outputValue reads the recorded value of one pinned output reference,
// loading the owning workflow (a child workflow for pins that cross a
// workflow-action boundary).
func (m *Manager) outputValue(ref *OutputReference) (json.RawMessage, error) {
	if ref == nil || ref.WorkflowID == "" {
		return nil, fmt.Errorf("%w: output reference has no workflow identity", ErrUnresolvedBinding)
	}
	snap, exists, err := m.store.loadCurrent(ref.WorkflowID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("%w: workflow %s not found for output reference", ErrUnresolvedBinding, ref.WorkflowID)
	}
	for _, out := range snap.Instance.Outputs {
		if out.ActivationID == ref.ActivationID && out.DefinitionID == ref.OutputID {
			return append(json.RawMessage(nil), out.Value...), nil
		}
	}
	return nil, nil
}
