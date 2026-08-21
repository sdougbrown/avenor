package workflow

// completion.go implements the manager's atomic machine-completion handler
// (workflow.complete, Stage 11 phase 2). It validates the lease/owner pair
// and the node's declared machine contract (outputs, outcome, terminal
// dependency), stages and hashes evidence first, then issues ONE atomic
// CommandComplete that captures evidence, outputs, outcome selection, and
// lease release in a single snapshot revision. The reducer stays
// authoritative: gate-awareness (a completion with unsatisfied required
// gates parks the activation in awaiting_gate) is decided there.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
)

type commandCompleteRequest struct {
	NodeID       NodeID             `json:"node_id"`
	ActivationID ActivationID       `json:"activation_id"`
	AttemptID    AttemptID          `json:"attempt_id"`
	LeaseID      LeaseID            `json:"lease_id"`
	OwnerToken   string             `json:"owner_token"`
	Outcome      OutcomeName        `json:"outcome"`
	Outputs      []completeOutput   `json:"outputs"`
	Artifacts    []completeArtifact `json:"artifacts"`
}

type completeOutput struct {
	DefinitionID OutputID        `json:"definition_id"`
	Value        json.RawMessage `json:"value"`
}

type completeArtifact struct {
	SrcPath    string `json:"src_path"`
	StoredPath string `json:"stored_path"`
	NonEmpty   bool   `json:"non_empty"`
	SHA256     string `json:"sha256"`
}

// commandComplete atomically completes a machine/external handoff activation.
// Flow: validate -> idempotent no-op short-circuit -> wait for the terminal
// fact when the contract depends on terminal output -> stage + hash evidence
// -> one atomic CommandComplete. On any failure after staging, the freshly
// staged evidence is removed so the store and filesystem never diverge, and
// no completion event is emitted (snapshot unchanged).
func (m *Manager) commandComplete(wf WorkflowID, payload json.RawMessage) (any, error) {
	var req commandCompleteRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("complete payload: %w", err)
	}
	if req.NodeID == "" {
		return nil, errors.New("complete requires node_id")
	}
	if req.AttemptID == "" {
		return nil, errors.New("complete requires attempt_id")
	}
	if req.LeaseID == "" {
		return nil, errors.New("complete requires lease_id")
	}
	if req.OwnerToken == "" {
		return nil, errors.New("complete requires owner_token")
	}
	if req.Outcome == "" {
		return nil, errors.New("complete requires outcome")
	}

	snap, exists, err := m.store.loadCurrent(wf)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("workflow not found: %s", wf)
	}
	act, err := findActivation(&snap.Instance, req.NodeID, req.ActivationID)
	if err != nil {
		return nil, err
	}
	if act == nil {
		return nil, fmt.Errorf("activation not found for node %q", req.NodeID)
	}

	// Idempotent duplicate: a repeated workflow.complete for the same attempt
	// is a safe no-op. Checked before status/lease validation because a
	// successful completion has already moved the activation (satisfied, or
	// parked awaiting_gate) and released the lease.
	if _, done := snap.Idempotency["complete-"+string(req.AttemptID)]; done {
		return map[string]any{
			"idempotent":        true,
			"already_completed": true,
			"status":            string(act.Status),
		}, nil
	}

	// This is the machine handoff path: only a running or awaiting-completion
	// activation with a live, matching lease can complete.
	if act.Status != ActivationRunning && act.Status != ActivationAwaitingCompletion {
		return nil, fmt.Errorf("cannot complete activation in status %q (machine completion requires a running or awaiting-completion activation)", act.Status)
	}
	if act.ActiveLease == nil {
		return nil, errors.New("activation has no active lease")
	}
	if req.LeaseID != act.ActiveLease.ID {
		return nil, errors.New("lease_id does not match the active lease")
	}
	if ownerTokenDigest(req.OwnerToken) != act.ActiveLease.TokenDigest {
		return nil, errors.New("owner token does not match the lease")
	}

	tmpl, err := m.templateFor(&snap)
	if err != nil {
		return nil, err
	}
	node, err := findNode(tmpl, req.NodeID)
	if err != nil {
		return nil, err
	}

	// Declared machine contract checks (no side effects on failure).
	declared := make([]OutputValue, len(req.Outputs))
	for i, o := range req.Outputs {
		declared[i] = OutputValue{DefinitionID: o.DefinitionID}
	}
	if err := validateDeclaredOutputs(node, declared); err != nil {
		return nil, err
	}
	if err := validateDeclaredOutcome(tmpl, node, req.Outcome); err != nil {
		return nil, err
	}
	target, _, _ := resolveOutcome(tmpl, node, req.Outcome)

	// Wait-for-terminal-fact contract: a completion whose contract observes
	// terminal output must not precede the attempt's terminal status. Explicit
	// worker handoff is only allowed for non-terminal-dependent contracts.
	if completionRequiresTerminal(node) {
		attempt := findAttempt(&snap, act, req.AttemptID)
		if attempt == nil || !attemptHasTerminalFact(attempt) {
			return nil, fmt.Errorf(
				"machine completion for node %q depends on terminal output and attempt %s has not reached a terminal status",
				node.ID, req.AttemptID)
		}
	}

	// Stage evidence FIRST (a filesystem side effect, no state transition).
	// Any failure removes what was already staged in this call.
	now := time.Now().UTC()
	var staged []StagedEvidence
	for _, a := range req.Artifacts {
		se, err := m.store.StageEvidence(wf, a.SrcPath, a.StoredPath, a.NonEmpty, a.SHA256)
		if err != nil {
			cleanupStaged(m, wf, staged)
			return nil, fmt.Errorf("stage evidence: %w", err)
		}
		staged = append(staged, se)
	}

	evidence := make([]Evidence, 0, len(staged))
	evIDs := make([]EvidenceID, 0, len(staged))
	owner := act.ActiveLease.Owner
	for _, se := range staged {
		evidence = append(evidence, Evidence{
			ID:           se.EvidenceID,
			Kind:         "artifact",
			Source:       EvidenceMachine,
			Authority:    owner,
			OriginalPath: se.OriginalPath,
			StoredPath:   se.StoredPath,
			Size:         se.Size,
			SHA256:       se.SHA256,
			ActivationID: act.ID,
			CreatedAt:    now,
		})
		evIDs = append(evIDs, se.EvidenceID)
	}

	// Bounded-retry apply (mirrors RecordAttemptTerminated): a concurrent
	// command can bump the revision in the window between the read and the
	// apply; re-read under a fresh lock and retry.
	const maxCompleteAttempts = 4
	for attempt := 0; attempt < maxCompleteAttempts; attempt++ {
		fresh, exists, err := m.store.loadCurrent(wf)
		if err != nil {
			cleanupStaged(m, wf, staged)
			return nil, err
		}
		if !exists {
			cleanupStaged(m, wf, staged)
			return nil, fmt.Errorf("workflow not found: %s", wf)
		}
		// When the completion is gated the branch payload is suppressed: the
		// activation is parked awaiting_gate and NO branch may be followed
		// until the gates resolve (a sibling EventTransition would otherwise
		// create the target activation prematurely).
		gated := len(unsatisfiedRequiredGates(&fresh.Instance, node.Gates, act)) > 0
		var branch json.RawMessage
		if !gated && target != "" {
			branch, err = json.Marshal(&Transition{Outcome: req.Outcome, TargetNodeID: target})
			if err != nil {
				cleanupStaged(m, wf, staged)
				return nil, fmt.Errorf("encode transition: %w", err)
			}
		}
		outputs := buildCompleteOutputs(node, &fresh.Instance, req, act.ID, evIDs, now)
		result, err := m.store.ApplyCommand(wf, Command{
			ID:               NewCommandID(),
			Kind:             CommandComplete,
			ExpectedRevision: fresh.Instance.Revision,
			IdempotencyKey:   "complete-" + string(req.AttemptID),
			Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: req.NodeID, ActivationID: act.ID, AttemptID: req.AttemptID},
			LeaseID:          req.LeaseID,
			Outcome:          req.Outcome,
			Evidence:         evidence,
			Outputs:          outputs,
			Payload:          branch,
		})
		if err == nil {
			status := ActivationSatisfied
			for i := range result.Instance.Activations {
				if result.Instance.Activations[i].ID == act.ID {
					status = result.Instance.Activations[i].Status
					break
				}
			}
			outputIDs := make([]string, 0, len(outputs))
			for _, o := range outputs {
				outputIDs = append(outputIDs, string(o.ID))
			}
			return map[string]any{
				"status":            "completed",
				"activation_status": string(status),
				"outcome":           string(req.Outcome),
				"evidence_ids":      evIDs,
				"output_ids":        outputIDs,
				"revision":          result.Instance.Revision,
			}, nil
		}
		if errors.Is(err, errDuplicateIdempotency) {
			cleanupStaged(m, wf, staged)
			return map[string]any{
				"idempotent":        true,
				"already_completed": true,
				"status":            string(act.Status),
			}, nil
		}
		if !errors.Is(err, errRevisionMismatch) {
			cleanupStaged(m, wf, staged)
			return nil, err
		}
		// Optimistic-concurrency conflict; re-read under a fresh lock and retry.
	}
	cleanupStaged(m, wf, staged)
	return nil, fmt.Errorf("complete attempt %s: revision kept moving under concurrent commands", req.AttemptID)
}

// buildCompleteOutputs materializes the OutputValue records for a completion
// from the request's declared outputs, assigning append-only monotonic
// per-definition revisions against the current snapshot. Later authorized
// activations produce new entries without mutating prior facts. Outputs
// declared as file type reference the staged artifact evidence.
func buildCompleteOutputs(node *NodeDefinition, inst *WorkflowInstance, req commandCompleteRequest, actID ActivationID, evIDs []EvidenceID, now time.Time) []OutputValue {
	fileTypes := make(map[OutputID]bool, len(node.Outputs))
	for _, def := range node.Outputs {
		if def.Type == OutputFile {
			fileTypes[def.ID] = true
		}
	}
	outputs := make([]OutputValue, 0, len(req.Outputs))
	for _, o := range req.Outputs {
		value := o.Value
		if len(value) == 0 {
			value = json.RawMessage("null")
		}
		out := OutputValue{
			ID:           NewOutputID(),
			DefinitionID: o.DefinitionID,
			ActivationID: actID,
			Revision:     nextRevisionFor(inst, o.DefinitionID),
			Value:        value,
			CreatedAt:    now,
		}
		if fileTypes[o.DefinitionID] && len(evIDs) > 0 {
			out.EvidenceIDs = evIDs
		}
		outputs = append(outputs, out)
	}
	return outputs
}

// nextRevisionFor returns the next append-only revision for an output
// definition: the max revision already recorded on the instance plus one, or
// 1 when the definition has no recorded values yet.
func nextRevisionFor(inst *WorkflowInstance, defID OutputID) int64 {
	var rev int64
	for _, o := range inst.Outputs {
		if o.DefinitionID == defID && o.Revision > rev {
			rev = o.Revision
		}
	}
	return rev + 1
}

// cleanupStaged best-effort removes the evidence directories staged during a
// failed completion so the store and filesystem never diverge. Errors are
// ignored: the snapshot was not advanced, so a leftover directory is inert
// (never referenced by any committed evidence record).
func cleanupStaged(m *Manager, wf WorkflowID, staged []StagedEvidence) {
	for _, se := range staged {
		os.RemoveAll(evidenceDir(m.store.instanceDir(wf), se.EvidenceID))
	}
}
