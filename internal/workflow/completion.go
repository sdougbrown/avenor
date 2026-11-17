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
	"log"
	"time"
)

type commandCompleteRequest struct {
	NodeID       NodeID             `json:"node_id"`
	ActivationID ActivationID       `json:"activation_id"`
	AttemptID    AttemptID          `json:"attempt_id"`
	LeaseID      LeaseID            `json:"lease_id"`
	OwnerToken   string             `json:"owner_token"`
	Outcome      OutcomeName        `json:"outcome"`
	Outputs      []CompleteOutput   `json:"outputs"`
	Artifacts    []CompleteArtifact `json:"artifacts"`
	// captures carries the supervisor's own in-memory evidence, which is never
	// part of the wire. An unexported field is invisible to the JSON decoder, so
	// this path is reachable only from CompleteAuto. The public complete request
	// keeps its path-sourced artifacts and gains no way to supply bytes.
	captures []CommandCapture
	// supervised marks the supervisor's own evaluated completion. It is
	// unexported, so no JSON request can set it: a command contract's verdict is
	// the supervisor's check execution, never a caller's claim.
	supervised bool
}

// commandComplete is the public workflow.complete handler: it decodes the
// request and applies it. Every request it accepts names file-sourced artifacts
// only.
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
	return m.applyComplete(wf, req)
}

// applyComplete atomically completes a machine/external handoff activation.
// Flow: validate -> idempotent no-op short-circuit -> wait for the terminal
// fact when the contract depends on terminal output -> stage + hash evidence
// -> one atomic CommandComplete. A failure after staging that left no durable
// events removes the freshly staged evidence, so the store and filesystem never
// diverge. No completion event is emitted, so the snapshot is unchanged. Both
// the public handler and the supervisor's own completion reach it, so it also
// enforces the workflow id rule the public handler's router applies.
func (m *Manager) applyComplete(wf WorkflowID, req commandCompleteRequest) (any, error) {
	if err := validateWorkflowID(string(wf)); err != nil {
		return nil, err
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

	// Attempt ownership: the presenting attempt must belong to this
	// activation. Without this check a foreign attempt ID could borrow that
	// attempt's terminal fact and consume the idempotency key
	// complete-<foreignAttempt>, permanently blocking the owning
	// activation's legitimate completion.
	if !activationHasAttempt(act, req.AttemptID) {
		return nil, fmt.Errorf("attempt %q does not belong to activation %q for node %q", req.AttemptID, act.ID, req.NodeID)
	}

	// Idempotent duplicate: a repeated workflow.complete for the same attempt
	// is a safe no-op. Checked before status/lease validation because a
	// successful completion has already moved the activation (satisfied, or
	// parked awaiting_gate) and released the lease. A fresh loadCurrent
	// reports the CURRENT status (a later command may have advanced the
	// activation since the initial snapshot read), falling back to the
	// snapshot's status if the fresh read fails.
	if _, done := snap.Idempotency["complete-"+string(req.AttemptID)]; done {
		return map[string]any{
			"idempotent":        true,
			"already_completed": true,
			"status":            string(m.currentActivationStatus(wf, req.NodeID, act.ID, act.Status)),
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

	// A command contract is a supervisor-side check, not a declaration a lease
	// holder may assert. Its verdict comes from running the check against the
	// attempt's working directory. Only CompleteAuto, which evaluates first, may
	// present for such a node. The public workflow.complete therefore cannot skip
	// the check or consume the attempt's completion idempotency key ahead of the
	// supervisor.
	if node.Completion != nil && node.Completion.Kind == CompletionCommand && !req.supervised {
		return nil, fmt.Errorf("node %q declares completion.kind %q, which only the supervisor's evaluated completion can satisfy (the check runs as a supervisor step after the worker exits)", req.NodeID, CompletionCommand)
	}

	// Declared machine contract checks (no side effects on failure).
	declared := make([]OutputValue, len(req.Outputs))
	for i, o := range req.Outputs {
		declared[i] = OutputValue{DefinitionID: o.DefinitionID}
	}
	if err := validateDeclaredOutputs(node, declared); err != nil {
		return nil, err
	}
	if err := validateCompleteOutputValues(tmpl, node, req.Outputs); err != nil {
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
	now := m.now()
	evidence, staged, err := m.stageEvidenceRecords(wf, req.Artifacts, req.captures, act.ActiveLease.Owner, act.ID, req.AttemptID, now)
	if err != nil {
		return nil, fmt.Errorf("stage evidence: %w", discardStaged(m, wf, staged, err))
	}
	evIDs := make([]EvidenceID, 0, len(staged))
	for _, se := range staged {
		evIDs = append(evIDs, se.EvidenceID)
	}

	// Bounded-retry apply (mirrors RecordAttemptTerminated): a concurrent
	// command can bump the revision in the window between the read and the
	// apply; re-read under a fresh lock and retry.
	const maxCompleteAttempts = 4
	for attempt := 0; attempt < maxCompleteAttempts; attempt++ {
		fresh, exists, err := m.store.loadCurrent(wf)
		if err != nil {
			return nil, discardStaged(m, wf, staged, err)
		}
		if !exists {
			return nil, discardStaged(m, wf, staged, fmt.Errorf("workflow not found: %s", wf))
		}
		// When the completion is gated the branch payload is suppressed: the
		// activation is parked awaiting_gate and NO branch may be followed
		// until the gates resolve (a sibling EventTransition would otherwise
		// create the target activation prematurely).
		gated := len(unsatisfiedRequiredGates(&fresh.Instance, node.Gates, act)) > 0
		var branch json.RawMessage
		if !gated && target != "" {
			branch, err = json.Marshal(&Transition{Outcome: req.Outcome, TargetNodeID: target, ActivationID: act.ID})
			if err != nil {
				return nil, fmt.Errorf("encode transition: %w", discardStaged(m, wf, staged, err))
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
			// Already recorded for this attempt, so its evidence is already on the
			// instance: this call's freshly staged copies belong to no fact. There
			// is no error to carry a leftover, so it is logged.
			reportDiscardFailure(wf, cleanupStaged(m, wf, staged))
			return map[string]any{
				"idempotent":        true,
				"already_completed": true,
				"status":            string(m.currentActivationStatus(wf, req.NodeID, act.ID, act.Status)),
			}, nil
		}
		if !errors.Is(err, errRevisionMismatch) {
			// A durable command's evidence is referenced by the event log; a replay
			// would recover the reference, so it must stay.
			if !errors.Is(err, ErrCommandDurable) {
				err = discardStaged(m, wf, staged, err)
			}
			return nil, err
		}
		// Optimistic-concurrency conflict; re-read under a fresh lock and retry.
	}
	return nil, discardStaged(m, wf, staged, fmt.Errorf("complete attempt %s: revision kept moving under concurrent commands", req.AttemptID))
}

// validateCompleteOutputValues hardens the primitive values of a completion
// against the node's declared output types: strings must be JSON strings,
// booleans strict true/false, and numbers finite. Numbers referenced as a
// pull_request subject or as a number-typed adapter input anywhere in the
// template must additionally be integral within the safe integer range, so a
// gate binding can never pin a value no consumer can read. Null and empty
// values are rejected for every declared output — even json and file, whose
// nulls would otherwise be legitimate — because the canonical snapshot wire
// format rejects null for any struct field, including the raw-JSON output
// value field, so an explicit null would fail at snapshot write after the
// completion had landed.
// Any failure rejects the whole completion before evidence staging, so no
// output and no completion is recorded.
func validateCompleteOutputValues(tmpl *Template, node *NodeDefinition, outputs []CompleteOutput) error {
	integral := integralOutputRefs(tmpl)
	declared := make(map[OutputID]OutputDefinition, len(node.Outputs))
	for _, def := range node.Outputs {
		declared[def.ID] = def
	}
	for _, o := range outputs {
		def, ok := declared[o.DefinitionID]
		if !ok {
			continue
		}
		trimmed := trimmedJSON(o.Value)
		if len(trimmed) == 0 || string(trimmed) == "null" {
			// A null output value is never persistable, even for json and file
			// outputs: the canonical snapshot wire format rejects null for any
			// struct field (requireCanonicalKeys), including the raw-JSON output
			// value field, so an explicit null would fail at snapshot write
			// after the completion had landed.
			return fmt.Errorf("workflow output %q on node %q cannot be null", o.DefinitionID, node.ID)
		}
		what := fmt.Sprintf("workflow output %q on node %q", o.DefinitionID, node.ID)
		switch def.Type {
		case OutputString:
			if trimmed[0] != '"' {
				return fmt.Errorf("%s requires a string value", what)
			}
		case OutputBoolean:
			if string(trimmed) != "true" && string(trimmed) != "false" {
				return fmt.Errorf("%s requires a boolean value", what)
			}
		case OutputNumber:
			if err := validateSafeJSONNumber(trimmed, integral[node.ID][o.DefinitionID], what); err != nil {
				return err
			}
		}
	}
	return nil
}

// integralOutputRefs collects the outputs the template's gate bindings
// consume as exact integers: every subject_binding pull_request reference
// and every adapter input reference whose declared output type is number.
// The map is per source node because bindings resolve against a specific
// node's outputs.
func integralOutputRefs(tmpl *Template) map[NodeID]map[OutputID]bool {
	outputTypes := make(map[NodeID]map[OutputID]OutputType, len(tmpl.Nodes))
	for _, n := range tmpl.Nodes {
		types := make(map[OutputID]OutputType, len(n.Outputs))
		for _, def := range n.Outputs {
			types[def.ID] = def.Type
		}
		outputTypes[n.ID] = types
	}
	refs := make(map[NodeID]map[OutputID]bool)
	add := func(nodeID NodeID, outputID OutputID) {
		if refs[nodeID] == nil {
			refs[nodeID] = make(map[OutputID]bool)
		}
		refs[nodeID][outputID] = true
	}
	for _, n := range tmpl.Nodes {
		for _, gate := range n.Gates {
			if binding := gate.SubjectBinding; binding != nil && binding.PullRequest != nil {
				add(binding.PullRequest.FromNodeOutput.NodeID, binding.PullRequest.FromNodeOutput.OutputID)
			}
			for _, value := range gate.Inputs {
				if value.FromNodeOutput == nil {
					continue
				}
				if outputTypes[value.FromNodeOutput.NodeID][value.FromNodeOutput.OutputID] == OutputNumber {
					add(value.FromNodeOutput.NodeID, value.FromNodeOutput.OutputID)
				}
			}
		}
	}
	return refs
}

// activationHasAttempt reports whether the activation's append-only attempt
// list contains the given attempt ID. The list is never cleared, so a
// legitimate duplicate re-send of the same attempt still passes.
func activationHasAttempt(act *Activation, attemptID AttemptID) bool {
	for _, id := range act.AttemptIDs {
		if id == attemptID {
			return true
		}
	}
	return false
}

// currentActivationStatus reads the activation's status via a fresh
// loadCurrent, falling back to the supplied stale status if the read fails or
// the activation is gone.
func (m *Manager) currentActivationStatus(wf WorkflowID, nodeID NodeID, actID ActivationID, stale ActivationStatus) ActivationStatus {
	fresh, ok, err := m.store.loadCurrent(wf)
	if err != nil || !ok {
		return stale
	}
	fa, err := findActivation(&fresh.Instance, nodeID, actID)
	if err != nil || fa == nil {
		return stale
	}
	return fa.Status
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

// cleanupStaged removes the evidence directories staged during a failed call so
// the store and filesystem do not diverge. A leftover directory is inert (no
// committed evidence record references it), but it still holds the captured
// content, so each removal that fails is reported with its evidence ID.
func cleanupStaged(m *Manager, wf WorkflowID, staged []StagedEvidence) error {
	var failures []error
	for _, se := range staged {
		if err := m.store.DiscardEvidence(wf, se.EvidenceID); err != nil {
			failures = append(failures, fmt.Errorf("evidence: discard orphaned evidence %s: %w", se.EvidenceID, err))
		}
	}
	return errors.Join(failures...)
}

// discardStaged removes the copies a failed call staged and reports any it could
// not remove alongside the failure that stopped the call, so the original error
// keeps its identity and the orphan keeps its name. It is only for callers that
// already return a failure: a successful or durable outcome keeps its copies, so
// a leftover there is logged by reportDiscardFailure instead.
func discardStaged(m *Manager, wf WorkflowID, staged []StagedEvidence, cause error) error {
	if cleanupErr := cleanupStaged(m, wf, staged); cleanupErr != nil {
		return errors.Join(cause, cleanupErr)
	}
	return cause
}

// reportDiscardFailure logs a copy a successful or durable outcome could not
// discard. There is no error channel for it, and turning the outcome into a
// failure would reclassify a command whose events are already recorded.
func reportDiscardFailure(wf WorkflowID, err error) {
	if err != nil {
		log.Printf("workflow %s: %v", wf, err)
	}
}

// CommandRecorded reports whether a workflow command's answer recorded a new
// fact, rather than answering an idempotent replay. Callers that staged evidence
// for the command use it to decide whether their copy belongs to a fact: an
// idempotent answer means the fact was already recorded, so this call's copy is
// the caller's to discard. It is the one place that reads the manager's
// "idempotent" marker, so a new idempotent answer cannot be misclassified at a
// distant call site.
func CommandRecorded(answer any) bool {
	result, ok := answer.(map[string]any)
	if !ok {
		return true
	}
	idempotent, _ := result["idempotent"].(bool)
	return !idempotent
}

// maxEvidenceResultBytes bounds the inert provenance one evidence entry may
// carry, so no single entry can grow a snapshot through it.
const maxEvidenceResultBytes = 8 << 10

// stageEvidenceRecords stages the evidence for one command and returns the
// records to record with it. It stages the file-sourced artifacts a request
// names first, then the supervisor's own captures. A staging failure returns the
// copies that landed so the caller can discard them.
func (m *Manager) stageEvidenceRecords(wf WorkflowID, artifacts []CompleteArtifact, captures []CommandCapture, authority string, activationID ActivationID, attemptID AttemptID, now time.Time) ([]Evidence, []StagedEvidence, error) {
	records := make([]Evidence, 0, len(artifacts)+len(captures))
	staged := make([]StagedEvidence, 0, len(artifacts)+len(captures))
	stagedCopy := func(se StagedEvidence, result json.RawMessage) {
		staged = append(staged, se)
		records = append(records, Evidence{
			ID:           se.EvidenceID,
			Kind:         "artifact",
			Source:       EvidenceMachine,
			Authority:    authority,
			OriginalPath: se.OriginalPath,
			StoredPath:   se.StoredPath,
			Size:         se.Size,
			SHA256:       se.SHA256,
			Result:       result,
			ActivationID: activationID,
			AttemptID:    attemptID,
			CreatedAt:    now,
		})
	}
	for _, artifact := range artifacts {
		se, err := m.store.StageEvidence(wf, artifact.SrcPath, artifact.StoredPath, artifact.NonEmpty, artifact.SHA256)
		if err != nil {
			return nil, staged, err
		}
		stagedCopy(se, nil)
	}
	for _, capture := range captures {
		if len(capture.Result) > maxEvidenceResultBytes {
			return nil, staged, fmt.Errorf("evidence %q result exceeds %d bytes", capture.StoredPath, maxEvidenceResultBytes)
		}
		se, err := m.store.stageEvidenceBytes(wf, capture.Data, capture.StoredPath, false, "")
		if err != nil {
			return nil, staged, err
		}
		stagedCopy(se, capture.Result)
	}
	return records, staged, nil
}
