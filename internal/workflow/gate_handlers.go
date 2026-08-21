package workflow

// gate_handlers.go implements the manager's gate control surface
// (Stage 12, phase 2: human and external gates). It owns the command-boundary
// validation for CommandGate (the closed operation enum, the external result
// enum, and per-operation required fields) plus the optional transition
// payload that resolves a parked awaiting_gate activation. The atomic apply
// mirrors commandComplete: bounded retry on revision mismatch, a stable
// idempotency key, and a result map. The reducer stays authoritative:
// validateGateCommand re-checks field/status consistency and applyGate
// settles the activation.

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// gateIdempotencyKey returns the stable idempotency key for one gate
// decision. Human operations are fully discriminated by the operation; an
// external_result is additionally discriminated by its observed result so a
// later report on the same gate (e.g. pending -> passed) is a distinct
// command rather than a swallowed duplicate.
func gateIdempotencyKey(op GateOperation, gateID GateID, actID ActivationID, result string) string {
	key := "gate-" + string(op) + "-" + string(gateID) + "-" + string(actID)
	if op == GateOpExternalResult {
		key += "-" + result
	}
	return key
}

// commandGateRequest is the wire shape of a gate decision command. Fields are
// required per operation; the operation determines the required set (the
// reducer re-validates the durable gate instance itself).
type commandGateRequest struct {
	NodeID       NodeID       `json:"node_id"`
	ActivationID ActivationID `json:"activation_id"`
	GateID       GateID       `json:"gate_id"`
	Operation    string       `json:"operation"`
	Actor        string       `json:"actor,omitempty"`
	Reason       string       `json:"reason,omitempty"`
	Outcome      OutcomeName  `json:"outcome,omitempty"`
	Subject      *Subject     `json:"subject,omitempty"`
	PollID       string       `json:"poll_id,omitempty"`
	Source       string       `json:"source,omitempty"`
	Result       string       `json:"result,omitempty"`
	ResponseHash string       `json:"response_hash,omitempty"`
	ObservedAt   *time.Time   `json:"observed_at,omitempty"`
	EvidenceIDs  []EvidenceID `json:"evidence_ids,omitempty"`
}

type commandSkipRequest struct {
	NodeID       NodeID       `json:"node_id"`
	ActivationID ActivationID `json:"activation_id"`
	Actor        string       `json:"actor,omitempty"`
	Reason       string       `json:"reason,omitempty"`
	EvidenceIDs  []EvidenceID `json:"evidence_ids,omitempty"`
}

type commandUnblockRequest struct {
	NodeID       NodeID       `json:"node_id"`
	ActivationID ActivationID `json:"activation_id"`
	Actor        string       `json:"actor,omitempty"`
	Reason       string       `json:"reason,omitempty"`
}

// commandGate records one gate decision (satisfy, reject, waive, or
// external_result) on a parked awaiting_gate activation. The closed operation
// enum is validated before any store load or mutation; unknown values fail
// here (the CLI validates the same set earlier for fail-fast). The reducer
// re-validates the recorded gate instance via validateGateCommand.
func (m *Manager) commandGate(wf WorkflowID, payload json.RawMessage) (any, error) {
	var req commandGateRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("gate payload: %w", err)
	}
	if req.NodeID == "" {
		return nil, errors.New("gate requires node_id")
	}
	if req.ActivationID == "" {
		return nil, errors.New("gate requires activation_id")
	}
	if req.GateID == "" {
		return nil, errors.New("gate requires gate_id")
	}
	if req.Operation == "" {
		return nil, errors.New("gate requires operation")
	}
	op := GateOperation(req.Operation)
	if !validGateOperation(op) {
		return nil, fmt.Errorf("unknown gate operation %q", req.Operation)
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
	// Idempotent duplicate: a re-issued decision for the same gate operation
	// on the same activation is a safe no-op. Checked before the status
	// check because a successful decision has already resolved the
	// activation (satisfied or rejected), and the fresh status is reported
	// via a fresh read.
	if _, done := snap.Idempotency[gateIdempotencyKey(op, req.GateID, act.ID, req.Result)]; done {
		postStatus := m.currentActivationStatus(wf, req.NodeID, act.ID, act.Status)
		return map[string]any{
			"idempotent":        true,
			"status":            gateResultStatus(postStatus),
			"activation_status": string(postStatus),
		}, nil
	}
	if act.Status != ActivationAwaitingGate {
		return nil, fmt.Errorf("cannot gate activation in status %q", act.Status)
	}
	tmpl, err := m.templateFor(&snap)
	if err != nil {
		return nil, err
	}
	node, err := findNode(tmpl, req.NodeID)
	if err != nil {
		return nil, err
	}
	var def *GateDefinition
	for i := range node.Gates {
		if node.Gates[i].ID == req.GateID {
			def = &node.Gates[i]
			break
		}
	}
	if def == nil {
		return nil, fmt.Errorf("gate %q is not declared on node %q", req.GateID, req.NodeID)
	}

	// Map the operation to the durable gate status and run the operation's
	// friendly, pre-mutation field checks (the reducer re-checks the
	// resulting instance, but failing early keeps the error message clear).
	var status GateStatus
	switch op {
	case GateOpSatisfy:
		status = GatePassed
	case GateOpReject:
		status = GateRejected
	case GateOpWaive:
		status = GateWaived
	case GateOpExternalResult:
		// The raw result enum is validated and mapped BEFORE any mutation.
		status, err = externalResultStatus(req.Result)
		if err != nil {
			return nil, err
		}
		if req.PollID == "" {
			return nil, errors.New("external_result requires poll_id")
		}
		if req.Source == "" {
			return nil, errors.New("external_result requires source")
		}
		if req.ObservedAt == nil || req.ObservedAt.IsZero() {
			return nil, errors.New("external_result requires a non-zero observed_at")
		}
		if req.Subject == nil || req.Subject.Type == "" {
			return nil, errors.New("external_result requires a subject")
		}
		if req.ResponseHash == "" {
			return nil, errors.New("external_result requires response_hash")
		}
		if len(req.EvidenceIDs) == 0 {
			return nil, errors.New("external_result requires at least one evidence id")
		}
	}
	if op == GateOpSatisfy || op == GateOpReject || op == GateOpWaive {
		if req.Actor == "" {
			return nil, fmt.Errorf("gate operation %q requires an actor", op)
		}
		if req.Reason == "" {
			return nil, fmt.Errorf("gate operation %q requires a reason", op)
		}
		if len(req.EvidenceIDs) == 0 {
			return nil, fmt.Errorf("gate operation %q requires at least one evidence id", op)
		}
		if def.SubjectType != "" && (req.Subject == nil || req.Subject.Type == "") {
			return nil, fmt.Errorf("gate %q requires a subject (node declares subject_type %q)", req.GateID, def.SubjectType)
		}
	}
	return m.applyGateDecision(wf, snap, tmpl, node, act, req.GateID, op, status, req)
}

// externalResultStatus maps the closed external result enum to the durable
// gate status, rejecting any value outside it.
func externalResultStatus(result string) (GateStatus, error) {
	switch result {
	case "pending":
		return GatePending, nil
	case "passed":
		return GatePassed, nil
	case "failed":
		return GateFailed, nil
	case "action_required":
		return GateActionRequired, nil
	case "changes_requested":
		return GateChangesRequested, nil
	}
	return "", fmt.Errorf("unknown external result %q", result)
}

// commandSkip waives every still-unsatisfied REQUIRED gate on a parked
// awaiting_gate activation by delegating to the shared gate-decision apply,
// so the final waive resolves the node's branch exactly as commandGate does.
func (m *Manager) commandSkip(wf WorkflowID, payload json.RawMessage) (any, error) {
	var req commandSkipRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("skip payload: %w", err)
	}
	if req.NodeID == "" {
		return nil, errors.New("skip requires node_id")
	}
	if req.Actor == "" {
		return nil, errors.New("skip requires actor")
	}
	if req.Reason == "" {
		return nil, errors.New("skip requires reason")
	}
	if len(req.EvidenceIDs) == 0 {
		return nil, errors.New("skip requires at least one evidence id")
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
	if act.Status != ActivationAwaitingGate {
		return nil, fmt.Errorf("cannot skip activation in status %q", act.Status)
	}
	tmpl, err := m.templateFor(&snap)
	if err != nil {
		return nil, err
	}
	node, err := findNode(tmpl, req.NodeID)
	if err != nil {
		return nil, err
	}
	missing := remainingRequiredGates(&snap.Instance, node, act.ID, "", "")
	if len(missing) == 0 {
		return nil, fmt.Errorf("node has no unsatisfied required gate to skip")
	}

	var waived []string
	var last map[string]any
	for i := range missing {
		def := missing[i]
		gateID := def.ID
		// Re-read per gate: each waive advances the revision, and the
		// final waive computes the resolving transition from the fresh
		// instance state.
		fresh, exists, err := m.store.loadCurrent(wf)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, fmt.Errorf("workflow not found: %s", wf)
		}
		fa, err := findActivation(&fresh.Instance, req.NodeID, act.ID)
		if err != nil {
			return nil, err
		}
		if fa == nil {
			return nil, fmt.Errorf("activation not found for node %q", req.NodeID)
		}
		gateReq := commandGateRequest{
			NodeID:       req.NodeID,
			ActivationID: act.ID,
			GateID:       gateID,
			Operation:    string(GateOpWaive),
			Actor:        req.Actor,
			Reason:       req.Reason,
			EvidenceIDs:  req.EvidenceIDs,
		}
		last, err = m.applyGateDecision(wf, fresh, tmpl, node, fa, gateID, GateOpWaive, GateWaived, gateReq)
		if err != nil {
			return nil, err
		}
		waived = append(waived, string(gateID))
	}
	return map[string]any{
		"skipped":           true,
		"activation_status": last["activation_status"],
		"waived_gates":      waived,
		"revision":          last["revision"],
	}, nil
}

// commandUnblock returns a blocked activation to ready after an authorized
// unblock.
func (m *Manager) commandUnblock(wf WorkflowID, payload json.RawMessage) (any, error) {
	var req commandUnblockRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("unblock payload: %w", err)
	}
	if req.NodeID == "" {
		return nil, errors.New("unblock requires node_id")
	}
	if req.Actor == "" {
		return nil, errors.New("unblock requires actor")
	}
	if req.Reason == "" {
		return nil, errors.New("unblock requires reason")
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
	// Idempotent duplicate: a re-issued unblock for the same activation is a
	// safe no-op (the activation may already be back to ready).
	// Scope the idempotency key to the block episode by the activation's
	// append-only attempt count: each block requires a new started attempt,
	// so the count strictly increases per episode and a later re-block gets a
	// distinct key rather than hitting the permanent key as a false no-op.
	episodeKey := "unblock-" + string(act.ID) + "-" + strconv.Itoa(len(act.AttemptIDs))
	if _, done := snap.Idempotency[episodeKey]; done {
		return map[string]any{
			"idempotent":        true,
			"status":            "unblocked",
			"activation_status": string(m.currentActivationStatus(wf, req.NodeID, act.ID, act.Status)),
		}, nil
	}
	if act.Status != ActivationBlocked {
		return nil, fmt.Errorf("cannot unblock activation in status %q", act.Status)
	}

	const maxUnblockAttempts = 4
	for attempt := 0; attempt < maxUnblockAttempts; attempt++ {
		fresh, exists, err := m.store.loadCurrent(wf)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, fmt.Errorf("workflow not found: %s", wf)
		}
		result, err := m.store.ApplyCommand(wf, Command{
			ID:               NewCommandID(),
			Kind:             CommandUnblock,
			ExpectedRevision: fresh.Instance.Revision,
			IdempotencyKey:   episodeKey,
			Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: req.NodeID, ActivationID: act.ID},
			Actor:            req.Actor,
			Reason:           req.Reason,
		})
		if err == nil {
			return map[string]any{
				"status":            "unblocked",
				"activation_status": "ready",
				"revision":          result.Instance.Revision,
			}, nil
		}
		if errors.Is(err, errDuplicateIdempotency) {
			return map[string]any{
				"idempotent":        true,
				"status":            "unblocked",
				"activation_status": string(m.currentActivationStatus(wf, req.NodeID, act.ID, act.Status)),
			}, nil
		}
		if !errors.Is(err, errRevisionMismatch) {
			return nil, err
		}
		// Optimistic-concurrency conflict; re-read under a fresh lock and retry.
	}
	return nil, fmt.Errorf("unblock activation %s: revision kept moving under concurrent commands", act.ID)
}

// applyGateDecision builds one gate-decision command (the durable gate
// instance plus the optional transition payload) and applies it atomically
// with bounded retry. It is the shared core of commandGate (single
// decision) and commandSkip (a waive per unsatisfied required gate).
func (m *Manager) applyGateDecision(wf WorkflowID, snap Snapshot, tmpl *Template, node *NodeDefinition, act *Activation, gateID GateID, op GateOperation, status GateStatus, req commandGateRequest) (map[string]any, error) {
	now := time.Now().UTC()
	gateInstance := &GateInstance{
		ID:           NewGateInstanceID(),
		GateID:       gateID,
		ActivationID: act.ID,
		Status:       status,
		Actor:        req.Actor,
		Reason:       req.Reason,
		Outcome:      req.Outcome,
		Subject:      req.Subject,
		PollID:       req.PollID,
		Source:       req.Source,
		ResponseHash: req.ResponseHash,
		EvidenceIDs:  append([]EvidenceID(nil), req.EvidenceIDs...),
		DecidedAt:    &now,
	}
	if req.ObservedAt != nil {
		v := *req.ObservedAt
		gateInstance.ObservedAt = &v
	}

	// Bounded-retry apply (mirrors commandComplete): a concurrent command
	// can bump the revision in the window between the read and the apply;
	// re-read under a fresh lock and retry.
	const maxGateAttempts = 4
	for attempt := 0; attempt < maxGateAttempts; attempt++ {
		fresh, exists, err := m.store.loadCurrent(wf)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, fmt.Errorf("workflow not found: %s", wf)
		}
		fa, err := findActivation(&fresh.Instance, node.ID, act.ID)
		if err != nil {
			return nil, err
		}
		if fa == nil {
			return nil, fmt.Errorf("activation not found for node %q", node.ID)
		}
		if fa.Status != ActivationAwaitingGate {
			// A concurrent decision already resolved this activation; this is
			// a benign no-op (do not re-apply a payload computed from stale
			// state, which would fire a phantom sibling transition).
			return map[string]any{
				"status":            gateResultStatus(fa.Status),
				"activation_status": string(fa.Status),
				"idempotent":        true,
				"gate_instance_id":  string(gateInstance.ID),
				"gate_status":       string(status),
			}, nil
		}
		payload, err := m.gateTransitionPayload(&fresh.Instance, tmpl, node, fa, gateID, op, status, req)
		if err != nil {
			return nil, err
		}
		result, err := m.store.ApplyCommand(wf, Command{
			ID:               NewCommandID(),
			Kind:             CommandGate,
			ExpectedRevision: fresh.Instance.Revision,
			IdempotencyKey:   gateIdempotencyKey(op, gateID, act.ID, req.Result),
			Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: node.ID, ActivationID: act.ID},
			Operation:        op,
			Outcome:          req.Outcome,
			Gate:             gateInstance,
			Payload:          payload,
		})
		if err == nil {
			postStatus := gatePostStatus(&result.Instance, act.ID)
			return map[string]any{
				"status":            gateResultStatus(postStatus),
				"activation_status": string(postStatus),
				"gate_instance_id":  string(gateInstance.ID),
				"gate_status":       string(gateInstance.Status),
				"revision":          result.Instance.Revision,
			}, nil
		}
		if errors.Is(err, errDuplicateIdempotency) {
			postStatus := m.currentActivationStatus(wf, node.ID, act.ID, act.Status)
			return map[string]any{
				"idempotent":        true,
				"status":            gateResultStatus(postStatus),
				"activation_status": string(postStatus),
			}, nil
		}
		if !errors.Is(err, errRevisionMismatch) {
			return nil, err
		}
		// Optimistic-concurrency conflict; re-read under a fresh lock and retry.
	}
	return nil, fmt.Errorf("gate %q on activation %s: revision kept moving under concurrent commands", gateID, act.ID)
}

// gateTransitionPayload computes the optional sibling-transition payload for
// a gate decision from the CURRENT instance state. It returns a marshaled
// Transition for exactly two cases: a pass/waive that leaves every required
// gate resolved (following the declared branch target, or the terminal
// resolution when there is none), and a reject/fail whose explicit outcome
// resolves to a declared failure/correction/checkpoint target. Everything
// else returns nil (no payload): pending/action_required/changes_requested,
// a pass with remaining required gates, or a reject/fail without a declared
// branch.
func (m *Manager) gateTransitionPayload(inst *WorkflowInstance, tmpl *Template, node *NodeDefinition, act *Activation, gateID GateID, op GateOperation, status GateStatus, req commandGateRequest) (json.RawMessage, error) {
	now := time.Now().UTC()
	switch {
	case (status == GatePassed || status == GateWaived) && len(remainingRequiredGates(inst, node, act.ID, gateID, status)) == 0:
		outcome := req.Outcome
		if outcome == "" {
			outcome = act.SelectedOutcome
		}
		if outcome == "" {
			return nil, fmt.Errorf("gate %q resolution requires an outcome", gateID)
		}
		if err := validateDeclaredOutcome(tmpl, node, outcome); err != nil {
			return nil, err
		}
		target, _, _ := resolveOutcome(tmpl, node, outcome)
		return json.Marshal(&Transition{ActivationID: act.ID, Outcome: outcome, TargetNodeID: target, CreatedAt: now})
	case (status == GateRejected || status == GateFailed) && req.Outcome != "":
		if target, _, declared := resolveOutcome(tmpl, node, req.Outcome); declared && target != "" {
			return json.Marshal(&Transition{ActivationID: act.ID, Outcome: req.Outcome, TargetNodeID: target, CreatedAt: now})
		}
	}
	return nil, nil
}

// remainingRequiredGates returns the required gate definitions of node that
// have no passed/waived instance for actID, counting resolvedGate (when its
// status is a pass/waive) as already satisfied. The snapshot may predate the
// recorded decision, so the in-flight decision must be folded in.
func remainingRequiredGates(inst *WorkflowInstance, node *NodeDefinition, actID ActivationID, resolvedGate GateID, resolvedStatus GateStatus) []GateDefinition {
	satisfied := make(map[GateID]bool)
	for _, gi := range inst.Gates {
		if gi.ActivationID != actID {
			continue
		}
		if gi.Status == GatePassed || gi.Status == GateWaived {
			satisfied[gi.GateID] = true
		}
	}
	if resolvedGate != "" && (resolvedStatus == GatePassed || resolvedStatus == GateWaived) {
		satisfied[resolvedGate] = true
	}
	var remaining []GateDefinition
	for _, def := range node.Gates {
		if !def.Required || satisfied[def.ID] {
			continue
		}
		remaining = append(remaining, def)
	}
	return remaining
}

// gatePostStatus finds the post-apply status of one activation in an
// instance, falling back to the last known status when it is gone.
func gatePostStatus(inst *WorkflowInstance, actID ActivationID) ActivationStatus {
	for i := range inst.Activations {
		if inst.Activations[i].ID == actID {
			return inst.Activations[i].Status
		}
	}
	return ActivationSatisfied
}

// gateResultStatus maps the post-apply activation status to the result's
// compact "status" field.
func gateResultStatus(status ActivationStatus) string {
	switch status {
	case ActivationSatisfied:
		return "completed"
	case ActivationRejected:
		return "rejected"
	default:
		return "gated"
	}
}
