package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// buildCommandEvents projects a command into its event batch. Every emitted
// event is stamped with a fresh ID, the command's identity/idempotency/ID, and
// a workflow-local sequence that continues from the snapshot revision. Most
// commands emit a single event; CommandComplete may emit EventCompleted plus
// an EventTransition that advances the run to the declared branch target.
func buildCommandEvents(state Snapshot, command Command) ([]Event, error) {
	seq := state.Instance.Revision
	newEvent := func(kind EventKind) Event {
		seq++
		return Event{
			ID:             NewEventID(),
			Kind:           kind,
			Sequence:       seq,
			CommandID:      command.ID,
			IdempotencyKey: command.IdempotencyKey,
			Identity:       command.Identity,
		}
	}

	switch command.Kind {
	case CommandInstantiate:
		rec := &InstanceRecord{}
		if len(command.Payload) == 0 {
			return nil, errors.New("instantiate command requires an instance record payload")
		}
		if err := json.Unmarshal(command.Payload, rec); err != nil {
			return nil, fmt.Errorf("instantiate command: %w", err)
		}
		e := newEvent(EventInstantiated)
		e.Instantiated = rec
		return []Event{e}, nil

	case CommandClaim:
		e := newEvent(EventLeased)
		e.LeaseID = command.LeaseID
		e.Actor = command.Actor
		e.Lease = command.Lease
		return []Event{e}, nil

	case CommandStart:
		e := newEvent(EventStarted)
		e.AttemptID = command.Identity.AttemptID
		e.LeaseID = command.LeaseID
		e.Selection = command.Selection
		return []Event{e}, nil

	case CommandComplete:
		e := newEvent(EventCompleted)
		e.Outcome = command.Outcome
		e.Evidence = command.Evidence
		e.Outputs = command.Outputs
		e.LeaseID = command.LeaseID
		if len(command.Payload) == 0 {
			// No declared branch: the outcome terminates the workflow.
			return []Event{e}, nil
		}
		t := &Transition{}
		if err := json.Unmarshal(command.Payload, t); err != nil {
			return nil, fmt.Errorf("complete command: %w", err)
		}
		if t.Outcome == "" {
			t.Outcome = command.Outcome
		}
		e.Transition = t
		if t.TargetNodeID == "" {
			// Declared branch resolved to no target: terminal outcome.
			return []Event{e}, nil
		}
		next := newEvent(EventTransition)
		next.Transition = t
		next.Outcome = command.Outcome
		return []Event{e, next}, nil

	case CommandGate:
		if command.Gate == nil {
			return nil, errors.New("gate command requires a gate instance")
		}
		e := newEvent(EventGate)
		e.Gate = command.Gate
		return []Event{e}, nil

	case CommandSkip:
		e := newEvent(EventSkipped)
		e.Actor = command.Actor
		e.Reason = command.Reason
		return []Event{e}, nil

	case CommandUnblock:
		e := newEvent(EventUnblocked)
		e.Actor = command.Actor
		e.Reason = command.Reason
		return []Event{e}, nil

	case CommandReroute:
		e := newEvent(EventRerouted)
		if len(command.Payload) > 0 {
			var p struct {
				Selection *ExecutionSelection `json:"selection,omitempty"`
				Iteration int                 `json:"iteration,omitempty"`
			}
			if err := json.Unmarshal(command.Payload, &p); err != nil {
				return nil, fmt.Errorf("reroute command: %w", err)
			}
			e.Selection = p.Selection
			e.Iteration = p.Iteration
		}
		return []Event{e}, nil

	case CommandHeartbeat:
		e := newEvent(EventHeartbeat)
		e.LeaseID = command.LeaseID
		return []Event{e}, nil

	case CommandTerminate:
		e := newEvent(EventAttemptTerminated)
		e.AttemptStatus = command.AttemptStatus
		e.LeaseID = command.LeaseID
		e.AttemptID = command.Identity.AttemptID
		e.MarkerKind = command.MarkerKind
		e.MarkerLabel = command.MarkerLabel
		return []Event{e}, nil

	case CommandChildAttach:
		e := newEvent(EventChildAttached)
		e.LeaseID = command.LeaseID
		return []Event{e}, nil

	case CommandChildOutcome:
		e := newEvent(EventChildOutcome)
		e.Outcome = command.Outcome
		e.ChildOutputs = command.ChildOutputs
		e.LeaseID = command.LeaseID
		if len(command.Payload) == 0 {
			// No declared branch: the mapped outcome terminates the workflow.
			return []Event{e}, nil
		}
		t := &Transition{}
		if err := json.Unmarshal(command.Payload, t); err != nil {
			return nil, fmt.Errorf("child_outcome command: %w", err)
		}
		if t.Outcome == "" {
			t.Outcome = command.Outcome
		}
		e.Transition = t
		if t.TargetNodeID == "" {
			// Declared branch resolved to no target: terminal outcome.
			return []Event{e}, nil
		}
		next := newEvent(EventTransition)
		next.Transition = t
		next.Outcome = command.Outcome
		return []Event{e, next}, nil

	default:
		return nil, fmt.Errorf("unsupported command kind %q", command.Kind)
	}
}

// applyEvent mutates a snapshot with the durable effect of one event. It is
// the replay-side of the reducer: given the same event log it always yields
// the same state. All events except EventInstantiated target one activation,
// so authority is enforced first: the event's workflow must match, and its
// (node, activation) identity must resolve to a live activation.
func applyEvent(next *Snapshot, event Event) error {
	if next.Instance.WorkflowID != "" && event.Identity.WorkflowID != "" &&
		event.Identity.WorkflowID != next.Instance.WorkflowID {
		return fmt.Errorf("workflow identity mismatch: event targets %q, instance is %q",
			event.Identity.WorkflowID, next.Instance.WorkflowID)
	}

	// EventInstantiated and EventRerouted create activations and therefore do
	// not require an existing one; every other kind must resolve one.
	var act *Activation
	if event.Kind != EventInstantiated && event.Kind != EventRerouted {
		a, err := findActivation(&next.Instance, event.Identity.NodeID, event.Identity.ActivationID)
		if err != nil {
			return err
		}
		if a == nil {
			return fmt.Errorf("activation not found for node %q activation %q",
				event.Identity.NodeID, event.Identity.ActivationID)
		}
		act = a
	}

	switch event.Kind {
	case EventInstantiated:
		if err := applyInstantiated(next, event); err != nil {
			return err
		}
	case EventLeased:
		return applyLeased(next, act, event)
	case EventStarted:
		return applyStarted(next, act, event)
	case EventAttemptTerminated:
		return applyAttemptTerminated(next, act, event)
	case EventCompleted:
		return applyCompleted(next, act, event)
	case EventGate:
		return applyGate(next, act, event)
	case EventSkipped:
		return applySkipped(act, event)
	case EventUnblocked:
		return applyUnblocked(act, event)
	case EventRerouted:
		applyRerouted(next, event)
	case EventHeartbeat:
		return applyHeartbeat(act, event)
	case EventLeaseExpired:
		return applyLeaseExpired(act, event)
	case EventChildAttached:
		return applyChildAttached(next, act, event)
	case EventChildOutcome:
		return applyChildOutcome(next, act, event)
	case EventTransition:
		if event.Transition == nil {
			return errors.New("transition event requires a transition")
		}
		if event.Transition.TargetNodeID == "" {
			return errors.New("transition event requires a target node")
		}
		now := nowUTC()
		next.Instance.Activations = append(next.Instance.Activations, Activation{
			ID:              NewActivationID(),
			NodeID:          event.Transition.TargetNodeID,
			IncomingOutcome: event.Transition.Outcome,
			Status:          ActivationPending,
			CreatedAt:       now,
			UpdatedAt:       now,
		})
	default:
		return fmt.Errorf("unsupported event kind %q", event.Kind)
	}

	next.Instance.UpdatedAt = nowUTC()
	return nil
}

// findActivation resolves the live, mutable activation record for a visit to
// a node. When only the node is given and it has exactly one activation, that
// activation is returned; otherwise both node and activation ID must match.
func findActivation(inst *WorkflowInstance, nodeID NodeID, activationID ActivationID) (*Activation, error) {
	matches := make([]*Activation, 0, len(inst.Activations))
	for i := range inst.Activations {
		a := &inst.Activations[i]
		if a.NodeID != nodeID {
			continue
		}
		if activationID == "" || a.ID == activationID {
			matches = append(matches, a)
		}
	}
	if activationID != "" {
		if len(matches) == 0 {
			return nil, nil
		}
		return matches[0], nil
	}
	// activationID == ""
	if len(matches) == 1 {
		return matches[0], nil
	}
	if len(matches) == 0 {
		return nil, nil
	}
	return nil, fmt.Errorf("ambiguous activation for node %q: activation_id required", nodeID)
}

// applyInstantiated materializes the initial instance from the event's
// InstanceRecord: one pending activation per entry node. Workflow and instance
// IDs are taken from the identity/record when supplied and otherwise freshly
// generated.
func applyInstantiated(next *Snapshot, event Event) error {
	if next.Instance.WorkflowID != "" {
		return errors.New("instance already instantiated")
	}
	rec := event.Instantiated
	if rec == nil {
		return errors.New("instantiate event requires an instance record")
	}
	workflowID := next.Instance.WorkflowID
	if workflowID == "" {
		workflowID = event.Identity.WorkflowID
	}
	if workflowID == "" {
		workflowID = NewWorkflowID()
	}
	instanceID := next.Instance.InstanceID
	if instanceID == "" {
		instanceID = NewInstanceID()
	}
	now := nowUTC()
	next.Instance.WorkflowID = workflowID
	next.Instance.InstanceID = instanceID
	next.Instance.TemplateID = rec.TemplateID
	next.Instance.TemplateVersion = rec.TemplateVersion
	next.Instance.Revision = event.Sequence
	next.Instance.Status = WorkflowActive
	next.Instance.CreatedAt = now
	next.Instance.UpdatedAt = now
	// The composition manifest (durable child references) is carried on the
	// instance record so replay reconstructs it without re-derivation.
	next.Instance.Children = append(next.Instance.Children, rec.Children...)
	for _, nodeID := range rec.EntryNodes {
		next.Instance.Activations = append(next.Instance.Activations, Activation{
			ID:        NewActivationID(),
			NodeID:    nodeID,
			Status:    ActivationPending,
			CreatedAt: now,
			UpdatedAt: now,
		})
	}
	return nil
}

// applyLeased claims an activation: pending/ready advances to leased.
func applyLeased(next *Snapshot, act *Activation, event Event) error {
	if act == nil {
		return errors.New("leased event requires an activation")
	}
	if event.LeaseID == "" {
		return errors.New("leased event requires a lease id")
	}
	if act.Status != ActivationPending && act.Status != ActivationReady {
		return fmt.Errorf("cannot lease activation in status %q", act.Status)
	}
	if act.ActiveLease != nil && act.ActiveLease.ID != event.LeaseID {
		return errors.New("activation already carries a different lease")
	}
	act.Status = ActivationLeased
	act.ActiveLease = &Lease{ID: event.LeaseID, ActivationID: act.ID, Owner: event.Actor}
	if event.Lease != nil {
		// Deterministically carry the claim's expiry metadata so a lease
		// reconstructed from replay keeps the same real TTL. A bare
		// (legacy/crash-window) leased event with no lease metadata keeps a
		// zero expiry and is conservatively swept on recovery.
		act.ActiveLease.TokenDigest = event.Lease.TokenDigest
		act.ActiveLease.AcquiredAt = event.Lease.AcquiredAt
		act.ActiveLease.ExpiresAt = event.Lease.ExpiresAt
	}
	act.UpdatedAt = nowUTC()
	return nil
}

// applyStarted moves a leased activation to running and records its first (or
// next) attempt on the same activation, leaving the lease in place until the
// attempt terminates or completes.
func applyStarted(next *Snapshot, act *Activation, event Event) error {
	if act == nil {
		return errors.New("started event requires an activation")
	}
	if act.Status != ActivationLeased {
		return fmt.Errorf("cannot start activation in status %q", act.Status)
	}
	attemptID := event.AttemptID
	if attemptID == "" {
		attemptID = NewAttemptID()
	}
	attempt := Attempt{
		ID:       attemptID,
		Identity: event.Identity,
		Status:   AttemptStarting,
	}
	act.AttemptIDs = append(act.AttemptIDs, attemptID)
	act.Status = ActivationRunning
	act.UpdatedAt = nowUTC()
	if event.Selection != nil {
		// Pin the resolved ExecutionSelection on the start; retries/runtime
		// inherit it. Deterministic because it is carried on the event.
		act.Selection = event.Selection
	}
	next.Instance.Attempts = append(next.Instance.Attempts, attempt)
	return nil
}

// applyAttemptTerminated records the outcome of an attempt. It never satisfies
// the activation on its own: retry and retry-exhaustion are handled here, but
// only a follow-up completion (or a reroute) advances the workflow.
func applyAttemptTerminated(next *Snapshot, act *Activation, event Event) error {
	if act == nil {
		return errors.New("attempt_terminated event requires an activation")
	}
	status := event.AttemptStatus
	switch status {
	case AttemptSucceeded, AttemptFailed, AttemptCanceled, AttemptTimedOut, AttemptPanicked:
	default:
		return fmt.Errorf("attempt_terminated event has invalid status %q", status)
	}
	attempt := findAttempt(next, act, event.AttemptID)
	if attempt == nil {
		return errors.New("terminated attempt not found")
	}
	attempt.Status = status
	ended := nowUTC()
	attempt.EndedAt = &ended
	// The marker kind/label is inert evidence of the terminal directive the
	// backend observed (e.g. the loop exit marker). It is recorded on every
	// terminal path — including the early returns below — but must never
	// affect status, lease, retry, or exhaustion logic.
	attempt.MarkerKind = event.MarkerKind
	attempt.MarkerLabel = event.MarkerLabel
	// A successful termination is a recordable terminal fact. It never
	// satisfies the node on its own — acceptance requires explicit completion
	// (evidence + completed event, Stage 11) — so it must not regress the
	// activation status or release the worker's lease.
	if status == AttemptSucceeded {
		return nil
	}
	// Out-of-order termination: if the activation was already satisfied by an
	// earlier completion, or the workflow is already completed, the termination
	// is a distinct terminal attempt-fact and must not regress satisfaction or
	// the workflow outcome. Record the attempt fact and return, skipping lease
	// release and the retry/exhaustion logic entirely.
	if act.Status == ActivationSatisfied || next.Instance.Status == WorkflowCompleted {
		return nil
	}
	// A terminated attempt ends its worker's lease regardless of what follows
	// (retry re-arm, exhaustion, or a plain non-failed termination), so the
	// activation can be freshly claimed afterwards.
	act.Status = ActivationAttemptFailed
	act.ActiveLease = nil
	act.UpdatedAt = nowUTC()

	// Retry applies only to plain failures, only on the same activation, and
	// never consumes loop iteration budget. Exhaustion uses the node's retry
	// policy; without a template the reducer falls back to a single attempt
	// (any failure exhausts to ActivationBlocked).
	if status != AttemptFailed {
		return nil
	}
	policy := retryPolicyFor(next, act.NodeID)
	if policy != nil && len(act.AttemptIDs) < policy.MaximumAttempts {
		// Re-arm the same activation for a new attempt; the stale lease was
		// already released above so the new attempt is freshly claimed.
		act.Status = ActivationReady
		return nil
	}
	exhaustion := RetryExhaustionBlock
	if policy != nil {
		exhaustion = policy.Exhaustion
	}
	switch exhaustion {
	case RetryExhaustionFail:
		act.Status = ActivationAttemptFailed
		next.Instance.Status = WorkflowFailed
	case RetryExhaustionOutcome:
		// policy is non-nil here (nil policies default to block above).
		act.SelectedOutcome = policy.Outcome
		act.Status = ActivationAttemptFailed
	default: // RetryExhaustionBlock
		act.Status = ActivationBlocked
	}
	return nil
}

// findAttempt locates the attempt record for an activation, preferring the
// event's attempt ID and falling back to the activation's most recent attempt.
func findAttempt(next *Snapshot, act *Activation, attemptID AttemptID) *Attempt {
	for i := range next.Instance.Attempts {
		if attemptID != "" && next.Instance.Attempts[i].ID == attemptID {
			return &next.Instance.Attempts[i]
		}
	}
	if attemptID == "" && len(act.AttemptIDs) > 0 {
		last := act.AttemptIDs[len(act.AttemptIDs)-1]
		for i := range next.Instance.Attempts {
			if next.Instance.Attempts[i].ID == last {
				return &next.Instance.Attempts[i]
			}
		}
	}
	return nil
}

// applyCompleted marks an activation satisfied on a terminal or branching
// outcome. A declared branch is carried by a sibling EventTransition (and by
// event.Transition on this event); without one the outcome terminates the
// workflow. Completion is accepted even if no attempt_terminated was applied
// first, as long as the activation is running or awaiting completion.
func applyCompleted(next *Snapshot, act *Activation, event Event) error {
	if act == nil {
		return errors.New("completed event requires an activation")
	}
	if event.Outcome == "" {
		return errors.New("completed event requires an outcome")
	}
	switch act.Status {
	case ActivationRunning, ActivationAwaitingCompletion, ActivationReady,
		ActivationAttemptFailed, ActivationBlocked, ActivationLeaseExpired:
	default:
		return fmt.Errorf("cannot complete activation in status %q", act.Status)
	}
	// A live worker's run is still guarded: completion must carry the active
	// lease ID. When the activation no longer carries a lease (terminated,
	// blocked, expired, re-armed ready), the completion is accepted as a
	// distinct acceptance fact without a lease match.
	if act.ActiveLease != nil {
		if event.LeaseID == "" || act.ActiveLease.ID != event.LeaseID {
			return errors.New("completed event lease does not match the active lease")
		}
	}
	return satisfyActivation(next, act, event)
}

// satisfyActivation is the shared satisfaction tail of the completion path:
// it marks the activation satisfied with the event's outcome, releases the
// lease, records the event's evidence/outputs, and — when the event carries
// no declared branch transition — terminates the workflow with the outcome.
// Both applyCompleted and applyChildOutcome delegate here so the
// child-terminal-outcome resolution stays in lockstep with completion
// semantics. When the node declares required gates that are still
// unsatisfied, the activation is parked awaiting_gate instead (evidence and
// outputs are still recorded; the workflow is not completed).
func satisfyActivation(next *Snapshot, act *Activation, event Event) error {
	// Gate-aware completion (Stage 11): if the node declares required gates
	// that are not yet satisfied, completion parks the activation in
	// awaiting_gate instead of satisfying it. Evidence and outputs are still
	// recorded and the lease released atomically; the workflow is NOT
	// completed and NO branch is followed until the gates resolve.
	defs := []GateDefinition(nil)
	if completionGateResolve != nil {
		defs = completionGateResolve(next.Instance.TemplateID, next.Instance.TemplateVersion, act.NodeID)
	}
	if missing := unsatisfiedRequiredGates(&next.Instance, defs, act); len(missing) > 0 {
		act.Status = ActivationAwaitingGate
		act.SelectedOutcome = event.Outcome
		act.ActiveLease = nil
		act.UpdatedAt = nowUTC()
		if len(event.Evidence) > 0 {
			next.Instance.Evidence = append(next.Instance.Evidence, event.Evidence...)
		}
		if len(event.Outputs) > 0 {
			next.Instance.Outputs = append(next.Instance.Outputs, event.Outputs...)
		}
		return nil
	}
	act.Status = ActivationSatisfied
	act.SelectedOutcome = event.Outcome
	act.ActiveLease = nil
	act.UpdatedAt = nowUTC()
	if len(event.Evidence) > 0 {
		next.Instance.Evidence = append(next.Instance.Evidence, event.Evidence...)
	}
	if len(event.Outputs) > 0 {
		next.Instance.Outputs = append(next.Instance.Outputs, event.Outputs...)
	}
	if event.Transition == nil || event.Transition.TargetNodeID == "" {
		next.Instance.Status = WorkflowCompleted
		next.Instance.TerminalOutcome = event.Outcome
	}
	return nil
}

// findChildReference locates the composition-manifest child reference for a
// node (one per workflow-action node, keyed by NodeID).
func findChildReference(inst *WorkflowInstance, nodeID NodeID) *ChildReference {
	for i := range inst.Children {
		if inst.Children[i].NodeID == nodeID {
			return &inst.Children[i]
		}
	}
	return nil
}

// applyChildAttached durably parks a leased/running activation in the
// awaiting_child state and records the attach on the durable child reference.
// It is the parent side of kernel-local composition: no child state is
// copied, only the manifest reference is claimed. Re-attaching the same child
// to a different activation is rejected; an attach without a manifest entry
// for the node is rejected as well.
func applyChildAttached(next *Snapshot, act *Activation, event Event) error {
	if act == nil {
		return errors.New("child_attached event requires an activation")
	}
	switch act.Status {
	case ActivationLeased, ActivationRunning:
	default:
		return fmt.Errorf("cannot attach child to activation in status %q", act.Status)
	}
	// A leased activation must be attached by its lease holder — mirroring
	// the completion and child-outcome lease guards — so a foreign attach can
	// never park a leased/running activation into awaiting_child.
	if act.ActiveLease != nil {
		if event.LeaseID == "" || act.ActiveLease.ID != event.LeaseID {
			return errors.New("child_attached event lease does not match the active lease")
		}
	}
	ref := findChildReference(&next.Instance, act.NodeID)
	if ref == nil {
		return fmt.Errorf("no child reference for node %q in the composition manifest", act.NodeID)
	}
	if ref.ParentActivation != "" && ref.ParentActivation != act.ID {
		return fmt.Errorf("child %s already attached to activation %q", ref.WorkflowID, ref.ParentActivation)
	}
	ref.ParentActivation = act.ID
	act.Status = ActivationAwaitingChild
	act.UpdatedAt = nowUTC()
	return nil
}

// applyChildOutcome resolves the parent activation for a durable child
// terminal outcome. It requires the activation to be awaiting_child (only a
// child's terminal outcome advances it from that state), records the mapped
// parent outcome and the selected child output references on the child
// reference, and satisfies the activation through the same completion tail
// as applyCompleted.
func applyChildOutcome(next *Snapshot, act *Activation, event Event) error {
	if act == nil {
		return errors.New("child_outcome event requires an activation")
	}
	if event.Outcome == "" {
		return errors.New("child_outcome event requires an outcome")
	}
	if act.Status != ActivationAwaitingChild {
		return fmt.Errorf("cannot resolve child outcome for activation in status %q", act.Status)
	}
	// The attach keeps the claim lease live until the child resolves, so a
	// live lease must be presented — mirroring the completion lease guard.
	if act.ActiveLease != nil {
		if event.LeaseID == "" || act.ActiveLease.ID != event.LeaseID {
			return errors.New("child_outcome event lease does not match the active lease")
		}
	}
	ref := findChildReference(&next.Instance, act.NodeID)
	if ref == nil || ref.ParentActivation != act.ID {
		return fmt.Errorf("child reference for node %q is not attached to this activation", act.NodeID)
	}
	ref.Outcome = event.Outcome
	ref.Outputs = append([]OutputReference(nil), event.ChildOutputs...)
	return satisfyActivation(next, act, event)
}

// applyGate records a gate decision on the activation. The reducer has no
// template, so an accepted gate satisfies the activation directly, a rejected
// or failed gate rejects it, and an undecided gate parks it awaiting_gate.
func applyGate(next *Snapshot, act *Activation, event Event) error {
	if act == nil {
		return errors.New("gate event requires an activation")
	}
	gate := event.Gate
	if gate == nil {
		return errors.New("gate event requires a gate instance")
	}
	replaced := false
	for i := range next.Instance.Gates {
		gi := &next.Instance.Gates[i]
		if gate.ID != "" && gi.ID == gate.ID {
			*gi = *gate
			replaced = true
			break
		}
		if gate.ID == "" && gi.GateID == gate.GateID && gi.ActivationID == gate.ActivationID {
			*gi = *gate
			replaced = true
			break
		}
	}
	if !replaced {
		next.Instance.Gates = append(next.Instance.Gates, *gate)
	}
	switch gate.Status {
	case GatePassed, GateWaived:
		act.Status = ActivationSatisfied
		if gate.Outcome != "" {
			act.SelectedOutcome = gate.Outcome
		}
	case GateRejected, GateFailed:
		act.Status = ActivationRejected
	default: // pending, action_required, changes_requested
		act.Status = ActivationAwaitingGate
	}
	act.UpdatedAt = nowUTC()
	return nil
}

// applySkipped records an authorized skip of a pending activation.
func applySkipped(act *Activation, event Event) error {
	if act == nil {
		return errors.New("skipped event requires an activation")
	}
	if event.Actor == "" || event.Reason == "" {
		return errors.New("skipped event requires both actor and reason")
	}
	if act.Status != ActivationPending {
		return fmt.Errorf("cannot skip activation in status %q", act.Status)
	}
	act.Status = ActivationSkipped
	act.UpdatedAt = nowUTC()
	return nil
}

// applyUnblocked returns a blocked activation to ready after an authorized
// unblock.
func applyUnblocked(act *Activation, event Event) error {
	if act == nil {
		return errors.New("unblocked event requires an activation")
	}
	if event.Actor == "" || event.Reason == "" {
		return errors.New("unblocked event requires both actor and reason")
	}
	if act.Status != ActivationBlocked {
		return fmt.Errorf("cannot unblock activation in status %q", act.Status)
	}
	act.Status = ActivationReady
	act.UpdatedAt = nowUTC()
	return nil
}

// applyRerouted introduces a fresh activation for the target node with the
// supplied selection. It is a creation event, so no pre-existing activation
// is required.
func applyRerouted(next *Snapshot, event Event) {
	now := nowUTC()
	next.Instance.Activations = append(next.Instance.Activations, Activation{
		ID:        NewActivationID(),
		NodeID:    event.Identity.NodeID,
		Iteration: event.Iteration,
		Status:    ActivationPending,
		Selection: event.Selection,
		CreatedAt: now,
		UpdatedAt: now,
	})
}

// applyHeartbeat requires a live lease and records a deterministic heartbeat
// marker. Wall-clock renewal is owned by the store; the reducer only touches
// LastHeartbeatAt with a fixed sentinel so replay stays stable.
func applyHeartbeat(act *Activation, event Event) error {
	if act == nil {
		return errors.New("heartbeat event requires an activation")
	}
	if act.ActiveLease == nil {
		return errors.New("heartbeat requires an active lease")
	}
	if event.LeaseID != "" && event.LeaseID != act.ActiveLease.ID {
		return errors.New("heartbeat lease does not match the active lease")
	}
	marker := time.Time{}
	act.ActiveLease.LastHeartbeatAt = &marker
	return nil
}

// applyLeaseExpired releases a stale lease and parks the activation in the
// expired state so the store can requeue it.
func applyLeaseExpired(act *Activation, event Event) error {
	if act == nil {
		return errors.New("lease_expired event requires an activation")
	}
	if act.ActiveLease == nil {
		return errors.New("lease_expired requires an active lease")
	}
	if event.LeaseID != "" && event.LeaseID != act.ActiveLease.ID {
		return errors.New("lease_expired lease does not match the active lease")
	}
	act.Status = ActivationLeaseExpired
	act.ActiveLease = nil
	act.UpdatedAt = nowUTC()
	return nil
}

// completionGateResolve is an optional template-aware gate lookup installed
// by the workflow manager. The reducer cannot see templates, so without it
// gate-awareness on completion is disabled entirely (a completion always
// satisfies). A template is versioned, so the resolver also carries the
// template version.
var completionGateResolve func(templateID TemplateID, templateVersion TemplateVersion, nodeID NodeID) []GateDefinition

// SetCompletionGateResolver wires the template-aware gate lookup used by the
// completion path: when a completion leaves required gates unsatisfied, the
// activation is parked awaiting_gate instead of being satisfied. It is owned
// by the manager (which can resolve the instance's versioned template); the
// reducer defaults to no-gate behavior when unset.
func SetCompletionGateResolver(fn func(templateID TemplateID, templateVersion TemplateVersion, nodeID NodeID) []GateDefinition) {
	completionGateResolve = fn
}

// retryResolve is an optional template-aware retry policy lookup installed by
// the workflow store/manager. The reducer cannot see templates, so without it
// any failure exhausts immediately (blocked).
var retryResolve func(templateID TemplateID, nodeID NodeID) *RetryPolicy

// SetRetryPolicyResolver wires the template-aware retry policy lookup used by
// attempt_terminated handling. It is owned by the store (which can resolve the
// instance's template); the reducer defaults to single-attempt behavior when
// unset.
func SetRetryPolicyResolver(fn func(templateID TemplateID, nodeID NodeID) *RetryPolicy) {
	retryResolve = fn
}

// retryPolicyFor returns the node's retry policy as resolved by the store, or
// nil when no resolver/template is available.
func retryPolicyFor(next *Snapshot, nodeID NodeID) *RetryPolicy {
	if retryResolve == nil {
		return nil
	}
	return retryResolve(next.Instance.TemplateID, nodeID)
}

// nowUTC returns the current time in UTC for event/record timestamps.
func nowUTC() time.Time {
	return time.Now().UTC()
}
