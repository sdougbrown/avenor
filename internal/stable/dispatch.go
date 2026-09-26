package stable

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/sdougbrown/avenor/internal/admission"
	"github.com/sdougbrown/avenor/internal/rosterconfig"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/spawnselection"
	"github.com/sdougbrown/avenor/internal/workflow"
	"github.com/sdougbrown/avenor/internal/workflowcontroller"
)

// DispatchOutcomeKind classifies the result of one controller dispatch
// attempt.
type DispatchOutcomeKind string

const (
	// Dispatched means the node was claimed and its runtime started.
	Dispatched DispatchOutcomeKind = "dispatched"
	// DispatchCapacityBlocked means admission was unavailable; no workflow
	// event was appended and the node remains a candidate.
	DispatchCapacityBlocked DispatchOutcomeKind = "capacity_blocked"
	// DispatchStale means the candidate was no longer dispatchable (stale
	// revision, competing claim, or policy mismatch).
	DispatchStale DispatchOutcomeKind = "stale"
	// DispatchKeyHeld means another live attempt holds the node's
	// concurrency key.
	DispatchKeyHeld DispatchOutcomeKind = "key_held"
	// DispatchNotLeader means the caller's controller leader lease did not
	// validate.
	DispatchNotLeader DispatchOutcomeKind = "not_leader"
	// DispatchCanceled means the dispatch was canceled through its context.
	// Before the attempt intent is committed nothing is reserved and no event
	// is appended; after the commit the granted lease stays authoritative and
	// the attempt is finalized as canceled (retry semantics apply).
	DispatchCanceled DispatchOutcomeKind = "canceled"
	// DispatchStartFailed means the executor's runtime start failed after the
	// attempt intent was durably recorded; kernel attempt/retry semantics
	// apply.
	DispatchStartFailed DispatchOutcomeKind = "start_failed"
)

// DispatchRequest asks the supervisor to dispatch one ready workflow node on
// behalf of a controller leader.
type DispatchRequest struct {
	WorkflowID       string
	NodeID           string
	ActivationID     string
	ExpectedRevision int64
	ControllerID     string
	LeaderLeaseID    string
	OwnerEpoch       int64
	// Selection pins the execution selection for this dispatch. Nil means the
	// node's declared assignment is resolved here: an already-pinned
	// activation (a retry) keeps its pin unchanged, otherwise the assignment
	// resolves through the roster and spawn-selection rules.
	Selection *workflow.ExecutionSelection
}

// DispatchOutcome reports the result of one dispatch attempt. Only
// Dispatched carries runtime identity.
type DispatchOutcome struct {
	Kind         DispatchOutcomeKind
	Source       string // capacity source: "local" or "tree"
	WorkflowID   string
	NodeID       string
	ActivationID string
	AttemptID    string
	RuntimeID    string
	Detail       string
}

// dispatchWorkflowNode dispatches one ready workflow node through stable
// admission and the manager's dispatch boundary. It is a Go-internal method
// for the controller runner: it is never registered on the control socket,
// CLI, MCP, or client.
//
// Order: selection validation → cancellation check → admission reservation
// (no locks held) → cancellation check → controller lease validation → root
// dispatch lock + claim/attempt-intent commit → cancellation check → executor
// start with the reservation handed off → finalization. Cancellation is
// honored only between these boundaries; the executor start itself runs
// uninterruptibly.
func (s *Supervisor) dispatchWorkflowNode(ctx context.Context, req DispatchRequest) (DispatchOutcome, error) {
	out := DispatchOutcome{WorkflowID: req.WorkflowID, NodeID: req.NodeID, ActivationID: req.ActivationID}
	mgr, controllerStore, err := s.workflowBarrierResult()
	if err != nil {
		return out, err
	}

	// Resolve the effective selection before anything is reserved: a pinned
	// selection (a retry) is used unchanged, otherwise the node's declared
	// assignment resolves through the roster path. A resolution failure is a
	// start_failed-class outcome with no reservation held and no event
	// appended; the node stays a candidate for operator attention.
	selection := req.Selection
	if selection == nil {
		selection, err = s.resolveDispatchSelection(mgr, req.WorkflowID, req.NodeID, req.ActivationID)
		if err != nil {
			out.Kind = DispatchStartFailed
			out.Detail = err.Error()
			return out, nil
		}
	}

	// Validate the effective selection against the same spawn-selection and
	// thinking rules the ordinary spawn path enforces.
	if selection != nil {
		if err := spawnselection.Validate(spawnselection.Input{
			Agent:   selection.Agent,
			Model:   selection.Model,
			Backend: selection.Backend,
		}, false); err != nil {
			return out, err
		}
		if err := runtime.ValidateThinkingForBackend(selection.Backend, selection.Thinking); err != nil {
			return out, err
		}
	}

	// A cancel before the reservation leaves nothing reserved and no event
	// appended.
	if err := ctx.Err(); err != nil {
		out.Kind = DispatchCanceled
		return out, nil
	}

	// Reserve admission while holding no workflow-store, controller-store, or
	// dispatch lock. A capacity denial is a no-event, no-revision-change
	// result; the node stays a candidate.
	res, err := s.reserveAdmission()
	if err != nil {
		var ce *admission.CapacityError
		if errors.As(err, &ce) {
			out.Kind, out.Source = DispatchCapacityBlocked, ce.Source
			return out, nil
		}
		return out, err
	}
	// Every path from here to the executor start releases the reservation.
	// On a successful start the ownership transferred to the runtime inside
	// spawnReserved (consumeLocked marks the reservation released), so the
	// deferred Release below is a no-op.
	defer res.Release()

	// A cancel after the reservation but before any workflow state: the
	// reservation is released unused (deferred) and nothing is appended.
	if err := ctx.Err(); err != nil {
		out.Kind = DispatchCanceled
		return out, nil
	}

	// Under the validated leader lease, claim the node and record the attempt
	// intent. Stale candidates and held keys release the reservation after
	// the locks are released.
	var begin workflow.BeginDispatchResult
	var beginErr error
	withLeaderErr := controllerStore.WithLeader(req.ControllerID, req.LeaderLeaseID, req.OwnerEpoch, func() error {
		begin, beginErr = mgr.BeginDispatch(workflow.BeginDispatchRequest{
			WorkflowID:       workflow.WorkflowID(req.WorkflowID),
			NodeID:           workflow.NodeID(req.NodeID),
			ActivationID:     workflow.ActivationID(req.ActivationID),
			ExpectedRevision: req.ExpectedRevision,
			ControllerID:     req.ControllerID,
			LeaderLeaseID:    req.LeaderLeaseID,
			Selection:        selection,
		})
		return nil
	})
	if withLeaderErr != nil {
		if errors.Is(withLeaderErr, workflowcontroller.ErrNotLeader) {
			out.Kind = DispatchNotLeader
			return out, nil
		}
		return out, withLeaderErr
	}
	if beginErr != nil {
		switch {
		case errors.Is(beginErr, workflow.ErrStaleCandidate):
			out.Kind = DispatchStale
			return out, nil
		case errors.Is(beginErr, workflow.ErrConcurrencyKeyHeld):
			out.Kind = DispatchKeyHeld
			return out, nil
		default:
			return out, beginErr
		}
	}
	out.AttemptID = string(begin.AttemptID)

	// A cancel after the attempt intent was committed: the workflow-node lease
	// is already granted and stays authoritative, so the attempt is finalized
	// as a pre-start failure (not a cancellation) — a runner shutting down
	// between BeginDispatch and executor start never ran the node, so it must
	// fall back to the node's retry policy rather than exhaust the activation.
	// The reservation is released (deferred) and the cancellation is reported.
	if err := ctx.Err(); err != nil {
		if ferr := mgr.FinalizeDispatch(workflow.FinalizeDispatchRequest{
			WorkflowID:    workflow.WorkflowID(req.WorkflowID),
			NodeID:        workflow.NodeID(req.NodeID),
			ActivationID:  workflow.ActivationID(req.ActivationID),
			AttemptID:     begin.AttemptID,
			LeaseID:       begin.LeaseID,
			FailureStatus: workflow.AttemptFailed,
			MarkerKind:    "dispatch",
			MarkerLabel:   "dispatch_canceled",
		}); ferr != nil {
			fmt.Fprintf(os.Stderr, "avenor stable: workflow %s node %s attempt %s: record canceled dispatch: %v\n",
				req.WorkflowID, req.NodeID, begin.AttemptID, ferr)
		}
		out.Kind = DispatchCanceled
		return out, nil
	}

	// Start the declared executor with the reservation handed off, so the
	// node's first runtime consumes it instead of reserving again.
	ec := workflow.ExecutorContext{
		WorkflowID:   workflow.WorkflowID(req.WorkflowID),
		NodeID:       workflow.NodeID(req.NodeID),
		ActivationID: workflow.ActivationID(req.ActivationID),
		AttemptID:    begin.AttemptID,
		LeaseID:      begin.LeaseID,
		OwnerToken:   begin.OwnerToken,
		LeaseTTL:     begin.LeaseTTL,
		Action:       begin.Action,
		Selection:    begin.Selection,
		Admission:    res,
	}
	startErr := mgr.DispatchExecutor(begin.Action.Kind, ec)
	kind, label := workflowMarkerForKind(begin.Action.Kind)
	if startErr != nil {
		// Pre-start failure: record the terminal attempt fact (idempotent
		// with the executor's own termination record) and release the
		// reservation (deferred). Provider/session startup failures keep
		// kernel attempt/retry semantics and are not capacity failures.
		if ferr := mgr.FinalizeDispatch(workflow.FinalizeDispatchRequest{
			WorkflowID:    ec.WorkflowID,
			NodeID:        ec.NodeID,
			ActivationID:  ec.ActivationID,
			AttemptID:     begin.AttemptID,
			LeaseID:       begin.LeaseID,
			FailureStatus: workflow.AttemptFailed,
			MarkerKind:    kind,
			MarkerLabel:   label,
		}); ferr != nil {
			fmt.Fprintf(os.Stderr, "avenor stable: workflow %s node %s attempt %s: record start failure: %v\n",
				req.WorkflowID, req.NodeID, begin.AttemptID, ferr)
		}
		out.Kind = DispatchStartFailed
		out.Detail = startErr.Error()
		return out, nil
	}

	// Record the actual runtime identity of the started attempt.
	rtID, sessionID := s.runtimeForAttempt(string(begin.AttemptID))
	if ferr := mgr.FinalizeDispatch(workflow.FinalizeDispatchRequest{
		WorkflowID:   ec.WorkflowID,
		NodeID:       ec.NodeID,
		ActivationID: ec.ActivationID,
		AttemptID:    begin.AttemptID,
		LeaseID:      begin.LeaseID,
		RuntimeID:    rtID,
		RunID:        s.runID,
		SessionID:    sessionID,
	}); ferr != nil {
		fmt.Fprintf(os.Stderr, "avenor stable: workflow %s node %s attempt %s: record runtime identity: %v\n",
			req.WorkflowID, req.NodeID, begin.AttemptID, ferr)
	}
	out.Kind = Dispatched
	out.RuntimeID = rtID
	return out, nil
}

// stableRunnerDeps adapts the supervisor's workflow manager and dispatch
// boundary to the workflowcontroller.RunnerDeps interface. The manager and
// controller store are resolved lazily so a runner started before the
// workflow barrier completes still resolves them correctly.
type stableRunnerDeps struct {
	s            *Supervisor
	controllerID string
}

// barrier returns the lazily-resolved workflow manager.
func (d *stableRunnerDeps) manager() (*workflow.Manager, error) {
	mgr, _, err := d.s.workflowBarrierResult()
	return mgr, err
}

// Candidates returns the controller's ready candidates as policy candidates.
func (d *stableRunnerDeps) Candidates(controllerID string) ([]workflowcontroller.Candidate, error) {
	mgr, err := d.manager()
	if err != nil {
		return nil, err
	}
	ready, err := mgr.CandidatesForController(controllerID, 0)
	if err != nil {
		return nil, err
	}
	candidates := make([]workflowcontroller.Candidate, 0, len(ready))
	for _, rc := range ready {
		kind := workflowcontroller.CandidateProvider
		if rc.Kind == workflow.ReadyCandidateExternalPark {
			kind = workflowcontroller.CandidateExternalPark
		}
		candidates = append(candidates, workflowcontroller.Candidate{
			Identity:       rc.Identity,
			Kind:           kind,
			ControllerID:   rc.ControllerID,
			Revision:       rc.Revision,
			ReadyAt:        rc.ReadyAt,
			Priority:       rc.Priority,
			ConcurrencyKey: rc.ConcurrencyKey,
		})
	}
	return candidates, nil
}

// InFlight returns the live attempts visible in the candidate index. Only
// starting/running attempts are returned, so Terminal is always false.
func (d *stableRunnerDeps) InFlight() ([]workflowcontroller.InFlightAttempt, error) {
	mgr, err := d.manager()
	if err != nil {
		return nil, err
	}
	live := mgr.LiveAttempts()
	attempts := make([]workflowcontroller.InFlightAttempt, 0, len(live))
	for _, la := range live {
		attempts = append(attempts, workflowcontroller.InFlightAttempt{
			Identity:       la.Identity,
			ControllerID:   la.ControllerID,
			ConcurrencyKey: la.ConcurrencyKey,
			Terminal:       false,
		})
	}
	return attempts, nil
}

// Refresh rebuilds the manager's candidate index for this supervisor,
// shared across every controller's runner deps: a rebuild performed within
// the runner's anti-entropy window is skipped, so N enabled controllers on
// one anti-entropy tick cause one catalog scan instead of N. The first
// refresh after startup always rebuilds.
func (d *stableRunnerDeps) Refresh() error {
	mgr, err := d.manager()
	if err != nil {
		return err
	}
	s := d.s
	s.candidateRefreshMu.Lock()
	if !s.lastCandidateRebuild.IsZero() && time.Since(s.lastCandidateRebuild) < s.candidateRefreshWindow {
		s.candidateRefreshMu.Unlock()
		return nil
	}
	s.candidateRefreshMu.Unlock()
	if err := mgr.RebuildCandidateIndex(s.supervisorIdentity()); err != nil {
		return err
	}
	s.candidateRebuilds.Add(1)
	s.candidateRefreshMu.Lock()
	s.lastCandidateRebuild = time.Now()
	s.candidateRefreshMu.Unlock()
	return nil
}

// ParkedGates returns the external gate references of this controller's
// parked awaiting_gate activations, read-only from the manager's
// candidate index.
func (d *stableRunnerDeps) ParkedGates(controllerID string) ([]workflow.ParkedGateRef, error) {
	mgr, err := d.manager()
	if err != nil {
		return nil, err
	}
	return mgr.ParkedExternalGates(controllerID)
}

// Dispatch dispatches one selected candidate through the supervisor's
// dispatch boundary under the runner's lease. External-park candidates park
// kernel-locally instead of consuming admission; provider candidates use the
// activation's own declared selection (Selection is nil on the request).
func (d *stableRunnerDeps) Dispatch(ctx context.Context, dec workflowcontroller.Decision, lease workflowcontroller.LeaderLease) (workflowcontroller.DispatchResult, error) {
	identity := dec.Candidate.Identity
	if dec.Candidate.Kind == workflowcontroller.CandidateExternalPark {
		return d.s.parkExternalNode(dec, lease)
	}
	out, err := d.s.dispatchWorkflowNode(ctx, DispatchRequest{
		WorkflowID:       string(identity.WorkflowID),
		NodeID:           string(identity.NodeID),
		ActivationID:     string(identity.ActivationID),
		ExpectedRevision: dec.Candidate.Revision,
		ControllerID:     d.controllerID,
		LeaderLeaseID:    lease.LeaseID,
		OwnerEpoch:       lease.OwnerEpoch,
		Selection:        nil,
	})
	if err != nil {
		return workflowcontroller.DispatchResult{}, err
	}
	return workflowcontroller.DispatchResult{
		Kind:   translateDispatchOutcome(out.Kind),
		Source: out.Source,
	}, nil
}

// translateDispatchOutcome maps the stable dispatch outcome kinds onto the
// runner's result kinds.
func translateDispatchOutcome(kind DispatchOutcomeKind) workflowcontroller.DispatchResultKind {
	switch kind {
	case Dispatched:
		return workflowcontroller.ResultDispatched
	case DispatchCapacityBlocked:
		return workflowcontroller.ResultCapacityBlocked
	case DispatchStale:
		return workflowcontroller.ResultStale
	case DispatchKeyHeld:
		return workflowcontroller.ResultKeyHeld
	case DispatchNotLeader:
		return workflowcontroller.ResultNotLeader
	case DispatchStartFailed:
		return workflowcontroller.ResultStartFailed
	default:
		return workflowcontroller.ResultCanceled
	}
}

// runtimeForAttempt finds the child runtime registered for a workflow
// attempt, if it is live.
func (s *Supervisor) runtimeForAttempt(attemptID string) (runtimeID, sessionID string) {
	s.controlMu.Lock()
	child := (*childRuntime)(nil)
	for _, rt := range s.runtimes {
		rt.mu.Lock()
		match := rt.attemptID == attemptID && !rt.completed
		if match {
			child = rt
		}
		rt.mu.Unlock()
		if match {
			break
		}
	}
	s.controlMu.Unlock()
	if child == nil {
		return "", ""
	}
	return child.id, child.sessionID()
}

// resolveDispatchSelection resolves the execution selection for one
// controller dispatch of a node whose request carries no selection. A pinned
// activation (a retry) keeps its pin unchanged; otherwise the node's
// declared assignment resolves through the roster and spawn-selection rules
// via resolveAssignmentSelection. A node with no declared assignment
// dispatches without a pin.
func (s *Supervisor) resolveDispatchSelection(mgr *workflow.Manager, workflowID, nodeID, activationID string) (*workflow.ExecutionSelection, error) {
	insp, err := mgr.WorkflowInspect(workflowID)
	if err != nil {
		return nil, err
	}
	inst, ok := insp.(map[string]any)["instance"].(workflow.WorkflowInstance)
	if !ok {
		return nil, fmt.Errorf("workflow %s: unreadable instance", workflowID)
	}
	var act *workflow.Activation
	for i := range inst.Activations {
		if inst.Activations[i].ID == workflow.ActivationID(activationID) && inst.Activations[i].NodeID == workflow.NodeID(nodeID) {
			act = &inst.Activations[i]
			break
		}
	}
	if act == nil {
		return nil, fmt.Errorf("workflow %s: activation %s not found for node %s", workflowID, activationID, nodeID)
	}
	if act.Selection != nil {
		return act.Selection, nil
	}
	tmpl, err := mgr.Store().LoadTemplate(inst.TemplateID, inst.TemplateVersion)
	if err != nil {
		return nil, fmt.Errorf("workflow %s: load template %s@%s: %w", workflowID, inst.TemplateID, inst.TemplateVersion, err)
	}
	var assignment *workflow.Assignment
	for i := range tmpl.Nodes {
		if tmpl.Nodes[i].ID == workflow.NodeID(nodeID) {
			assignment = tmpl.Nodes[i].Assignment
			break
		}
	}
	return resolveAssignmentSelection(assignment)
}

// resolveAssignmentSelection resolves a node's declared assignment into an
// execution selection through the same roster and spawn-selection rules the
// direct spawn path enforces: the roster entry supplies the complete
// backend/agent/model identity, the roster file's SHA-256 digest pins the
// configuration the selection was resolved from, and thinking is validated
// for the effective backend. A nil or empty assignment resolves to nil (no
// pin). It is shared by the automatic dispatch boundary and the manual
// start path's assignment resolution.
func resolveAssignmentSelection(assignment *workflow.Assignment) (*workflow.ExecutionSelection, error) {
	if assignment == nil || assignmentEmpty(assignment) {
		return nil, nil
	}
	if err := spawnselection.Validate(spawnselection.Input{
		Agent:       assignment.Agent,
		Model:       assignment.Model,
		Backend:     assignment.Backend,
		RosterFile:  assignment.RosterFile,
		RosterEntry: assignment.RosterEntry,
	}, false); err != nil {
		return nil, fmt.Errorf("node assignment: %w", err)
	}
	selection := &workflow.ExecutionSelection{
		Role:    assignment.Role,
		Backend: assignment.Backend,
		Agent:   assignment.Agent,
		Model:   assignment.Model,
	}
	if assignment.RosterEntry != "" {
		roster, err := rosterconfig.Load(assignment.RosterFile)
		if err != nil {
			return nil, fmt.Errorf("node assignment: %w", err)
		}
		entry, err := roster.Lookup(assignment.RosterEntry)
		if err != nil {
			return nil, fmt.Errorf("node assignment: %w", err)
		}
		selection.Backend, selection.Agent, selection.Model = entry.Backend, entry.Agent, entry.Model
		digest, err := rosterFileDigest(assignment.RosterFile)
		if err != nil {
			return nil, fmt.Errorf("node assignment: %w", err)
		}
		selection.RosterDigest = digest
	}
	if err := runtime.ValidateThinkingForBackend(selection.Backend, assignment.Thinking); err != nil {
		return nil, fmt.Errorf("node assignment: %w", err)
	}
	selection.Thinking = assignment.Thinking
	return selection, nil
}

// assignmentEmpty reports whether an assignment declares nothing to resolve.
func assignmentEmpty(assignment *workflow.Assignment) bool {
	return assignment.Role == "" && assignment.RosterFile == "" && assignment.RosterEntry == "" &&
		assignment.Backend == "" && assignment.Agent == "" && assignment.Model == "" && assignment.Thinking == ""
}

// rosterFileDigest returns the "sha256:<hex>" digest of a roster file's
// bytes, the immutable fingerprint pinned with the selection the roster
// resolved.
func rosterFileDigest(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read roster file: %w", err)
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}
