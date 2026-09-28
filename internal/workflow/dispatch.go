package workflow

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/sdougbrown/avenor/internal/durablefile"
)

// ErrStaleCandidate reports that a candidate activation is no longer
// dispatchable: wrong revision, not claimable, already leased, or its
// dispatch policy does not match the caller. No event is appended.
var ErrStaleCandidate = errors.New("workflow candidate is stale")

// ErrConcurrencyKeyHeld reports that another live attempt already holds the
// activation's concurrency key across the workflow root. No event is
// appended.
var ErrConcurrencyKeyHeld = errors.New("concurrency key is held by a live attempt")

// ErrSelectionConflict reports that a caller-supplied selection conflicts
// with the selection already pinned on the activation. No event is appended.
var ErrSelectionConflict = errors.New("selection conflicts with the pinned selection")

// BeginDispatchRequest asks the manager to claim one candidate activation and
// record its attempt intent in a single atomic command.
type BeginDispatchRequest struct {
	WorkflowID       WorkflowID
	NodeID           NodeID
	ActivationID     ActivationID
	ExpectedRevision int64
	ControllerID     string
	LeaderLeaseID    string
	// Selection pins the resolved execution selection for this attempt and
	// all its retries. A selection conflicting with an already pinned one is
	// rejected; a nil selection inherits the pinned selection.
	Selection *ExecutionSelection
}

// BeginDispatchResult reports the claim + attempt intent recorded by
// BeginDispatch. The owner token exists only in memory; it is never written
// to events or snapshots.
type BeginDispatchResult struct {
	WorkflowID   WorkflowID
	NodeID       NodeID
	ActivationID ActivationID
	AttemptID    AttemptID
	LeaseID      LeaseID
	OwnerToken   string
	// LeaseTTL is the effective TTL of the granted lease; the executor uses
	// it as the base for its heartbeat cadence (TTL/3).
	LeaseTTL  time.Duration
	Action    Action
	Selection *ExecutionSelection
}

// FinalizeDispatchRequest records the outcome of one dispatch: the actual
// runtime identity on success, or a terminal pre-start failure. Repeating
// with the same attempt ID is idempotent.
type FinalizeDispatchRequest struct {
	WorkflowID   WorkflowID
	NodeID       NodeID
	ActivationID ActivationID
	AttemptID    AttemptID
	LeaseID      LeaseID
	// Runtime identity recorded on success.
	RuntimeID string
	RunID     string
	SessionID string
	// FailureStatus records a terminal pre-start attempt fact via the
	// existing terminate path (retry semantics apply). Zero means success.
	FailureStatus AttemptStatus
	MarkerKind    string
	MarkerLabel   string
}

// lockDispatch takes the root-scoped dispatch flock. It never creates the
// workflow root just to lock: a missing root has no workflows and therefore
// no held keys, so an no-op unlock is returned.
func (m *Manager) lockDispatch() (func() error, error) {
	root := m.store.Root()
	if _, err := os.Stat(root); err != nil {
		if os.IsNotExist(err) {
			return func() error { return nil }, nil
		}
		return nil, err
	}
	return durablefile.Lock(filepath.Join(root, "dispatch.lock"))
}

// heldConcurrencyKeys returns the set of concurrency keys held by live
// (non-terminal) attempts across every non-terminal workflow in the root,
// read from current on-disk snapshots. Attempts on skipWorkflow/skipActivation
// are excluded so a node's own dead attempt never blocks its replacement. No
// key table is ever persisted.
func (m *Manager) heldConcurrencyKeys(skipWorkflow WorkflowID, skipActivation ActivationID) (map[string]bool, error) {
	held := map[string]bool{}
	// Recovery-style sweeps would expire stale leases and move revisions
	// under the caller; key derivation must be a pure read, so snapshots are
	// loaded without the recovery sweep.
	//
	// Unreadable snapshots are fail-open: the workflow is skipped (with a
	// log line) rather than failing every dispatch on one bad instance.
	instancesDir := filepath.Join(m.store.Root(), "instances")
	entries, err := os.ReadDir(instancesDir)
	if err != nil {
		if os.IsNotExist(err) {
			return held, nil
		}
		return nil, err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		snap, ok, err := m.store.loadCurrent(WorkflowID(entry.Name()))
		if err != nil {
			log.Printf("workflow %s: skipping unreadable snapshot while deriving held concurrency keys: %v", entry.Name(), err)
			continue
		}
		if !ok {
			continue
		}
		collectHeldKeys(held, snap, skipWorkflow, skipActivation)
	}
	return held, nil
}

func collectHeldKeys(held map[string]bool, snap Snapshot, skipWorkflow WorkflowID, skipActivation ActivationID) {
	if isTerminalStatus(snap.Instance.Status) {
		return
	}
	for i := range snap.Instance.Activations {
		act := &snap.Instance.Activations[i]
		if act.Dispatch == nil || act.Dispatch.ConcurrencyKey == "" {
			continue
		}
		if snap.Instance.WorkflowID == skipWorkflow && act.ID == skipActivation {
			continue
		}
		for _, id := range act.AttemptIDs {
			at := findAttempt(&snap, act, id)
			if at == nil {
				continue
			}
			switch at.Status {
			case AttemptStarting, AttemptRunning:
				held[act.Dispatch.ConcurrencyKey] = true
			}
		}
	}
}

// sameSelection reports whether two selections are identical. Two nils are
// the same; a nil and a non-nil are not.
func sameSelection(a, b *ExecutionSelection) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// BeginDispatch claims one candidate activation for a controller and records
// its attempt intent (claim lease + attempt in starting) as one atomic
// command under one idempotency key, pinning the effective execution
// selection. The root dispatch lock spans the final concurrency-key check and
// the commit; the caller must validate its controller leader lease before
// calling and must not hold any admission reservation requirement here.
// Stale candidates and held keys return typed errors with no event appended.
func (m *Manager) BeginDispatch(req BeginDispatchRequest) (BeginDispatchResult, error) {
	unlock, err := m.lockDispatch()
	if err != nil {
		return BeginDispatchResult{}, err
	}
	defer unlock()

	snap, exists, err := m.store.loadCurrent(req.WorkflowID)
	if err != nil {
		return BeginDispatchResult{}, err
	}
	if !exists {
		return BeginDispatchResult{}, fmt.Errorf("workflow not found: %s", req.WorkflowID)
	}
	stale := func() (BeginDispatchResult, error) {
		return BeginDispatchResult{}, ErrStaleCandidate
	}
	act, err := findActivation(&snap.Instance, req.NodeID, req.ActivationID)
	if err != nil {
		return BeginDispatchResult{}, err
	}
	if act == nil {
		return BeginDispatchResult{}, fmt.Errorf("activation not found for node %q", req.NodeID)
	}
	if snap.Instance.Revision != req.ExpectedRevision {
		return stale()
	}
	if !claimableActivationStatus(act.Status) || act.ActiveLease != nil {
		return stale()
	}
	if act.Dispatch == nil || !act.Dispatch.IsAuto() || act.Dispatch.ControllerID != req.ControllerID {
		return stale()
	}
	if act.Selection != nil && req.Selection != nil && !sameSelection(act.Selection, req.Selection) {
		return BeginDispatchResult{}, ErrSelectionConflict
	}
	selection := req.Selection
	if selection == nil {
		selection = act.Selection
	}
	concurrencyKey := act.Dispatch.ConcurrencyKey
	// Key derivation is a pure read (no lease recovery), so it cannot move
	// the revision the commit below validates against.
	held, err := m.heldConcurrencyKeys(req.WorkflowID, act.ID)
	if err != nil {
		return BeginDispatchResult{}, err
	}
	if held[concurrencyKey] {
		return BeginDispatchResult{}, ErrConcurrencyKeyHeld
	}
	if h := m.testHooks.beginDispatchPreCommit; h != nil {
		h()
	}
	tmpl, err := m.templateFor(&snap)
	if err != nil {
		return BeginDispatchResult{}, err
	}
	node, err := findNode(tmpl, req.NodeID)
	if err != nil {
		return BeginDispatchResult{}, err
	}
	now := time.Now().UTC()
	ttl := leaseTTL(node, tmpl.DefaultLease)
	leaseID := NewLeaseID()
	attemptID := NewAttemptID()
	token := newOwnerToken()
	if _, err := m.store.ApplyCommand(req.WorkflowID, Command{
		ID:               NewCommandID(),
		Kind:             CommandBeginDispatch,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "begin-dispatch-" + string(leaseID),
		Identity:         ExecutionIdentity{WorkflowID: req.WorkflowID, NodeID: req.NodeID, ActivationID: act.ID, AttemptID: attemptID},
		LeaseID:          leaseID,
		Actor:            req.ControllerID,
		Lease: &Lease{
			ID:           leaseID,
			ActivationID: act.ID,
			Owner:        req.ControllerID,
			TokenDigest:  ownerTokenDigest(token),
			AcquiredAt:   now,
			ExpiresAt:    now.Add(ttl),
		},
		Selection: selection,
		Diagnostics: &AttemptDiagnostics{
			ControllerID:   req.ControllerID,
			LeaderLeaseID:  req.LeaderLeaseID,
			ConcurrencyKey: concurrencyKey,
		},
	}); err != nil {
		if errors.Is(err, errRevisionMismatch) {
			// Another command on the same workflow committed between the read
			// above and this commit: the candidate is stale by definition.
			return BeginDispatchResult{}, fmt.Errorf("begin dispatch %s/%s: revision advanced under a concurrent command: %w",
				req.WorkflowID, req.NodeID, ErrStaleCandidate)
		}
		return BeginDispatchResult{}, err
	}
	return BeginDispatchResult{
		WorkflowID:   req.WorkflowID,
		NodeID:       req.NodeID,
		ActivationID: act.ID,
		AttemptID:    attemptID,
		LeaseID:      leaseID,
		OwnerToken:   token,
		LeaseTTL:     ttl,
		Action:       node.Action,
		Selection:    selection,
	}, nil
}

// FinalizeDispatch records the actual runtime identity of a dispatched
// attempt on success, or a terminal pre-start failure through the existing
// terminate path (retry semantics apply). Repeating with the same attempt ID
// is idempotent.
func (m *Manager) FinalizeDispatch(req FinalizeDispatchRequest) error {
	if req.FailureStatus != "" {
		return m.RecordAttemptTerminated(req.WorkflowID, req.NodeID, req.ActivationID,
			req.AttemptID, req.LeaseID, req.FailureStatus, req.MarkerKind, req.MarkerLabel)
	}
	const maxIdentifyAttempts = 4
	for attempt := 0; attempt < maxIdentifyAttempts; attempt++ {
		snap, exists, err := m.store.loadCurrent(req.WorkflowID)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("workflow not found: %s", req.WorkflowID)
		}
		if attempt == 0 && m.testHooks.finalizeDispatchPreCommit != nil {
			m.testHooks.finalizeDispatchPreCommit()
		}
		_, err = m.store.ApplyCommand(req.WorkflowID, Command{
			ID:               NewCommandID(),
			Kind:             CommandAttemptIdentified,
			ExpectedRevision: snap.Instance.Revision,
			IdempotencyKey:   "identify-" + string(req.AttemptID),
			Identity: ExecutionIdentity{
				WorkflowID:   req.WorkflowID,
				NodeID:       req.NodeID,
				ActivationID: req.ActivationID,
				AttemptID:    req.AttemptID,
				RuntimeID:    req.RuntimeID,
				RunID:        req.RunID,
				SessionID:    req.SessionID,
			},
			LeaseID: req.LeaseID,
		})
		if err == nil {
			return nil
		}
		if errors.Is(err, errDuplicateIdempotency) {
			return nil
		}
		if !errors.Is(err, errRevisionMismatch) {
			return err
		}
	}
	return fmt.Errorf("identify attempt %s: revision kept moving under concurrent commands", req.AttemptID)
}

// DispatchExecutor hands a prepared executor context to the executor
// registered for the action kind. The caller owns admission and
// finalization; this only performs the backend start.
func (m *Manager) DispatchExecutor(kind ActionKind, ec ExecutorContext) error {
	exec := m.executor(kind)
	if exec == nil {
		return fmt.Errorf("executor for action %q is unsupported until a later stage (executor not registered)", kind)
	}
	return exec.Dispatch(context.Background(), ec)
}
