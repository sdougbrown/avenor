package workflow

// Manager is the workflow-store API surface for the workflow manager. It
// wraps a Store and exposes create/instantiate/read/wait operations. It is
// stdlib-only and imports nothing from internal/control so it can later be
// registered directly as the control package's WorkflowHandler.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type Manager struct {
	store *Store

	mu        sync.Mutex
	executors map[ActionKind]Executor

	candidateMu sync.Mutex
	// candidates caches one recovered snapshot per workflow. The index is a
	// discardable optimization rebuilt only from Catalog() results; the final
	// claim revalidation always happens under the workflow store lock. It
	// reflects this process's commits (via the store commit hook) plus the
	// last rebuild.
	candidates     map[WorkflowID]Snapshot
	candidateSuper string
	candidatesOK   bool
}

func NewManager(store *Store) *Manager {
	m := &Manager{store: store, executors: make(map[ActionKind]Executor)}
	// Keep the candidate index fresh across commands: every committed
	// snapshot upserts into the index once it has been recovered.
	store.SetCommitObserver(m.observeCommit)
	// The workflow action is kernel-local composition (no provider
	// admission), so its executor ships with the manager by default.
	m.executors[ActionWorkflow] = &workflowExecutor{manager: m}
	// Completion is gate-aware: the reducer resolves the node's declared
	// required gates from the instance's versioned template, so a completion
	// with unsatisfied required gates parks the activation in awaiting_gate.
	// Load errors return nil, which disables gating for that node (a
	// template that loads fine with no required gates is unaffected).
	SetCompletionGateResolver(func(templateID TemplateID, templateVersion TemplateVersion, nodeID NodeID) []GateDefinition {
		tmpl, err := m.store.LoadTemplate(templateID, templateVersion)
		if err != nil {
			return nil
		}
		node, err := findNode(&tmpl, nodeID)
		if err != nil {
			return nil
		}
		return node.Gates
	})
	// Retry policies are resolved from the instance's versioned template so
	// the reducer's retry/exhaustion logic sees the node's durable policy
	// (matching production): without this resolver every failure would
	// exhaust to a single attempt. Load errors return nil, which keeps
	// single-attempt behavior for that node.
	SetRetryPolicyResolver(func(templateID TemplateID, templateVersion TemplateVersion, nodeID NodeID) *RetryPolicy {
		tmpl, err := m.store.LoadTemplate(templateID, templateVersion)
		if err != nil {
			return nil
		}
		node, err := findNode(&tmpl, nodeID)
		if err != nil {
			return nil
		}
		return node.RetryPolicy
	})
	// Dispatch policies are resolved from the instance's versioned template so
	// activation creation copies the node's auto-dispatch metadata into the
	// snapshot (mirroring the retry resolver). Load errors return nil, which
	// reads as manual for that node.
	SetDispatchPolicyResolver(func(templateID TemplateID, templateVersion TemplateVersion, nodeID NodeID) *DispatchPolicy {
		tmpl, err := m.store.LoadTemplate(templateID, templateVersion)
		if err != nil {
			return nil
		}
		node, err := findNode(&tmpl, nodeID)
		if err != nil {
			return nil
		}
		return node.Dispatch
	})
	return m
}

// AdmissionHandle is an opaque, never-persisted admission reservation handed
// to an executor so its first runtime start consumes the caller's reservation
// instead of acquiring a second one. Kernel transitions never read it, and
// executors must never marshal it (or any ExecutorContext carrying it) into
// the workflow store's snapshots or event log.
type AdmissionHandle interface {
	Release()
}

// Executor dispatches a started action to its runtime backend. The manager
// records the attempt durably before calling Dispatch, so an executor that
// crashes leaves the attempt in the log for recovery. Stage 6 registers no
// real providers; tests use a fake.
type Executor interface {
	Dispatch(ctx context.Context, ec ExecutorContext) error
}

// ExecutorContext carries everything a backend needs to run one attempt.
type ExecutorContext struct {
	WorkflowID   WorkflowID
	NodeID       NodeID
	ActivationID ActivationID
	AttemptID    AttemptID
	LeaseID      LeaseID
	// OwnerToken is the raw claim owner token for this attempt's lease. It is
	// additive and inert: the reducer/store never sees it, but the executor
	// layer uses it (with LeaseID) to renew its own lease via
	// Manager.Heartbeat — the owner-token heartbeat seam. Executors must
	// never marshal ExecutorContext (or OwnerToken) into the workflow store's
	// snapshots or event log, so the raw claim token can never become durable.
	OwnerToken string
	// LeaseTTL is the effective TTL of this attempt's lease at claim time. It
	// sets the executor heartbeat cadence (TTL/3); zero falls back to
	// DefaultLeaseTTL.
	LeaseTTL  time.Duration
	Action    Action
	Selection *ExecutionSelection
	// Admission optionally carries a pre-reserved runtime admission for this
	// attempt's first start. Nil means the executor's start self-reserves
	// through the ordinary spawn path (manual and legacy starts).
	Admission AdmissionHandle
}

// RegisterExecutor attaches the dispatch backend for one action kind.
func (m *Manager) RegisterExecutor(kind ActionKind, exec Executor) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.executors[kind] = exec
}

// executor returns the registered backend for an action kind, or nil when no
// executor is registered (the action is unsupported until a later stage).
func (m *Manager) executor(kind ActionKind) Executor {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.executors[kind]
}

// safeComponent rejects empty, ".", "..", and any component containing a path
// separator or NUL byte so ids can never escape their template/instance dirs.
func safeComponent(s string) bool {
	if s == "" || s == "." || s == ".." {
		return false
	}
	return !strings.ContainsAny(s, "/\\\x00")
}

func (m *Manager) checkWorkflowID(id string) (WorkflowID, error) {
	if !safeComponent(id) {
		return "", errors.New("invalid workflow id")
	}
	return WorkflowID(id), nil
}

// WorkflowCreate validates and stores a versioned template.
func (m *Manager) WorkflowCreate(payload json.RawMessage) (any, error) {
	if err := ValidateTemplateJSON(payload); err != nil {
		return nil, err
	}
	var template Template
	if err := json.Unmarshal(payload, &template); err != nil {
		return nil, err
	}
	if !safeComponent(string(template.TemplateID)) {
		return nil, errors.New("invalid template id")
	}
	if !safeComponent(string(template.TemplateVersion)) {
		return nil, errors.New("invalid template version")
	}
	if err := m.store.StoreTemplate(template.TemplateID, template.TemplateVersion, template); err != nil {
		return nil, err
	}
	return map[string]any{
		"template_id":      string(template.TemplateID),
		"template_version": string(template.TemplateVersion),
	}, nil
}

// WorkflowInstantiate instantiates a stored template as a new active workflow.
// When the template composes child workflows, the compose prerequisites are
// validated up front (pinned versions resolve across the whole descendant
// tree, no cycles, all bounds respected) and the child instances are
// materialized eagerly — idempotently, under deterministic derived IDs —
// before the parent's instantiate command is applied, so the parent's event
// log never references children that do not exist. The eager materialization
// is intentional: every composed child is created as an active instance (its
// entry activation pending) ahead of the parent's own commit, and child
// execution stays gated on the parent's workflow-node attach in a later
// phase — nothing auto-claims a child. Child creation is idempotent through
// the derived IDs: a replay that finds the child already present resumes it
// as-is, while a concurrent creator surfaces as a revision mismatch rather
// than being silently absorbed. If materialization fails partway, the
// already-created children are left on disk as active, unparented workflows
// and the caller receives only the error; orphan cleanup is out of scope for
// this stage. The composition manifest rides on the instantiate event record
// and is applied into the instance by the reducer.
func (m *Manager) WorkflowInstantiate(payload json.RawMessage) (any, error) {
	var req struct {
		TemplateID      TemplateID      `json:"template_id"`
		TemplateVersion TemplateVersion `json:"template_version"`
		Metadata        map[string]any  `json:"metadata,omitempty"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("instantiate payload: %w", err)
	}
	if req.TemplateID == "" || req.TemplateVersion == "" {
		return nil, errors.New("template_id and template_version are required")
	}
	if !safeComponent(string(req.TemplateID)) {
		return nil, errors.New("invalid template id")
	}
	if !safeComponent(string(req.TemplateVersion)) {
		return nil, errors.New("invalid template version")
	}
	template, err := m.store.LoadTemplate(req.TemplateID, req.TemplateVersion)
	if err != nil {
		return nil, err
	}
	wf := NewWorkflowID()
	var materialized int
	snap, err := m.instantiateTemplate(wf, template, &materialized)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"workflow_id": string(wf),
		"revision":    snap.Instance.Revision,
	}, nil
}

// instantiateTemplate materializes one workflow instance under wf for
// template. It validates the template's composition — BuildComposition
// enforcing the global instance bound over the whole descendant tree —
// idempotently ensures each declared child instance exists (recursing into
// nested compositions), and applies the parent's instantiate command with the
// composition manifest recorded on the instance record. materialized, when
// non-nil, is threaded through the recursion as a running count of instances
// this call has materialized; the path rejects should the cumulative count
// ever exceed DefaultMaximumCompositionInstances (defense in depth on top of
// BuildComposition's tree bound).
func (m *Manager) instantiateTemplate(wf WorkflowID, template Template, materialized *int) (Snapshot, error) {
	comp, _, err := BuildComposition(wf, template, m.resolveTemplate)
	if err != nil {
		return Snapshot{}, err
	}
	if materialized != nil {
		*materialized++
		if *materialized > DefaultMaximumCompositionInstances {
			return Snapshot{}, fmt.Errorf(
				"composition: template %s@%s would exceed the maximum of %d total materialized instances",
				template.TemplateID, template.TemplateVersion, DefaultMaximumCompositionInstances,
			)
		}
	}
	for _, child := range comp.Children {
		if _, err := m.ensureChildInstance(child, materialized); err != nil {
			return Snapshot{}, err
		}
	}
	record := InstanceRecord{
		TemplateID:       template.TemplateID,
		TemplateVersion:  template.TemplateVersion,
		TerminalOutcomes: template.TerminalOutcomes,
		EntryNodes:       template.EntryNodes,
	}
	for _, child := range comp.Children {
		record.Children = append(record.Children, ChildReference{
			ID:              NewChildReferenceID(),
			NodeID:          child.NodeID,
			WorkflowID:      child.ChildWorkflowID,
			TemplateID:      child.Template.TemplateID,
			TemplateVersion: child.Template.TemplateVersion,
		})
	}
	recordJSON, err := json.Marshal(record)
	if err != nil {
		return Snapshot{}, err
	}
	return m.store.ApplyCommand(wf, Command{
		Kind:             CommandInstantiate,
		ExpectedRevision: 0,
		IdempotencyKey:   "inst-" + string(wf),
		Identity:         ExecutionIdentity{WorkflowID: wf},
		Payload:          recordJSON,
	})
}

// ensureChildInstance idempotently ensures the child instance named by child
// exists, threading materialized (the running count of instances this
// instantiation has materialized) into the recursion. The child's workflow ID
// is deterministic in the parent's identity, node, and child key (see
// DeriveChildWorkflowID), so replay and retries always target the same
// instance: the real reuse path is the loadCurrent hit below, which resumes
// the existing child as-is, and a fresh one is created through the same
// instantiation path (recursing into its own composition). A concurrent
// creator is not absorbed and must not be: instantiate applies at
// ExpectedRevision 0 and the reducer checks the expected revision before the
// idempotency key, so a racing second creator of an existing child gets
// errRevisionMismatch (never errDuplicateIdempotency), and that error is
// surfaced to the caller rather than swallowed.
func (m *Manager) ensureChildInstance(child CompositionChild, materialized *int) (WorkflowID, error) {
	childID := child.ChildWorkflowID
	if _, exists, err := m.store.loadCurrent(childID); err == nil && exists {
		return childID, nil
	}
	if _, err := m.instantiateTemplate(childID, child.Template, materialized); err != nil {
		return "", err
	}
	return childID, nil
}

// resolveTemplate is the store-backed TemplateResolver for composition:
// a pinned child template must be resolvable, and a missing pin is reported
// as a clear pinned-version error rather than a raw store error.
func (m *Manager) resolveTemplate(templateID TemplateID, templateVersion TemplateVersion) (Template, error) {
	// Defense-in-depth: the pin is validated with safeComponent at create time
	// (validateAction), but re-check here so no caller can reach LoadTemplate's
	// filepath.Join with a path-breaking component (template-ID path traversal).
	if !safeComponent(string(templateID)) || !safeComponent(string(templateVersion)) {
		return Template{}, fmt.Errorf("invalid pinned child template reference %s@%s", templateID, templateVersion)
	}
	template, err := m.store.LoadTemplate(templateID, templateVersion)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Template{}, fmt.Errorf("pinned child template %s@%s not found", templateID, templateVersion)
		}
		return Template{}, err
	}
	return template, nil
}

// WorkflowStatus returns a compact status view of a workflow instance.
func (m *Manager) WorkflowStatus(id string) (any, error) {
	wf, err := m.checkWorkflowID(id)
	if err != nil {
		return nil, err
	}
	snap, exists, err := m.store.loadCurrent(wf)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("workflow not found: %s", id)
	}
	inst := snap.Instance
	return map[string]any{
		"workflow_id":      string(wf),
		"template_id":      string(inst.TemplateID),
		"template_version": string(inst.TemplateVersion),
		"status":           string(inst.Status),
		"revision":         inst.Revision,
		"terminal_outcome": string(inst.TerminalOutcome),
	}, nil
}

// inspectMap builds the full-detail inspect shape for a snapshot.
func inspectMap(snap Snapshot) map[string]any {
	inst := snap.Instance
	return map[string]any{
		"workflow_id": string(inst.WorkflowID),
		"instance":    inst,
		"revision":    inst.Revision,
		"activations": inst.Activations,
		"attempts":    inst.Attempts,
		"evidence":    inst.Evidence,
		"gates":       inst.Gates,
		"outputs":     inst.Outputs,
	}
}

// WorkflowInspect returns the full instance detail.
func (m *Manager) WorkflowInspect(id string) (any, error) {
	wf, err := m.checkWorkflowID(id)
	if err != nil {
		return nil, err
	}
	snap, exists, err := m.store.loadCurrent(wf)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("workflow not found: %s", id)
	}
	return inspectMap(snap), nil
}

func isTerminalStatus(status WorkflowStatus) bool {
	switch status {
	case WorkflowCompleted, WorkflowFailed, WorkflowCanceled:
		return true
	}
	return false
}

// WorkflowWait polls the instance until its status is terminal or the timeout
// elapses. A timeout <= 0 returns after the first poll.
func (m *Manager) WorkflowWait(id string, timeout time.Duration) (any, error) {
	wf, err := m.checkWorkflowID(id)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		snap, exists, err := m.store.loadCurrent(wf)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, fmt.Errorf("workflow not found: %s", id)
		}
		terminal := isTerminalStatus(snap.Instance.Status)
		timedOut := !terminal && time.Now().After(deadline)
		result := inspectMap(snap)
		result["terminal"] = terminal
		result["timed_out"] = timedOut
		if terminal || timeout <= 0 || timedOut {
			return result, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return result, nil
		}
		select {
		case <-ticker.C:
		case <-time.After(remaining):
		}
	}
}

// WorkflowEvents returns log events with Sequence > afterSeq, capped at limit
// (limit <= 0 caps at 1000).
func (m *Manager) WorkflowEvents(id string, afterSeq int64, limit int) (any, error) {
	wf, err := m.checkWorkflowID(id)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 1000
	}
	data, err := os.ReadFile(m.store.eventsPath(wf))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	events := make([]map[string]any, 0)
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var e Event
		if err := json.Unmarshal(line, &e); err != nil {
			continue
		}
		if e.Sequence <= afterSeq {
			continue
		}
		encoded, err := json.Marshal(e)
		if err != nil {
			return nil, err
		}
		var asMap map[string]any
		if err := json.Unmarshal(encoded, &asMap); err != nil {
			return nil, err
		}
		events = append(events, asMap)
		if len(events) >= limit {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return map[string]any{
		"workflow_id": string(wf),
		"after_seq":   afterSeq,
		"limit":       limit,
		"events":      events,
	}, nil
}

// WorkflowCommand routes an instance command by its "op" discriminator.
func (m *Manager) WorkflowCommand(id string, payload json.RawMessage) (any, error) {
	wf, err := m.checkWorkflowID(id)
	if err != nil {
		return nil, err
	}
	var op struct {
		Op string `json:"op"`
	}
	if err := json.Unmarshal(payload, &op); err != nil {
		return nil, fmt.Errorf("command payload: %w", err)
	}
	switch op.Op {
	case "claim":
		return m.commandClaim(wf, payload)
	case "start":
		return m.commandStart(wf, payload)
	case "heartbeat":
		return m.commandHeartbeat(wf, payload)
	case "complete":
		return m.commandComplete(wf, payload)
	case "gate":
		return m.commandGate(wf, payload)
	case "skip":
		return m.commandSkip(wf, payload)
	case "unblock":
		return m.commandUnblock(wf, payload)
	default:
		return nil, fmt.Errorf("workflow command op %q is unsupported until a later stage", op.Op)
	}
}

// templateFor loads the instance's versioned template from the store.
func (m *Manager) templateFor(snap *Snapshot) (*Template, error) {
	tmpl, err := m.store.LoadTemplate(snap.Instance.TemplateID, snap.Instance.TemplateVersion)
	if err != nil {
		return nil, err
	}
	return &tmpl, nil
}

// findNode locates a node definition in the template by ID.
func findNode(tmpl *Template, nodeID NodeID) (*NodeDefinition, error) {
	for i := range tmpl.Nodes {
		if tmpl.Nodes[i].ID == nodeID {
			return &tmpl.Nodes[i], nil
		}
	}
	return nil, fmt.Errorf("node %q not found in template %s@%s", nodeID, tmpl.TemplateID, tmpl.TemplateVersion)
}

type commandClaimRequest struct {
	NodeID       NodeID       `json:"node_id"`
	ActivationID ActivationID `json:"activation_id"`
	Actor        string       `json:"actor"`
}

// claimableActivationStatus is the single claim-eligibility status rule shared
// by the command boundary and the read-only candidate query. Checkpoint and
// gate parking stay ineligible because they rest in non-claimable statuses.
func claimableActivationStatus(status ActivationStatus) bool {
	switch status {
	case ActivationPending, ActivationReady, ActivationLeaseExpired:
		return true
	}
	return false
}

// ErrCandidatesNotRecovered is returned by CandidatesForController until the
// candidate index has been rebuilt from a full catalog recovery. Stage 2's
// startup barrier owns the rebuild call.
var ErrCandidatesNotRecovered = errors.New("workflow candidate index not recovered; rebuild after catalog recovery")

// ReadyCandidate is one ready auto-dispatch node exposed to a controller.
// It is advisory: the query never mutates state and never grants a lease.
type ReadyCandidate struct {
	Identity       ExecutionIdentity `json:"identity"`
	Revision       int64             `json:"revision"`
	ReadyAt        time.Time         `json:"ready_at"`
	Priority       int               `json:"priority"`
	ConcurrencyKey string            `json:"concurrency_key,omitempty"`
	ControllerID   string            `json:"controller_id"`
}

// RebuildCandidateIndex rebuilds the discardable in-memory candidate index
// from Store.Catalog() results. supervisorID is stamped onto every candidate
// identity. Stage 2's startup barrier calls this once catalog recovery has
// completed; until then the query returns ErrCandidatesNotRecovered. Between
// rebuilds the index stays fresh via the store's commit hook.
func (m *Manager) RebuildCandidateIndex(supervisorID string) error {
	catalog, err := m.store.Catalog()
	if err != nil {
		return err
	}
	entries := make(map[WorkflowID]Snapshot, len(catalog))
	for _, c := range catalog {
		entries[c.WorkflowID] = c.Snapshot
	}
	m.candidateMu.Lock()
	defer m.candidateMu.Unlock()
	m.candidates = entries
	m.candidateSuper = supervisorID
	m.candidatesOK = true
	return nil
}

// MarkCandidateIndexEmpty marks the candidate index recovered and empty for
// a workflow root that does not exist yet (an absent root is an empty catalog)
// without creating any on-disk state. New workflows land in the index through
// the store's commit hook.
func (m *Manager) MarkCandidateIndexEmpty(supervisorID string) {
	m.candidateMu.Lock()
	defer m.candidateMu.Unlock()
	m.candidates = map[WorkflowID]Snapshot{}
	m.candidateSuper = supervisorID
	m.candidatesOK = true
}

// observeCommit is the store commit hook: it upserts the committed snapshot
// into the candidate index once the index has been recovered. Older revisions
// are ignored so out-of-order observations from concurrent commits do not
// regress the index. Before recovery it is a no-op, and it never fails or
// blocks the command.
func (m *Manager) observeCommit(wf WorkflowID, snap Snapshot) {
	m.candidateMu.Lock()
	defer m.candidateMu.Unlock()
	if !m.candidatesOK {
		return
	}
	if cur, ok := m.candidates[wf]; ok && cur.Instance.Revision > snap.Instance.Revision {
		return
	}
	m.candidates[wf] = snap
}

// CandidatesForController returns the ready auto-dispatch activations owned
// by controllerID, per the cached recovered snapshots. Manual nodes, other
// controllers' nodes, claimed/leased, blocked, and legacy activations without
// a ready timestamp never appear. Attempt identity is absent until dispatch.
// A limit of zero or less returns every candidate (no truncation).
func (m *Manager) CandidatesForController(controllerID string, limit int) ([]ReadyCandidate, error) {
	m.candidateMu.Lock()
	defer m.candidateMu.Unlock()
	if !m.candidatesOK {
		return nil, ErrCandidatesNotRecovered
	}
	ids := make([]WorkflowID, 0, len(m.candidates))
	for id := range m.candidates {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	candidates := make([]ReadyCandidate, 0)
	for _, wf := range ids {
		snap := m.candidates[wf]
		if isTerminalStatus(snap.Instance.Status) {
			continue
		}
		for i := range snap.Instance.Activations {
			act := &snap.Instance.Activations[i]
			if !claimableActivationStatus(act.Status) || act.ReadyAt == nil || act.Dispatch == nil {
				continue
			}
			policy := *act.Dispatch
			if !policy.IsAuto() {
				continue
			}
			if policy.ControllerID != controllerID {
				continue
			}
			candidates = append(candidates, ReadyCandidate{
				Identity: ExecutionIdentity{
					SupervisorID: m.candidateSuper,
					WorkflowID:   wf,
					NodeID:       act.NodeID,
					ActivationID: act.ID,
				},
				Revision:       snap.Instance.Revision,
				ReadyAt:        *act.ReadyAt,
				Priority:       policy.priority(),
				ConcurrencyKey: policy.ConcurrencyKey,
				ControllerID:   policy.ControllerID,
			})
			if limit > 0 && len(candidates) >= limit {
				return candidates, nil
			}
		}
	}
	return candidates, nil
}

// commandClaim grants a lease on a pending/ready activation and hands the
// caller the raw owner token. It does not start any provider or allocate an
// attempt.
func (m *Manager) commandClaim(wf WorkflowID, payload json.RawMessage) (any, error) {
	var req commandClaimRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("claim payload: %w", err)
	}
	if req.NodeID == "" {
		return nil, errors.New("claim requires node_id")
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
	if !claimableActivationStatus(act.Status) {
		return nil, fmt.Errorf("cannot claim activation in status %q", act.Status)
	}
	tmpl, err := m.templateFor(&snap)
	if err != nil {
		return nil, err
	}
	node, err := findNode(tmpl, req.NodeID)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	ttl := leaseTTL(node, tmpl.DefaultLease)
	expiresAt := now.Add(ttl)
	token := newOwnerToken()
	leaseID := NewLeaseID()
	snap, err = m.store.ApplyCommand(wf, Command{
		ID:               NewCommandID(),
		Kind:             CommandClaim,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "claim-" + string(leaseID),
		Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: req.NodeID, ActivationID: act.ID},
		LeaseID:          leaseID,
		Actor:            req.Actor,
		Lease: &Lease{
			ID:           leaseID,
			ActivationID: act.ID,
			Owner:        req.Actor,
			TokenDigest:  ownerTokenDigest(token),
			AcquiredAt:   now,
			ExpiresAt:    expiresAt,
		},
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"lease_id":    string(leaseID),
		"owner_token": token,
		"expires_at":  expiresAt,
		"action":      node.Action,
		"revision":    snap.Instance.Revision,
	}, nil
}

type commandStartRequest struct {
	NodeID       NodeID              `json:"node_id"`
	ActivationID ActivationID        `json:"activation_id"`
	LeaseID      LeaseID             `json:"lease_id"`
	OwnerToken   string              `json:"owner_token"`
	Selection    *ExecutionSelection `json:"selection"`
}

// commandStart validates the lease/owner pair against the activation, records
// a new attempt durably, and dispatches it. Manual/external actions park in
// awaiting-completion; provider-backed actions require a registered executor.
func (m *Manager) commandStart(wf WorkflowID, payload json.RawMessage) (any, error) {
	var req commandStartRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("start payload: %w", err)
	}
	if req.NodeID == "" {
		return nil, errors.New("start requires node_id")
	}
	if req.LeaseID == "" {
		return nil, errors.New("start requires lease_id")
	}
	if req.OwnerToken == "" {
		return nil, errors.New("start requires owner_token")
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
	if act.Status != ActivationLeased {
		return nil, fmt.Errorf("cannot start activation in status %q", act.Status)
	}
	if act.Selection != nil && req.Selection != nil && !sameSelection(act.Selection, req.Selection) {
		// Never silently replace an already-pinned selection.
		return nil, ErrSelectionConflict
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
	attemptID := NewAttemptID()
	if node.Action.Kind == ActionManual || node.Action.Kind == ActionExternal {
		_, err := m.applyStart(wf, snap, act, req, attemptID)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"attempt_id":        string(attemptID),
			"status":            string(ActivationRunning),
			"requires_complete": true,
			"action":            node.Action,
		}, nil
	}
	exec := m.executor(node.Action.Kind)
	if exec == nil {
		return nil, fmt.Errorf("executor for action %q is unsupported until a later stage (executor not registered)", node.Action.Kind)
	}
	// Provider-backed starts share the controller dispatch boundary: the root
	// dispatch lock spans the final concurrency-key check and the attempt
	// commit. It is released before the executor starts so admission is never
	// acquired while holding it.
	concurrencyKey := ""
	if act.Dispatch != nil {
		concurrencyKey = act.Dispatch.ConcurrencyKey
	}
	unlockDispatch, err := m.lockDispatch()
	if err != nil {
		return nil, err
	}
	// Key derivation is a pure read (no lease recovery), so it cannot move
	// the revision the attempt commit below validates against.
	if concurrencyKey != "" {
		held, err := m.heldConcurrencyKeys(wf, act.ID)
		if err != nil {
			unlockDispatch()
			return nil, err
		}
		if held[concurrencyKey] {
			unlockDispatch()
			return nil, ErrConcurrencyKeyHeld
		}
	}
	selection := req.Selection
	if selection == nil {
		selection = act.Selection
	}
	startReq := req
	startReq.Selection = selection
	if _, err := m.applyStart(wf, snap, act, startReq, attemptID); err != nil {
		unlockDispatch()
		return nil, err
	}
	unlockDispatch()
	if err := exec.Dispatch(context.Background(), ExecutorContext{
		WorkflowID:   wf,
		NodeID:       req.NodeID,
		ActivationID: act.ID,
		AttemptID:    attemptID,
		LeaseID:      req.LeaseID,
		OwnerToken:   req.OwnerToken,
		LeaseTTL:     leaseTTL(node, tmpl.DefaultLease),
		Action:       node.Action,
		Selection:    selection,
	}); err != nil {
		return nil, err
	}
	return map[string]any{
		"attempt_id": string(attemptID),
		"status":     string(ActivationRunning),
		"action":     node.Action,
	}, nil
}

// applyStart durably records a new attempt on a leased activation. It must run
// under the store's revision guard so a concurrent start is rejected without
// mutating the snapshot.
func (m *Manager) applyStart(wf WorkflowID, snap Snapshot, act *Activation, req commandStartRequest, attemptID AttemptID) (Snapshot, error) {
	return m.store.ApplyCommand(wf, Command{
		ID:               NewCommandID(),
		Kind:             CommandStart,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "start-" + string(attemptID),
		Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: req.NodeID, ActivationID: act.ID, AttemptID: attemptID},
		LeaseID:          req.LeaseID,
		Selection:        req.Selection,
	})
}

// Heartbeat is the executor-facing convenience wrapper around the "heartbeat"
// command (see commandHeartbeat): it renews the activation's active lease for
// a caller identified by the claim's (leaseID, ownerToken) pair — the same
// pair a claim returns and that the start carries into
// ExecutorContext.OwnerToken. It never transitions the activation status.
func (m *Manager) Heartbeat(wf WorkflowID, nodeID NodeID, activationID ActivationID, leaseID LeaseID, ownerToken string) error {
	payload, err := json.Marshal(map[string]string{
		"op":            "heartbeat",
		"node_id":       string(nodeID),
		"activation_id": string(activationID),
		"lease_id":      string(leaseID),
		"owner_token":   ownerToken,
	})
	if err != nil {
		return err
	}
	_, err = m.commandHeartbeat(wf, payload)
	return err
}

// RecordAttemptTerminated records the terminal status of an already-started
// attempt after its backend run finishes. It is called by a runtime executor
// (the stable supervisor's direct-run executor) on every terminal path:
// success, failure, panic, cancellation, timeout, and provider-start error.
// It is idempotent per attempt so duplicate terminations are safe.
//
// The optional trailing marker args carry inert terminal-marker evidence
// (marker kind first, marker label second). Markers are recorded on the
// attempt as evidence only; they are never a workflow-store command and
// cannot satisfy an activation or select an outcome.
func (m *Manager) RecordAttemptTerminated(wf WorkflowID, nodeID NodeID, activationID ActivationID, attemptID AttemptID, leaseID LeaseID, status AttemptStatus, marker ...string) error {
	if status == "" {
		return errors.New("attempt termination status is required")
	}
	var markerKind, markerLabel string
	if len(marker) >= 1 {
		markerKind = marker[0]
	}
	if len(marker) >= 2 {
		markerLabel = marker[1]
	}
	// The command is idempotent per attempt (stable "terminate-<attemptID>"
	// key), so it is safe to retry: a concurrent command on the same instance
	// can bump the revision in the window between the read and the apply. Without
	// this, a lost termination leaves the activation running with its lease held
	// and no reaper, deadlocking the instance. Bounded to avoid spinning.
	const maxTerminateAttempts = 4
	for attempt := 0; attempt < maxTerminateAttempts; attempt++ {
		snap, exists, err := m.store.loadCurrent(wf)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("workflow not found: %s", wf)
		}
		_, err = m.store.ApplyCommand(wf, Command{
			ID:               NewCommandID(),
			Kind:             CommandTerminate,
			ExpectedRevision: snap.Instance.Revision,
			IdempotencyKey:   "terminate-" + string(attemptID),
			Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: nodeID, ActivationID: activationID, AttemptID: attemptID},
			LeaseID:          leaseID,
			AttemptStatus:    status,
			MarkerKind:       markerKind,
			MarkerLabel:      markerLabel,
		})
		if err == nil {
			return nil
		}
		if errors.Is(err, errDuplicateIdempotency) {
			// Already recorded for this attempt.
			return nil
		}
		if !errors.Is(err, errRevisionMismatch) {
			return err
		}
		// Optimistic-concurrency conflict; re-read under a fresh lock and retry.
	}
	return fmt.Errorf("terminate attempt %s: revision kept moving under concurrent commands", attemptID)
}
