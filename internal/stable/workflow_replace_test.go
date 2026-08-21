package stable

// Internal tests for the Stage 13 (phase 3) supervisor-side replacement path
// and the owner-token heartbeat seam, driving the stable supervisor's
// workflow manager (sup.workflowManager()). No provider is spawned: the
// template node is manual, so a start parks the attempt in the kernel and
// the lease/replacement/heartbeat machinery is exercised without a live
// runtime. Sockets are scoped to t.TempDir() via newStableSocketPath.

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/workflow"
)

// wfReplaceTemplateJSON is a single-node manual template carrying a retry
// policy (block, budget 2). The action kind is irrelevant to the
// lease/replacement contract; manual keeps the test hermetic (a run action
// would dispatch a real spawn through the executor).
const wfReplaceTemplateJSON = `{
  "schema_version": 1,
  "template_id": "wf-replace",
  "template_version": "1",
  "entry_nodes": ["start"],
  "nodes": [{"id": "start", "action": {"type": "manual", "instructions": "do the thing"}, "retry_policy": {"max_attempts": 2, "exhaustion": "block"}}],
  "terminal_outcomes": ["done"]
}`

// newWfReplaceSupervisor builds a supervisor whose workflow manager is rooted
// in an isolated temp dir with the replacement template stored, and returns
// the supervisor, its manager, a store over the same root, and a fresh
// instance of the template.
func newWfReplaceSupervisor(t *testing.T, socketName string) (*Supervisor, *workflow.Manager, *workflow.Store, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "wfroot")
	store := workflow.New(root)
	if err := store.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	sup := NewSupervisor(Config{ControlSocket: newStableSocketPath(t, socketName), MaxRuntimes: 1, WorkflowRoot: root})
	mgr := sup.workflowManager()
	if _, err := mgr.WorkflowCreate([]byte(wfReplaceTemplateJSON)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	inst, err := mgr.WorkflowInstantiate([]byte(`{"template_id": "wf-replace", "template_version": "1"}`))
	if err != nil {
		t.Fatalf("WorkflowInstantiate: %v", err)
	}
	wfID, _ := inst.(map[string]any)["workflow_id"].(string)
	if wfID == "" {
		t.Fatalf("instantiate result missing workflow_id: %#v", inst)
	}
	return sup, mgr, store, wfID
}

// wfReplaceActivation returns the activation for the start node and the
// current instance revision.
func wfReplaceActivation(t *testing.T, m *workflow.Manager, wfID string) (workflow.Activation, int64) {
	t.Helper()
	res, err := m.WorkflowInspect(wfID)
	if err != nil {
		t.Fatalf("WorkflowInspect: %v", err)
	}
	mm, ok := res.(map[string]any)
	if !ok {
		t.Fatalf("inspect result = %#v, want map", res)
	}
	acts, ok := mm["activations"].([]workflow.Activation)
	if !ok || len(acts) == 0 {
		t.Fatalf("inspect activations = %#v, want one activation", mm["activations"])
	}
	rev, ok := mm["revision"].(int64)
	if !ok {
		t.Fatalf("inspect revision = %#v, want int64", mm["revision"])
	}
	return acts[0], rev
}

// revisionOf returns the current instance revision (helper for crafted store
// commands, which need an exact ExpectedRevision).
func revisionOf(t *testing.T, m *workflow.Manager, wfID string) int64 {
	t.Helper()
	_, rev := wfReplaceActivation(t, m, wfID)
	return rev
}

// mustWfReplaceCommand issues a workflow command through the manager.
func mustWfReplaceCommand(t *testing.T, m *workflow.Manager, wfID string, payload map[string]any) map[string]any {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal %s: %v", payload["op"], err)
	}
	out, err := m.WorkflowCommand(wfID, data)
	if err != nil {
		t.Fatalf("command %s: %v", payload["op"], err)
	}
	mm, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("command %s result = %#v, want map", payload["op"], out)
	}
	return mm
}

// TestWorkflowReplacementClaimAfterExpiry proves the supervisor-side
// replacement path through the stable manager: a node's first attempt
// (A1) runs on a lease whose expiry has passed; the manager's live detector
// expires it durably; a replacement claim + start (A2) through the manager
// lands on the SAME activation with the prior attempt preserved, and the
// stable manager reports the activation running for A2.
func TestWorkflowReplacementClaimAfterExpiry(t *testing.T) {
	_, mgr, store, wfID := newWfReplaceSupervisor(t, "wf-replace")

	act, _ := wfReplaceActivation(t, mgr, wfID)

	// A1: a crafted lease whose expiry is already in the past, claimed and
	// started at the store level (the holder's clock slipped past the TTL).
	past := time.Now().UTC().Add(-time.Hour)
	if _, err := store.ApplyCommand(workflow.WorkflowID(wfID), workflow.Command{
		Kind:             workflow.CommandClaim,
		ExpectedRevision: revisionOf(t, mgr, wfID),
		IdempotencyKey:   "wfreplace-claim-a1",
		Identity:         workflow.ExecutionIdentity{WorkflowID: workflow.WorkflowID(wfID), NodeID: "start", ActivationID: act.ID},
		LeaseID:          "wfreplace-lease-1",
		Actor:            "alice",
		Lease: &workflow.Lease{
			ID:           "wfreplace-lease-1",
			ActivationID: act.ID,
			Owner:        "alice",
			AcquiredAt:   past.Add(-time.Minute),
			ExpiresAt:    past,
		},
	}); err != nil {
		t.Fatalf("crafted claim A1: %v", err)
	}
	if _, err := store.ApplyCommand(workflow.WorkflowID(wfID), workflow.Command{
		Kind:             workflow.CommandStart,
		ExpectedRevision: revisionOf(t, mgr, wfID),
		IdempotencyKey:   "wfreplace-start-a1",
		Identity:         workflow.ExecutionIdentity{WorkflowID: workflow.WorkflowID(wfID), NodeID: "start", ActivationID: act.ID, AttemptID: "wfreplace-att-1"},
		LeaseID:          "wfreplace-lease-1",
	}); err != nil {
		t.Fatalf("start A1: %v", err)
	}

	// The manager's live detector expires the stale lease.
	if _, err := mgr.ExpireStaleLeases(); err != nil {
		t.Fatalf("ExpireStaleLeases: %v", err)
	}
	expired, _ := wfReplaceActivation(t, mgr, wfID)
	if expired.Status != workflow.ActivationLeaseExpired {
		t.Fatalf("status after expiry = %s, want lease_expired", expired.Status)
	}
	if expired.ActiveLease != nil {
		t.Fatalf("lease after expiry = %+v, want released", expired.ActiveLease)
	}

	// Replacement claim + start A2 through the manager.
	claim := mustWfReplaceCommand(t, mgr, wfID, map[string]any{
		"op": "claim", "node_id": "start", "activation_id": string(expired.ID), "actor": "bob",
	})
	lease2, _ := claim["lease_id"].(string)
	token2, _ := claim["owner_token"].(string)
	if lease2 == "" || token2 == "" || lease2 == "wfreplace-lease-1" {
		t.Fatalf("replacement claim = %#v, want a fresh lease distinct from A1's", claim)
	}
	start := mustWfReplaceCommand(t, mgr, wfID, map[string]any{
		"op":            "start",
		"node_id":       "start",
		"activation_id": string(expired.ID),
		"lease_id":      lease2,
		"owner_token":   token2,
	})
	attempt2, _ := start["attempt_id"].(string)
	if attempt2 == "" || attempt2 == "wfreplace-att-1" {
		t.Fatalf("replacement start attempt = %q, want a distinct attempt", attempt2)
	}

	// SAME activation, prior attempt preserved, running for A2.
	running, _ := wfReplaceActivation(t, mgr, wfID)
	if running.ID != act.ID {
		t.Fatalf("replacement start moved to activation %s, want the same %s", running.ID, act.ID)
	}
	if running.Status != workflow.ActivationRunning {
		t.Fatalf("status for A2 = %s, want running", running.Status)
	}
	if len(running.AttemptIDs) != 2 || string(running.AttemptIDs[0]) != "wfreplace-att-1" || string(running.AttemptIDs[1]) != attempt2 {
		t.Fatalf("attempt ids = %v, want [wfreplace-att-1 %s] on the same activation", running.AttemptIDs, attempt2)
	}
	res, err := mgr.WorkflowInspect(wfID)
	if err != nil {
		t.Fatalf("WorkflowInspect: %v", err)
	}
	attempts, ok := res.(map[string]any)["attempts"].([]workflow.Attempt)
	if !ok || len(attempts) != 2 {
		t.Fatalf("attempt records = %#v, want 2 (prior attempt preserved)", res.(map[string]any)["attempts"])
	}
}

// TestWorkflowOwnerTokenHeartbeatSeam proves the executor-layer seam: after a
// claim returns an owner token, manager.Heartbeat(wf, node, activation,
// lease, token) — the same manager the executors reach through
// sup.workflowManager() — renews the lease (ExpiresAt advances,
// LastHeartbeatAt stamped). A compile-time assertion ties the seam to the
// executor context field the start now carries.
func TestWorkflowOwnerTokenHeartbeatSeam(t *testing.T) {
	sup, mgr, _, wfID := newWfReplaceSupervisor(t, "wf-hb-seam")
	_ = sup

	// Compile-time proof of the executor-layer seam: the executor context
	// carries the raw owner token, and the manager the executor reaches via
	// e.sup.workflowManager() exposes the typed Heartbeat.
	var ec workflow.ExecutorContext
	_ = ec.OwnerToken
	var heartbeat func(workflow.WorkflowID, workflow.NodeID, workflow.ActivationID, workflow.LeaseID, string) error = mgr.Heartbeat

	act, _ := wfReplaceActivation(t, mgr, wfID)
	claim := mustWfReplaceCommand(t, mgr, wfID, map[string]any{
		"op": "claim", "node_id": "start", "activation_id": string(act.ID), "actor": "alice",
	})
	lease, _ := claim["lease_id"].(string)
	token, _ := claim["owner_token"].(string)
	if lease == "" || token == "" {
		t.Fatalf("claim = %#v, want lease_id and owner_token", claim)
	}
	before, _ := claim["expires_at"].(time.Time)

	// The seam renews the lease.
	if err := heartbeat(workflow.WorkflowID(wfID), "start", act.ID, workflow.LeaseID(lease), token); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	// A wrong token is rejected through the seam.
	if err := heartbeat(workflow.WorkflowID(wfID), "start", act.ID, workflow.LeaseID(lease), "wrong"); err == nil {
		t.Fatal("wrong-token heartbeat succeeded, want owner token mismatch")
	}

	renewed, _ := wfReplaceActivation(t, mgr, wfID)
	if renewed.ActiveLease == nil || renewed.ActiveLease.ID != workflow.LeaseID(lease) {
		t.Fatalf("lease after heartbeat = %+v, want %s retained", renewed.ActiveLease, lease)
	}
	if !renewed.ActiveLease.ExpiresAt.After(before) {
		t.Fatalf("expiry after heartbeat = %v, want advanced past %v", renewed.ActiveLease.ExpiresAt, before)
	}
	if renewed.ActiveLease.LastHeartbeatAt == nil || renewed.ActiveLease.LastHeartbeatAt.IsZero() {
		t.Fatalf("LastHeartbeatAt after heartbeat = %v, want a stamped time", renewed.ActiveLease.LastHeartbeatAt)
	}
}
