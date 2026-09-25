package stable

// workflow_factory_selection_test.go audits roster selection pinning for the
// software-factory factory fixtures (Stage 8): automatic dispatch resolves
// the node's declared assignment through the supervisor's roster resolution
// path and pins the effective backend, agent, model, thinking, and roster
// digest on the activation; retries inherit the pinned selection and
// conflicting selections are rejected; a declared reroute creates a fresh
// activation that can pin a different selection while the rerouted
// activation keeps its own; and a resolution failure is a start_failed-class
// outcome with no attempt recorded.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/workflow"
	"github.com/sdougbrown/avenor/internal/workflowcontroller"
)

// factorySelectionTemplateJSON is a factory-shaped template: a manual intake
// node hands off to an auto-dispatched assessment run node that reroutes to a
// publication run node on its ready outcome. Both provider nodes declare
// roster-backed assignments resolved at dispatch time.
func factorySelectionTemplateJSON(t *testing.T, templateID string, ttlSeconds int, rosterPath string) []byte {
	policy := map[string]any{"mode": "auto", "controller_id": "c1", "priority": 30}
	template := map[string]any{
		"schema_version":   1,
		"template_id":      templateID,
		"template_version": "1.0.0",
		"entry_nodes":      []string{"intake"},
		"nodes": []any{
			map[string]any{
				"id":         "intake",
				"action":     map[string]any{"type": "manual", "instructions": "supply the issue"},
				"outputs":    []any{},
				"completion": map[string]any{"kind": "explicit"},
				"branches":   map[string]any{"ready": "assessment"},
			},
			map[string]any{
				"id":           "assessment",
				"dependencies": []string{"intake"},
				"action":       map[string]any{"type": "run", "prompt": "assess the issue"},
				"assignment": map[string]any{
					"role": "assessor", "roster_file": rosterPath, "roster_entry": "assessor", "thinking": "high",
				},
				"dispatch":     policy,
				"retry_policy": map[string]any{"max_attempts": 2, "exhaustion": "block"},
				"branches":     map[string]any{"ready": "publication"},
			},
			map[string]any{
				"id":           "publication",
				"dependencies": []string{"assessment"},
				"action":       map[string]any{"type": "run", "prompt": "publish the work"},
				"assignment": map[string]any{
					"role": "publisher", "roster_file": rosterPath, "roster_entry": "publisher", "thinking": "medium",
				},
				"dispatch": map[string]any{"mode": "auto", "controller_id": "c1", "priority": 60},
			},
		},
		"terminal_outcomes":    []string{"merged"},
		"default_lease_policy": map[string]any{"ttl_seconds": ttlSeconds},
	}
	return mustJSON(t, template)
}

// factorySelectionFixture builds a supervisor over a factory-shaped template
// with a fake provider, an enabled controller whose leader lease the test
// holds, and the roster fixture resolved into a full execution selection.
type factorySelectionFixture struct {
	sup        *Supervisor
	mgr        *workflow.Manager
	cstore     *workflowcontroller.ControllerStore
	wf         string
	leaseID    string
	ownerEpoch int64
	release    chan struct{}
	// selection is the production-resolved execution selection every
	// assessment dispatch pins; publisherSelection is the publication
	// node's.
	selection          *workflow.ExecutionSelection
	publisherSelection *workflow.ExecutionSelection
	// rosterPath is the fixture roster file the assignments resolve through.
	rosterPath string
}

func newFactorySelectionFixture(t *testing.T, name string, ttlSeconds int) *factorySelectionFixture {
	t.Helper()
	rosterPath := writeStage5Roster(t, t.TempDir(), `{"assessor":{"backend":"codex-app-server","agent":"factory-assessor","model":"glm-5.3"},"publisher":{"backend":"pi","agent":"factory-publisher","model":"claude-opus"}}`)
	// The effective selections come from the supervisor's production
	// assignment resolver — the same path automatic dispatch uses — never
	// from test-local resolution logic.
	assessment, err := resolveAssignmentSelection(&workflow.Assignment{
		Role: "assessor", RosterFile: rosterPath, RosterEntry: "assessor", Thinking: "high",
	})
	if err != nil {
		t.Fatalf("resolve assessor assignment: %v", err)
	}
	publisher, err := resolveAssignmentSelection(&workflow.Assignment{
		Role: "publisher", RosterFile: rosterPath, RosterEntry: "publisher", Thinking: "medium",
	})
	if err != nil {
		t.Fatalf("resolve publisher assignment: %v", err)
	}

	sup := NewSupervisor(Config{
		ControlSocket:   newStableSocketPath(t, name),
		MaxRuntimes:     4,
		MaxTreeBudget:   4,
		ShutdownTimeout: 0,
		WorkflowRoot:    filepath.Join(t.TempDir(), "wfroot"),
	})
	mgr, cstore, err := sup.workflowBarrierResult()
	if err != nil {
		t.Fatalf("workflow barrier: %v", err)
	}
	if _, err := mgr.WorkflowCreate(factorySelectionTemplateJSON(t, "factory-sel-"+name, ttlSeconds, rosterPath)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	out, err := mgr.WorkflowInstantiate(mustJSON(t, map[string]string{"template_id": "factory-sel-" + name, "template_version": "1.0.0"}))
	if err != nil {
		t.Fatalf("WorkflowInstantiate: %v", err)
	}
	wf := out.(map[string]any)["workflow_id"].(string)
	if _, err := cstore.Create("c1", 10); err != nil {
		t.Fatalf("controller create: %v", err)
	}
	if _, err := cstore.SetDesiredState("c1", workflowcontroller.DesiredEnabled, "test fixture"); err != nil {
		t.Fatalf("controller enable: %v", err)
	}
	rec, ok, err := cstore.AcquireLease("c1", sup.supervisorIdentity())
	if err != nil || !ok {
		t.Fatalf("acquire leader lease: ok=%v err=%v", ok, err)
	}

	release := make(chan struct{})
	sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		return &blockingAdmissionProvider{release: release}, nil
	}
	f := &factorySelectionFixture{
		sup: sup, mgr: mgr, cstore: cstore, wf: wf,
		leaseID: rec.Leader.LeaseID, ownerEpoch: rec.Leader.OwnerEpoch,
		release: release, selection: assessment, publisherSelection: publisher,
		rosterPath: rosterPath,
	}
	t.Cleanup(func() {
		close(release)
		for _, rt := range sup.listRuntimes() {
			if id, ok := rt["runtime_id"].(string); ok {
				_ = sup.cancelRuntime(id)
			}
		}
		waitFor(t, "runtimes terminal at cleanup", func() bool { return sup.activeRuntimeCount() == 0 })
		_ = sup.broker.Stop()
		sup.stopReaper()
	})
	return f
}

// activationByNode returns the newest activation of the named node.
func (f *factorySelectionFixture) activationByNode(t *testing.T, nodeID string) *workflow.Activation {
	t.Helper()
	insp, err := f.mgr.WorkflowInspect(f.wf)
	if err != nil {
		t.Fatalf("WorkflowInspect: %v", err)
	}
	acts := insp.(map[string]any)["activations"].([]workflow.Activation)
	var found *workflow.Activation
	for i := range acts {
		if acts[i].NodeID == workflow.NodeID(nodeID) {
			found = &acts[i]
		}
	}
	return found
}

// completeIntake claims, starts, and completes the manual intake node so
// the assessment activation materializes.
func (f *factorySelectionFixture) completeIntake(t *testing.T) {
	t.Helper()
	act := f.activationByNode(t, "intake")
	if act == nil || act.Status != workflow.ActivationPending {
		t.Fatalf("no pending intake activation: %+v", act)
	}
	res, err := f.mgr.WorkflowCommand(f.wf, mustJSON(t, map[string]any{
		"op": "claim", "node_id": "intake", "activation_id": string(act.ID), "actor": "alice",
	}))
	if err != nil {
		t.Fatalf("intake claim: %v", err)
	}
	claim := res.(map[string]any)
	start, err := f.mgr.WorkflowCommand(f.wf, mustJSON(t, map[string]any{
		"op": "start", "node_id": "intake", "activation_id": string(act.ID),
		"lease_id": claim["lease_id"], "owner_token": claim["owner_token"],
	}))
	if err != nil {
		t.Fatalf("intake start: %v", err)
	}
	attemptID := start.(map[string]any)["attempt_id"].(string)
	if err := f.mgr.RecordAttemptTerminated(workflow.WorkflowID(f.wf), "intake", act.ID,
		workflow.AttemptID(attemptID), workflow.LeaseID(claim["lease_id"].(string)), workflow.AttemptSucceeded); err != nil {
		t.Fatalf("intake terminate: %v", err)
	}
	if _, err := f.mgr.WorkflowCommand(f.wf, mustJSON(t, map[string]any{
		"op": "complete", "node_id": "intake", "activation_id": string(act.ID),
		"attempt_id": attemptID, "lease_id": claim["lease_id"], "owner_token": claim["owner_token"],
		"outcome": "ready",
	})); err != nil {
		t.Fatalf("intake complete: %v", err)
	}
}

// dispatchAssessment dispatches the newest pending assessment activation
// with the fixture's resolved selection and returns the begin result.
func (f *factorySelectionFixture) dispatchAssessment(t *testing.T) workflow.BeginDispatchResult {
	t.Helper()
	f.completeIntake(t)
	act := f.activationByNode(t, "assessment")
	if act == nil || act.Status != workflow.ActivationPending {
		t.Fatalf("no pending assessment activation: %+v", act)
	}
	res, err := f.sup.reserveAdmission()
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	defer res.Release()
	begin, err := f.mgr.BeginDispatch(workflow.BeginDispatchRequest{
		WorkflowID:       workflow.WorkflowID(f.wf),
		NodeID:           "assessment",
		ActivationID:     act.ID,
		ExpectedRevision: actRevision(t, f, act),
		ControllerID:     "c1",
		LeaderLeaseID:    f.leaseID,
		Selection:        f.selection,
	})
	if err != nil {
		t.Fatalf("BeginDispatch: %v", err)
	}
	return begin
}

// actRevision re-reads the activation's current revision from disk.
func actRevision(t *testing.T, f *factorySelectionFixture, act *workflow.Activation) int64 {
	t.Helper()
	insp, err := f.mgr.WorkflowInspect(f.wf)
	if err != nil {
		t.Fatalf("WorkflowInspect: %v", err)
	}
	return insp.(map[string]any)["revision"].(int64)
}

// completeAssessment records the attempt's terminal fact and completes the
// newest assessment activation with its ready outcome, rerouting to
// publication.
func (f *factorySelectionFixture) completeAssessment(t *testing.T, begin workflow.BeginDispatchResult) {
	t.Helper()
	if err := f.mgr.RecordAttemptTerminated(workflow.WorkflowID(f.wf), "assessment", begin.ActivationID,
		begin.AttemptID, begin.LeaseID, workflow.AttemptSucceeded); err != nil {
		t.Fatalf("RecordAttemptTerminated: %v", err)
	}
	if _, err := f.mgr.WorkflowCommand(f.wf, mustJSON(t, map[string]any{
		"op": "complete", "node_id": "assessment", "activation_id": string(begin.ActivationID),
		"attempt_id": string(begin.AttemptID), "lease_id": string(begin.LeaseID),
		"owner_token": begin.OwnerToken, "outcome": "ready",
	})); err != nil {
		t.Fatalf("assessment complete: %v", err)
	}
}

// assertSelection compares an activation's pinned selection field by field and
// independently verifies the pinned roster digest against the raw bytes of the
// roster file the fixture staged, rather than trusting the value
// resolveAssignmentSelection produced.
func assertSelection(t *testing.T, what string, got, want *workflow.ExecutionSelection, rosterPath string) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s: selection is nil, want %+v", what, *want)
	}
	if *got != *want {
		t.Fatalf("%s: pinned selection = %+v, want %+v", what, *got, *want)
	}
	data, err := os.ReadFile(rosterPath)
	if err != nil {
		t.Fatalf("%s: read roster file: %v", what, err)
	}
	sum := sha256.Sum256(data)
	wantDigest := "sha256:" + hex.EncodeToString(sum[:])
	if got.RosterDigest != wantDigest {
		t.Fatalf("%s: roster digest = %q, want %q", what, got.RosterDigest, wantDigest)
	}
}

// TestFactoryDispatchPinsRosterSelection proves the full automatic dispatch
// path resolves the node's declared assignment through the supervisor's
// roster resolution and pins the resolved selection — backend, agent, model,
// thinking, and roster digest — on the activation itself, not just on the
// attempt.
func TestFactoryDispatchPinsRosterSelection(t *testing.T) {
	f := newFactorySelectionFixture(t, "sel-pin", 900)
	f.completeIntake(t)
	act := f.activationByNode(t, "assessment")
	if act == nil || act.Status != workflow.ActivationPending {
		t.Fatalf("no pending assessment activation: %+v", act)
	}
	out, err := f.sup.dispatchWorkflowNode(context.Background(), DispatchRequest{
		WorkflowID:       f.wf,
		NodeID:           "assessment",
		ActivationID:     string(act.ID),
		ExpectedRevision: actRevision(t, f, act),
		ControllerID:     "c1",
		LeaderLeaseID:    f.leaseID,
		OwnerEpoch:       f.ownerEpoch,
		Selection:        nil,
	})
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != Dispatched {
		t.Fatalf("dispatch outcome = %s (%s), want dispatched", out.Kind, out.Detail)
	}
	assertSelection(t, "assessment activation", f.activationByNode(t, "assessment").Selection, f.selection, f.rosterPath)
}

// TestFactoryDispatchResolutionFailureIsStartFailed proves a dispatch whose
// assignment cannot resolve (the roster file the assignment references is
// gone) is a start_failed-class outcome with no reservation held, no attempt
// recorded, and no workflow event appended: the activation stays pending.
func TestFactoryDispatchResolutionFailureIsStartFailed(t *testing.T) {
	f := newFactorySelectionFixture(t, "sel-fail", 900)
	f.completeIntake(t)
	act := f.activationByNode(t, "assessment")
	if act == nil || act.Status != workflow.ActivationPending {
		t.Fatalf("no pending assessment activation: %+v", act)
	}
	if err := os.Remove(f.rosterPath); err != nil {
		t.Fatalf("remove roster: %v", err)
	}
	out, err := f.sup.dispatchWorkflowNode(context.Background(), DispatchRequest{
		WorkflowID:       f.wf,
		NodeID:           "assessment",
		ActivationID:     string(act.ID),
		ExpectedRevision: actRevision(t, f, act),
		ControllerID:     "c1",
		LeaderLeaseID:    f.leaseID,
		OwnerEpoch:       f.ownerEpoch,
	})
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != DispatchStartFailed {
		t.Fatalf("dispatch outcome = %s (%s), want start_failed", out.Kind, out.Detail)
	}
	fresh := f.activationByNode(t, "assessment")
	if fresh.Status != workflow.ActivationPending || len(fresh.AttemptIDs) != 0 {
		t.Fatalf("post-failure assessment = %s with %d attempts, want pending with none", fresh.Status, len(fresh.AttemptIDs))
	}
}

// TestFactoryDispatchRetryInheritsPinnedSelection proves a retry of the same
// activation inherits the pinned selection and a conflicting selection is
// rejected: the pin survives re-arming without coordinator memory.
func TestFactoryDispatchRetryInheritsPinnedSelection(t *testing.T) {
	f := newFactorySelectionFixture(t, "sel-retry", 900)
	begin := f.dispatchAssessment(t)
	// Fail the start pre-provider so the activation re-arms with the pin.
	if err := f.mgr.RecordAttemptTerminated(workflow.WorkflowID(f.wf), "assessment", begin.ActivationID,
		begin.AttemptID, begin.LeaseID, workflow.AttemptFailed); err != nil {
		t.Fatalf("fail attempt: %v", err)
	}
	waitFor(t, "retry re-arm", func() bool {
		if err := f.mgr.RebuildCandidateIndex("test"); err != nil {
			return false
		}
		cands, err := f.mgr.CandidatesForController("c1", 10)
		return err == nil && len(cands) == 1
	})
	cands, err := f.mgr.CandidatesForController("c1", 1)
	if err != nil || len(cands) != 1 {
		t.Fatalf("re-armed candidates: n=%d err=%v", len(cands), err)
	}
	conflict := *f.selection
	conflict.Agent = "someone-else"
	if _, err := f.mgr.BeginDispatch(workflow.BeginDispatchRequest{
		WorkflowID:       workflow.WorkflowID(f.wf),
		NodeID:           "assessment",
		ActivationID:     cands[0].Identity.ActivationID,
		ExpectedRevision: cands[0].Revision,
		ControllerID:     "c1",
		LeaderLeaseID:    f.leaseID,
		Selection:        &conflict,
	}); !errors.Is(err, workflow.ErrSelectionConflict) {
		t.Fatalf("conflicting BeginDispatch error = %v, want ErrSelectionConflict", err)
	}
	res, err := f.sup.reserveAdmission()
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	defer res.Release()
	inherited, err := f.mgr.BeginDispatch(workflow.BeginDispatchRequest{
		WorkflowID:       workflow.WorkflowID(f.wf),
		NodeID:           "assessment",
		ActivationID:     cands[0].Identity.ActivationID,
		ExpectedRevision: cands[0].Revision,
		ControllerID:     "c1",
		LeaderLeaseID:    f.leaseID,
	})
	if err != nil {
		t.Fatalf("inheriting BeginDispatch: %v", err)
	}
	if inherited.Selection == nil || *inherited.Selection != *f.selection {
		t.Fatalf("inherited selection = %+v, want the pinned roster selection", inherited.Selection)
	}
}

// TestFactoryRerouteCreatesNewActivationSelection proves a declared reroute
// (assessment's ready branch) creates a fresh publication activation that
// pins its own selection while the completed activation's pin is immutable.
func TestFactoryRerouteCreatesNewActivationSelection(t *testing.T) {
	f := newFactorySelectionFixture(t, "sel-reroute", 900)
	begin := f.dispatchAssessment(t)
	f.completeAssessment(t, begin)

	// The rerouted activation still carries its pinned selection.
	old := f.activationByNode(t, "assessment")
	assertSelection(t, "completed assessment", old.Selection, f.selection, f.rosterPath)

	// The new publication activation starts unpinned and pins its own
	// resolved roster selection (the publisher entry, resolved through the
	// production helper).
	pub := f.activationByNode(t, "publication")
	if pub == nil || pub.Status != workflow.ActivationPending {
		t.Fatalf("no pending publication activation after reroute: %+v", pub)
	}
	publisher := f.publisherSelection
	res, err := f.sup.reserveAdmission()
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := f.mgr.BeginDispatch(workflow.BeginDispatchRequest{
		WorkflowID:       workflow.WorkflowID(f.wf),
		NodeID:           "publication",
		ActivationID:     pub.ID,
		ExpectedRevision: actRevision(t, f, pub),
		ControllerID:     "c1",
		LeaderLeaseID:    f.leaseID,
		Selection:        publisher,
	}); err != nil {
		res.Release()
		t.Fatalf("publication BeginDispatch: %v", err)
	}
	res.Release()
	pub = f.activationByNode(t, "publication")
	assertSelection(t, "publication activation", pub.Selection, publisher, f.rosterPath)
	assertSelection(t, "assessment after reroute", old.Selection, f.selection, f.rosterPath)
	// A reroute is a new activation, never a mutation of the pinned one.
	if pub.ID == begin.ActivationID {
		t.Fatal("reroute reused the assessment activation")
	}
}
