package stable

// workflow_factory_e2e_test.go runs the shipped software-factory work
// template (templates/software-factory/work.json@1.2.0) end to end over a
// real supervisor with fake providers and the Stage 7b fixture adapters:
// one work item flows from intake through publication, the auto external
// review parks and polls its bound gates, a clean verdict stops at the
// manual merge-authorization human gate, a changes_requested verdict routes
// correction and re-publishes under a new exact head the old results cannot
// land on, two work items resolving different worktree params run
// concurrently while two sharing one worktree param serialize, and a fresh
// supervisor on the same root resumes the parked work without coordinator
// memory.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/workflow"
	"github.com/sdougbrown/avenor/internal/workflowcontroller"
)

// factoryTemplateDir is the repo-relative location of the shipped factory
// template and its prompt/loop/team fixtures.
const factoryTemplateDir = "../../templates/software-factory"

// factoryWorkTemplateJSON loads the shipped work template and rewrites its
// prompt_file/loop_file/team_file/roster_file references to absolute repo
// paths so the template dispatches from the test's working directory without
// modification of the shipped fixture.
func factoryWorkTemplateJSON(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(factoryTemplateDir, "work.json"))
	if err != nil {
		t.Fatalf("read work template: %v", err)
	}
	var template map[string]any
	if err := json.Unmarshal(data, &template); err != nil {
		t.Fatalf("decode work template: %v", err)
	}
	abs := func(rel string) string {
		absPath, err := filepath.Abs(filepath.Join(factoryTemplateDir, rel))
		if err != nil {
			t.Fatalf("resolve %s: %v", rel, err)
		}
		return absPath
	}
	for _, raw := range template["nodes"].([]any) {
		node := raw.(map[string]any)
		action := node["action"].(map[string]any)
		if rel, ok := action["prompt_file"].(string); ok {
			action["prompt_file"] = abs(rel)
		}
		if rel, ok := action["loop_file"].(string); ok {
			action["loop_file"] = abs(rel)
		}
		if rel, ok := action["team_file"].(string); ok {
			action["team_file"] = abs(rel)
		}
		if assignment, ok := node["assignment"].(map[string]any); ok {
			if rel, ok := assignment["roster_file"].(string); ok {
				assignment["roster_file"] = abs(rel)
			}
		}
	}
	out, err := json.Marshal(template)
	if err != nil {
		t.Fatalf("re-encode work template: %v", err)
	}
	return out
}

// factoryE2E drives the shipped work template over a supervisor with fixture
// adapters and a fake provider, with the software-factory controller
// registered (disabled until the test enables it).
type factoryE2E struct {
	sup         *Supervisor
	mgr         *workflow.Manager
	cstore      *workflowcontroller.ControllerStore
	root        string
	adapterDir  string
	wf          string
	release     chan struct{}
	releaseOnce sync.Once
}

// newFactoryE2E stages the named fixture adapter scripts as circleci-pipeline
// and github-pr-review manifests, registers and instantiates the shipped
// work template, and creates the disabled software-factory controller.
func newFactoryE2E(t *testing.T, name string, ciScript, reviewScript string) *factoryE2E {
	t.Helper()
	root := filepath.Join(t.TempDir(), "wfroot")
	adapterDir := t.TempDir()
	for id, script := range map[string]string{
		"circleci-pipeline": ciScript,
		"github-pr-review":  reviewScript,
	} {
		exe := stagePollFixture(t, adapterDir, script)
		writePollManifest(t, adapterDir, id+".json", id, exe)
	}
	sup := NewSupervisor(Config{
		ControlSocket:      newStableSocketPath(t, name),
		WorkflowRoot:       root,
		WorkflowAdapterDir: adapterDir,
		MaxRuntimes:        16,
		MaxTreeBudget:      16,
		ShutdownTimeout:    0,
	})
	sup.controllerRenewInterval = 25 * time.Millisecond
	sup.controllerPollBaseDelay = 200 * time.Millisecond
	mgr, cstore, err := sup.workflowBarrierResult()
	if err != nil {
		t.Fatalf("workflow barrier: %v", err)
	}
	release := make(chan struct{})
	sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		return &blockingAdmissionProvider{release: release}, nil
	}
	if _, err := mgr.WorkflowCreate(factoryWorkTemplateJSON(t)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	out, err := mgr.WorkflowInstantiate(mustJSON(t, map[string]any{
		"template_id": "software-factory-work", "template_version": "1.2.0",
		"params": map[string]string{"worktree": "avenor-issue-115"},
	}))
	if err != nil {
		t.Fatalf("WorkflowInstantiate: %v", err)
	}
	f := &factoryE2E{
		sup: sup, mgr: mgr, cstore: cstore, root: root, adapterDir: adapterDir,
		wf: out.(map[string]any)["workflow_id"].(string), release: release,
	}
	if _, err := cstore.Create("software-factory", 2); err != nil {
		t.Fatalf("controller create: %v", err)
	}
	t.Cleanup(func() { f.stop(t) })
	return f
}

// endTurns unblocks every fake provider session so the live runtimes park
// and their attempts terminate. Safe to call multiple times.
func (f *factoryE2E) endTurns() {
	f.releaseOnce.Do(func() { close(f.release) })
}

// stop tears the fixture down: disable first so no fresh poll starts, then
// stop the leader loops and the broker.
func (f *factoryE2E) stop(t *testing.T) {
	t.Helper()
	if f.cstore != nil {
		_, _ = f.cstore.SetDesiredState("software-factory", workflowcontroller.DesiredDisabled, "test cleanup")
	}
	f.endTurns()
	f.sup.stopControllerLoops()
	for _, rt := range f.sup.listRuntimes() {
		if id, ok := rt["runtime_id"].(string); ok {
			_ = f.sup.cancelRuntime(id)
		}
	}
	waitFor(t, "runtimes terminal at cleanup", func() bool { return f.sup.activeRuntimeCount() == 0 })
	_ = f.sup.broker.Stop()
	f.sup.stopReaper()
}

// enable turns the controller on and lets its leader loop run.
func (f *factoryE2E) enable(t *testing.T) {
	t.Helper()
	if _, err := f.sup.WorkflowControllerEnable("software-factory"); err != nil {
		t.Fatalf("controller enable: %v", err)
	}
}

// disable stops future provider-backed dispatch and releases the leader
// lease so the test can drive auto nodes manually without racing the
// runner.
func (f *factoryE2E) disable(t *testing.T) {
	t.Helper()
	if _, err := f.cstore.SetDesiredState("software-factory", workflowcontroller.DesiredDisabled, "test drives auto nodes"); err != nil {
		t.Fatalf("controller disable: %v", err)
	}
	f.sup.stopControllerLoop("software-factory")
}

// instance returns the workflow's current instance.
func (f *factoryE2E) instance(t *testing.T) workflow.WorkflowInstance {
	t.Helper()
	insp, err := f.mgr.WorkflowInspect(f.wf)
	if err != nil {
		t.Fatalf("WorkflowInspect: %v", err)
	}
	return insp.(map[string]any)["instance"].(workflow.WorkflowInstance)
}

// activationByNode returns the newest activation of the named node.
func (f *factoryE2E) activationByNode(t *testing.T, nodeID string) *workflow.Activation {
	t.Helper()
	return f.activationOn(t, f.wf, nodeID)
}

// activationOn is activationByNode against another workflow in the same
// root.
func (f *factoryE2E) activationOn(t *testing.T, wf, nodeID string) *workflow.Activation {
	t.Helper()
	insp, err := f.mgr.WorkflowInspect(wf)
	if err != nil {
		t.Fatalf("WorkflowInspect: %v", err)
	}
	inst := insp.(map[string]any)["instance"].(workflow.WorkflowInstance)
	var found *workflow.Activation
	for i := range inst.Activations {
		a := inst.Activations[i]
		if a.NodeID == workflow.NodeID(nodeID) {
			found = &a
		}
	}
	return found
}

// waitActivationStatus polls until the newest activation of the node has
// the wanted status.
func (f *factoryE2E) waitActivationStatus(t *testing.T, nodeID string, want workflow.ActivationStatus) {
	t.Helper()
	f.waitActivationStatusOn(t, f.wf, nodeID, want)
}

// waitActivationStatusOn is waitActivationStatus against another workflow in
// the same root.
func (f *factoryE2E) waitActivationStatusOn(t *testing.T, wf, nodeID string, want workflow.ActivationStatus) {
	t.Helper()
	prev := f.wf
	f.wf = wf
	defer func() { f.wf = prev }()
	waitFor(t, nodeID+" status "+string(want), func() bool {
		act := f.activationByNode(t, nodeID)
		return act != nil && act.Status == want
	})
}

// factoryArtifact stages a non-empty artifact file for a completion request.
func factoryArtifact(t *testing.T, storedPath, content string) map[string]any {
	t.Helper()
	src := filepath.Join(t.TempDir(), strings.ReplaceAll(storedPath, "/", "-"))
	if err := os.WriteFile(src, []byte(content), 0o600); err != nil {
		t.Fatalf("write artifact %s: %v", storedPath, err)
	}
	return map[string]any{"src_path": src, "stored_path": storedPath, "non_empty": true}
}

// completeNode claims, starts, terminates, and completes the newest pending
// activation of the node: the test acts as the node's worker exactly as a
// manual claim on an auto node is allowed to.
func (f *factoryE2E) completeNode(t *testing.T, nodeID, outcome string, outputs []map[string]any, artifacts []map[string]any) {
	t.Helper()
	act := f.activationByNode(t, nodeID)
	if act == nil || act.Status != workflow.ActivationPending {
		t.Fatalf("no pending %s activation: %+v", nodeID, act)
	}
	res, err := f.mgr.WorkflowCommand(f.wf, mustJSON(t, map[string]any{
		"op": "claim", "node_id": nodeID, "activation_id": string(act.ID), "actor": "factory-e2e",
	}))
	if err != nil {
		t.Fatalf("%s claim: %v", nodeID, err)
	}
	claim := res.(map[string]any)
	start, err := f.mgr.WorkflowCommand(f.wf, mustJSON(t, map[string]any{
		"op": "start", "node_id": nodeID, "activation_id": string(act.ID),
		"lease_id": claim["lease_id"], "owner_token": claim["owner_token"],
	}))
	if err != nil {
		t.Fatalf("%s start: %v", nodeID, err)
	}
	attemptID := start.(map[string]any)["attempt_id"].(string)
	if err := f.mgr.RecordAttemptTerminated(workflow.WorkflowID(f.wf), workflow.NodeID(nodeID), act.ID,
		workflow.AttemptID(attemptID), workflow.LeaseID(claim["lease_id"].(string)), workflow.AttemptSucceeded); err != nil {
		t.Fatalf("%s terminate: %v", nodeID, err)
	}
	cmd := map[string]any{
		"op": "complete", "node_id": nodeID, "activation_id": string(act.ID),
		"attempt_id": attemptID, "lease_id": claim["lease_id"], "owner_token": claim["owner_token"],
		"outcome": outcome,
	}
	if outputs != nil {
		cmd["outputs"] = outputs
	}
	if artifacts != nil {
		cmd["artifacts"] = artifacts
	}
	if _, err := f.mgr.WorkflowCommand(f.wf, mustJSON(t, cmd)); err != nil {
		t.Fatalf("%s complete: %v", nodeID, err)
	}
}

// driveToIntakeThroughPublication walks the primary pipeline from intake to
// publication and completes publication with the given exact subject.
func (f *factoryE2E) driveThroughPublication(t *testing.T, repository string, pullNumber int, head string) {
	t.Helper()
	f.completeNode(t, "intake", "ready",
		[]map[string]any{
			{"definition_id": "issue", "value": "Harden the candidate index rebuild path"},
			{"definition_id": "base_sha", "value": "6e77a0d"},
		}, nil)
	f.completeNode(t, "assessment", "ready",
		[]map[string]any{{"definition_id": "assessment", "value": "assessment.md"}},
		[]map[string]any{factoryArtifact(t, "assessment.md", "## Assessment\n...")})
	f.completeNode(t, "draft-plan", "ready",
		[]map[string]any{{"definition_id": "plan", "value": "plan.md"}},
		[]map[string]any{factoryArtifact(t, "plan.md", "## Plan\n...")})
	f.completeNode(t, "hardening", "ready",
		[]map[string]any{{"definition_id": "hardened_plan", "value": "plan.md"}},
		[]map[string]any{factoryArtifact(t, "plan.md", "## Hardened plan\n...")})
	f.completeNode(t, "execution", "done", nil,
		[]map[string]any{factoryArtifact(t, "execution.md", "## Execution\n...")})
	f.completeNode(t, "verification", "passed",
		[]map[string]any{{"definition_id": "verification", "value": "verification.md"}},
		[]map[string]any{factoryArtifact(t, "verification.md", "PASS")})
	f.completeNode(t, "publication", "published",
		[]map[string]any{
			{"definition_id": "repository", "value": repository},
			{"definition_id": "pr_number", "value": pullNumber},
			{"definition_id": "pr_head", "value": head},
		},
		[]map[string]any{factoryArtifact(t, "pr-info.md", "PR 143 head "+head)})
}

// findCursor returns the poll cursor for one activation's gate.
func (f *factoryE2E) findCursor(actID workflow.ActivationID, gateID string) (workflowcontroller.PollCursor, bool) {
	rec, _, err := f.cstore.Get("software-factory")
	if err != nil {
		return workflowcontroller.PollCursor{}, false
	}
	for _, c := range rec.PollCursors {
		if c.WorkflowID == f.wf && c.ActivationID == string(actID) && c.GateID == gateID {
			return *c, true
		}
	}
	return workflowcontroller.PollCursor{}, false
}

// waitCursor waits until a cursor exists for the activation's gate.
func (f *factoryE2E) waitCursor(t *testing.T, actID workflow.ActivationID, gateID string) workflowcontroller.PollCursor {
	t.Helper()
	waitFor(t, "poll cursor for "+gateID, func() bool {
		_, ok := f.findCursor(actID, gateID)
		return ok
	})
	cursor, ok := f.findCursor(actID, gateID)
	if !ok {
		t.Fatalf("cursor for %s vanished", gateID)
	}
	return cursor
}

// gateInstances returns the gate instances recorded for one activation.
func (f *factoryE2E) gateInstances(t *testing.T, actID workflow.ActivationID) []workflow.GateInstance {
	t.Helper()
	var out []workflow.GateInstance
	for _, gi := range f.instance(t).Gates {
		if gi.ActivationID == actID {
			out = append(out, gi)
		}
	}
	return out
}

// TestFactoryWorkEndToEndCleanStopsAtHumanMergeAuth proves the shipped
// template's clean path: intake through publication drives by explicit
// claims, the controller parks the auto external review without an attempt,
// both bound gates pass through their trusted fixture adapters with staged
// evidence, the pinned clean success outcome routes to merge-auth, and the
// work stops there behind the human gate — no gate decision, no merge, no
// reconciliation.
func TestFactoryWorkEndToEndCleanStopsAtHumanMergeAuth(t *testing.T) {
	f := newFactoryE2E(t, "factory-e2e-clean", "passed.sh", "passed.sh")
	f.driveThroughPublication(t, "sdougbrown/avenor", 143, "cc793f7")
	f.enable(t)

	// The review parks kernel-locally: no attempt, no admission.
	f.waitActivationStatus(t, "review", workflow.ActivationAwaitingGate)
	review := f.activationByNode(t, "review")
	if len(review.AttemptIDs) != 0 || review.ActiveLease != nil {
		t.Fatalf("parked review recorded runtime state: %+v", review)
	}
	for _, gateID := range []string{"ci", "review-verdict"} {
		f.waitCursor(t, review.ID, gateID)
		waitFor(t, "committed poll for "+gateID, func() bool {
			cursor, ok := f.findCursor(review.ID, gateID)
			return ok && cursor.PollCount >= 1 && cursor.PollID != ""
		})
	}

	// Both fixture adapters pass: the pinned success_outcome resolves the
	// activation with staged evidence.
	f.waitActivationStatus(t, "review", workflow.ActivationSatisfied)
	review = f.activationByNode(t, "review")
	if review.SelectedOutcome != workflow.OutcomeName("clean") {
		t.Fatalf("review outcome = %q, want clean", review.SelectedOutcome)
	}
	gates := f.gateInstances(t, review.ID)
	if len(gates) != 2 {
		t.Fatalf("review gate instances = %d, want 2", len(gates))
	}
	for _, gi := range gates {
		if gi.Status != workflow.GatePassed || len(gi.EvidenceIDs) != 1 || gi.ResponseHash == "" {
			t.Fatalf("gate instance %s = %+v, want passed with staged evidence", gi.GateID, gi)
		}
	}

	// The work stops at merge-auth: pending behind the human gate, no
	// decision, and reconciliation never starts.
	f.waitActivationStatus(t, "merge-auth", workflow.ActivationPending)
	for _, gi := range f.instance(t).Gates {
		if gi.GateID == "merge-authorization" {
			t.Fatalf("human merge gate decided: %+v", gi)
		}
	}
	if act := f.activationByNode(t, "reconciliation"); act != nil {
		t.Fatalf("reconciliation activation exists before human authorization: %+v", act)
	}
	// The parked pipeline holds no in-flight controller work.
	rec, _, err := f.cstore.Get("software-factory")
	if err != nil {
		t.Fatalf("controller get: %v", err)
	}
	if rec.DesiredState != workflowcontroller.DesiredEnabled {
		t.Fatalf("controller desired state = %q, want enabled", rec.DesiredState)
	}
}

// TestFactoryWorkChangesRequestedCorrectsAndRepublishes proves the shipped
// template's correction loop: a changes_requested external result routes
// correction → reverify → a second publication with a new exact head, the
// new review parks on a new pinned subject, and the old activation's results
// can never land again.
func TestFactoryWorkChangesRequestedCorrectsAndRepublishes(t *testing.T) {
	f := newFactoryE2E(t, "factory-e2e-changes", "passed.sh", "changes-requested.sh")
	f.driveThroughPublication(t, "sdougbrown/avenor", 143, "cc793f7")
	f.enable(t)

	f.waitActivationStatus(t, "review", workflow.ActivationAwaitingGate)
	review1 := f.activationByNode(t, "review")
	oldCursor := f.waitCursor(t, review1.ID, "review-verdict")

	// The changes_requested result routes through result_outcomes onto the
	// declared correction branch. Disable the controller before driving the
	// correction loop manually so the runner never races the test's claims.
	f.waitActivationStatus(t, "review", workflow.ActivationRejected)
	f.disable(t)
	f.waitActivationStatus(t, "correction", workflow.ActivationPending)
	f.completeNode(t, "correction", "fixed", nil,
		[]map[string]any{factoryArtifact(t, "correction.md", "Fixed review findings")})
	f.waitActivationStatus(t, "reverify", workflow.ActivationPending)
	f.completeNode(t, "reverify", "passed",
		[]map[string]any{{"definition_id": "reverify", "value": "reverify.md"}},
		[]map[string]any{factoryArtifact(t, "reverify.md", "PASS after correction")})

	// Re-publication creates a NEW publication activation with a new head.
	f.waitActivationStatus(t, "publication", workflow.ActivationPending)
	f.completeNode(t, "publication", "published",
		[]map[string]any{
			{"definition_id": "repository", "value": "sdougbrown/avenor"},
			{"definition_id": "pr_number", "value": 143},
			{"definition_id": "pr_head", "value": "def456"},
		},
		[]map[string]any{factoryArtifact(t, "pr-info.md", "PR 143 head def456")})

	// A fresh review activation parks on the new subject; the old cursor is
	// obsolete and the old activation can never land another result.
	f.enable(t)
	review2 := f.activationByNode(t, "review")
	if review2.ID == review1.ID {
		t.Fatal("re-publication reused the old review activation")
	}
	f.waitActivationStatus(t, "review", workflow.ActivationAwaitingGate)
	newCursor := f.waitCursor(t, review2.ID, "review-verdict")
	if newCursor.SubjectHash == oldCursor.SubjectHash {
		t.Fatalf("new head reused subject hash %q", newCursor.SubjectHash)
	}
	if _, err := f.mgr.WorkflowCommand(f.wf, mustJSON(t, map[string]any{
		"op": "gate", "node_id": "review", "activation_id": string(review1.ID), "gate_id": "review-verdict",
		"operation": "external_result", "result": "passed", "poll_id": oldCursor.PollID,
		"source": "github", "subject": pollSubject, "response_hash": "hash-stale",
		"observed_at": time.Now().UTC(), "evidence_ids": []string{"ev-stale"},
	})); err == nil {
		t.Fatal("stale review result landed on the superseded activation")
	}
	// The superseded activation stays resolved on its changes_requested
	// outcome: a late poll result on its old-head subject records at most an
	// inert gate fact and can never re-advance the correction loop.
	for _, gi := range f.gateInstances(t, review1.ID) {
		if gi.GateID == "review-verdict" && gi.Status == workflow.GatePassed {
			t.Fatalf("superseded review verdict flipped to passed: %+v", gi)
		}
	}
	final := f.activationOn(t, f.wf, "review")
	if final.ID != review2.ID || final.Status != workflow.ActivationAwaitingGate {
		t.Fatalf("newest review = %s/%s, want the new activation parked on the new head", final.ID, final.Status)
	}
}

// TestFactoryWorkItemsResolveWorktreeKeysProvesParamConcurrency proves two
// independent work items instantiated from the same template resolve their
// concurrency keys from the instance's worktree param: two items pinned to
// different worktrees run concurrently under one controller, while two items
// sharing a worktree param serialize — item B's assessment is never
// dispatched while item A's attempt is live, and the controller replenishes
// B once A's attempt terminates.
func TestFactoryWorkItemsResolveWorktreeKeysProveParamConcurrency(t *testing.T) {
	f := newFactoryE2E(t, "factory-e2e-key", "passed.sh", "passed.sh")
	instantiate := func(name string, worktree string) string {
		t.Helper()
		out, err := f.mgr.WorkflowInstantiate(mustJSON(t, map[string]any{
			"template_id": "software-factory-work", "template_version": "1.2.0",
			"params": map[string]string{"worktree": worktree},
		}))
		if err != nil {
			t.Fatalf("instantiate %s: %v", name, err)
		}
		return out.(map[string]any)["workflow_id"].(string)
	}
	// Item B shares item A's worktree param; item C pins a different one.
	wfB := instantiate("B", "avenor-issue-115")
	wfC := instantiate("C", "avenor-issue-130")

	// All three items sit at intake; complete all intakes so the
	// assessments become ready candidates under their resolved keys.
	for _, wf := range []string{f.wf, wfB, wfC} {
		prev := f.wf
		f.wf = wf
		f.completeNode(t, "intake", "ready",
			[]map[string]any{
				{"definition_id": "issue", "value": "issue body"},
				{"definition_id": "base_sha", "value": "6e77a0d"},
			}, nil)
		f.wf = prev
	}
	f.enable(t)

	// Items A and C resolve different worktree keys, so both assessments
	// dispatch concurrently under the same controller.
	waitFor(t, "items A and C assessment running concurrently", func() bool {
		concurrent := 0
		for _, wf := range []string{f.wf, wfC} {
			act := f.activationOn(t, wf, "assessment")
			if act != nil && act.Status == workflow.ActivationRunning && len(act.AttemptIDs) > 0 {
				concurrent++
			}
		}
		return concurrent == 2
	})

	// While A holds the shared worktree key, item B's assessment never
	// starts.
	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		b := f.activationOn(t, wfB, "assessment")
		if b == nil {
			t.Fatalf("item B has no assessment activation")
		}
		if b.Status != workflow.ActivationPending {
			t.Fatalf("item B assessment = %s with A still live, want pending (shared worktree key held)", b.Status)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// End item A's attempt (the fake runtime parks); the shared key
	// releases and the controller replenishes item B.
	f.endTurns()
	f.waitActivationStatusOn(t, wfB, "assessment", workflow.ActivationRunning)
}

// TestFactoryWorkRecoveryOnFreshSupervisor proves a new supervisor on the
// same workflow root resumes the parked factory work without coordinator
// memory: the recovered snapshot still parks the review on its exact
// subject, the controller record and poll cursors survive, and the enabled
// controller re-polls through to the same clean stop at merge-auth.
func TestFactoryWorkRecoveryOnFreshSupervisor(t *testing.T) {
	f := newFactoryE2E(t, "factory-e2e-recover", "passed.sh", "passed.sh")
	f.driveThroughPublication(t, "sdougbrown/avenor", 143, "cc793f7")
	f.enable(t)
	f.waitActivationStatus(t, "review", workflow.ActivationAwaitingGate)
	review1 := f.activationByNode(t, "review")
	f.waitCursor(t, review1.ID, "ci")

	// Tear the first supervisor down without disabling the controller: the
	// desired state stays enabled on disk.
	f.sup.stopControllerLoops()
	_ = f.sup.broker.Stop()
	f.sup.stopReaper()

	// A fresh supervisor recovers the same root and adapter registry.
	sup2 := NewSupervisor(Config{
		ControlSocket:      newStableSocketPath(t, "factory-e2e-recover-2"),
		WorkflowRoot:       f.root,
		WorkflowAdapterDir: f.adapterDir,
		ShutdownTimeout:    0,
	})
	sup2.controllerRenewInterval = 25 * time.Millisecond
	sup2.controllerPollBaseDelay = 200 * time.Millisecond
	mgr2, cstore2, err := sup2.workflowBarrierResult()
	if err != nil {
		t.Fatalf("second supervisor barrier: %v", err)
	}
	t.Cleanup(func() {
		_, _ = cstore2.SetDesiredState("software-factory", workflowcontroller.DesiredDisabled, "test cleanup")
		sup2.stopControllerLoops()
		_ = sup2.broker.Stop()
		sup2.stopReaper()
	})

	// The controller record recovered enabled, with its poll cursors.
	rec, _, err := cstore2.Get("software-factory")
	if err != nil {
		t.Fatalf("recovered controller record: %v", err)
	}
	if rec.DesiredState != workflowcontroller.DesiredEnabled {
		t.Fatalf("recovered desired state = %q, want enabled", rec.DesiredState)
	}
	insp, err := mgr2.WorkflowInspect(f.wf)
	if err != nil {
		t.Fatalf("recovered inspect: %v", err)
	}
	recovered := insp.(map[string]any)["instance"].(workflow.WorkflowInstance)
	var recoveredReview *workflow.Activation
	for i := range recovered.Activations {
		if recovered.Activations[i].NodeID == workflow.NodeID("review") {
			recoveredReview = &recovered.Activations[i]
		}
	}
	if recoveredReview == nil || recoveredReview.Status != workflow.ActivationAwaitingGate {
		t.Fatalf("recovered review = %+v, want parked awaiting_gate", recoveredReview)
	}

	// The recovered controller resumes leadership and re-polls the parked
	// gates through the same adapters to the same clean stop.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		inst, err := mgr2.WorkflowInspect(f.wf)
		if err != nil {
			t.Fatalf("inspect: %v", err)
		}
		inst2 := inst.(map[string]any)["instance"].(workflow.WorkflowInstance)
		done := false
		for i := range inst2.Activations {
			a := inst2.Activations[i]
			if a.NodeID == workflow.NodeID("review") && a.Status == workflow.ActivationSatisfied {
				done = true
			}
		}
		if done {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	insp, err = mgr2.WorkflowInspect(f.wf)
	if err != nil {
		t.Fatalf("final inspect: %v", err)
	}
	final := insp.(map[string]any)["instance"].(workflow.WorkflowInstance)
	for i := range final.Activations {
		a := final.Activations[i]
		if a.NodeID == workflow.NodeID("review") && a.SelectedOutcome != workflow.OutcomeName("clean") && a.Status == workflow.ActivationSatisfied {
			t.Fatalf("recovered review outcome = %q, want clean", a.SelectedOutcome)
		}
		if a.NodeID == workflow.NodeID("merge-auth") && a.Status != workflow.ActivationPending {
			t.Fatalf("recovered merge-auth = %s, want pending behind the human gate", a.Status)
		}
	}
}
