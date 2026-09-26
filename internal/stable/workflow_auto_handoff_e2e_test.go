package stable

// workflow_auto_handoff_e2e_test.go runs the auto-dispatch worker→supervisor
// handoff end to end over a real supervisor: real registered executors
// (directRunExecutor), a real enabled controller with its leader loop, real
// admission, the real workflow.Manager over a durable store, and real
// completion validation. Only the inference provider is replaced (the
// packaged stableScriptedProvider seam).
//
// The gap under test: a successfully exiting worker never satisfies its node.
// The executors hold the lease's owner token in ExecutorContext but never
// call workflow.complete, so a succeeded attempt is a fact only — the
// activation stays running with a held lease, the heartbeat stops, and once
// the lease is swept to lease_expired the controller re-dispatches the node
// and the provider runs again. The tests assert the post-fix behavior
// (supervisor-side completion from the worker's declared result) without
// depending on the exact result-delivery mechanism: everything a worker can
// declare is produced through produceWorkerDeclaredResult, the single
// handoff seam below.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/workflow"
	"github.com/sdougbrown/avenor/internal/workflowcontroller"
)

// declaredWorkerResult is the declared result a worker produces for one node:
// the branch outcome it followed, the artifact file it wrote, and the output
// value it declares. The supervisor-side handoff fix is expected to consume
// exactly this (by whatever delivery mechanism it chooses) and turn it into a
// workflow.complete call carrying the lease's owner token.
type declaredWorkerResult struct {
	Outcome      string
	OutputID     string
	OutputValue  string
	ArtifactPath string
}

// produceWorkerDeclaredResult is the worker→supervisor result-handoff seam
// for these tests. It does everything a worker can do today to declare its
// result on a successful exit: write the artifact file into its environment
// and end its session with the success stop reason (end_turn, scripted here
// as the attempt's terminal event). It never touches workflow state — only
// the supervisor may complete the node, with the owner token it already
// holds. The Phase 2 fix is expected to replace the supervisor side of this
// seam (reading the declared result and calling workflow.complete) without
// changing this helper.
func produceWorkerDeclaredResult(t *testing.T, provider *stableScriptedProvider, sessionID, artifactName, outputValue string) declaredWorkerResult {
	t.Helper()
	provider.mu.Lock()
	provider.scripts = append(provider.scripts, stableScriptedAttempt{
		sessionID: sessionID,
		events: []stableScriptedEvent{{event: events.Event{
			Event:     "session.end",
			SessionID: sessionID,
			Fields:    map[string]any{"stop_reason": "end_turn"},
		}}},
	})
	provider.mu.Unlock()
	res := declaredWorkerResult{Outcome: "done", OutputID: "summary", OutputValue: outputValue}
	if artifactName != "" {
		path := filepath.Join(t.TempDir(), artifactName)
		if err := os.WriteFile(path, []byte(outputValue+"\n"), 0o600); err != nil {
			t.Fatalf("worker artifact write: %v", err)
		}
		res.ArtifactPath = path
	}
	return res
}

// autoHandoffFixture wires a supervisor over a fresh workflow root with the
// real executors registered by the startup barrier and a scripted provider
// injected through the production provider-factory seam.
type autoHandoffFixture struct {
	sup          *Supervisor
	mgr          *workflow.Manager
	cstore       *workflowcontroller.ControllerStore
	controllerID string
	// providerCalls counts provider-factory invocations: one per spawned
	// attempt, so it is the observable "the provider was invoked" measure.
	providerCalls atomic.Int32
}

func newAutoHandoffFixture(t *testing.T, name string, provider *stableScriptedProvider) *autoHandoffFixture {
	t.Helper()
	sup := NewSupervisor(Config{
		ControlSocket:   newStableSocketPath(t, name),
		MaxRuntimes:     4,
		MaxTreeBudget:   4,
		ShutdownTimeout: 0,
		WorkflowRoot:    filepath.Join(t.TempDir(), "wfroot"),
	})
	// The startup barrier starts leader loops for recovered enabled
	// controllers, so the fast test cadence must be set before the barrier
	// runs.
	sup.controllerRenewInterval = 25 * time.Millisecond
	mgr, cstore, err := sup.workflowBarrierResult()
	if err != nil {
		t.Fatalf("workflow barrier: %v", err)
	}
	f := &autoHandoffFixture{sup: sup, mgr: mgr, cstore: cstore, controllerID: "c1"}
	sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		f.providerCalls.Add(1)
		return provider, nil
	}
	t.Cleanup(func() { f.stop(t) })
	return f
}

// stop tears the fixture down in the runner-fixture order: disable the
// controller so no fresh dispatch starts, stop the leader loops, cancel the
// runtimes, and wait for them to go terminal before the workflow root
// disappears.
func (f *autoHandoffFixture) stop(t *testing.T) {
	t.Helper()
	if _, err := f.cstore.SetDesiredState(f.controllerID, workflowcontroller.DesiredDisabled, "test cleanup"); err != nil {
		t.Logf("cleanup disable: %v", err)
	}
	f.sup.stopControllerLoops()
	for _, rt := range f.sup.listRuntimes() {
		if id, ok := rt["runtime_id"].(string); ok {
			_ = f.sup.cancelRuntime(id)
		}
	}
	waitFor(t, f.sup.supervisorIdentity()+" runtimes terminal at cleanup", func() bool {
		return f.sup.activeRuntimeCount() == 0
	})
	_ = f.sup.broker.Stop()
	f.sup.stopReaper()
}

// addWorkflow registers the given template and instantiates it, returning the
// workflow id.
func (f *autoHandoffFixture) addWorkflow(t *testing.T, templateID string, template []byte) string {
	t.Helper()
	if _, err := f.mgr.WorkflowCreate(template); err != nil {
		t.Fatalf("WorkflowCreate %s: %v", templateID, err)
	}
	out, err := f.mgr.WorkflowInstantiate(mustJSON(t, map[string]string{
		"template_id": templateID, "template_version": "1",
	}))
	if err != nil {
		t.Fatalf("WorkflowInstantiate %s: %v", templateID, err)
	}
	wf, ok := out.(map[string]any)["workflow_id"].(string)
	if !ok || wf == "" {
		t.Fatalf("instantiate result missing workflow_id: %#v", out)
	}
	return wf
}

// enableController creates, enables, and starts the leader loop for the
// controller, waiting until this supervisor holds the lease.
func (f *autoHandoffFixture) enableController(t *testing.T, maxInflight int) {
	t.Helper()
	if _, err := f.sup.WorkflowControllerCreate(createControllerParams(t, f.controllerID, maxInflight)); err != nil &&
		!errors.Is(err, workflowcontroller.ErrConflict) {
		t.Fatalf("controller create: %v", err)
	}
	if _, err := f.sup.WorkflowControllerEnable(f.controllerID); err != nil {
		t.Fatalf("controller enable: %v", err)
	}
	waitForControllerLeader(t, f.cstore, f.controllerID, func(rec workflowcontroller.ControllerRecord) bool {
		return rec.Leader.OwnerID == f.sup.supervisorIdentity()
	})
}

// instance reads the instance snapshot for a workflow.
func (f *autoHandoffFixture) instance(t *testing.T, wf string) workflow.WorkflowInstance {
	t.Helper()
	insp, err := f.mgr.WorkflowInspect(wf)
	if err != nil {
		t.Fatalf("WorkflowInspect %s: %v", wf, err)
	}
	inst, ok := insp.(map[string]any)["instance"].(workflow.WorkflowInstance)
	if !ok {
		t.Fatalf("inspect %s missing instance: %#v", wf, insp)
	}
	return inst
}

// activationFor returns the activation for a node, or nil.
func activationFor(inst *workflow.WorkflowInstance, nodeID workflow.NodeID) *workflow.Activation {
	for i := range inst.Activations {
		if inst.Activations[i].NodeID == nodeID {
			return &inst.Activations[i]
		}
	}
	return nil
}

// attemptsForNode returns the workflow's attempts belonging to a node.
func attemptsForNode(inst *workflow.WorkflowInstance, nodeID workflow.NodeID) []workflow.Attempt {
	var out []workflow.Attempt
	for _, a := range inst.Attempts {
		if a.Identity.NodeID == nodeID {
			out = append(out, a)
		}
	}
	return out
}

// describeInstance renders the observable state for failure messages.
func describeInstance(inst *workflow.WorkflowInstance, providerCalls int32) string {
	s := fmt.Sprintf("workflow status=%s terminal_outcome=%q provider_calls=%d activations=[", inst.Status, inst.TerminalOutcome, providerCalls)
	for i, a := range inst.Activations {
		if i > 0 {
			s += ", "
		}
		s += fmt.Sprintf("%s:%s(outcome=%q, attempts=%d)", a.NodeID, a.Status, a.SelectedOutcome, len(a.AttemptIDs))
	}
	s += "] attempts=["
	for i, a := range inst.Attempts {
		if i > 0 {
			s += ", "
		}
		s += fmt.Sprintf("%s:%s", a.Identity.NodeID, a.Status)
	}
	return s + "]"
}

// waitForInstance polls cond against a fresh instance snapshot, failing with
// the full observed state after a bounded deadline.
func (f *autoHandoffFixture) waitForInstance(t *testing.T, wf, what string, cond func(inst *workflow.WorkflowInstance) bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var inst workflow.WorkflowInstance
	for {
		inst = f.instance(t, wf)
		if cond(&inst) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s; observed %s", what, describeInstance(&inst, f.providerCalls.Load()))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// autoHandoffChainTemplate is the two-node template for the dependent-node
// test: auto run node "produce" declares a required string output, a files
// completion contract with a non-empty artifact, and a "done" branch to auto
// run node "consume".
func autoHandoffChainTemplate(t *testing.T, templateID string, ttlSeconds int64) []byte {
	t.Helper()
	template := map[string]any{
		"schema_version":   1,
		"template_id":      templateID,
		"template_version": "1",
		"entry_nodes":      []string{"produce"},
		"nodes": []any{
			map[string]any{
				"id":       "produce",
				"action":   map[string]any{"type": "run", "prompt": "produce the artifact"},
				"dispatch": map[string]any{"mode": "auto", "controller_id": "c1"},
				"outputs": []any{map[string]any{
					"id": "summary", "name": "Summary", "type": "string", "required": true,
				}},
				"completion": map[string]any{
					"kind":      "files",
					"artifacts": []any{map[string]any{"path": "result.md", "non_empty": true}},
				},
				"branches":     map[string]any{"done": "consume"},
				"retry_policy": map[string]any{"max_attempts": 3, "exhaustion": "block"},
			},
			map[string]any{
				"id":           "consume",
				"dependencies": []string{"produce"},
				"action":       map[string]any{"type": "run", "prompt": "consume the result"},
				"dispatch":     map[string]any{"mode": "auto", "controller_id": "c1"},
				"retry_policy": map[string]any{"max_attempts": 3, "exhaustion": "block"},
			},
		},
		"terminal_outcomes":    []string{"done"},
		"default_lease_policy": map[string]any{"ttl_seconds": ttlSeconds},
	}
	return mustJSON(t, template)
}

// TestAutoHandoffSuccessExitIsNotRedispatched proves a successful worker exit
// satisfies the node exactly once: an enabled controller dispatches one auto
// run node with a short lease TTL, the scripted worker ends successfully, and
// after the live lease-expiry sweep has had every chance to run (the
// production Manager.ExpireStaleLeases stall detector, driven in a bounded
// loop well past the TTL) the provider is still invoked exactly once and the
// activation carries exactly one attempt. It fails today because the
// supervisor never completes the node: the succeeded attempt leaves the
// activation running with a held lease, the sweep marks it lease_expired, and
// the controller re-dispatches the node for a second provider run.
func TestAutoHandoffSuccessExitIsNotRedispatched(t *testing.T) {
	const sessionID = "ses_handoff_once"
	provider := &stableScriptedProvider{attempt: -1}
	f := newAutoHandoffFixture(t, "auto-handoff-once", provider)
	_ = produceWorkerDeclaredResult(t, provider, sessionID, "", "")
	template := map[string]any{
		"schema_version":   1,
		"template_id":      "tmpl-auto-handoff-once",
		"template_version": "1",
		"entry_nodes":      []string{"start"},
		"nodes": []any{map[string]any{
			"id":           "start",
			"action":       map[string]any{"type": "run", "prompt": "do the thing"},
			"dispatch":     map[string]any{"mode": "auto", "controller_id": "c1"},
			"retry_policy": map[string]any{"max_attempts": 3, "exhaustion": "block"},
		}},
		"terminal_outcomes":    []string{"done"},
		"default_lease_policy": map[string]any{"ttl_seconds": 1},
	}
	wf := f.addWorkflow(t, "tmpl-auto-handoff-once", mustJSON(t, template))
	f.enableController(t, 2)

	// The node dispatches automatically, the worker runs, and its attempt
	// terminates successfully.
	f.waitForInstance(t, wf, "the auto node's attempt to succeed", func(inst *workflow.WorkflowInstance) bool {
		attempts := attemptsForNode(inst, "start")
		return len(attempts) == 1 && attempts[0].Status == workflow.AttemptSucceeded
	})
	if calls := f.providerCalls.Load(); calls != 1 {
		t.Fatalf("provider invoked %d times before the successful exit, want exactly 1", calls)
	}

	// Bounded sweep window: drive the production live stall detector
	// repeatedly for well past the 1s TTL, giving the lease-expiry sweep and
	// the controller's re-dispatch every chance to run. The activation must
	// never become lease_expired and no second attempt may ever start. The
	// lease_expired status itself is recorded (not fatal) so the failure
	// observes the actual re-dispatch it causes. The window exceeds the
	// runner's 5s anti-entropy cadence because the sweep applies the
	// lease_expired event through the store without a manager change
	// notification, so re-discovery waits for the next candidate refresh.
	sawLeaseExpired := false
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := f.mgr.ExpireStaleLeases(); err != nil {
			t.Fatalf("ExpireStaleLeases: %v", err)
		}
		inst := f.instance(t, wf)
		if act := activationFor(&inst, "start"); act != nil && act.Status == workflow.ActivationLeaseExpired {
			sawLeaseExpired = true
		}
		if len(inst.Attempts) > 1 {
			t.Fatalf("second attempt dispatched after a successful exit (activation went lease_expired=%v); observed %s",
				sawLeaseExpired, describeInstance(&inst, f.providerCalls.Load()))
		}
		if calls := f.providerCalls.Load(); calls > 1 {
			t.Fatalf("provider invoked %d times after a successful exit (activation went lease_expired=%v); observed %s",
				calls, sawLeaseExpired, describeInstance(&inst, calls))
		}
		time.Sleep(25 * time.Millisecond)
	}

	// Final state: exactly one attempt, exactly one provider invocation, and
	// the node resolved by the supervisor's own completion (satisfied), never
	// re-claimed.
	inst := f.instance(t, wf)
	if calls := f.providerCalls.Load(); calls != 1 {
		t.Fatalf("provider invoked %d times after a successful exit, want exactly 1; observed %s",
			calls, describeInstance(&inst, calls))
	}
	if len(inst.Attempts) != 1 {
		t.Fatalf("workflow recorded %d attempts after a successful exit, want exactly 1; observed %s",
			len(inst.Attempts), describeInstance(&inst, f.providerCalls.Load()))
	}
	act := activationFor(&inst, "start")
	if act == nil {
		t.Fatalf("start activation missing; observed %s", describeInstance(&inst, f.providerCalls.Load()))
	}
	if act.Status != workflow.ActivationSatisfied {
		t.Fatalf("start activation status = %s after a successful exit, want satisfied by the supervisor's handoff completion; observed %s",
			act.Status, describeInstance(&inst, f.providerCalls.Load()))
	}
}

// TestAutoHandoffSatisfiesNodeAndDispatchesDependent proves the supervisor's
// handoff advances a dependent node: auto run node "produce" declares a
// required output, an artifact completion contract, and a "done" branch to
// auto run node "consume". The scripted worker for "produce" writes its
// declared artifact through the handoff seam and exits successfully. The test
// asserts "produce" reaches satisfied with the declared outcome, the output
// and artifact evidence are recorded, and the controller automatically
// dispatches "consume" (its provider invoked). It fails today because nothing
// ever completes "produce": the succeeded attempt is a fact only, the
// activation stays running, and "consume" is never created.
func TestAutoHandoffSatisfiesNodeAndDispatchesDependent(t *testing.T) {
	provider := &stableScriptedProvider{attempt: -1}
	f := newAutoHandoffFixture(t, "auto-handoff-chain", provider)
	// Each worker declares its result before dispatch: produce writes the
	// artifact the completion contract names; both end with the success stop
	// reason.
	declared := produceWorkerDeclaredResult(t, provider, "ses_produce", "result.md", "the declared summary")
	_ = produceWorkerDeclaredResult(t, provider, "ses_consume", "", "")
	wf := f.addWorkflow(t, "tmpl-auto-handoff-chain", autoHandoffChainTemplate(t, "tmpl-auto-handoff-chain", 900))
	f.enableController(t, 2)

	// "produce" dispatches automatically and its attempt succeeds.
	f.waitForInstance(t, wf, "produce's attempt to succeed", func(inst *workflow.WorkflowInstance) bool {
		attempts := attemptsForNode(inst, "produce")
		return len(attempts) == 1 && attempts[0].Status == workflow.AttemptSucceeded
	})
	if calls := f.providerCalls.Load(); calls != 1 {
		t.Fatalf("provider invoked %d times for produce, want exactly 1", calls)
	}

	// The supervisor must complete "produce" from the worker's declared
	// result: satisfied with the declared outcome, output + artifact evidence
	// recorded, and "consume" dispatched automatically.
	f.waitForInstance(t, wf, "produce satisfied with outcome done and consume dispatched", func(inst *workflow.WorkflowInstance) bool {
		produce := activationFor(inst, "produce")
		consume := activationFor(inst, "consume")
		return produce != nil && produce.Status == workflow.ActivationSatisfied &&
			produce.SelectedOutcome == workflow.OutcomeName(declared.Outcome) &&
			consume != nil && len(consume.AttemptIDs) > 0
	})

	inst := f.instance(t, wf)
	produce := activationFor(&inst, "produce")
	if produce == nil || produce.Status != workflow.ActivationSatisfied {
		t.Fatalf("produce not satisfied; observed %s", describeInstance(&inst, f.providerCalls.Load()))
	}
	if produce.SelectedOutcome != workflow.OutcomeName(declared.Outcome) {
		t.Fatalf("produce selected outcome = %q, want %q; observed %s",
			produce.SelectedOutcome, declared.Outcome, describeInstance(&inst, f.providerCalls.Load()))
	}

	// The declared output is recorded against the produce activation, bound
	// to staged artifact evidence.
	var summary *workflow.OutputValue
	for i := range inst.Outputs {
		if inst.Outputs[i].DefinitionID == workflow.OutputID(declared.OutputID) {
			summary = &inst.Outputs[i]
			break
		}
	}
	if summary == nil {
		t.Fatalf("declared output %q not recorded; observed %s", declared.OutputID, describeInstance(&inst, f.providerCalls.Load()))
	}
	if summary.ActivationID != produce.ID {
		t.Fatalf("output %q recorded on activation %s, want produce activation %s", declared.OutputID, summary.ActivationID, produce.ID)
	}
	var value string
	if err := json.Unmarshal(summary.Value, &value); err != nil {
		t.Fatalf("output %q value %s is not a JSON string: %v", declared.OutputID, summary.Value, err)
	}
	if len(summary.EvidenceIDs) == 0 {
		t.Fatalf("output %q carries no evidence IDs; observed %s", declared.OutputID, describeInstance(&inst, f.providerCalls.Load()))
	}

	// The artifact is staged as machine evidence for the produce activation.
	var artifact *workflow.Evidence
	for i := range inst.Evidence {
		if inst.Evidence[i].Kind == "artifact" && inst.Evidence[i].ActivationID == produce.ID {
			artifact = &inst.Evidence[i]
			break
		}
	}
	if artifact == nil {
		t.Fatalf("no artifact evidence recorded for produce activation %s; observed %s", produce.ID, describeInstance(&inst, f.providerCalls.Load()))
	}
	if filepath.Base(artifact.OriginalPath) != filepath.Base(declared.ArtifactPath) {
		t.Fatalf("artifact evidence original path = %q, want the declared artifact %q", artifact.OriginalPath, declared.ArtifactPath)
	}
	if artifact.Size == 0 {
		t.Fatalf("artifact evidence is empty; observed %s", describeInstance(&inst, f.providerCalls.Load()))
	}

	// The controller dispatched consume's provider automatically — one
	// invocation per node, never a manual claim.
	if calls := f.providerCalls.Load(); calls != 2 {
		t.Fatalf("provider invoked %d times, want exactly 2 (produce + consume); observed %s",
			calls, describeInstance(&inst, calls))
	}
	consume := activationFor(&inst, "consume")
	if consume == nil {
		t.Fatalf("consume activation missing; observed %s", describeInstance(&inst, f.providerCalls.Load()))
	}
	if len(attemptsForNode(&inst, "consume")) == 0 {
		t.Fatalf("consume has no attempt despite its provider being invoked; observed %s", describeInstance(&inst, f.providerCalls.Load()))
	}
}
