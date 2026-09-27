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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/pprof"
	"strings"
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
// (the attempt's working directory — the test's current working directory,
// which the direct-run executor records as the spawn Dir) and end its
// session with the success stop reason (end_turn, scripted here as the
// attempt's terminal event). It never touches workflow state — only the
// supervisor may complete the node, with the owner token it already holds.
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
		path := filepath.Join(".", artifactName)
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

// newAutoHandoffFixture builds the fixture; opts run before the startup
// barrier, so they can set config (e.g. the lease-sweep interval) that the
// barrier consumes.
func newAutoHandoffFixture(t *testing.T, name string, provider *stableScriptedProvider, opts ...func(*autoHandoffFixture)) *autoHandoffFixture {
	t.Helper()
	sup := NewSupervisor(Config{
		ControlSocket:   newStableSocketPath(t, name),
		MaxRuntimes:     4,
		MaxTreeBudget:   4,
		ShutdownTimeout: 0,
		WorkflowRoot:    filepath.Join(t.TempDir(), "wfroot"),
	})
	f := &autoHandoffFixture{sup: sup, controllerID: "c1"}
	for _, opt := range opts {
		opt(f)
	}
	// The startup barrier starts leader loops for recovered enabled
	// controllers, so the fast test cadence must be set before the barrier
	// runs.
	sup.controllerRenewInterval = 25 * time.Millisecond
	mgr, cstore, err := sup.workflowBarrierResult()
	if err != nil {
		t.Fatalf("workflow barrier: %v", err)
	}
	f.mgr = mgr
	f.cstore = cstore
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
	f.sup.stopLeaseSweep()
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

// addWorkflow registers the given template and instantiates it, returning
// the workflow id. An optional params object supplies the template's
// instance params.
func (f *autoHandoffFixture) addWorkflow(t *testing.T, templateID string, template []byte, params ...map[string]string) string {
	t.Helper()
	if _, err := f.mgr.WorkflowCreate(template); err != nil {
		t.Fatalf("WorkflowCreate %s: %v", templateID, err)
	}
	req := map[string]any{
		"template_id": templateID, "template_version": "1",
	}
	if len(params) == 1 {
		req["params"] = params[0]
	}
	out, err := f.mgr.WorkflowInstantiate(mustJSON(t, req))
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
	deadline := time.Now().Add(15 * time.Second)
	var inst workflow.WorkflowInstance
	for {
		inst = f.instance(t, wf)
		if cond(&inst) {
			return
		}
		if time.Now().After(deadline) {
			logGoroutines(t)
			t.Fatalf("timed out waiting for %s; observed %s", what, describeInstance(&inst, f.providerCalls.Load()))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// consumeSucceeded reports whether the consume node's attempt reached a
// successful terminal status, which happens only after its provider ran.
func consumeSucceeded(inst *workflow.WorkflowInstance) bool {
	for _, a := range attemptsForNode(inst, "consume") {
		if a.Status == workflow.AttemptSucceeded {
			return true
		}
	}
	return false
}

// logGoroutines writes every goroutine's stack to the test log, so a wait
// that times out shows where the supervisor stalled.
func logGoroutines(t *testing.T) {
	t.Helper()
	var buf bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&buf, 2); err != nil {
		t.Logf("goroutine dump failed: %v", err)
		return
	}
	t.Logf("goroutine dump at timeout:\n%s", buf.String())
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
					"id": "summary", "name": "Summary", "type": "file", "required": true,
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
// run node with a short lease TTL and the supervisor's own live lease-expiry
// sweep running at a short interval, the scripted worker ends successfully,
// and the supervisor's handoff completion satisfies the node before the sweep
// could expire its lease. The bounded wait runs well past several sweep
// intervals, so a re-dispatch (the pre-fix behavior) would be observed.
func TestAutoHandoffSuccessExitIsNotRedispatched(t *testing.T) {
	// The attempt's working directory is the supervisor process cwd (the
	// direct-run executor's spawn Dir); chdir to a scratch dir so the worker's
	// environment is isolated.
	t.Chdir(t.TempDir())
	const sessionID = "ses_handoff_once"
	provider := &stableScriptedProvider{attempt: -1}
	f := newAutoHandoffFixture(t, "auto-handoff-once", provider, func(f *autoHandoffFixture) {
		f.sup.config.WorkflowLeaseSweepInterval = 250 * time.Millisecond
	})
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

	// The supervisor's completion must satisfy the node before the live sweep
	// (250ms cadence, 1s TTL) could expire its lease: bounded wait across
	// several sweep intervals, then exactly one attempt and one invocation.
	f.waitForInstance(t, wf, "the supervisor's completion to satisfy the start node", func(inst *workflow.WorkflowInstance) bool {
		act := activationFor(inst, "start")
		return act != nil && act.Status == workflow.ActivationSatisfied
	})

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
	t.Chdir(t.TempDir())
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

	// The supervisor must complete "produce" from the worker's declared
	// result: satisfied with the declared outcome, output + artifact evidence
	// recorded, and "consume" dispatched automatically.
	f.waitForInstance(t, wf, "produce satisfied with outcome done and consume's attempt succeeded", func(inst *workflow.WorkflowInstance) bool {
		produce := activationFor(inst, "produce")
		consume := activationFor(inst, "consume")
		return produce != nil && produce.Status == workflow.ActivationSatisfied &&
			produce.SelectedOutcome == workflow.OutcomeName(declared.Outcome) &&
			consume != nil && consumeSucceeded(inst)
	})

	inst := f.instance(t, wf)
	produce := activationFor(&inst, "produce")
	if produce == nil || produce.Status != workflow.ActivationSatisfied {
		t.Fatalf("produce not satisfied; observed %s", describeInstance(&inst, f.providerCalls.Load()))
	}
	wantDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for _, a := range attemptsForNode(&inst, "produce") {
		if a.Status == workflow.AttemptSucceeded && a.WorkingDirectory != wantDir {
			t.Fatalf("produce attempt working directory = %q, want the attempt's spawn dir %q", a.WorkingDirectory, wantDir)
		}
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

// appendSessionScript appends one scripted provider attempt: optional agent
// message chunks (full-line markers are parsed by the CLI session loop)
// followed by a session end with the given stop reason.
func appendSessionScript(t *testing.T, provider *stableScriptedProvider, sessionID string, chunks []string, stopReason string) {
	t.Helper()
	steps := make([]stableScriptedEvent, 0, len(chunks)+1)
	for _, chunk := range chunks {
		steps = append(steps, stableScriptedEvent{event: events.Event{
			Event:     "agent.message_chunk",
			SessionID: sessionID,
			Fields:    map[string]any{"delta": chunk},
		}})
	}
	steps = append(steps, stableScriptedEvent{event: events.Event{
		Event:     "session.end",
		SessionID: sessionID,
		Fields:    map[string]any{"stop_reason": stopReason},
	}})
	provider.mu.Lock()
	provider.scripts = append(provider.scripts, stableScriptedAttempt{sessionID: sessionID, events: steps})
	provider.mu.Unlock()
}

// TestAutoHandoffContractUnmetRetriesThenExhausts proves a clean exit that
// does not meet the declared files contract is recorded as a failed attempt
// with the contract_unmet marker, so the node's retry policy applies and
// exhaustion leaves the activation blocked — never running, never looping
// forever.
func TestAutoHandoffContractUnmetRetriesThenExhausts(t *testing.T) {
	t.Chdir(t.TempDir())
	provider := &stableScriptedProvider{attempt: -1}
	// Three attempts (the chain template's declared max_attempts), none
	// writes the required artifact.
	appendSessionScript(t, provider, "ses_try1", nil, "end_turn")
	appendSessionScript(t, provider, "ses_try2", nil, "end_turn")
	appendSessionScript(t, provider, "ses_try3", nil, "end_turn")
	f := newAutoHandoffFixture(t, "auto-handoff-unmet", provider)
	wf := f.addWorkflow(t, "tmpl-auto-handoff-unmet", autoHandoffChainTemplate(t, "tmpl-auto-handoff-unmet", 900))
	f.enableController(t, 2)

	f.waitForInstance(t, wf, "the produce node to exhaust its retries and block", func(inst *workflow.WorkflowInstance) bool {
		act := activationFor(inst, "produce")
		return act != nil && act.Status == workflow.ActivationBlocked
	})

	inst := f.instance(t, wf)
	if calls := f.providerCalls.Load(); calls != 3 {
		t.Fatalf("provider invoked %d times, want exactly 3 (max_attempts); observed %s", calls, describeInstance(&inst, calls))
	}
	attempts := attemptsForNode(&inst, "produce")
	if len(attempts) != 3 {
		t.Fatalf("produce recorded %d attempts, want exactly 3; observed %s", len(attempts), describeInstance(&inst, f.providerCalls.Load()))
	}
	for _, attempt := range attempts {
		if attempt.Status != workflow.AttemptFailed {
			t.Fatalf("produce attempt %s status = %s, want failed; observed %s", attempt.ID, attempt.Status, describeInstance(&inst, f.providerCalls.Load()))
		}
		if attempt.MarkerLabel != "contract_unmet" {
			t.Fatalf("produce attempt %s marker label = %q, want contract_unmet; observed %s", attempt.ID, attempt.MarkerLabel, describeInstance(&inst, f.providerCalls.Load()))
		}
	}
	if act := activationFor(&inst, "consume"); act != nil {
		t.Fatalf("consume dispatched despite produce never satisfying its contract; observed %s", describeInstance(&inst, f.providerCalls.Load()))
	}
}

// TestAutoHandoffNoContractChainCompletes proves a no-contract auto node
// auto-completes on a clean exit and its dependent dispatches and completes:
// the whole two-node workflow reaches completed with no human or worker
// completion call.
func TestAutoHandoffNoContractChainCompletes(t *testing.T) {
	t.Chdir(t.TempDir())
	provider := &stableScriptedProvider{attempt: -1}
	appendSessionScript(t, provider, "ses_first", nil, "end_turn")
	appendSessionScript(t, provider, "ses_second", nil, "end_turn")
	f := newAutoHandoffFixture(t, "auto-handoff-nocontract", provider)
	template := map[string]any{
		"schema_version":   1,
		"template_id":      "tmpl-auto-handoff-nocontract",
		"template_version": "1",
		"entry_nodes":      []string{"first"},
		"nodes": []any{
			map[string]any{
				"id":           "first",
				"action":       map[string]any{"type": "run", "prompt": "first"},
				"dispatch":     map[string]any{"mode": "auto", "controller_id": "c1"},
				"branches":     map[string]any{"done": "second"},
				"retry_policy": map[string]any{"max_attempts": 2, "exhaustion": "block"},
			},
			map[string]any{
				"id":           "second",
				"dependencies": []string{"first"},
				"action":       map[string]any{"type": "run", "prompt": "second"},
				"dispatch":     map[string]any{"mode": "auto", "controller_id": "c1"},
				"retry_policy": map[string]any{"max_attempts": 2, "exhaustion": "block"},
			},
		},
		"terminal_outcomes":    []string{"done"},
		"default_lease_policy": map[string]any{"ttl_seconds": 900},
	}
	wf := f.addWorkflow(t, "tmpl-auto-handoff-nocontract", mustJSON(t, template))
	f.enableController(t, 2)

	f.waitForInstance(t, wf, "the workflow to complete", func(inst *workflow.WorkflowInstance) bool {
		return inst.Status == workflow.WorkflowCompleted
	})
	inst := f.instance(t, wf)
	if calls := f.providerCalls.Load(); calls != 2 {
		t.Fatalf("provider invoked %d times, want exactly 2 (one per node); observed %s", calls, describeInstance(&inst, calls))
	}
	for _, nodeID := range []workflow.NodeID{"first", "second"} {
		act := activationFor(&inst, nodeID)
		if act == nil || act.Status != workflow.ActivationSatisfied {
			t.Fatalf("node %s not satisfied; observed %s", nodeID, describeInstance(&inst, f.providerCalls.Load()))
		}
		if act.SelectedOutcome != workflow.OutcomeName("done") {
			t.Fatalf("node %s selected outcome = %q, want done; observed %s", nodeID, act.SelectedOutcome, describeInstance(&inst, f.providerCalls.Load()))
		}
	}
}

// TestAutoHandoffPointerAndGitHeadSourcesAndGitContract proves pointer and
// git-head output sources resolve against the attempt's working directory,
// and a git completion contract is evaluated there too.
func TestAutoHandoffPointerAndGitHeadSourcesAndGitContract(t *testing.T) {
	workDir := t.TempDir()
	t.Chdir(workDir)
	provider := &stableScriptedProvider{attempt: -1}
	// produce writes its declared JSON artifact (the worker's declared
	// result); git-verify exits cleanly in the same working directory.
	declared := produceWorkerDeclaredResult(t, provider, "ses_produce", "data.json", `{"repository":"org/repo","n":3}`)
	appendSessionScript(t, provider, "ses_verify", nil, "end_turn")
	appendSessionScript(t, provider, "ses_after", nil, "end_turn")

	// The working directory is a git repo with everything committed, so the
	// git contract's clean check and head pin hold.
	runGit := func(args ...string) string {
		t.Helper()
		out, err := exec.Command("git", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
		return string(out)
	}
	runGit("init")
	runGit("config", "user.email", "workflow-test@example.invalid")
	runGit("config", "user.name", "workflow test")
	runGit("add", "data.json")
	runGit("commit", "-m", "declared result")
	head := strings.TrimSpace(runGit("rev-parse", "HEAD"))

	f := newAutoHandoffFixture(t, "auto-handoff-sources", provider)
	template := map[string]any{
		"schema_version":   1,
		"template_id":      "tmpl-auto-handoff-sources",
		"template_version": "1",
		"entry_nodes":      []string{"produce"},
		"nodes": []any{
			map[string]any{
				"id":       "produce",
				"action":   map[string]any{"type": "run", "prompt": "produce the json artifact"},
				"dispatch": map[string]any{"mode": "auto", "controller_id": "c1"},
				"outputs": []any{
					map[string]any{
						"id": "repository", "name": "Repository", "type": "string", "required": true,
						"source": map[string]any{"artifact": declared.ArtifactPath, "pointer": "/repository"},
					},
					map[string]any{
						"id": "head", "name": "Head", "type": "string", "required": true,
						"source": map[string]any{"git": "head"},
					},
				},
				"completion": map[string]any{
					"kind":      "files",
					"artifacts": []any{map[string]any{"path": declared.ArtifactPath, "non_empty": true}},
				},
				"branches":     map[string]any{"done": "git-verify"},
				"retry_policy": map[string]any{"max_attempts": 2, "exhaustion": "block"},
			},
			map[string]any{
				"id":           "git-verify",
				"dependencies": []string{"produce"},
				"action":       map[string]any{"type": "run", "prompt": "verify"},
				"dispatch":     map[string]any{"mode": "auto", "controller_id": "c1"},
				"completion":   map[string]any{"kind": "git", "git": map[string]any{"clean": true, "head": head}},
				"branches":     map[string]any{"done": "after"},
				"retry_policy": map[string]any{"max_attempts": 2, "exhaustion": "block"},
			},
			map[string]any{
				"id":           "after",
				"dependencies": []string{"git-verify"},
				"action":       map[string]any{"type": "run", "prompt": "after"},
				"dispatch":     map[string]any{"mode": "auto", "controller_id": "c1"},
				"retry_policy": map[string]any{"max_attempts": 2, "exhaustion": "block"},
			},
		},
		"terminal_outcomes":    []string{"done"},
		"default_lease_policy": map[string]any{"ttl_seconds": 900},
	}
	wf := f.addWorkflow(t, "tmpl-auto-handoff-sources", mustJSON(t, template))
	f.enableController(t, 2)

	f.waitForInstance(t, wf, "produce satisfied and git-verify dispatched", func(inst *workflow.WorkflowInstance) bool {
		produce := activationFor(inst, "produce")
		verify := activationFor(inst, "git-verify")
		return produce != nil && produce.Status == workflow.ActivationSatisfied && verify != nil && len(verify.AttemptIDs) > 0
	})
	f.waitForInstance(t, wf, "the workflow to complete", func(inst *workflow.WorkflowInstance) bool {
		return inst.Status == workflow.WorkflowCompleted
	})

	inst := f.instance(t, wf)
	outputValue := func(outputID string) string {
		t.Helper()
		for _, o := range inst.Outputs {
			if string(o.DefinitionID) == outputID {
				var value string
				if err := json.Unmarshal(o.Value, &value); err != nil {
					t.Fatalf("output %q value %s is not a JSON string: %v", outputID, o.Value, err)
				}
				return value
			}
		}
		t.Fatalf("output %q not recorded; observed %s", outputID, describeInstance(&inst, f.providerCalls.Load()))
		return ""
	}
	if got := outputValue("repository"); got != "org/repo" {
		t.Fatalf("pointer output repository = %q, want org/repo; observed %s", got, describeInstance(&inst, f.providerCalls.Load()))
	}
	if got := outputValue("head"); got != head {
		t.Fatalf("git-head output head = %q, want %q; observed %s", got, head, describeInstance(&inst, f.providerCalls.Load()))
	}
}

// TestAutoHandoffLoopMarkerSelectsDeclaredOutcome proves a loop node with
// two declared branches completes on the outcome its terminal marker label
// names: the loop child's exit marker is plumbed to the termination path and
// selects the branch, dispatching the dependent.
func TestAutoHandoffLoopMarkerSelectsDeclaredOutcome(t *testing.T) {
	t.Chdir(t.TempDir())
	provider := &stableScriptedProvider{attempt: -1}
	appendSessionScript(t, provider, "ses_loop", []string{"<|workflow: exit | passed|>\n"}, "end_turn")
	appendSessionScript(t, provider, "ses_consume", nil, "end_turn")
	loopPath := "loop.json"
	if err := os.WriteFile(loopPath, mustJSON(t, map[string]any{
		"max_iterations": 2,
		"loop":           []any{map[string]any{"name": "verify", "prompt": "emit the verdict"}},
	}), 0o600); err != nil {
		t.Fatal(err)
	}
	f := newAutoHandoffFixture(t, "auto-handoff-loop-marker", provider)
	wf := f.addWorkflow(t, "tmpl-auto-handoff-loop-marker", autoHandoffLoopTemplate(t, "tmpl-auto-handoff-loop-marker", loopPath))
	f.enableController(t, 2)

	f.waitForInstance(t, wf, "the workflow to complete", func(inst *workflow.WorkflowInstance) bool {
		return inst.Status == workflow.WorkflowCompleted
	})
	inst := f.instance(t, wf)
	act := activationFor(&inst, "step")
	if act == nil || act.Status != workflow.ActivationSatisfied {
		t.Fatalf("loop node not satisfied; observed %s", describeInstance(&inst, f.providerCalls.Load()))
	}
	if act.SelectedOutcome != workflow.OutcomeName("passed") {
		t.Fatalf("loop node selected outcome = %q, want passed (the terminal marker label); observed %s", act.SelectedOutcome, describeInstance(&inst, f.providerCalls.Load()))
	}
	if attempts := attemptsForNode(&inst, "step"); len(attempts) != 1 || attempts[0].MarkerLabel != "passed" {
		t.Fatalf("loop node attempts = %+v, want one attempt with marker label passed; observed %s", attempts, describeInstance(&inst, f.providerCalls.Load()))
	}
	if calls := f.providerCalls.Load(); calls != 2 {
		t.Fatalf("provider invoked %d times, want exactly 2 (loop + consume); observed %s", calls, describeInstance(&inst, f.providerCalls.Load()))
	}
}

// TestAutoHandoffLoopWithoutMarkerIsContractUnmet proves a multi-outcome
// loop node whose worker exits cleanly without a terminal marker cannot
// pick an outcome: the attempt is failed with contract_unmet and retry
// exhaustion blocks the node.
func TestAutoHandoffLoopWithoutMarkerIsContractUnmet(t *testing.T) {
	t.Chdir(t.TempDir())
	provider := &stableScriptedProvider{attempt: -1}
	appendSessionScript(t, provider, "ses_loop1", nil, "end_turn")
	appendSessionScript(t, provider, "ses_loop2", nil, "end_turn")
	loopPath := "loop.json"
	if err := os.WriteFile(loopPath, mustJSON(t, map[string]any{
		"max_iterations": 1,
		"loop":           []any{map[string]any{"name": "verify", "prompt": "emit the verdict"}},
	}), 0o600); err != nil {
		t.Fatal(err)
	}
	f := newAutoHandoffFixture(t, "auto-handoff-loop-nomarker", provider)
	wf := f.addWorkflow(t, "tmpl-auto-handoff-loop-nomarker", autoHandoffLoopTemplate(t, "tmpl-auto-handoff-loop-nomarker", loopPath))
	f.enableController(t, 2)

	f.waitForInstance(t, wf, "the loop node to exhaust and block", func(inst *workflow.WorkflowInstance) bool {
		act := activationFor(inst, "step")
		return act != nil && act.Status == workflow.ActivationBlocked
	})
	inst := f.instance(t, wf)
	if calls := f.providerCalls.Load(); calls != 2 {
		t.Fatalf("provider invoked %d times, want exactly 2 (max_attempts); observed %s", calls, describeInstance(&inst, calls))
	}
	attempts := attemptsForNode(&inst, "step")
	if len(attempts) != 2 {
		t.Fatalf("loop node recorded %d attempts, want exactly 2; observed %s", len(attempts), describeInstance(&inst, f.providerCalls.Load()))
	}
	for _, attempt := range attempts {
		if attempt.MarkerLabel != "contract_unmet" {
			t.Fatalf("loop attempt %s marker label = %q, want contract_unmet; observed %s", attempt.ID, attempt.MarkerLabel, describeInstance(&inst, f.providerCalls.Load()))
		}
	}
}

// TestAutoHandoffNonSuccessExitsDoNotComplete proves failed and canceled
// exits keep today's behavior: the terminal fact is recorded as-is (no
// supervisor completion, no contract_unmet relabeling) and the node's retry
// policy or cancellation handling applies.
func TestAutoHandoffNonSuccessExitsDoNotComplete(t *testing.T) {
	t.Run("failed exit retries then blocks", func(t *testing.T) {
		t.Chdir(t.TempDir())
		provider := &stableScriptedProvider{attempt: -1}
		// refusal maps to exit code 2: a failed, non-retryable-at-runtime
		// exit that still applies the node's kernel retry policy.
		appendSessionScript(t, provider, "ses_fail1", nil, "refusal")
		appendSessionScript(t, provider, "ses_fail2", nil, "refusal")
		appendSessionScript(t, provider, "ses_fail3", nil, "refusal")
		f := newAutoHandoffFixture(t, "auto-handoff-failed-exit", provider)
		wf := f.addWorkflow(t, "tmpl-auto-handoff-failed-exit", autoHandoffChainTemplate(t, "tmpl-auto-handoff-failed-exit", 900))
		f.enableController(t, 2)

		f.waitForInstance(t, wf, "the produce node to exhaust and block", func(inst *workflow.WorkflowInstance) bool {
			act := activationFor(inst, "produce")
			return act != nil && act.Status == workflow.ActivationBlocked
		})
		inst := f.instance(t, wf)
		if calls := f.providerCalls.Load(); calls != 3 {
			t.Fatalf("provider invoked %d times, want exactly 3; observed %s", calls, describeInstance(&inst, calls))
		}
		attempts := attemptsForNode(&inst, "produce")
		if len(attempts) != 3 {
			t.Fatalf("produce recorded %d attempts, want exactly 3; observed %s", len(attempts), describeInstance(&inst, f.providerCalls.Load()))
		}
		for _, attempt := range attempts {
			if attempt.Status != workflow.AttemptFailed {
				t.Fatalf("produce attempt %s status = %s, want failed; observed %s", attempt.ID, attempt.Status, describeInstance(&inst, f.providerCalls.Load()))
			}
			if attempt.MarkerLabel == "contract_unmet" {
				t.Fatalf("produce attempt %s carries the contract_unmet marker; a non-success exit must keep today's behavior; observed %s", attempt.ID, describeInstance(&inst, f.providerCalls.Load()))
			}
		}
		if act := activationFor(&inst, "consume"); act != nil {
			t.Fatalf("consume dispatched despite produce never completing; observed %s", describeInstance(&inst, f.providerCalls.Load()))
		}
	})

	t.Run("canceled exit does not retry", func(t *testing.T) {
		t.Chdir(t.TempDir())
		provider := &stableScriptedProvider{attempt: -1}
		// cancelled maps to exit code 130: an AttemptCanceled terminal fact
		// that never retries and never completes.
		appendSessionScript(t, provider, "ses_cancel", nil, "cancelled")
		f := newAutoHandoffFixture(t, "auto-handoff-cancel-exit", provider)
		wf := f.addWorkflow(t, "tmpl-auto-handoff-cancel-exit", autoHandoffChainTemplate(t, "tmpl-auto-handoff-cancel-exit", 900))
		f.enableController(t, 2)

		f.waitForInstance(t, wf, "the canceled attempt to settle", func(inst *workflow.WorkflowInstance) bool {
			attempts := attemptsForNode(inst, "produce")
			act := activationFor(inst, "produce")
			return len(attempts) == 1 && attempts[0].Status == workflow.AttemptCanceled &&
				act != nil && act.Status == workflow.ActivationAttemptFailed
		})
		inst := f.instance(t, wf)
		if calls := f.providerCalls.Load(); calls != 1 {
			t.Fatalf("provider invoked %d times, want exactly 1 (canceled attempts do not retry); observed %s", calls, describeInstance(&inst, calls))
		}
	})
}

// autoHandoffLoopTemplate builds a one-loop-node template with passed/failed
// branches to a dependent consume node.
func autoHandoffLoopTemplate(t *testing.T, templateID, loopPath string) []byte {
	t.Helper()
	template := map[string]any{
		"schema_version":   1,
		"template_id":      templateID,
		"template_version": "1",
		"entry_nodes":      []string{"step"},
		"nodes": []any{
			map[string]any{
				"id":           "step",
				"action":       map[string]any{"type": "loop", "loop_file": loopPath},
				"dispatch":     map[string]any{"mode": "auto", "controller_id": "c1"},
				"branches":     map[string]any{"passed": "consume", "failed": "consume"},
				"retry_policy": map[string]any{"max_attempts": 2, "exhaustion": "block"},
			},
			map[string]any{
				"id":           "consume",
				"dependencies": []string{"step"},
				"action":       map[string]any{"type": "run", "prompt": "consume the verdict"},
				"dispatch":     map[string]any{"mode": "auto", "controller_id": "c1"},
				"retry_policy": map[string]any{"max_attempts": 2, "exhaustion": "block"},
			},
		},
		"terminal_outcomes":    []string{"done"},
		"default_lease_policy": map[string]any{"ttl_seconds": 900},
	}
	return mustJSON(t, template)
}

// TestAutoHandoffRuntimeFinishingBeforeDispatchReturns proves an attempt
// whose runtime finishes its turn before the executor's Dispatch returns is
// still terminated and completed: the afterAttemptSpawn hook holds Dispatch
// until the runtime is done, and the workflow must still complete with no
// lease heartbeat left running.
func TestAutoHandoffRuntimeFinishingBeforeDispatchReturns(t *testing.T) {
	t.Chdir(t.TempDir())
	provider := &stableScriptedProvider{attempt: -1}
	_ = produceWorkerDeclaredResult(t, provider, "ses_fast", "", "")
	f := newAutoHandoffFixture(t, "auto-handoff-fast-exit", provider)
	f.sup.testHooks.afterAttemptSpawn = func(runtimeID string) {
		f.sup.controlMu.Lock()
		child := f.sup.runtimes[runtimeID]
		f.sup.controlMu.Unlock()
		if child == nil {
			return
		}
		select {
		case <-child.done:
		case <-time.After(5 * time.Second):
		}
	}
	template := map[string]any{
		"schema_version":   1,
		"template_id":      "tmpl-auto-handoff-fast-exit",
		"template_version": "1",
		"entry_nodes":      []string{"start"},
		"nodes": []any{map[string]any{
			"id":           "start",
			"action":       map[string]any{"type": "run", "prompt": "do the thing"},
			"dispatch":     map[string]any{"mode": "auto", "controller_id": "c1"},
			"retry_policy": map[string]any{"max_attempts": 1, "exhaustion": "block"},
		}},
		"terminal_outcomes": []string{"done"},
	}
	wf := f.addWorkflow(t, "tmpl-auto-handoff-fast-exit", mustJSON(t, template))
	f.enableController(t, 2)

	f.waitForInstance(t, wf, "the workflow to complete although the runtime finished before Dispatch returned", func(inst *workflow.WorkflowInstance) bool {
		return inst.Status == workflow.WorkflowCompleted
	})
	f.waitForInstance(t, wf, "every lease heartbeat to stop", func(*workflow.WorkflowInstance) bool {
		f.sup.heartbeatMu.Lock()
		defer f.sup.heartbeatMu.Unlock()
		return len(f.sup.heartbeats) == 0
	})
}
