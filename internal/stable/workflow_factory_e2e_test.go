package stable

// workflow_factory_e2e_test.go runs the shipped software-factory work
// template (templates/software-factory/work.json) end to end over a real
// supervisor: the real registered executors (run/loop/team dispatch through
// the production spawn path), a real enabled controller with its leader loop
// and poll runner, real admission, the real workflow.Manager over a durable
// store, and real completion validation. The only fakes are the scripted
// provider (standing in for the real inference backend behind the production
// provider-factory seam) and the fixture adapters for the external gates.
//
// AUTO nodes are never hand-driven: they advance only through the
// controller's dispatch, the scripted worker's session, and the
// supervisor-side completion (internal/workflow/autocompletion.go evaluates
// the node's declared contract in the attempt's working directory). Manual
// nodes (intake, hardening, merge-auth) are completed by the test as the
// human via the claim holder's token — the only token surface the harness
// ever touches. Workers never see it.
//
// All workers share the supervisor process's working directory (the known
// spawn-cwd gap), so the working dir is a real git repo with a commit:
// publication's pr_head output resolves from `git rev-parse HEAD` there and
// the review gates bind to that exact head.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/workflow"
	"github.com/sdougbrown/avenor/internal/workflowcontroller"
)

// factoryTemplateDir is the repo-relative location of the shipped factory
// template and its prompt/loop/team fixtures. It is resolved to an absolute
// path at init, before any test chdirs the process into a scratch working
// directory.
const factoryTemplateDir = "../../templates/software-factory"

var factoryTemplateDirAbs = func() string {
	abs, err := filepath.Abs(factoryTemplateDir)
	if err != nil {
		panic(fmt.Sprintf("resolve %s: %v", factoryTemplateDir, err))
	}
	return abs
}()

// factoryWorkTemplateJSON loads the shipped work template and rewrites its
// prompt_file/loop_file/team_file/roster_file references to absolute repo
// paths so the template dispatches from the test's working directory without
// modification of the shipped fixture.
func factoryWorkTemplateJSON(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(factoryTemplateDirAbs, "work.json"))
	if err != nil {
		t.Fatalf("read work template: %v", err)
	}
	var template map[string]any
	if err := json.Unmarshal(data, &template); err != nil {
		t.Fatalf("decode work template: %v", err)
	}
	abs := func(rel string) string {
		absPath := filepath.Join(factoryTemplateDirAbs, rel)
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

// factoryWorkerScript scripts one worker session: the files the worker
// writes into its working directory when it runs, the message chunks it
// streams (loop/team terminal markers travel here), an optional action run
// before the chunks (e.g. committing the republished head), the stop reason
// it ends with, and an optional hold that blocks the worker mid-run so the
// test can observe a live attempt holding a lease or concurrency key.
type factoryWorkerScript struct {
	sessionID  string
	write      map[string]string
	chunks     []string
	action     func() error
	stopReason string
	hold       chan struct{}
}

// factoryWorkerProvider is the scripted inference provider behind the
// production provider-factory seam. Sessions are handed out strictly in
// script order — one script per spawned attempt, exactly like the real
// backend — and a script's side effects run when its worker is prompted, not
// when it is queued.
type factoryWorkerProvider struct {
	mu        sync.Mutex
	scripts   []factoryWorkerScript
	next      int
	bySession map[string]int
	channels  map[string]chan events.Event
	started   []string // ordered session IDs handed to the runtime
}

func (p *factoryWorkerProvider) append(script factoryWorkerScript) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if script.stopReason == "" {
		script.stopReason = "end_turn"
	}
	if script.sessionID == "" {
		script.sessionID = fmt.Sprintf("ses_factory_%d", len(p.scripts))
	}
	p.scripts = append(p.scripts, script)
}

// releaseAllHolds unblocks every held worker; safe to call repeatedly.
func (p *factoryWorkerProvider) releaseAllHolds() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.scripts {
		if p.scripts[i].hold != nil {
			close(p.scripts[i].hold)
			p.scripts[i].hold = nil
		}
	}
}

// releaseHold unblocks one held worker by session ID; safe to call
// repeatedly.
func (p *factoryWorkerProvider) releaseHold(sessionID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.scripts {
		if p.scripts[i].sessionID == sessionID && p.scripts[i].hold != nil {
			close(p.scripts[i].hold)
			p.scripts[i].hold = nil
		}
	}
}

func (p *factoryWorkerProvider) openSession() (runtime.Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.next >= len(p.scripts) {
		return runtime.Session{}, fmt.Errorf("missing scripted worker attempt %d (only %d scripted)", p.next, len(p.scripts))
	}
	index := p.next
	p.next++
	sessionID := p.scripts[index].sessionID
	if p.bySession == nil {
		p.bySession = make(map[string]int)
		p.channels = make(map[string]chan events.Event)
	}
	p.bySession[sessionID] = index
	p.channels[sessionID] = make(chan events.Event, len(p.scripts[index].chunks)+1)
	p.started = append(p.started, sessionID)
	return runtime.Session{SessionID: sessionID}, nil
}

func (p *factoryWorkerProvider) Start(context.Context, runtime.StartOptions) (runtime.Session, error) {
	return p.openSession()
}

func (p *factoryWorkerProvider) Resume(context.Context, string) (runtime.Session, error) {
	return p.openSession()
}

// Prompt runs the worker: side effects first (the worker does its work),
// then the mid-run hold, then the streamed chunks and the session end.
func (p *factoryWorkerProvider) Prompt(ctx context.Context, sessionID, _ string) error {
	p.mu.Lock()
	index, ok := p.bySession[sessionID]
	ch := p.channels[sessionID]
	var script factoryWorkerScript
	if ok {
		script = p.scripts[index]
	}
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("no scripted worker session %q", sessionID)
	}
	for name, content := range script.write {
		if err := os.WriteFile(name, []byte(content), 0o600); err != nil {
			return err
		}
	}
	if script.action != nil {
		if err := script.action(); err != nil {
			return err
		}
	}
	if script.hold != nil {
		select {
		case <-script.hold:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	defer close(ch)
	for _, chunk := range script.chunks {
		select {
		case ch <- events.Event{Event: "agent.message_chunk", SessionID: sessionID, Fields: map[string]any{"delta": chunk}}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	select {
	case ch <- events.Event{Event: "session.end", SessionID: sessionID, Fields: map[string]any{"stop_reason": script.stopReason}}:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (p *factoryWorkerProvider) Cancel(context.Context, string) error { return nil }

func (p *factoryWorkerProvider) Events(_ context.Context, sessionID string) (<-chan events.Event, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ch, ok := p.channels[sessionID]; ok {
		return ch, nil
	}
	return nil, fmt.Errorf("no scripted worker session %q", sessionID)
}

func (p *factoryWorkerProvider) AnswerPermission(context.Context, string, string, runtime.PermissionResponse) error {
	return nil
}

func (p *factoryWorkerProvider) Capabilities(context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{}, nil
}

// sessionCount reports how many worker sessions the runtime has started.
func (p *factoryWorkerProvider) sessionCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.started)
}

// factoryNodeShape records the two properties the harness must respect when
// driving a node by hand: its action kind and whether it dispatches auto.
type factoryNodeShape struct {
	actionKind   string
	dispatchAuto bool
}

// factoryE2E drives the shipped work template over a supervisor with fixture
// adapters and the scripted worker provider, with the software-factory
// controller registered (disabled until the test enables it).
type factoryE2E struct {
	sup          *Supervisor
	mgr          *workflow.Manager
	cstore       *workflowcontroller.ControllerStore
	root         string
	adapterDir   string
	wf           string
	provider     *factoryWorkerProvider
	nodes        map[string]factoryNodeShape
	head         string // the git head the first publication publishes
	controllerID string
}

// newFactoryE2E stages the fixture adapters (each rendered with the working
// directory's git head, which the exact-subject fixtures need), prepares the
// working directory as a git repo with one commit, registers and
// instantiates the shipped work template pinned to the given worktree param,
// and creates the disabled software-factory controller with the given
// in-flight budget.
func newFactoryE2E(t *testing.T, name string, maxInflight int, worktree string, ciAdapter, reviewAdapter func(head string) string) *factoryE2E {
	t.Helper()
	// The attempt working directory is the supervisor process cwd (the
	// direct-run executor's spawn Dir); chdir to a scratch dir that is a real
	// git repo with a commit so publication's git-head output resolves.
	workDir := t.TempDir()
	t.Chdir(workDir)
	factoryGit(t, "init")
	factoryGit(t, "config", "user.email", "factory-e2e@example.invalid")
	factoryGit(t, "config", "user.name", "factory e2e")
	if err := os.WriteFile("README.md", []byte("# factory e2e\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	factoryGit(t, "add", "README.md")
	factoryGit(t, "commit", "-m", "initial")
	head := strings.TrimSpace(string(factoryGitOutput(t, "rev-parse", "HEAD")))

	root := filepath.Join(t.TempDir(), "wfroot")
	adapterDir := t.TempDir()
	if resolved, err := filepath.EvalSymlinks(adapterDir); err == nil {
		adapterDir = resolved
	}
	if err := os.Chmod(adapterDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for id, script := range map[string]string{
		"circleci-pipeline": ciAdapter(head),
		"github-pr-review":  reviewAdapter(head),
	} {
		exe := filepath.Join(adapterDir, id+".sh")
		if err := os.WriteFile(exe, []byte(script), 0o700); err != nil {
			t.Fatalf("stage adapter %s: %v", id, err)
		}
		writePollManifest(t, adapterDir, id+".json", id, exe)
	}
	// Fail loudly here rather than as a silent timeout later: a trust or
	// manifest load failure leaves the supervisor's registry empty and every
	// poll reporting adapter_unavailable forever.
	reg, err := workflowcontroller.LoadAdapterRegistry(adapterDir)
	if err != nil {
		t.Fatalf("adapter registry load from %s: %v", adapterDir, err)
	}
	for _, wantID := range []string{"circleci-pipeline", "github-pr-review"} {
		if _, ok := reg.Lookup(wantID); !ok {
			t.Fatalf("adapter registry missing %s", wantID)
		}
	}
	sup := NewSupervisor(Config{
		ControlSocket:      newStableSocketPath(t, name),
		WorkflowRoot:       root,
		WorkflowAdapterDir: adapterDir,
		MaxRuntimes:        16,
		MaxTreeBudget:      16,
		ShutdownTimeout:    0,
	})
	// The startup barrier starts leader loops for recovered enabled
	// controllers, so the fast test cadence must be set before the barrier
	// runs.
	sup.controllerRenewInterval = 25 * time.Millisecond
	sup.controllerPollBaseDelay = 200 * time.Millisecond
	mgr, cstore, err := sup.workflowBarrierResult()
	if err != nil {
		t.Fatalf("workflow barrier: %v", err)
	}
	f := &factoryE2E{
		sup: sup, mgr: mgr, cstore: cstore, root: root, adapterDir: adapterDir,
		provider:     &factoryWorkerProvider{},
		controllerID: "software-factory",
		head:         head,
	}
	sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		return f.provider, nil
	}
	templateBody := factoryWorkTemplateJSON(t)
	if _, err := mgr.WorkflowCreate(templateBody); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	var parsed struct {
		Nodes []struct {
			ID     string `json:"id"`
			Action struct {
				Type string `json:"type"`
			} `json:"action"`
			Dispatch *struct {
				Mode string `json:"mode"`
			} `json:"dispatch"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(templateBody, &parsed); err != nil {
		t.Fatalf("parse template nodes: %v", err)
	}
	f.nodes = make(map[string]factoryNodeShape, len(parsed.Nodes))
	for _, node := range parsed.Nodes {
		f.nodes[node.ID] = factoryNodeShape{actionKind: node.Action.Type, dispatchAuto: node.Dispatch != nil && node.Dispatch.Mode == "auto"}
	}
	out, err := mgr.WorkflowInstantiate(mustJSON(t, map[string]any{
		"template_id": "software-factory-work", "template_version": "1.2.0",
		"params": map[string]string{"worktree": worktree},
	}))
	if err != nil {
		t.Fatalf("WorkflowInstantiate: %v", err)
	}
	f.wf = out.(map[string]any)["workflow_id"].(string)
	if _, err := cstore.Create(f.controllerID, maxInflight); err != nil {
		t.Fatalf("controller create: %v", err)
	}
	t.Cleanup(func() { f.stop(t) })
	return f
}

// stop tears the fixture down: unblock held workers, disable the controller
// so no fresh dispatch starts, stop the leader loops, cancel the runtimes,
// and wait for them to go terminal before the workflow root disappears.
func (f *factoryE2E) stop(t *testing.T) {
	t.Helper()
	f.provider.releaseAllHolds()
	if _, err := f.cstore.SetDesiredState(f.controllerID, workflowcontroller.DesiredDisabled, "test cleanup"); err != nil {
		t.Logf("cleanup disable: %v", err)
	}
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

// enable turns the controller on and waits until this supervisor holds the
// leader lease.
func (f *factoryE2E) enable(t *testing.T) {
	t.Helper()
	if _, err := f.sup.WorkflowControllerEnable(f.controllerID); err != nil {
		t.Fatalf("controller enable: %v", err)
	}
	waitForControllerLeader(t, f.cstore, f.controllerID, func(rec workflowcontroller.ControllerRecord) bool {
		return rec.Leader != nil && rec.Leader.OwnerID == f.sup.supervisorIdentity()
	})
}

// disable stops future dispatch and releases the leader lease. Only for
// cleanup and end-of-test quiescence — never to hand-drive auto nodes.
func (f *factoryE2E) disable(t *testing.T) {
	t.Helper()
	if _, err := f.cstore.SetDesiredState(f.controllerID, workflowcontroller.DesiredDisabled, "test quiesce"); err != nil {
		t.Fatalf("controller disable: %v", err)
	}
	f.sup.stopControllerLoop(f.controllerID)
}

// instantiate creates another work item from the same template and returns
// its workflow id.
func (f *factoryE2E) instantiate(t *testing.T, worktree string) string {
	t.Helper()
	out, err := f.mgr.WorkflowInstantiate(mustJSON(t, map[string]any{
		"template_id": "software-factory-work", "template_version": "1.2.0",
		"params": map[string]string{"worktree": worktree},
	}))
	if err != nil {
		t.Fatalf("instantiate worktree %q: %v", worktree, err)
	}
	return out.(map[string]any)["workflow_id"].(string)
}

// instanceOn reads one workflow's instance snapshot.
func (f *factoryE2E) instanceOn(t *testing.T, wf string) workflow.WorkflowInstance {
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

func (f *factoryE2E) instance(t *testing.T) workflow.WorkflowInstance {
	t.Helper()
	return f.instanceOn(t, f.wf)
}

// newestActivation returns the newest activation of the node, or nil.
func (f *factoryE2E) newestActivation(t *testing.T, wf, nodeID string) *workflow.Activation {
	t.Helper()
	inst := f.instanceOn(t, wf)
	var found *workflow.Activation
	for i := range inst.Activations {
		if inst.Activations[i].NodeID == workflow.NodeID(nodeID) {
			found = &inst.Activations[i]
		}
	}
	return found
}

// waitInstanceOn polls cond against a fresh snapshot of wf, failing with the
// full observed state after a bounded deadline.
func (f *factoryE2E) waitInstanceOn(t *testing.T, wf, what string, cond func(inst *workflow.WorkflowInstance) bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var inst workflow.WorkflowInstance
	for {
		inst = f.instanceOn(t, wf)
		if cond(&inst) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s; observed %s", what, describeInstance(&inst, int32(f.provider.sessionCount())))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// newestActivationFor returns the newest activation of the node in the
// snapshot (repeated nodes append later activations), or nil.
func newestActivationFor(inst *workflow.WorkflowInstance, nodeID workflow.NodeID) *workflow.Activation {
	var found *workflow.Activation
	for i := range inst.Activations {
		if inst.Activations[i].NodeID == nodeID {
			found = &inst.Activations[i]
		}
	}
	return found
}

// waitNodeSatisfied waits until the node's newest activation is satisfied
// with the wanted outcome.
func (f *factoryE2E) waitNodeSatisfied(t *testing.T, wf, nodeID, outcome string) {
	t.Helper()
	f.waitInstanceOn(t, wf, nodeID+" satisfied with outcome "+outcome, func(inst *workflow.WorkflowInstance) bool {
		act := newestActivationFor(inst, workflow.NodeID(nodeID))
		return act != nil && act.Status == workflow.ActivationSatisfied && act.SelectedOutcome == workflow.OutcomeName(outcome)
	})
}

// waitNodeStatus waits until the node's newest activation has the status.
func (f *factoryE2E) waitNodeStatus(t *testing.T, wf, nodeID string, want workflow.ActivationStatus) {
	t.Helper()
	f.waitInstanceOn(t, wf, nodeID+" status "+string(want), func(inst *workflow.WorkflowInstance) bool {
		act := newestActivationFor(inst, workflow.NodeID(nodeID))
		return act != nil && act.Status == want
	})
}

// outputValue returns the recorded value of a node output as a string.
// Repeated nodes (a republishing publication) record the output once per
// activation in chronological order, so the LAST recording is the newest
// activation's value.
func (f *factoryE2E) outputValue(t *testing.T, inst *workflow.WorkflowInstance, outputID string) string {
	t.Helper()
	found := ""
	for _, o := range inst.Outputs {
		if string(o.DefinitionID) == outputID {
			found = string(o.Value)
		}
	}
	if found == "" {
		t.Fatalf("output %q not recorded; observed %s", outputID, describeInstance(inst, int32(f.provider.sessionCount())))
	}
	var value any
	if err := json.Unmarshal([]byte(found), &value); err != nil {
		return found
	}
	if s, ok := value.(string); ok {
		return s
	}
	return found
}

// completeManualNode completes a MANUAL node as the human: claim, start,
// then — for a provider-backed manual run like hardening — wait for the
// spawned worker's attempt to terminate through the production path before
// issuing the completion with the claim holder's token. Auto-dispatched
// nodes are refused: the harness never hand-drives them.
func (f *factoryE2E) completeManualNode(t *testing.T, wf, nodeID, outcome string, outputs []map[string]any, artifacts []map[string]any) {
	t.Helper()
	shape, ok := f.nodes[nodeID]
	if !ok {
		t.Fatalf("node %q is not in the template", nodeID)
	}
	if shape.dispatchAuto {
		t.Fatalf("refusing to hand-drive auto-dispatched node %q", nodeID)
	}
	act := f.newestActivation(t, wf, nodeID)
	if act == nil || act.Status != workflow.ActivationPending {
		t.Fatalf("no pending %s activation: %+v", nodeID, act)
	}
	res, err := f.mgr.WorkflowCommand(wf, mustJSON(t, map[string]any{
		"op": "claim", "node_id": nodeID, "activation_id": string(act.ID), "actor": "factory-e2e-human",
	}))
	if err != nil {
		t.Fatalf("%s claim: %v", nodeID, err)
	}
	claim := res.(map[string]any)
	start, err := f.mgr.WorkflowCommand(wf, mustJSON(t, map[string]any{
		"op": "start", "node_id": nodeID, "activation_id": string(act.ID),
		"lease_id": claim["lease_id"], "owner_token": claim["owner_token"],
	}))
	if err != nil {
		t.Fatalf("%s start: %v", nodeID, err)
	}
	attemptID := start.(map[string]any)["attempt_id"].(string)
	if shape.actionKind == "run" {
		// Provider-backed manual dispatch: the start spawned the worker
		// through the production run executor. Wait for the attempt to
		// terminate through the supervisor's own terminal path — the test
		// never records the termination itself.
		f.waitInstanceOn(t, wf, nodeID+"'s worker attempt to succeed", func(inst *workflow.WorkflowInstance) bool {
			for _, a := range inst.Attempts {
				if a.Identity.NodeID == workflow.NodeID(nodeID) && string(a.ID) == attemptID {
					return a.Status == workflow.AttemptSucceeded
				}
			}
			return false
		})
		// A manual node's clean exit is a plain terminal fact: the supervisor
		// neither completes it nor relabels the attempt, and the activation
		// waits for the claim holder's completion.
		inst := f.instanceOn(t, wf)
		for _, a := range inst.Attempts {
			if string(a.ID) == attemptID && a.MarkerLabel == "contract_unmet" {
				t.Fatalf("%s manual attempt relabeled contract_unmet by supervisor completion", nodeID)
			}
		}
		for _, a := range inst.Activations {
			if a.ID == act.ID && a.Status != workflow.ActivationRunning {
				t.Fatalf("%s manual activation status = %s after its worker exited, want running until the human completes it; observed %s",
					nodeID, a.Status, describeInstance(&inst, int32(f.provider.sessionCount())))
			}
		}
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
	if _, err := f.mgr.WorkflowCommand(wf, mustJSON(t, cmd)); err != nil {
		t.Fatalf("%s complete: %v", nodeID, err)
	}
}

// cwdArtifact builds an artifact reference for a file the worker wrote into
// the shared working directory.
func cwdArtifact(t *testing.T, storedPath string) map[string]any {
	t.Helper()
	abs, err := filepath.Abs(storedPath)
	if err != nil {
		t.Fatalf("resolve %s: %v", storedPath, err)
	}
	return map[string]any{"src_path": abs, "stored_path": storedPath, "non_empty": true}
}

// driveIntake completes the human intake node with the issue text and base
// SHA, which makes the auto assessment the first dispatchable candidate.
func (f *factoryE2E) driveIntake(t *testing.T, wf string) {
	t.Helper()
	f.completeManualNode(t, wf, "intake", "ready",
		[]map[string]any{
			{"definition_id": "issue", "value": "Harden the candidate index rebuild path"},
			{"definition_id": "base_sha", "value": "6e77a0d"},
		}, nil)
}

// waitThroughPublication drives intake → assessment → draft-plan → hardening
// (human) → execution → verification → publication: the auto nodes complete
// through the supervisor handoff, the human hardening checkpoint is claimed,
// started (spawning its worker through the production run executor), and
// completed with the worker's artifact.
func (f *factoryE2E) waitThroughPublication(t *testing.T) {
	t.Helper()
	f.driveIntake(t, f.wf)
	f.waitNodeSatisfied(t, f.wf, "assessment", "ready")
	f.waitNodeSatisfied(t, f.wf, "draft-plan", "ready")
	f.completeManualNode(t, f.wf, "hardening", "ready",
		[]map[string]any{{"definition_id": "hardened_plan", "value": "plan.md"}},
		[]map[string]any{cwdArtifact(t, "plan.md")})
	f.waitNodeSatisfied(t, f.wf, "execution", "done")
	f.waitNodeSatisfied(t, f.wf, "verification", "passed")
	f.waitNodeSatisfied(t, f.wf, "publication", "published")
}

// scriptRunWorker appends one run-node worker session that writes the given
// files and exits cleanly.
func scriptRunWorker(p *factoryWorkerProvider, sessionID string, files map[string]string) {
	p.append(factoryWorkerScript{sessionID: sessionID, write: files})
}

// scriptExecutionLoop appends the execution loop node's three phase
// sessions: pre-implement, the loop test phase that exits on the green
// marker, and the post-record phase that writes the declared artifact.
func scriptExecutionLoop(p *factoryWorkerProvider) {
	p.append(factoryWorkerScript{sessionID: "ses_exec_implement"})
	p.append(factoryWorkerScript{sessionID: "ses_exec_test", chunks: []string{"Focused verification ran clean.\n<|workflow: exit | tests green|>\n"}})
	p.append(factoryWorkerScript{sessionID: "ses_exec_record", write: map[string]string{
		"execution.md": "## Execution\nAll plan stages implemented; verification green.\n",
	}})
}

// scriptTeam appends the sessions a team node consumes in order: the pre
// scope phase, one session per team member (they run in parallel but their
// scripts are interchangeable), and the post synthesize phase that writes
// the declared artifact and ends with the verdict's terminal marker.
func scriptTeam(p *factoryWorkerProvider, prefix string, members int, artifact, markerLabel string) {
	p.append(factoryWorkerScript{sessionID: prefix + "_scope"})
	for i := 0; i < members; i++ {
		p.append(factoryWorkerScript{sessionID: fmt.Sprintf("%s_member_%d", prefix, i)})
	}
	p.append(factoryWorkerScript{
		sessionID: prefix + "_synthesize",
		write:     map[string]string{artifact: "Verdict: " + markerLabel + "\n"},
		chunks:    []string{"Synthesized the verdict.\n<|workflow: exit | " + markerLabel + "|>\n"},
	})
}

// scriptPublication appends one publication worker session. When republish
// is set the worker commits the accumulated working tree first, so the
// publication completes under a NEW git head.
func scriptPublication(p *factoryWorkerProvider, sessionID string, republish bool) {
	script := factoryWorkerScript{
		sessionID: sessionID,
		write:     map[string]string{"pr-info.json": `{"repository":"sdougbrown/avenor","pr_number":143}`},
	}
	if republish {
		script.action = func() error {
			if err := os.WriteFile("republish-note.txt", []byte("republished under a new head\n"), 0o600); err != nil {
				return err
			}
			if out, err := exec.Command("git", "add", "-A").CombinedOutput(); err != nil {
				return fmt.Errorf("git add: %v: %s", err, out)
			}
			if out, err := exec.Command("git", "commit", "-m", "republish").CombinedOutput(); err != nil {
				return fmt.Errorf("git commit: %v: %s", err, out)
			}
			return nil
		}
	}
	p.append(script)
}

// factoryGit runs one git command in the working directory and fails the
// test on error.
func factoryGit(t *testing.T, args ...string) []byte {
	t.Helper()
	out, err := exec.Command("git", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
	return out
}

func factoryGitOutput(t *testing.T, args ...string) []byte {
	t.Helper()
	out, err := exec.Command("git", args...).Output()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
	return out
}

// factoryAdapterBody is the shared shell preamble of the fixture adapters:
// the poll request arrives as one JSON object on stdin and the adapter
// extracts the pinned gate inputs to build the subject it reports on.
const factoryAdapterBody = `#!/bin/sh
input=$(cat)
repository=$(printf '%s' "$input" | sed -n 's/.*"repository":"\([^"]*\)".*/\1/p')
pull_number=$(printf '%s' "$input" | sed -n 's/.*"pull_number":\([0-9]*\).*/\1/p')
head_sha=$(printf '%s' "$input" | sed -n 's/.*"head_sha":"\([^"]*\)".*/\1/p')
`

// factoryAdapterPrintf is the shared result line; result must already be a
// shell expansion yielding the raw result enum value.
const factoryAdapterPrintf = `printf '%s' '{"version":1,"result":"'"$result"'","subject":{"type":"pull_request","repository":"'"$repository"'","pull_request":'"$pull_number"',"revision":"'"$revision"'"},"observed_at":"2026-01-01T00:00:00Z","summary":"fixture"}'
`

// factoryEchoAdapter reports the given result on the exact subject the gate
// pinned: every subject field is echoed back from the pinned inputs.
func factoryEchoAdapter(result string) string {
	return factoryAdapterBody + `result="` + result + `"
revision="$head_sha"
` + factoryAdapterPrintf
}

// factoryForeignSubjectAdapter reports the given result on a FIXED subject
// revision regardless of the pinned inputs — a result observed on a
// different head than the gate's bound subject.
func factoryForeignSubjectAdapter(result, revision string) string {
	return factoryAdapterBody + `result="` + result + `"
revision="` + revision + `"
` + factoryAdapterPrintf
}

// factoryHeadSwitchAdapter reports resultOnHead for exactly the given head
// and resultOtherwise for any other — the exact-head review that demands a
// fresh verdict after a republish.
func factoryHeadSwitchAdapter(head, resultOnHead, resultOtherwise string) string {
	return factoryAdapterBody + `if [ "$head_sha" = "` + head + `" ]; then
  result="` + resultOnHead + `"
else
  result="` + resultOtherwise + `"
fi
revision="$head_sha"
` + factoryAdapterPrintf
}

// findCursor returns the poll cursor for one activation's gate.
func (f *factoryE2E) findCursor(wf, actID, gateID string) (workflowcontroller.PollCursor, bool) {
	rec, _, err := f.cstore.Get(f.controllerID)
	if err != nil {
		return workflowcontroller.PollCursor{}, false
	}
	for _, c := range rec.PollCursors {
		if c.WorkflowID == wf && c.ActivationID == actID && c.GateID == gateID {
			return *c, true
		}
	}
	return workflowcontroller.PollCursor{}, false
}

// waitCursor waits until a committed cursor exists for the activation's gate.
func (f *factoryE2E) waitCursor(t *testing.T, wf, actID, gateID string) workflowcontroller.PollCursor {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if cursor, ok := f.findCursor(wf, actID, gateID); ok && cursor.PollID != "" {
			return cursor
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for poll cursor %s on %s", gateID, actID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// gateInstances returns the gate instances recorded for one activation.
func gateInstances(inst *workflow.WorkflowInstance, actID workflow.ActivationID) []workflow.GateInstance {
	var out []workflow.GateInstance
	for _, gi := range inst.Gates {
		if gi.ActivationID == actID {
			out = append(out, gi)
		}
	}
	return out
}

// TestFactoryWorkCleanPathStopsAtHumanMergeAuth proves the shipped template's
// clean path through the production handoff: intake (human) → assessment and
// draft-plan (auto run) → hardening (human checkpoint over a spawned worker)
// → execution (auto loop exiting on its green marker) → verification (auto
// team passing on its marker) → publication (auto run publishing the exact
// git head and pr-info outputs) → the external review parks on the bound
// exact subject with zero runtime state, both fixture adapters report clean
// through the poll runner with staged evidence, the review resolves clean,
// and the work stops behind the merge-authorization human gate: no decision,
// no reconciliation, no merge.
func TestFactoryWorkCleanPathStopsAtHumanMergeAuth(t *testing.T) {
	f := newFactoryE2E(t, "factory-e2e-clean", 4, "avenor-issue-115",
		func(string) string { return factoryEchoAdapter("passed") },
		func(string) string { return factoryEchoAdapter("passed") })
	scriptRunWorker(f.provider, "ses_assessment", map[string]string{"assessment.md": "## Assessment\n"})
	scriptRunWorker(f.provider, "ses_draft_plan", map[string]string{"plan.md": "## Plan\n"})
	scriptRunWorker(f.provider, "ses_hardening", map[string]string{"plan.md": "## Hardened plan\n"})
	scriptExecutionLoop(f.provider)
	scriptTeam(f.provider, "ses_verification", 3, "verification.md", "passed")
	scriptPublication(f.provider, "ses_publication_1", false)
	f.enable(t)

	f.driveIntake(t, f.wf)
	f.waitNodeSatisfied(t, f.wf, "assessment", "ready")
	f.waitNodeSatisfied(t, f.wf, "draft-plan", "ready")
	f.completeManualNode(t, f.wf, "hardening", "ready",
		[]map[string]any{{"definition_id": "hardened_plan", "value": "plan.md"}},
		[]map[string]any{cwdArtifact(t, "plan.md")})
	f.waitNodeSatisfied(t, f.wf, "execution", "done")
	f.waitNodeSatisfied(t, f.wf, "verification", "passed")
	f.waitNodeSatisfied(t, f.wf, "publication", "published")

	// Publication's outputs resolved from the worker's artifact and the git
	// head of the shared working directory.
	inst := f.instance(t)
	if got := f.outputValue(t, &inst, "pr_head"); got != f.head {
		t.Fatalf("publication pr_head = %q, want the repo head %q; observed %s", got, f.head, describeInstance(&inst, int32(f.provider.sessionCount())))
	}
	if got := f.outputValue(t, &inst, "repository"); got != "sdougbrown/avenor" {
		t.Fatalf("publication repository = %q, want sdougbrown/avenor", got)
	}
	if got := f.outputValue(t, &inst, "pr_number"); got != "143" {
		t.Fatalf("publication pr_number = %q, want 143", got)
	}

	// The review parks kernel-locally on the bound exact subject: no
	// attempt, no lease, cursors for both gates, committed polls.
	f.waitNodeStatus(t, f.wf, "review", workflow.ActivationAwaitingGate)
	review := f.newestActivation(t, f.wf, "review")
	if len(review.AttemptIDs) != 0 || review.ActiveLease != nil {
		t.Fatalf("parked review recorded runtime state: %+v", review)
	}
	for _, gateID := range []string{"ci", "review-verdict"} {
		f.waitCursor(t, f.wf, string(review.ID), gateID)
	}

	// Both fixture adapters report clean: the pinned success outcome resolves
	// the review with staged evidence per gate.
	f.waitNodeSatisfied(t, f.wf, "review", "clean")
	review = f.newestActivation(t, f.wf, "review")
	inst = f.instance(t)
	gates := gateInstances(&inst, review.ID)
	if len(gates) != 2 {
		t.Fatalf("review gate instances = %d, want 2", len(gates))
	}
	for _, gi := range gates {
		if gi.Status != workflow.GatePassed || len(gi.EvidenceIDs) != 1 || gi.ResponseHash == "" {
			t.Fatalf("gate instance %s = %+v, want passed with staged evidence", gi.GateID, gi)
		}
	}

	// The work stops at merge-auth: pending behind the human gate, the gate
	// undecided, reconciliation never started, nothing merged.
	f.waitNodeStatus(t, f.wf, "merge-auth", workflow.ActivationPending)
	inst = f.instance(t)
	for _, gi := range inst.Gates {
		if gi.GateID == "merge-authorization" {
			t.Fatalf("human merge gate decided: %+v", gi)
		}
	}
	if act := f.newestActivation(t, f.wf, "reconciliation"); act != nil {
		t.Fatalf("reconciliation activation exists before human authorization: %+v", act)
	}
	if inst.Status != workflow.WorkflowActive {
		t.Fatalf("workflow status = %s, want still active behind the human gate", inst.Status)
	}

	// The whole pipeline ran on scripted workers alone: one session per auto
	// dispatch (loop and team children consume one per phase), no duplicates,
	// no manual claims of auto nodes. The verification team's three member
	// sessions run in parallel, so their relative order is asserted as a set.
	wantPrefix := []string{
		"ses_assessment", "ses_draft_plan", "ses_hardening",
		"ses_exec_implement", "ses_exec_test", "ses_exec_record",
		"ses_verification_scope",
	}
	wantSuffix := []string{"ses_verification_synthesize", "ses_publication_1"}
	started := f.provider.started
	if len(started) != len(wantPrefix)+3+len(wantSuffix) {
		t.Fatalf("provider sessions = %d (%v), want exactly %d", len(started), started, len(wantPrefix)+3+len(wantSuffix))
	}
	for i, want := range wantPrefix {
		if started[i] != want {
			t.Fatalf("session %d = %q, want %q (full log %v)", i, started[i], want, started)
		}
	}
	members := map[string]bool{"ses_verification_member_0": true, "ses_verification_member_1": true, "ses_verification_member_2": true}
	for _, got := range started[len(wantPrefix) : len(wantPrefix)+3] {
		if !members[got] {
			t.Fatalf("session %q is not one of the verification team members (full log %v)", got, started)
		}
		delete(members, got)
	}
	if len(members) != 0 {
		t.Fatalf("verification team member sessions missing from %v", started)
	}
	for i, want := range wantSuffix {
		if started[len(wantPrefix)+3+i] != want {
			t.Fatalf("session %d = %q, want %q (full log %v)", len(wantPrefix)+3+i, started[len(wantPrefix)+3+i], want, started)
		}
	}
}

// TestFactoryWorkForeignSubjectResultIsNotApplied pins the exact-subject
// contract of the bound external gates: an adapter result the gate observed
// on a DIFFERENT revision than the pinned pr_head must not be applied to the
// parked review — the review stays parked, the controller records the
// mismatch, and nothing advances.
func TestFactoryWorkForeignSubjectResultIsNotApplied(t *testing.T) {
	f := newFactoryE2E(t, "factory-e2e-foreign-subject", 4, "avenor-issue-115",
		func(string) string { return factoryEchoAdapter("passed") },
		func(string) string {
			return factoryForeignSubjectAdapter("passed", "0000000000000000000000000000000000000000")
		})
	scriptRunWorker(f.provider, "ses_assessment", map[string]string{"assessment.md": "## Assessment\n"})
	scriptRunWorker(f.provider, "ses_draft_plan", map[string]string{"plan.md": "## Plan\n"})
	scriptRunWorker(f.provider, "ses_hardening", map[string]string{"plan.md": "## Hardened plan\n"})
	scriptExecutionLoop(f.provider)
	scriptTeam(f.provider, "ses_verification", 3, "verification.md", "passed")
	scriptPublication(f.provider, "ses_publication_1", false)
	f.enable(t)

	f.waitThroughPublication(t)

	// The review parks; the review-verdict adapter then reports a passed
	// result for a foreign revision. Nothing may advance on it.
	f.waitNodeStatus(t, f.wf, "review", workflow.ActivationAwaitingGate)
	review := f.newestActivation(t, f.wf, "review")
	cursor := f.waitCursor(t, f.wf, string(review.ID), "review-verdict")

	// The foreign result reaches the apply path and is refused there.
	mismatchKey := "poll_subject_mismatch/" + workflowcontroller.PollCursorKey(cursor)
	waitFor(t, "poll_subject_mismatch diagnostic for the review-verdict cursor", func() bool {
		rec, _, err := f.cstore.Get("software-factory")
		if err != nil {
			return false
		}
		_, open := rec.Diagnostics[mismatchKey]
		return open
	})

	// A bounded window in which the foreign result must never resolve the
	// gate: no poll result observed on a different subject is applicable.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		inst := f.instance(t)
		act := activationFor(&inst, review.NodeID)
		if act == nil || act.Status != workflow.ActivationAwaitingGate {
			t.Fatalf("review advanced on a foreign-subject result; observed %s", describeInstance(&inst, int32(f.provider.sessionCount())))
		}
		for _, gi := range gateInstances(&inst, review.ID) {
			if gi.GateID == "review-verdict" && gi.Status == workflow.GatePassed {
				t.Fatalf("foreign-subject result landed on the bound gate: %+v", gi)
			}
		}
		if act := f.newestActivation(t, f.wf, "merge-auth"); act != nil {
			t.Fatalf("merge-auth reached on a foreign-subject result: %+v", act)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestFactoryWorkChangesRequestedCorrectsRepublishesAndRejectsStaleHead
// proves the shipped template's correction loop through the production path:
// a changes_requested verdict routes correction (auto) → reverify (auto team
// passing on its marker) → a second publication under a NEW exact head (the
// correction worker commits the working tree) → a fresh review parks on the
// new subject and passes there — while a late result addressed to the
// superseded first review can never land, and the first review's verdict
// stays exactly as it resolved.
func TestFactoryWorkChangesRequestedCorrectsRepublishesAndRejectsStaleHead(t *testing.T) {
	f := newFactoryE2E(t, "factory-e2e-changes", 4, "avenor-issue-115",
		func(string) string { return factoryEchoAdapter("passed") },
		func(head string) string { return factoryHeadSwitchAdapter(head, "changes_requested", "passed") })
	scriptRunWorker(f.provider, "ses_assessment", map[string]string{"assessment.md": "## Assessment\n"})
	scriptRunWorker(f.provider, "ses_draft_plan", map[string]string{"plan.md": "## Plan\n"})
	scriptRunWorker(f.provider, "ses_hardening", map[string]string{"plan.md": "## Hardened plan\n"})
	scriptExecutionLoop(f.provider)
	scriptTeam(f.provider, "ses_verification", 3, "verification.md", "passed")
	scriptPublication(f.provider, "ses_publication_1", false)
	scriptRunWorker(f.provider, "ses_correction_1", map[string]string{"correction.md": "Fixed review findings\n"})
	scriptTeam(f.provider, "ses_reverify", 2, "reverify.md", "passed")
	scriptPublication(f.provider, "ses_publication_2", true)
	f.enable(t)

	f.waitThroughPublication(t)

	// The first review parks on the first head; the head-switch adapter
	// reports changes_requested for exactly that head.
	f.waitNodeStatus(t, f.wf, "review", workflow.ActivationAwaitingGate)
	review1 := f.newestActivation(t, f.wf, "review")
	oldCursor := f.waitCursor(t, f.wf, string(review1.ID), "review-verdict")

	// The correction loop runs entirely through the production handoff:
	// correction (auto) → reverify (auto team, passed marker) → publication
	// under a NEW head.
	f.waitNodeStatus(t, f.wf, "review", workflow.ActivationRejected)
	f.waitNodeSatisfied(t, f.wf, "correction", "fixed")
	f.waitNodeSatisfied(t, f.wf, "reverify", "passed")
	f.waitNodeSatisfied(t, f.wf, "publication", "published")

	inst := f.instance(t)
	newHead := f.outputValue(t, &inst, "pr_head")
	if newHead == f.head {
		t.Fatalf("republished under the old head %q; observed %s", f.head, describeInstance(&inst, int32(f.provider.sessionCount())))
	}

	// A fresh review activation parks on the new subject; the cursors hash
	// differently because the revision moved.
	f.waitNodeStatus(t, f.wf, "review", workflow.ActivationAwaitingGate)
	review2 := f.newestActivation(t, f.wf, "review")
	if review2.ID == review1.ID {
		t.Fatal("re-publication reused the old review activation")
	}
	newCursor := f.waitCursor(t, f.wf, string(review2.ID), "review-verdict")
	if newCursor.SubjectHash == oldCursor.SubjectHash {
		t.Fatalf("new head reused subject hash %q", newCursor.SubjectHash)
	}

	// The new review passes on the new head and the pipeline again stops at
	// the human gate.
	f.waitNodeSatisfied(t, f.wf, "review", "clean")
	f.waitNodeStatus(t, f.wf, "merge-auth", workflow.ActivationPending)

	// A late result addressed to the superseded first review (its old-head
	// cursor) is rejected: the activation is resolved, so the command errors
	// and nothing regresses.
	staleSubject := map[string]any{"type": "pull_request", "repository": "sdougbrown/avenor", "pull_request": 143, "revision": f.head}
	if _, err := f.mgr.WorkflowCommand(f.wf, mustJSON(t, map[string]any{
		"op": "gate", "node_id": "review", "activation_id": string(review1.ID), "gate_id": "review-verdict",
		"operation": "external_result", "result": "passed", "poll_id": oldCursor.PollID,
		"source": "github", "subject": staleSubject, "response_hash": "hash-stale",
		"observed_at": time.Now().UTC(), "evidence_ids": []string{"ev-stale"},
	})); err == nil {
		t.Fatal("stale review result landed on the superseded activation")
	}

	// The superseded review stays resolved on changes_requested; the newest
	// review is the new-head activation that already satisfied clean.
	inst = f.instance(t)
	for _, gi := range gateInstances(&inst, review1.ID) {
		if gi.GateID == "review-verdict" && gi.Status == workflow.GatePassed {
			t.Fatalf("superseded review verdict flipped to passed: %+v", gi)
		}
	}
	final := f.newestActivation(t, f.wf, "review")
	if final.ID != review2.ID || final.Status != workflow.ActivationSatisfied {
		t.Fatalf("newest review = %s/%s, want the new activation satisfied clean", final.ID, final.Status)
	}
	if act := f.newestActivation(t, f.wf, "reconciliation"); act != nil {
		t.Fatalf("reconciliation reached without the human gate: %+v", act)
	}
}

// TestFactoryWorkReverifyFailedRoutesCorrection proves the failed reverify
// route: a reverify team whose synthesize emits the FAIL terminal marker
// completes the reverify node with the failed outcome and routes back to
// correction, whose second activation dispatches automatically through the
// production handoff. The corrected work then flows on: reverify passes the
// second time, republishes, and the review stops the pipeline at the human
// gate again.
func TestFactoryWorkReverifyFailedRoutesCorrection(t *testing.T) {
	f := newFactoryE2E(t, "factory-e2e-reverify-failed", 4, "avenor-issue-115",
		func(string) string { return factoryEchoAdapter("passed") },
		func(head string) string { return factoryHeadSwitchAdapter(head, "changes_requested", "passed") })
	scriptRunWorker(f.provider, "ses_assessment", map[string]string{"assessment.md": "## Assessment\n"})
	scriptRunWorker(f.provider, "ses_draft_plan", map[string]string{"plan.md": "## Plan\n"})
	scriptRunWorker(f.provider, "ses_hardening", map[string]string{"plan.md": "## Hardened plan\n"})
	scriptExecutionLoop(f.provider)
	scriptTeam(f.provider, "ses_verification", 3, "verification.md", "passed")
	scriptPublication(f.provider, "ses_publication_1", false)
	scriptRunWorker(f.provider, "ses_correction_1", map[string]string{"correction.md": "First correction\n"})
	scriptTeam(f.provider, "ses_reverify_1", 2, "reverify.md", "failed")
	scriptRunWorker(f.provider, "ses_correction_2", map[string]string{"correction.md": "Second correction\n"})
	scriptTeam(f.provider, "ses_reverify_2", 2, "reverify.md", "passed")
	scriptPublication(f.provider, "ses_publication_2", true)
	f.enable(t)

	f.waitThroughPublication(t)

	// changes_requested routes correction → reverify, whose FAIL marker
	// completes reverify as failed and routes back to correction.
	f.waitNodeStatus(t, f.wf, "review", workflow.ActivationRejected)
	f.waitNodeSatisfied(t, f.wf, "correction", "fixed")
	f.waitNodeSatisfied(t, f.wf, "reverify", "failed")

	// The failed reverify recorded its terminal marker on the attempt.
	inst := f.instance(t)
	var marker string
	for _, a := range inst.Attempts {
		if a.Identity.NodeID == "reverify" {
			marker = a.MarkerLabel
		}
	}
	if marker != "failed" {
		t.Fatalf("reverify attempt marker label = %q, want failed; observed %s", marker, describeInstance(&inst, int32(f.provider.sessionCount())))
	}

	// A SECOND correction activation dispatched and completed automatically,
	// then the retried reverify passed and the pipeline republished to the
	// same human-gate stop.
	f.waitInstanceOn(t, f.wf, "two correction activations both satisfied fixed", func(inst *workflow.WorkflowInstance) bool {
		count, satisfied := 0, 0
		for _, a := range inst.Activations {
			if a.NodeID == "correction" {
				count++
				if a.Status == workflow.ActivationSatisfied && a.SelectedOutcome == workflow.OutcomeName("fixed") {
					satisfied++
				}
			}
		}
		return count == 2 && satisfied == 2
	})
	f.waitNodeSatisfied(t, f.wf, "reverify", "passed")
	f.waitNodeSatisfied(t, f.wf, "publication", "published")
	f.waitNodeSatisfied(t, f.wf, "review", "clean")
	f.waitNodeStatus(t, f.wf, "merge-auth", workflow.ActivationPending)
}

// TestFactoryWorkRecoveryOnFreshSupervisor proves a new supervisor on the
// same workflow root resumes the parked factory work without coordinator
// memory and without re-dispatching any auto node: the recovered snapshot
// still parks the review on its exact subject, the controller record and
// poll cursors survive, the enabled controller re-polls through to the same
// clean stop at merge-auth, and the recovered supervisor never starts a
// single worker session.
func TestFactoryWorkRecoveryOnFreshSupervisor(t *testing.T) {
	f := newFactoryE2E(t, "factory-e2e-recover", 4, "avenor-issue-115",
		func(string) string { return factoryEchoAdapter("passed") },
		func(string) string { return factoryEchoAdapter("passed") })
	scriptRunWorker(f.provider, "ses_assessment", map[string]string{"assessment.md": "## Assessment\n"})
	scriptRunWorker(f.provider, "ses_draft_plan", map[string]string{"plan.md": "## Plan\n"})
	scriptRunWorker(f.provider, "ses_hardening", map[string]string{"plan.md": "## Hardened plan\n"})
	scriptExecutionLoop(f.provider)
	scriptTeam(f.provider, "ses_verification", 3, "verification.md", "passed")
	scriptPublication(f.provider, "ses_publication_1", false)
	f.enable(t)
	f.waitThroughPublication(t)
	f.waitNodeStatus(t, f.wf, "review", workflow.ActivationAwaitingGate)
	review := f.newestActivation(t, f.wf, "review")
	cursorCI := f.waitCursor(t, f.wf, string(review.ID), "ci")
	cursorVerdict := f.waitCursor(t, f.wf, string(review.ID), "review-verdict")
	sessionsBefore := f.provider.sessionCount()

	// Tear the first supervisor down WITHOUT disabling the controller: the
	// desired state stays enabled on disk.
	f.sup.stopControllerLoops()
	_ = f.sup.broker.Stop()
	f.sup.stopReaper()

	// A fresh supervisor recovers the same root and adapter registry. Its
	// provider factory only counts: any auto re-dispatch after recovery
	// would show up here and fail the no-duplicate-dispatch assertion.
	sup2 := NewSupervisor(Config{
		ControlSocket:      newStableSocketPath(t, "factory-e2e-recover-2"),
		WorkflowRoot:       f.root,
		WorkflowAdapterDir: f.adapterDir,
		ShutdownTimeout:    0,
	})
	sup2.controllerRenewInterval = 25 * time.Millisecond
	sup2.controllerPollBaseDelay = 200 * time.Millisecond
	recoveredCalls := atomic.Int32{}
	sup2.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		recoveredCalls.Add(1)
		return &factoryWorkerProvider{}, nil
	}
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

	// The controller record recovered enabled, with its poll cursors intact.
	rec, _, err := cstore2.Get("software-factory")
	if err != nil {
		t.Fatalf("recovered controller record: %v", err)
	}
	if rec.DesiredState != workflowcontroller.DesiredEnabled {
		t.Fatalf("recovered desired state = %q, want enabled", rec.DesiredState)
	}
	if len(rec.PollCursors) < 2 {
		t.Fatalf("recovered poll cursors = %d, want at least the parked review's 2", len(rec.PollCursors))
	}
	insp, err := mgr2.WorkflowInspect(f.wf)
	if err != nil {
		t.Fatalf("recovered inspect: %v", err)
	}
	recovered := insp.(map[string]any)["instance"].(workflow.WorkflowInstance)
	recoveredReview := activationFor(&recovered, review.NodeID)
	if recoveredReview == nil || recoveredReview.Status != workflow.ActivationAwaitingGate {
		t.Fatalf("recovered review = %+v, want parked awaiting_gate", recoveredReview)
	}
	if recoveredReview.ID != review.ID {
		t.Fatalf("recovered review activation = %s, want the parked %s", recoveredReview.ID, review.ID)
	}

	// The recovered controller resumes leadership and re-polls the parked
	// gates through the same adapters to the same clean stop; surviving
	// cursors only ever move their poll counters forward.
	deadline := time.Now().Add(20 * time.Second)
	for {
		inst := f.instanceOn(t, f.wf)
		act := activationFor(&inst, review.NodeID)
		if act != nil && act.Status == workflow.ActivationSatisfied && act.SelectedOutcome == workflow.OutcomeName("clean") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for the recovered review to satisfy; observed %s", describeInstance(&inst, recoveredCalls.Load()))
		}
		time.Sleep(50 * time.Millisecond)
	}
	inst := f.instanceOn(t, f.wf)
	mergeAuth := activationFor(&inst, "merge-auth")
	if mergeAuth == nil || mergeAuth.Status != workflow.ActivationPending {
		t.Fatalf("recovered merge-auth = %+v, want pending behind the human gate", mergeAuth)
	}
	if act := f.newestActivation(t, f.wf, "reconciliation"); act != nil {
		t.Fatalf("reconciliation reached after recovery: %+v", act)
	}
	for _, want := range []workflowcontroller.PollCursor{cursorCI, cursorVerdict} {
		got, ok := f.findCursor(f.wf, string(review.ID), want.GateID)
		if !ok {
			continue // a resolving gate's cursor may be dropped once applied
		}
		if got.PollCount < want.PollCount {
			t.Fatalf("cursor %s poll count regressed %d -> %d", want.GateID, want.PollCount, got.PollCount)
		}
	}

	// The decisive assertion: no auto node was re-dispatched. The first
	// supervisor's session log is frozen at its pre-restart length and the
	// recovered supervisor started zero worker sessions.
	if got := f.provider.sessionCount(); got != sessionsBefore {
		t.Fatalf("first supervisor's session count moved %d -> %d after teardown", sessionsBefore, got)
	}
	if calls := recoveredCalls.Load(); calls != 0 {
		t.Fatalf("recovered supervisor dispatched %d worker sessions after restart, want 0 (no duplicate dispatch)", calls)
	}
}

// TestFactoryWorkSharedWorktreeKeySerializesThroughCompletion proves the
// worktree concurrency key serializes two work items pinned to the same
// param under the production handoff: the second item's assessment stays
// pending — with controller capacity to spare — while the first item's
// worker holds the key, and dispatches automatically once the supervisor's
// own completion of the first item's node releases the key.
func TestFactoryWorkSharedWorktreeKeySerializesThroughCompletion(t *testing.T) {
	f := newFactoryE2E(t, "factory-e2e-key", 3, "avenor-issue-115",
		func(string) string { return factoryEchoAdapter("passed") },
		func(string) string { return factoryEchoAdapter("passed") })
	// Items A (the fixture workflow) and B share the worktree param; item C
	// pins a different one. The first two queued sessions are held: they are
	// A's and C's assessments in dispatch order, so whichever workflow grabs
	// them, both live assessments park mid-run. Later sessions write BOTH
	// artifacts because the interleaving of B's assessment with A's and C's
	// draft plans is not deterministic and every contract only checks
	// existence — the shared working directory (the known spawn-cwd gap) is
	// what makes identical content harmless.
	held1, held2 := "ses_assess_held_1", "ses_assess_held_2"
	f.provider.append(factoryWorkerScript{sessionID: held1, hold: make(chan struct{}),
		write: map[string]string{"assessment.md": "## Assessment\n", "plan.md": "## Plan\n"}})
	f.provider.append(factoryWorkerScript{sessionID: held2, hold: make(chan struct{}),
		write: map[string]string{"assessment.md": "## Assessment\n", "plan.md": "## Plan\n"}})
	for _, id := range []string{"ses_worker_3", "ses_worker_4", "ses_worker_5", "ses_worker_6"} {
		f.provider.append(factoryWorkerScript{sessionID: id,
			write: map[string]string{"assessment.md": "## Assessment\n", "plan.md": "## Plan\n"}})
	}
	wfB := f.instantiate(t, "avenor-issue-115")
	wfC := f.instantiate(t, "avenor-issue-130")
	f.enable(t)

	// All three items sit at intake; complete all intakes so the assessments
	// become ready candidates under their resolved keys.
	f.driveIntake(t, f.wf)
	f.driveIntake(t, wfB)
	f.driveIntake(t, wfC)

	// Items A and C dispatch concurrently (different keys) and their workers
	// park mid-run holding their sessions open.
	f.waitInstanceOn(t, f.wf, "item A's assessment to run", func(inst *workflow.WorkflowInstance) bool {
		act := activationFor(inst, "assessment")
		return act != nil && act.Status == workflow.ActivationRunning && len(act.AttemptIDs) > 0
	})
	f.waitInstanceOn(t, wfC, "item C's assessment to run", func(inst *workflow.WorkflowInstance) bool {
		act := activationFor(inst, "assessment")
		return act != nil && act.Status == workflow.ActivationRunning && len(act.AttemptIDs) > 0
	})
	f.waitInstanceOn(t, f.wf, "both held worker sessions to start", func(*workflow.WorkflowInstance) bool {
		return f.provider.sessionCount() >= 2
	})

	// While A holds the shared worktree key, item B's assessment never
	// starts — with a controller slot free (inflight 3, two in use), so the
	// block is the key, not capacity.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		b := f.newestActivation(t, wfB, "assessment")
		if b == nil {
			t.Fatalf("item B has no assessment activation")
		}
		if b.Status != workflow.ActivationPending {
			t.Fatalf("item B assessment = %s with the shared key still held, want pending", b.Status)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Release the held workers: their attempts end, the supervisor completes
	// both assessment nodes itself, the key releases, and B's assessment
	// dispatches and completes through the same handoff — no human help.
	f.provider.releaseHold(held1)
	f.provider.releaseHold(held2)
	f.waitNodeSatisfied(t, wfB, "assessment", "ready")
	bInst := f.instanceOn(t, wfB)
	bAttempts := 0
	for _, a := range bInst.Attempts {
		if a.Identity.NodeID == "assessment" {
			bAttempts++
			if a.Status != workflow.AttemptSucceeded {
				t.Fatalf("item B assessment attempt status = %s, want succeeded via the supervisor handoff", a.Status)
			}
		}
	}
	if bAttempts != 1 {
		t.Fatalf("item B assessment recorded %d attempts, want exactly 1; observed %s", bAttempts, describeInstance(&bInst, int32(f.provider.sessionCount())))
	}
	f.disable(t)
}

// TestFactoryWorkHumanGateAuthorizedDecisionAdvances proves the merge-auth
// human gate: the node parks behind its bound human gate, malformed and
// unauthorized decisions are rejected without advancing anything, and one
// exact-subject authorized decision releases the node — reconciliation then
// dispatches through the production handoff and merges.
func TestFactoryWorkHumanGateAuthorizedDecisionAdvances(t *testing.T) {
	f := newFactoryE2E(t, "factory-e2e-human-gate", 4, "avenor-issue-115",
		func(string) string { return factoryEchoAdapter("passed") },
		func(string) string { return factoryEchoAdapter("passed") })
	scriptRunWorker(f.provider, "ses_assessment", map[string]string{"assessment.md": "## Assessment\n"})
	scriptRunWorker(f.provider, "ses_draft_plan", map[string]string{"plan.md": "## Plan\n"})
	scriptRunWorker(f.provider, "ses_hardening", map[string]string{"plan.md": "## Hardened plan\n"})
	scriptExecutionLoop(f.provider)
	scriptTeam(f.provider, "ses_verification", 3, "verification.md", "passed")
	scriptPublication(f.provider, "ses_publication_1", false)
	scriptRunWorker(f.provider, "ses_reconciliation", map[string]string{"reconciliation.md": "## Reconciliation\nmerged\n"})
	f.enable(t)
	f.waitThroughPublication(t)
	f.waitNodeSatisfied(t, f.wf, "review", "clean")

	// The human completes merge-auth (claim, start, complete with the
	// authorized head) and the activation parks awaiting its gate.
	f.waitNodeStatus(t, f.wf, "merge-auth", workflow.ActivationPending)
	f.completeManualNode(t, f.wf, "merge-auth", "authorized",
		[]map[string]any{{"definition_id": "authorized_head", "value": f.head}}, nil)
	f.waitNodeStatus(t, f.wf, "merge-auth", workflow.ActivationAwaitingGate)
	mergeAuth := f.newestActivation(t, f.wf, "merge-auth")

	// Malformed decisions are rejected without advancing: missing fields,
	// then a subject that does not equal the pinned published subject.
	pinned := map[string]any{"type": "pull_request", "repository": "sdougbrown/avenor", "pull_request": 143, "revision": f.head}
	rejected := []struct {
		name    string
		payload map[string]any
	}{
		{"missing actor", map[string]any{"operation": "satisfy", "reason": "ok", "evidence_ids": []string{"ev"}, "subject": pinned}},
		{"missing reason", map[string]any{"operation": "satisfy", "actor": "alice", "evidence_ids": []string{"ev"}, "subject": pinned}},
		{"missing evidence", map[string]any{"operation": "satisfy", "actor": "alice", "reason": "ok", "subject": pinned}},
		{"missing subject", map[string]any{"operation": "satisfy", "actor": "alice", "reason": "ok", "evidence_ids": []string{"ev"}}},
		{"foreign subject", map[string]any{"operation": "satisfy", "actor": "alice", "reason": "ok", "evidence_ids": []string{"ev"},
			"subject": map[string]any{"type": "pull_request", "repository": "sdougbrown/avenor", "pull_request": 143, "revision": "deadbeef"}}},
	}
	for _, tc := range rejected {
		payload := tc.payload
		payload["op"] = "gate"
		payload["node_id"] = "merge-auth"
		payload["activation_id"] = string(mergeAuth.ID)
		payload["gate_id"] = "merge-authorization"
		if _, err := f.mgr.WorkflowCommand(f.wf, mustJSON(t, payload)); err == nil {
			t.Fatalf("%s decision was accepted", tc.name)
		}
		if act := f.newestActivation(t, f.wf, "merge-auth"); act == nil || act.Status != workflow.ActivationAwaitingGate {
			t.Fatalf("%s decision advanced merge-auth: %+v", tc.name, act)
		}
	}
	if act := f.newestActivation(t, f.wf, "reconciliation"); act != nil {
		t.Fatalf("reconciliation reached on a rejected decision: %+v", act)
	}

	// The authorized decision: actor, reason, evidence, and the exact pinned
	// subject. It resolves the gate and the node, and reconciliation
	// dispatches automatically through the production handoff.
	if _, err := f.mgr.WorkflowCommand(f.wf, mustJSON(t, map[string]any{
		"op": "gate", "node_id": "merge-auth", "activation_id": string(mergeAuth.ID), "gate_id": "merge-authorization",
		"operation": "satisfy", "actor": "alice", "reason": "authorized the reviewed head",
		"evidence_ids": []string{"ev-auth"}, "subject": pinned,
	})); err != nil {
		t.Fatalf("authorized satisfy: %v", err)
	}
	f.waitNodeSatisfied(t, f.wf, "merge-auth", "authorized")
	f.waitNodeSatisfied(t, f.wf, "reconciliation", "merged")
	inst := f.instance(t)
	if inst.Status != workflow.WorkflowCompleted || inst.TerminalOutcome != workflow.OutcomeName("merged") {
		t.Fatalf("workflow = %s/%s, want completed/merged", inst.Status, inst.TerminalOutcome)
	}
}

// TestFactoryWorkContractUnmetRetriesThenSucceeds proves the full files
// contract failure cycle on an auto node: a worker that exits cleanly
// without the declared artifact is recorded as a failed attempt with the
// contract_unmet marker, the node's retry policy re-dispatches, and the
// second worker — which writes the artifact — completes the node through the
// same handoff. The node declares no retry_policy, so the template's
// default_retry_policy supplies the second attempt.
func TestFactoryWorkContractUnmetRetriesThenSucceeds(t *testing.T) {
	f := newFactoryE2E(t, "factory-e2e-contract", 4, "avenor-issue-115",
		func(string) string { return factoryEchoAdapter("passed") },
		func(string) string { return factoryEchoAdapter("passed") })
	// The first assessment worker exits cleanly WITHOUT its artifact; the
	// retry writes it.
	f.provider.append(factoryWorkerScript{sessionID: "ses_assess_unmet"})
	scriptRunWorker(f.provider, "ses_assess_retry", map[string]string{"assessment.md": "## Assessment\n"})
	scriptRunWorker(f.provider, "ses_draft_plan", map[string]string{"plan.md": "## Plan\n"})
	f.enable(t)

	f.driveIntake(t, f.wf)
	f.waitNodeSatisfied(t, f.wf, "assessment", "ready")
	f.waitNodeSatisfied(t, f.wf, "draft-plan", "ready")

	inst := f.instance(t)
	attempts := attemptsForNode(&inst, "assessment")
	if len(attempts) != 2 {
		t.Fatalf("assessment recorded %d attempts, want exactly 2 (contract_unmet retry then success); observed %s",
			len(attempts), describeInstance(&inst, int32(f.provider.sessionCount())))
	}
	if attempts[0].Status != workflow.AttemptFailed || attempts[0].MarkerLabel != "contract_unmet" {
		t.Fatalf("first assessment attempt = %s/%s, want failed/contract_unmet", attempts[0].Status, attempts[0].MarkerLabel)
	}
	if attempts[1].Status != workflow.AttemptSucceeded {
		t.Fatalf("second assessment attempt = %s, want succeeded", attempts[1].Status)
	}
	if got := f.provider.sessionCount(); got != 3 {
		t.Fatalf("provider sessions = %d, want exactly 3 (unmet, retry, draft-plan)", got)
	}
	f.disable(t)
}

// TestFactoryWorkContractUnmetFailsTheAttempt pins the observable half of
// the files-contract failure on an auto node through the production handoff:
// every clean exit without the declared artifact is rejected by the
// supervisor's contract evaluation and recorded failed with the
// contract_unmet marker, nothing downstream dispatches, and the activation
// exhausts to blocked once the template's default_retry_policy (two
// attempts) is spent.
func TestFactoryWorkContractUnmetFailsTheAttempt(t *testing.T) {
	f := newFactoryE2E(t, "factory-e2e-contract-unmet", 4, "avenor-issue-115",
		func(string) string { return factoryEchoAdapter("passed") },
		func(string) string { return factoryEchoAdapter("passed") })
	f.provider.append(factoryWorkerScript{sessionID: "ses_assess_unmet_1"})
	f.provider.append(factoryWorkerScript{sessionID: "ses_assess_unmet_2"})
	f.enable(t)

	f.driveIntake(t, f.wf)
	f.waitNodeStatus(t, f.wf, "assessment", workflow.ActivationBlocked)

	inst := f.instance(t)
	attempts := attemptsForNode(&inst, "assessment")
	if len(attempts) != 2 {
		t.Fatalf("assessment recorded %d attempts, want exactly 2 (the template default max_attempts); observed %s", len(attempts), describeInstance(&inst, int32(f.provider.sessionCount())))
	}
	for i, a := range attempts {
		if a.Status != workflow.AttemptFailed || a.MarkerLabel != "contract_unmet" {
			t.Fatalf("assessment attempt %d = %s/%s, want failed/contract_unmet", i, a.Status, a.MarkerLabel)
		}
	}
	if act := activationFor(&inst, "draft-plan"); act != nil {
		t.Fatalf("draft-plan dispatched despite assessment never satisfying its contract; observed %s", describeInstance(&inst, int32(f.provider.sessionCount())))
	}
	if got := f.provider.sessionCount(); got != 2 {
		t.Fatalf("provider sessions = %d, want exactly 2 (both unmet workers, never re-dispatched after exhaustion)", got)
	}
	f.disable(t)
}
