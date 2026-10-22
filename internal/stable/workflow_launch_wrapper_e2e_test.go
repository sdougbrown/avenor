package stable

// workflow_launch_wrapper_e2e_test.go runs the template-declared node launch
// wrapper end to end over a real supervisor: real executors, a real enabled
// controller, real admission, and the real workflow.Manager. The only fake is
// the scripted provider behind the production provider-factory seam, whose
// StartOptions are captured to observe the resolved argv the executors hand
// to the runtime layer.

import (
	"os"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/workflow"
)

// launchWrapperTestValue is an instance-param value with a space and shell
// metacharacters: the resolver must pass it through as one exact argv element
// with no expansion.
const launchWrapperTestValue = `value with spaces $(echo pwned)`

// launchWrapperTestArgv is the argv the "wrap-argv" launch wrapper resolves
// to: one literal element plus the wrapper_arg instance param.
var launchWrapperTestArgv = []string{"/usr/bin/extra-wrap", launchWrapperTestValue}

// wrapperCapture records the supervisor runtime ID and the LaunchWrapper of
// every provider start observed through the provider-factory seam. The
// runtime ID is the supervisor-assigned ID (rt_N) that identifies the owning
// workflow attempt, so a captured start can be attributed to the node it
// belongs to.
type wrapperCapture struct {
	mu    sync.Mutex
	items []capturedStart
}

// capturedStart is one provider start: the supervisor runtime ID of the
// owning attempt, and a copy of the launch wrapper argv the start carried
// (nil when the node launched unwrapped).
type capturedStart struct {
	runtimeID string
	wrapper   []string
}

// install wraps the fixture's provider factory so every runtime.StartOptions
// is recorded while the real executors, admission, and controller stay in the
// path.
func (w *wrapperCapture) install(t *testing.T, f *autoHandoffFixture) {
	t.Helper()
	base := f.sup.newProviderFunc
	f.sup.newProviderFunc = func(opts runtime.StartOptions, backend string) (runtime.Provider, error) {
		w.mu.Lock()
		w.items = append(w.items, capturedStart{
			runtimeID: opts.RuntimeID,
			wrapper:   append([]string(nil), opts.LaunchWrapper...),
		})
		w.mu.Unlock()
		return base(opts, backend)
	}
}

// all returns the wrappers of every captured start, in order.
func (w *wrapperCapture) all() [][]string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([][]string, len(w.items))
	for i, s := range w.items {
		out[i] = s.wrapper
	}
	return out
}

// starts returns a copy of every captured start.
func (w *wrapperCapture) starts() []capturedStart {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]capturedStart(nil), w.items...)
}

// runtimeIDsForNode returns the set of supervisor runtime IDs recorded on a
// node's attempts.
func runtimeIDsForNode(inst *workflow.WorkflowInstance, nodeID workflow.NodeID) map[string]bool {
	out := map[string]bool{}
	for _, a := range attemptsForNode(inst, nodeID) {
		if a.Identity.RuntimeID != "" {
			out[a.Identity.RuntimeID] = true
		}
	}
	return out
}

// assertWrapperAttribution checks each captured provider start against the
// node it belongs to, by supervisor runtime ID: a start for the start node's
// attempt must carry the resolved wrapper (or be unwrapped), and a start for
// the consume node's attempt must never carry the wrapper — a wrapper applied
// to the wrong node cannot pass.
func assertWrapperAttribution(t *testing.T, capture *wrapperCapture, startRTs, consumeRTs map[string]bool, withWrapper bool) {
	t.Helper()
	starts := capture.starts()
	if len(starts) == 0 {
		t.Fatalf("no provider starts observed")
	}
	sawStart := false
	for _, s := range starts {
		switch {
		case startRTs[s.runtimeID]:
			sawStart = true
			if withWrapper {
				if !slices.Equal(s.wrapper, launchWrapperTestArgv) {
					t.Fatalf("start %s carried wrapper %#v, want exactly %#v", s.runtimeID, s.wrapper, launchWrapperTestArgv)
				}
			} else if len(s.wrapper) != 0 {
				t.Fatalf("unwrapped start %s launched behind wrapper %#v", s.runtimeID, s.wrapper)
			}
		case consumeRTs[s.runtimeID]:
			if len(s.wrapper) != 0 {
				t.Fatalf("consume %s launched behind wrapper %#v; the wrapper must not leak across nodes", s.runtimeID, s.wrapper)
			}
		default:
			t.Fatalf("provider start %s is not attributable to any node attempt; observed %v", s.runtimeID, starts)
		}
	}
	if !sawStart {
		t.Fatalf("no provider start attributable to the start node's attempts; observed %v", starts)
	}
}

// launchWrapperTemplate builds a one-node template whose entry node runs the
// given action type with the given dispatch mode, optionally bound to the
// "wrap-argv" launch wrapper (one literal element plus the required
// wrapper_arg instance param).
func launchWrapperTemplate(t *testing.T, templateID, actionType, cfgPath, dispatchMode string, withWrapper bool) []byte {
	t.Helper()
	var action map[string]any
	switch actionType {
	case "loop":
		action = map[string]any{"type": "loop", "loop_file": cfgPath}
	case "team":
		action = map[string]any{"type": "team", "team_file": cfgPath}
	default:
		action = map[string]any{"type": "run", "prompt": "do the thing"}
	}
	node := map[string]any{
		"id":           "start",
		"action":       action,
		"retry_policy": map[string]any{"max_attempts": 2, "exhaustion": "block"},
	}
	if dispatchMode == "auto" {
		node["dispatch"] = map[string]any{"mode": "auto", "controller_id": "c1"}
	} else {
		node["dispatch"] = map[string]any{"mode": dispatchMode}
	}
	if withWrapper {
		node["launch_wrapper"] = "wrap-argv"
	}
	tmpl := map[string]any{
		"schema_version":   1,
		"template_id":      templateID,
		"template_version": "1",
		"entry_nodes":      []string{"start"},
		"params":           []any{map[string]any{"id": "wrapper_arg", "type": "string", "required": true}},
		"launch_wrappers": []any{map[string]any{
			"id": "wrap-argv",
			"argv": []any{
				map[string]any{"type": "literal", "value": "/usr/bin/extra-wrap"},
				map[string]any{"type": "instance_param", "from_instance_param": "wrapper_arg"},
			},
		}},
		"nodes":             []any{node},
		"terminal_outcomes": []string{"done"},
	}
	if actionType == "loop" {
		node["branches"] = map[string]any{"passed": "consume", "failed": "consume"}
		tmpl["nodes"] = []any{node, map[string]any{
			"id":           "consume",
			"dependencies": []string{"start"},
			"action":       map[string]any{"type": "run", "prompt": "consume the verdict"},
			"dispatch":     map[string]any{"mode": "auto", "controller_id": "c1"},
			"retry_policy": map[string]any{"max_attempts": 2, "exhaustion": "block"},
		}}
	}
	return mustJSON(t, tmpl)
}

// TestWorkflowLaunchWrapperControllerDispatchCarriesResolvedArgv proves the
// controller dispatch path resolves the node's launch wrapper from the
// immutable template + instance params and the run/loop/team executors hand
// the exact argv (spaces and shell metacharacters intact, no expansion) to
// the runtime layer, and that a node without a selection launches unwrapped.
func TestWorkflowLaunchWrapperControllerDispatchCarriesResolvedArgv(t *testing.T) {
	for _, tc := range []struct {
		mode        string
		withWrapper bool
	}{
		{"run", true}, {"loop", true}, {"team", true}, {"run", false},
	} {
		tc := tc
		t.Run(tc.mode+"/"+map[bool]string{true: "wrapped", false: "unwrapped"}[tc.withWrapper], func(t *testing.T) {
			t.Chdir(t.TempDir())
			provider := &stableScriptedProvider{attempt: -1}
			if tc.mode == "loop" {
				appendSessionScript(t, provider, "ses_loop", []string{"<|workflow: exit | passed|>\n"}, "end_turn")
				appendSessionScript(t, provider, "ses_consume", nil, "end_turn")
			} else {
				appendSessionScript(t, provider, "ses_start", nil, "end_turn")
			}
			cfgPath := ""
			if tc.mode == "loop" {
				cfgPath = "loop.json"
				if err := os.WriteFile(cfgPath, mustJSON(t, map[string]any{
					"max_iterations": 1,
					"loop":           []any{map[string]any{"name": "verify", "prompt": "emit the verdict"}},
				}), 0o600); err != nil {
					t.Fatal(err)
				}
			} else if tc.mode == "team" {
				cfgPath = "team.json"
				if err := os.WriteFile(cfgPath, mustJSON(t, map[string]any{
					"team": []any{map[string]any{"name": "work", "prompt": "work"}},
				}), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			f := newAutoHandoffFixture(t, "wf-launch-wrapper-"+tc.mode, provider)
			capture := &wrapperCapture{}
			capture.install(t, f)
			wf := f.addWorkflow(t, "tmpl-wrapper-"+tc.mode,
				launchWrapperTemplate(t, "tmpl-wrapper-"+tc.mode, tc.mode, cfgPath, "auto", tc.withWrapper),
				map[string]string{"wrapper_arg": launchWrapperTestValue})
			f.enableController(t, 2)

			// Wait on durable workflow evidence: every node satisfies through
			// the supervisor handoff (the loop completes via its terminal
			// marker branch, then its consume node runs and satisfies).
			f.waitForInstance(t, wf, "the entry node to satisfy", func(inst *workflow.WorkflowInstance) bool {
				act := activationFor(inst, "start")
				return act != nil && act.Status == workflow.ActivationSatisfied
			})
			if tc.mode == "loop" {
				f.waitForInstance(t, wf, "the consume node to satisfy", func(inst *workflow.WorkflowInstance) bool {
					act := activationFor(inst, "consume")
					return act != nil && act.Status == workflow.ActivationSatisfied
				})
			}

			inst := f.instance(t, wf)
			assertWrapperAttribution(t, capture, runtimeIDsForNode(&inst, "start"), runtimeIDsForNode(&inst, "consume"), tc.withWrapper)
		})
	}
}

// TestWorkflowLaunchWrapperSessionOwnershipRecordsResolvedArgv proves the
// workflow spawn path records the resolved wrapper in the session ownership
// mapping, so a later resume with a different wrapper is rejected as an
// identity conflict (the comparison behavior itself is covered by
// launch_wrapper_resume_test.go; this proves the wiring carries it).
func TestWorkflowLaunchWrapperSessionOwnershipRecordsResolvedArgv(t *testing.T) {
	t.Chdir(t.TempDir())
	provider := &stableScriptedProvider{attempt: -1}
	appendSessionScript(t, provider, "ses_wrapped", nil, "end_turn")
	f := newAutoHandoffFixture(t, "wf-launch-wrapper-ownership", provider)
	capture := &wrapperCapture{}
	capture.install(t, f)
	wf := f.addWorkflow(t, "tmpl-wrapper-ownership",
		launchWrapperTemplate(t, "tmpl-wrapper-ownership", "run", "", "auto", true),
		map[string]string{"wrapper_arg": launchWrapperTestValue})
	f.enableController(t, 2)

	f.waitForInstance(t, wf, "the node to satisfy through the supervisor handoff", func(inst *workflow.WorkflowInstance) bool {
		act := activationFor(inst, "start")
		return act != nil && act.Status == workflow.ActivationSatisfied
	})

	mapped, ok := f.sup.sessionIdentity("ses_wrapped")
	if !ok {
		t.Fatalf("session ses_wrapped was never recorded in the ownership mapping")
	}
	if !slices.Equal(mapped.LaunchWrapper, launchWrapperTestArgv) {
		t.Fatalf("session ownership LaunchWrapper = %#v, want %#v", mapped.LaunchWrapper, launchWrapperTestArgv)
	}
	if _, err := f.sup.resolveWorkflowResumeIdentity("ses_wrapped", effectiveIdentity{
		Backend: mapped.Backend, LaunchWrapper: []string{"firejail"},
	}); err == nil || !strings.Contains(err.Error(), "launch_wrapper") {
		t.Fatalf("mismatched workflow resume: err = %v, want launch_wrapper conflict", err)
	}
	if _, err := f.sup.resolveWorkflowResumeIdentity("ses_wrapped", effectiveIdentity{
		Backend: mapped.Backend, LaunchWrapper: launchWrapperTestArgv,
	}); err != nil {
		t.Fatalf("matching workflow resume rejected: %v", err)
	}
}

// TestWorkflowLaunchWrapperManualStartCarriesResolvedArgv proves the manual
// start path (claim + start through the real Manager command boundary) backs
// a provider node with the same resolved wrapper — manual dispatch never
// silently omits it.
func TestWorkflowLaunchWrapperManualStartCarriesResolvedArgv(t *testing.T) {
	t.Chdir(t.TempDir())
	provider := &stableScriptedProvider{attempt: -1}
	appendSessionScript(t, provider, "ses_manual", nil, "end_turn")
	f := newAutoHandoffFixture(t, "wf-launch-wrapper-manual", provider)
	capture := &wrapperCapture{}
	capture.install(t, f)
	wf := f.addWorkflow(t, "tmpl-wrapper-manual",
		launchWrapperTemplate(t, "tmpl-wrapper-manual", "run", "", "manual", true),
		map[string]string{"wrapper_arg": launchWrapperTestValue})

	inst := f.instance(t, wf)
	act := activationFor(&inst, "start")
	if act == nil {
		t.Fatal("start activation missing after instantiation")
	}
	out, err := f.mgr.WorkflowCommand(wf, mustJSON(t, map[string]any{
		"op": "claim", "node_id": "start", "activation_id": string(act.ID), "actor": "manual-1",
	}))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	claim := out.(map[string]any)
	leaseID, _ := claim["lease_id"].(string)
	token, _ := claim["owner_token"].(string)
	if leaseID == "" || token == "" {
		t.Fatalf("claim result incomplete: %#v", claim)
	}
	if _, err := f.mgr.WorkflowCommand(wf, mustJSON(t, map[string]any{
		"op": "start", "node_id": "start", "activation_id": string(act.ID),
		"lease_id": leaseID, "owner_token": token,
	})); err != nil {
		t.Fatalf("start: %v", err)
	}

	waitFor(t, "a provider start for the manually started attempt", func() bool {
		return f.providerCalls.Load() >= 1
	})
	wrappers := capture.all()
	sawWrapper := false
	for _, wrapper := range wrappers {
		if len(wrapper) > 0 {
			if slices.Equal(wrapper, launchWrapperTestArgv) {
				sawWrapper = true
				continue
			}
			t.Fatalf("provider start carried wrapper %#v, want exactly %#v", wrapper, launchWrapperTestArgv)
		}
	}
	if !sawWrapper {
		t.Fatalf("manual start never carried the resolved wrapper; observed %#v", wrappers)
	}
}
