package stable

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/workflow"
)

func TestManualStartMessagingRefusalReleasesKey(t *testing.T) {
	for _, kind := range []string{"loop", "team"} {
		t.Run(kind, func(t *testing.T) {
			name := "manual-messaging-" + kind
			cfg := map[string]any{kind: []map[string]any{{"name": "worker", "prompt": "work"}}}
			if kind == "loop" {
				cfg["max_iterations"] = 1
			}
			file := itestWorkflowConfig(t, kind, cfg)
			var template map[string]any
			if err := json.Unmarshal(dispatchActionTemplateJSON(t, "tmpl-dispatch-"+name, "c1", kind, file), &template); err != nil {
				t.Fatal(err)
			}
			template["nodes"].([]any)[0].(map[string]any)["dispatch"].(map[string]any)["concurrency_key"] = "shared"
			f := newDispatchFixtureTemplate(t, name, 4, 4, mustJSON(t, template))
			claim := mustWfReplaceCommand(t, f.mgr, f.wf, map[string]any{"op": "claim", "node_id": "start", "activation_id": f.activation, "actor": "manual"})
			_, err := f.mgr.WorkflowCommand(f.wf, mustJSON(t, map[string]any{
				"op": "start", "node_id": "start", "activation_id": f.activation,
				"lease_id": claim["lease_id"], "owner_token": claim["owner_token"], "selection": map[string]any{"messaging": true},
			}))
			if err == nil || !strings.Contains(err.Error(), "messaging is not supported for loop and team phases") {
				t.Fatalf("start error: %v", err)
			}
			inspect, err := f.mgr.WorkflowInspect(f.wf)
			if err != nil {
				t.Fatal(err)
			}
			attempts := inspect.(map[string]any)["attempts"].([]workflow.Attempt)
			if len(attempts) != 1 || attempts[0].Status != workflow.AttemptFailed {
				t.Fatalf("attempts: %+v", attempts)
			}
			out, err := f.mgr.WorkflowInstantiate(mustJSON(t, map[string]string{"template_id": "tmpl-dispatch-" + name, "template_version": "1"}))
			if err != nil {
				t.Fatal(err)
			}
			wf2 := out.(map[string]any)["workflow_id"].(string)
			act, _ := wfReplaceActivation(t, f.mgr, wf2)
			claim2 := mustWfReplaceCommand(t, f.mgr, wf2, map[string]any{"op": "claim", "node_id": "start", "activation_id": act.ID, "actor": "manual"})
			_, err = f.mgr.WorkflowCommand(wf2, mustJSON(t, map[string]any{
				"op": "start", "node_id": "start", "activation_id": act.ID,
				"lease_id": claim2["lease_id"], "owner_token": claim2["owner_token"], "selection": map[string]any{"messaging": true},
			}))
			if err == nil || !strings.Contains(err.Error(), "messaging is not supported for loop and team phases") {
				t.Fatalf("second start should reach messaging refusal, got: %v", err)
			}
		})
	}
}

func TestWorkflowAssignmentMessagingDispatch(t *testing.T) {
	for _, kind := range []string{"run", "loop", "team"} {
		for _, on := range []bool{false, true} {
			if kind != "run" && on {
				continue
			}
			t.Run(fmt.Sprintf("%s-%v", kind, on), func(t *testing.T) {
				name := fmt.Sprintf("message-%s-%v", kind, on)
				file := ""
				if kind != "run" {
					cfg := map[string]any{kind: []map[string]any{{"name": "worker", "prompt": "work"}}}
					if kind == "loop" {
						cfg["max_iterations"] = 1
					}
					file = itestWorkflowConfig(t, kind, cfg)
				}
				var template map[string]any
				if err := json.Unmarshal(dispatchActionTemplateJSON(t, "tmpl-dispatch-"+name, "c1", kind, file), &template); err != nil {
					t.Fatal(err)
				}
				template["nodes"].([]any)[0].(map[string]any)["assignment"] = map[string]any{"messaging": on}
				f := newDispatchFixtureTemplate(t, name, 4, 4, mustJSON(t, template))
				observed := make(chan runtime.StartOptions, 2)
				f.sup.newProviderFunc = func(opts runtime.StartOptions, _ string) (runtime.Provider, error) {
					observed <- opts
					return f.provider, nil
				}
				out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
				if err != nil {
					t.Fatal(err)
				}
				if out.Kind != Dispatched {
					t.Fatalf("outcome: %+v", out)
				}
				if kind == "run" {
					select {
					case opts := <-observed:
						if opts.Messaging == nil || *opts.Messaging != on || opts.MessagingOn != on {
							t.Fatalf("options: %+v", opts)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("no provider")
					}
				}
			})
		}
	}
}
