package stable

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
)

func TestWorkflowAssignmentMessagingDispatch(t *testing.T) {
	for _, kind := range []string{"run", "loop", "team"} {
		for _, on := range []bool{false, true} {
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
				if kind != "run" && on {
					if out.Kind != DispatchStartFailed || out.Detail != "messaging is not supported for loop and team phases" {
						t.Fatalf("outcome: %+v", out)
					}
					return
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
