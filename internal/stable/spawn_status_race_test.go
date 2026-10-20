package stable

// Race-detector coverage for the spawn publication boundary. A spawn registers
// the child in the runtime registry before it finishes initializing it, so
// every concurrent List/RuntimeStatus reader can reach the child. This test
// keeps a reader polling through the spawn of a direct, a loop and a team
// child; under -race it fails if a reader observes a field the spawn writes
// without the lock that reader holds.

import (
	"context"
	"sync"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
)

func TestSpawnSurvivesConcurrentStatusReaders(t *testing.T) {
	t.Parallel()
	gate := newItestGate()
	defer gate.open()
	direct := newItestProvider(itestAttempt("ses_pub_direct", itestSessionEnd("ses_pub_direct", "end_turn", "direct done")))
	// Hold the direct child inside Start until the reader has seen the
	// published runtime, so its reads land inside the initialization window.
	direct.onStart = func(ctx context.Context) {
		select {
		case <-gate.ch:
		case <-ctx.Done():
		}
	}
	loop := newItestProvider(itestAttempt("ses_pub_loop", itestSessionEnd("ses_pub_loop", "end_turn", "loop done")))
	team := newItestProvider(itestAttempt("ses_pub_team", itestSessionEnd("ses_pub_team", "end_turn", "team done")))
	h := newItestHarness(t, Config{}, func(opts runtime.StartOptions, _ string) (runtime.Provider, error) {
		switch opts.Label {
		case "pub-loop":
			return loop, nil
		case "pub-team":
			return team, nil
		}
		return direct, nil
	})

	stop := make(chan struct{})
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			rows, _ := h.sup.List().([]map[string]any)
			for _, row := range rows {
				id, _ := row["runtime_id"].(string)
				if id == "" {
					continue
				}
				if _, err := h.sup.RuntimeStatus(id); err != nil {
					continue
				}
				gate.open()
			}
		}
	}()
	defer func() {
		close(stop)
		readers.Wait()
	}()

	directRT := h.spawn(map[string]any{"label": "pub-direct"})
	h.awaitStatus(directRT, "done")

	loopFile := itestWorkflowConfig(t, "loop", map[string]any{
		"max_iterations": 1,
		"loop":           []map[string]any{{"name": "work", "prompt": "loop work"}},
	})
	teamFile := itestWorkflowConfig(t, "team", map[string]any{
		"team": []map[string]any{{"name": "solo", "prompt": "team work"}},
	})
	loopRT := h.spawnWorkflow("loop", loopFile, map[string]any{"label": "pub-loop"})
	teamRT := h.spawnWorkflow("team", teamFile, map[string]any{"label": "pub-team"})
	h.awaitStatus(loopRT, "done")
	h.awaitStatus(teamRT, "done")
}
