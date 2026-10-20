package stable

// Coverage of the spawn publication boundary. A spawn registers the child in
// the runtime registry before it finishes initializing it, so every concurrent
// List/RuntimeStatus reader can reach the child. This test keeps a reader
// polling through the spawn of direct, loop and team children: the first
// sighting of a held direct child must already carry the fields its spawn
// parameters determine, and under -race a reader that observes a field written
// without the lock it holds fails.

import (
	"context"
	"fmt"
	goruntime "runtime"
	"sync"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
)

// spawnDeterministicFields are the status fields a direct spawn knows from its
// parameters, so a reader can never observe them empty on a published row.
var spawnDeterministicFields = []string{"dir", "backend", "on_event", "sentinel_file"}

func TestSpawnSurvivesConcurrentStatusReaders(t *testing.T) {
	t.Parallel()
	// Hold a direct child inside Start until the reader has seen the published
	// row and opened its gate. The reader keeps polling afterwards, so its
	// reads overlap the writes the spawn makes once Start returns; detection
	// comes from that polling, never from the gate.
	holdCh := make(chan *itestGate, 1)
	newDirect := func(sessionID string) *itestProvider {
		p := newItestProvider(itestAttempt(sessionID, itestSessionEnd(sessionID, "end_turn", "direct done")))
		p.onStart = func(ctx context.Context) {
			var gate *itestGate
			select {
			case gate = <-holdCh:
			default:
				return
			}
			select {
			case <-gate.ch:
			case <-ctx.Done():
			}
		}
		return p
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
		return newDirect("ses_pub_" + opts.RuntimeID), nil
	})

	var readerMu sync.Mutex
	gates := map[string]*itestGate{} // label -> gate of the direct spawn to release
	seen := map[string]bool{}        // runtime IDs already inspected
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
			// Yield so the spawn side runs between polls: the overlapping
			// reads this test looks for need both sides scheduled.
			goruntime.Gosched()
			rows, _ := h.sup.List().([]map[string]any)
			for _, row := range rows {
				id, _ := row["runtime_id"].(string)
				if id == "" {
					continue
				}
				readerMu.Lock()
				first := !seen[id]
				seen[id] = true
				label, _ := row["label"].(string)
				gate := gates[label]
				readerMu.Unlock()
				if _, err := h.sup.RuntimeStatus(id); err != nil {
					t.Errorf("RuntimeStatus(%s): %v", id, err)
				}
				if !first || gate == nil {
					continue
				}
				for _, key := range spawnDeterministicFields {
					if value, _ := row[key].(string); value == "" {
						t.Errorf("published row for %q has empty %s while its spawn is still initializing", label, key)
					}
				}
				gate.open()
			}
		}
	}()
	defer func() {
		close(stop)
		readers.Wait()
	}()

	for i := range 10 {
		label := fmt.Sprintf("pub-direct-%d", i)
		gate := newItestGate()
		readerMu.Lock()
		gates[label] = gate
		readerMu.Unlock()
		holdCh <- gate
		h.awaitStatus(h.spawn(map[string]any{"label": label}), "done")
	}

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
