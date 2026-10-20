package stable

// Coverage of the spawn publication boundary. A spawn registers the child in
// the runtime registry before it finishes initializing it, so every concurrent
// List/RuntimeStatus reader can reach the child. This test keeps a reader
// polling through the spawn of a direct, a loop and a team child: the first
// sighting of each row must already carry the paths its spawn parameters
// determine, and under -race a reader that observes a field written without the
// lock it holds fails.

import (
	"context"
	"os"
	"path/filepath"
	goruntime "runtime"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
)

// spawnDeterministicFields are the status fields a spawn sets before it
// publishes the child, so a reader can never observe them empty on a row's
// first sighting.
var spawnDeterministicFields = []string{"on_event", "sentinel_file"}

func TestSpawnSurvivesConcurrentStatusReaders(t *testing.T) {
	t.Parallel()
	// Hold the direct child inside Start until the reader has seen the
	// published row and opened its gate. The reader keeps polling afterwards,
	// so its reads overlap the writes the spawn makes once Start returns;
	// detection comes from that polling, never from the gate.
	gate := newItestGate()
	direct := newItestProvider(itestAttempt("ses_pub_direct", itestSessionEnd("ses_pub_direct", "end_turn", "direct done")))
	direct.onStart = func(ctx context.Context) {
		select {
		case <-gate.ch:
		case <-ctx.Done():
		case <-time.After(itestTimeout):
			t.Errorf("gate was never opened: no reader sighted the published row of the direct spawn")
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

	var readerMu sync.Mutex
	seen := map[string]bool{} // runtime IDs already inspected
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
				readerMu.Unlock()
				if _, err := h.sup.RuntimeStatus(id); err != nil {
					t.Errorf("RuntimeStatus(%s): %v", id, err)
				}
				if !first {
					continue
				}
				label, _ := row["label"].(string)
				for _, key := range spawnDeterministicFields {
					if value, _ := row[key].(string); value == "" {
						t.Errorf("first sighting of %q row has empty %s", label, key)
					}
				}
				if label == "pub-direct" {
					gate.open()
				}
			}
		}
	}()
	defer func() {
		close(stop)
		readers.Wait()
	}()

	h.awaitStatus(h.spawn(map[string]any{"label": "pub-direct"}), "done")

	loopFile := itestWorkflowConfig(t, "loop", map[string]any{
		"max_iterations": 1,
		"loop":           []map[string]any{{"name": "work", "prompt": "loop work"}},
	})
	teamFile := itestWorkflowConfig(t, "team", map[string]any{
		"team": []map[string]any{{"name": "solo", "prompt": "team work"}},
	})
	h.awaitStatus(h.spawnWorkflow("loop", loopFile, map[string]any{"label": "pub-loop"}), "done")
	h.awaitStatus(h.spawnWorkflow("team", teamFile, map[string]any{"label": "pub-team"}), "done")
}

// A spawn only creates the per-runtime artifact directory when one of its paths
// defaults into it; one that supplies both paths has nothing to put there.
func TestSpawnCreatesArtifactDirOnlyForDefaults(t *testing.T) {
	t.Parallel()
	// Created before the harness so it outlives the harness's shutdown, which
	// writes the supplied sentinel during cleanup.
	suppliedDir := t.TempDir()
	h := newItestHarness(t, Config{}, func(opts runtime.StartOptions, _ string) (runtime.Provider, error) {
		sessionID := "ses_" + opts.RuntimeID
		return newItestProvider(itestAttempt(sessionID, itestSessionEnd(sessionID, "end_turn", "done"))), nil
	})
	defaulted := h.spawn(map[string]any{"label": "artdir-default"})
	supplied := h.spawn(map[string]any{
		"label":         "artdir-supplied",
		"on_event":      filepath.Join(suppliedDir, "events.ndjson"),
		"sentinel_file": filepath.Join(suppliedDir, "sentinel.env"),
	})
	for _, tc := range []struct {
		name string
		rtID string
		want bool
	}{
		{"defaulted paths", defaulted, true},
		{"supplied paths", supplied, false},
	} {
		artifactDir := h.sup.runtimeArtifactDir(tc.rtID)
		_, err := os.Stat(artifactDir)
		if got := err == nil; got != tc.want {
			t.Errorf("%s: artifact dir %s exists = %v, want %v", tc.name, artifactDir, got, tc.want)
		}
	}
}
