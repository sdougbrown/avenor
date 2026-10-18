package control

// Snapshot phase normalization: a missing or empty agent.status phase leaves
// the stored phase untouched, a known phase is stored verbatim, and an
// unrecognized phase is stored as the plain-string "unknown" on the wire.

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runstate"
)

func TestSnapshotAgentStatusPhaseNormalization(t *testing.T) {
	state := NewState("run_1", "label", 0)
	s := NewServer(state)

	publish := func(fields map[string]any) {
		t.Helper()
		s.PublishCanonicalEvent(events.Event{Event: "agent.status", SessionID: "ses_1", Fields: fields})
	}

	// A known phase is stored.
	publish(map[string]any{"phase": "working"})
	if got := state.Snapshot().Phase; got != runstate.PhaseWorking {
		t.Fatalf("phase = %q, want working", got)
	}

	// A missing phase leaves the stored phase untouched.
	publish(map[string]any{"label": "step"})
	if got := state.Snapshot().Phase; got != runstate.PhaseWorking {
		t.Fatalf("phase after missing field = %q, want working", got)
	}

	// An empty phase leaves the stored phase untouched.
	publish(map[string]any{"phase": ""})
	if got := state.Snapshot().Phase; got != runstate.PhaseWorking {
		t.Fatalf("phase after empty field = %q, want working", got)
	}

	// An unrecognized phase normalizes to unknown and marshals as a plain
	// string for JSON consumers.
	publish(map[string]any{"phase": "warp"})
	if got := state.Snapshot().Phase; got != runstate.PhaseUnknown {
		t.Fatalf("phase = %q, want unknown", got)
	}
	data, err := json.Marshal(state.Snapshot())
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !strings.Contains(string(data), `"phase":"unknown"`) {
		t.Fatalf("snapshot JSON = %s, want an \"unknown\" phase field", data)
	}

	// A later known phase replaces the unknown one.
	publish(map[string]any{"phase": "done"})
	if got := state.Snapshot().Phase; got != runstate.PhaseDone {
		t.Fatalf("phase = %q, want done", got)
	}
}
