package phaseconfig

import (
	"testing"

	"github.com/sdougbrown/avenor/internal/events"
)

type eventRecorder struct {
	events []events.Event
}

func (r *eventRecorder) Write(ev events.Event) error {
	r.events = append(r.events, ev)
	return nil
}

// TestPhaseEventsCarryRunKindWithoutChangingSectionKind pins the additive
// phase-event contract: run_kind names the containing runner on both the start
// and the end event, the start keeps its section kind, the end keeps its
// terminal shape, and neither event grows per-attempt fields.
func TestPhaseEventsCarryRunKindWithoutChangingSectionKind(t *testing.T) {
	recorder := &eventRecorder{}
	if err := EmitPhaseStart(recorder, "run_1", "code_review", 1, "pre", "loop"); err != nil {
		t.Fatalf("EmitPhaseStart: %v", err)
	}
	marker := &LoopMarker{Directive: "abort", Label: "stop-here"}
	if err := EmitPhaseEnd(recorder, "run_1", "code_review", 1, "loop", "end_turn", marker); err != nil {
		t.Fatalf("EmitPhaseEnd: %v", err)
	}
	if len(recorder.events) != 2 {
		t.Fatalf("events = %d, want a start and an end", len(recorder.events))
	}

	start := recorder.events[0]
	if start.Event != "avenor.phase.start" {
		t.Fatalf("event = %q, want avenor.phase.start", start.Event)
	}
	if got := start.Fields["run_kind"]; got != "loop" {
		t.Fatalf("start run_kind = %v, want loop", got)
	}
	if got := start.Fields["kind"]; got != "pre" {
		t.Fatalf("start kind = %v, want the unchanged section kind pre", got)
	}
	if got := start.Fields["phase"]; got != "code_review" {
		t.Fatalf("start phase = %v, want code_review", got)
	}
	if got := start.Fields["iteration"]; got != 1 {
		t.Fatalf("start iteration = %v, want 1", got)
	}
	if got := start.Fields["run_id"]; got != "run_1" {
		t.Fatalf("start run_id = %v, want run_1", got)
	}
	if _, ok := start.Fields["attempt"]; ok {
		t.Fatalf("start carries per-attempt fields: %v", start.Fields)
	}

	end := recorder.events[1]
	if end.Event != "avenor.phase.end" {
		t.Fatalf("event = %q, want avenor.phase.end", end.Event)
	}
	if got := end.Fields["run_kind"]; got != "loop" {
		t.Fatalf("end run_kind = %v, want loop", got)
	}
	if _, ok := end.Fields["kind"]; ok {
		t.Fatalf("end gained a section kind: %v", end.Fields)
	}
	if got := end.Fields["stop_reason"]; got != "end_turn" {
		t.Fatalf("end stop_reason = %v, want end_turn", got)
	}
	if got := end.Fields["abort_marker"]; got != true {
		t.Fatalf("end abort_marker = %v, want true", got)
	}
	if got := end.Fields["abort_marker_label"]; got != "stop-here" {
		t.Fatalf("end abort_marker_label = %v, want stop-here", got)
	}
	if _, ok := end.Fields["attempt"]; ok {
		t.Fatalf("end carries per-attempt fields: %v", end.Fields)
	}
}

// TestPhaseEventWithoutRunIDOmitsIt keeps the existing conditional run_id
// behavior intact for callers that tag the event elsewhere.
func TestPhaseEventWithoutRunIDOmitsIt(t *testing.T) {
	recorder := &eventRecorder{}
	if err := EmitPhaseStart(recorder, "", "body", 0, "loop", "loop"); err != nil {
		t.Fatalf("EmitPhaseStart: %v", err)
	}
	if _, ok := recorder.events[0].Fields["run_id"]; ok {
		t.Fatalf("run_id present for an empty run id: %v", recorder.events[0].Fields)
	}
	if got := recorder.events[0].Fields["run_kind"]; got != "loop" {
		t.Fatalf("run_kind = %v, want loop even without a run id", got)
	}
}
