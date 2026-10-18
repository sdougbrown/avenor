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

// TestPhaseEventsCarryRunKind pins the additive phase-event contract: run_kind
// names the containing runner on both events, the start keeps its section kind,
// the end keeps its terminal shape, and neither event grows per-attempt fields.
func TestPhaseEventsCarryRunKind(t *testing.T) {
	tests := []struct {
		name        string
		runID       string
		runKind     string
		wantRunID   bool
		wantSection string // section kind the start must keep
	}{
		{name: "loop run", runID: "run_1", runKind: "loop", wantRunID: true, wantSection: "pre"},
		{name: "team run", runID: "run_2", runKind: "team", wantRunID: true, wantSection: "post"},
		{name: "empty run id still reports the run kind", runKind: "loop", wantSection: "loop"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := &eventRecorder{}
			if err := EmitPhaseStart(recorder, tt.runID, "code_review", 1, tt.wantSection, tt.runKind); err != nil {
				t.Fatalf("EmitPhaseStart: %v", err)
			}
			marker := &LoopMarker{Directive: "abort", Label: "stop-here"}
			if err := EmitPhaseEnd(recorder, tt.runID, "code_review", 1, tt.runKind, "end_turn", marker); err != nil {
				t.Fatalf("EmitPhaseEnd: %v", err)
			}
			if len(recorder.events) != 2 {
				t.Fatalf("events = %d, want a start and an end", len(recorder.events))
			}

			start, end := recorder.events[0], recorder.events[1]
			if start.Event != "avenor.phase.start" || end.Event != "avenor.phase.end" {
				t.Fatalf("events = %q, %q", start.Event, end.Event)
			}
			for _, ev := range recorder.events {
				if got := ev.Fields["run_kind"]; got != tt.runKind {
					t.Errorf("%s run_kind = %v, want %q", ev.Event, got, tt.runKind)
				}
				if _, ok := ev.Fields["attempt"]; ok {
					t.Errorf("%s carries per-attempt fields: %v", ev.Event, ev.Fields)
				}
				runID, ok := ev.Fields["run_id"]
				if ok != tt.wantRunID {
					t.Errorf("%s run_id present = %v, want %v", ev.Event, ok, tt.wantRunID)
				}
				if ok && runID != tt.runID {
					t.Errorf("%s run_id = %v, want %q", ev.Event, runID, tt.runID)
				}
			}
			if got := start.Fields["kind"]; got != tt.wantSection {
				t.Errorf("start kind = %v, want the unchanged section kind %q", got, tt.wantSection)
			}
			if _, ok := end.Fields["kind"]; ok {
				t.Errorf("end gained a section kind: %v", end.Fields)
			}
			if got := end.Fields["stop_reason"]; got != "end_turn" {
				t.Errorf("end stop_reason = %v, want end_turn", got)
			}
			if got := end.Fields["abort_marker"]; got != true {
				t.Errorf("end abort_marker = %v, want true", got)
			}
			if got := end.Fields["abort_marker_label"]; got != "stop-here" {
				t.Errorf("end abort_marker_label = %v, want stop-here", got)
			}
		})
	}
}
