package cli

import (
	"testing"

	"github.com/sdougbrown/avenor/internal/events"
)

func makeEvent(name string, fields map[string]any) events.Event {
	return events.Event{Event: name, SessionID: "ses_1", Fields: fields}
}

func TestStatusTrackerTransitions(t *testing.T) {
	tests := []struct {
		name       string
		sequence   []events.Event
		wantPhases []string // expected phase per emitted status event, in order
	}{
		{
			name: "thought_chunk triggers thinking",
			sequence: []events.Event{
				makeEvent("agent.thought_chunk", nil),
			},
			wantPhases: []string{phaseThink},
		},
		{
			name: "repeated thought_chunks emit only one transition",
			sequence: []events.Event{
				makeEvent("agent.thought_chunk", nil),
				makeEvent("agent.thought_chunk", nil),
				makeEvent("agent.thought_chunk", nil),
			},
			wantPhases: []string{phaseThink},
		},
		{
			name: "tool.call triggers working with label",
			sequence: []events.Event{
				makeEvent("tool.call", map[string]any{"title": "go test ./..."}),
			},
			wantPhases: []string{phaseWork},
		},
		{
			name: "thought then tool: thinking then working",
			sequence: []events.Event{
				makeEvent("agent.thought_chunk", nil),
				makeEvent("tool.call", map[string]any{"title": "go build"}),
			},
			wantPhases: []string{phaseThink, phaseWork},
		},
		{
			name: "new tool.call with different title emits while already working",
			sequence: []events.Event{
				makeEvent("tool.call", map[string]any{"title": "go build"}),
				makeEvent("tool.call", map[string]any{"title": "go test"}),
			},
			wantPhases: []string{phaseWork, phaseWork},
		},
		{
			name: "same tool.call title does not re-emit",
			sequence: []events.Event{
				makeEvent("tool.call", map[string]any{"title": "go test"}),
				makeEvent("tool.call", map[string]any{"title": "go test"}),
			},
			wantPhases: []string{phaseWork},
		},
		{
			name: "permission.request triggers waiting",
			sequence: []events.Event{
				makeEvent("tool.call", map[string]any{"title": "write file"}),
				makeEvent("permission.request", map[string]any{"question": "Allow write?", "tool": "write"}),
			},
			wantPhases: []string{phaseWork, phaseWait},
		},
		{
			name: "session.end triggers done",
			sequence: []events.Event{
				makeEvent("agent.thought_chunk", nil),
				makeEvent("session.end", map[string]any{"stop_reason": "end_turn"}),
			},
			wantPhases: []string{phaseThink, phaseDone},
		},
		{
			name: "no events after done",
			sequence: []events.Event{
				makeEvent("session.end", nil),
				makeEvent("agent.thought_chunk", nil),
				makeEvent("tool.call", map[string]any{"title": "something"}),
				makeEvent("permission.request", nil),
			},
			wantPhases: []string{phaseDone},
		},
		{
			name: "tool.call during waiting is suppressed",
			sequence: []events.Event{
				makeEvent("permission.request", map[string]any{"question": "Allow?"}),
				makeEvent("tool.call", map[string]any{"title": "exec"}),
			},
			wantPhases: []string{phaseWait},
		},
		{
			name: "unrecognised events produce no status",
			sequence: []events.Event{
				makeEvent("session.plan", nil),
				makeEvent("user.message_chunk", nil),
				makeEvent("tool.call_update", map[string]any{"status": "completed"}),
			},
			wantPhases: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker := newStatusTracker("ses_1", "")
			var gotPhases []string
			for _, ev := range tt.sequence {
				if statusEv, ok := tracker.Observe(ev); ok {
					gotPhases = append(gotPhases, statusField(statusEv.Fields, "phase"))
				}
			}
			if len(gotPhases) != len(tt.wantPhases) {
				t.Fatalf("emitted phases %v, want %v", gotPhases, tt.wantPhases)
			}
			for i, want := range tt.wantPhases {
				if gotPhases[i] != want {
					t.Errorf("phase[%d] = %q, want %q", i, gotPhases[i], want)
				}
			}
		})
	}
}

func TestStatusTrackerLabel(t *testing.T) {
	tracker := newStatusTracker("ses_1", "")
	ev, ok := tracker.Observe(makeEvent("tool.call", map[string]any{"title": "go test ./..."}))
	if !ok {
		t.Fatal("expected status event, got none")
	}
	if got := statusField(ev.Fields, "label"); got != "go test ./..." {
		t.Fatalf("label = %q, want %q", got, "go test ./...")
	}
	if got := statusField(ev.Fields, "source"); got != "avenor" {
		t.Fatalf("source = %q, want %q", got, "avenor")
	}
	if ev.Event != "agent.status" {
		t.Fatalf("event = %q, want %q", ev.Event, "agent.status")
	}
	if ev.SessionID != "ses_1" {
		t.Fatalf("session_id = %q, want %q", ev.SessionID, "ses_1")
	}
}

func TestStatusTrackerPermissionLabel(t *testing.T) {
	tracker := newStatusTracker("ses_1", "")
	// question takes precedence over tool
	ev, _ := tracker.Observe(makeEvent("permission.request", map[string]any{
		"question": "Allow exec?",
		"tool":     "exec",
	}))
	if got := statusField(ev.Fields, "label"); got != "Allow exec?" {
		t.Fatalf("label = %q, want %q", got, "Allow exec?")
	}

	// empty question falls back to tool
	tracker2 := newStatusTracker("ses_1", "")
	ev2, _ := tracker2.Observe(makeEvent("permission.request", map[string]any{
		"question": "",
		"tool":     "write",
	}))
	if got := statusField(ev2.Fields, "label"); got != "write" {
		t.Fatalf("label = %q, want %q", got, "write")
	}
}

func TestStatusTrackerNoLabelOnThinkOrDone(t *testing.T) {
	tracker := newStatusTracker("ses_1", "")
	ev, _ := tracker.Observe(makeEvent("agent.thought_chunk", nil))
	if label := statusField(ev.Fields, "label"); label != "" {
		t.Fatalf("thinking event should have no label, got %q", label)
	}
	ev2, _ := tracker.Observe(makeEvent("session.end", nil))
	if label := statusField(ev2.Fields, "label"); label != "" {
		t.Fatalf("done event should have no label, got %q", label)
	}
}

func TestStatusTrackerObserveMarker(t *testing.T) {
	tracker := newStatusTracker("ses_1", "run123")

	ev, ok := tracker.ObserveMarker("working", "Parsing source files")
	if !ok {
		t.Fatal("ObserveMarker() ok = false, want true")
	}
	if ev.Event != "agent.status" {
		t.Fatalf("event = %q, want %q", ev.Event, "agent.status")
	}
	if got := statusField(ev.Fields, "phase"); got != "working" {
		t.Errorf("phase = %q, want %q", got, "working")
	}
	if got := statusField(ev.Fields, "label"); got != "Parsing source files" {
		t.Errorf("label = %q, want %q", got, "Parsing source files")
	}
	if got := statusField(ev.Fields, "source"); got != "agent" {
		t.Errorf("source = %q, want %q", got, "agent")
	}
	if got := statusField(ev.Fields, "run_id"); got != "run123" {
		t.Errorf("run_id = %q, want %q", got, "run123")
	}

	// Tracker state should reflect the marker phase.
	if tracker.phase != "working" {
		t.Errorf("tracker.phase = %q, want %q", tracker.phase, "working")
	}
}

func TestStatusTrackerObserveMarkerSuppressesSynth(t *testing.T) {
	// An ObserveMarker call should update state so the subsequent synthesized
	// Observe for the same kind of event is a no-op (same phase, no label change).
	tracker := newStatusTracker("ses_1", "")
	tracker.ObserveMarker("working", "step one")

	// Same phase, same label → no synthesized event.
	_, ok := tracker.Observe(makeEvent("tool.call", map[string]any{"title": "step one"}))
	if ok {
		t.Error("Observe() should not emit when phase and label unchanged after ObserveMarker")
	}
}

func TestChunkText(t *testing.T) {
	tests := []struct {
		name string
		ev   events.Event
		want string
	}{
		{
			name: "text content",
			ev: makeEvent("agent.message_chunk", map[string]any{
				"content": map[string]any{"text": "hello world", "type": "text"},
			}),
			want: "hello world",
		},
		{
			name: "delta fallback",
			ev: makeEvent("agent.message_chunk", map[string]any{
				"delta": "hello from delta",
			}),
			want: "hello from delta",
		},
		{
			name: "content wins over delta",
			ev: makeEvent("agent.message_chunk", map[string]any{
				"content": map[string]any{"text": "preferred", "type": "text"},
				"delta":   "ignored",
			}),
			want: "preferred",
		},
		{
			name: "missing content",
			ev:   makeEvent("agent.message_chunk", nil),
			want: "",
		},
		{
			name: "array content",
			ev: makeEvent("agent.message_chunk", map[string]any{
				"content": []any{map[string]any{"text": "ignored"}},
			}),
			want: "",
		},
		{
			name: "empty text",
			ev: makeEvent("agent.message_chunk", map[string]any{
				"content": map[string]any{"text": "", "type": "text"},
			}),
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := chunkText(tt.ev); got != tt.want {
				t.Fatalf("chunkText() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestStatusTrackerRunIDAndTimestamp(t *testing.T) {
	const runID = "a1b2c3d4e5f60718"
	tracker := newStatusTracker("ses_1", runID)
	ev, ok := tracker.Observe(makeEvent("agent.thought_chunk", nil))
	if !ok {
		t.Fatal("expected status event")
	}

	if got, _ := ev.Fields["run_id"].(string); got != runID {
		t.Fatalf("run_id = %q, want %q", got, runID)
	}
	if ts, ok := ev.Fields["ts"].(int64); !ok || ts <= 0 {
		t.Fatalf("ts missing or non-positive: %v", ev.Fields["ts"])
	}
}

func TestStatusTrackerEmptyRunIDOmitted(t *testing.T) {
	tracker := newStatusTracker("ses_1", "")
	ev, _ := tracker.Observe(makeEvent("agent.thought_chunk", nil))
	if _, hasRunID := ev.Fields["run_id"]; hasRunID {
		t.Fatal("run_id should be absent when empty")
	}
}

func TestStatusTrackerPermissionAnswered(t *testing.T) {
	tracker := newStatusTracker("ses_1", "")

	// PermissionAnswered does nothing if not in waiting state.
	_, ok := tracker.PermissionAnswered()
	if ok {
		t.Fatal("PermissionAnswered() should not emit when not waiting")
	}

	// Enter waiting, then answer.
	tracker.Observe(makeEvent("permission.request", map[string]any{"question": "Allow?"}))
	ev, ok := tracker.PermissionAnswered()
	if !ok {
		t.Fatal("PermissionAnswered() should emit after permission.request")
	}
	if got := statusField(ev.Fields, "phase"); got != phaseWork {
		t.Fatalf("phase after answered = %q, want %q", got, phaseWork)
	}

	// Second call does nothing — no longer in waiting.
	_, ok = tracker.PermissionAnswered()
	if ok {
		t.Fatal("PermissionAnswered() should not emit twice")
	}
}

// TestStatusTrackerPermissionAnsweredPreservesLabel verifies that a label set
// via ObserveMarker before a permission gate is preserved on resume.
// Regression for the empty-string second arg to s.set in PermissionAnswered.
func TestStatusTrackerPermissionAnsweredPreservesLabel(t *testing.T) {
	tracker := newStatusTracker("ses_1", "")

	// Establish a label via ObserveMarker (simulates a [status: waiting | checking auth] marker).
	tracker.ObserveMarker(phaseWait, "checking auth")

	// Resolve the permission — must carry the label forward.
	ev, ok := tracker.PermissionAnswered()
	if !ok {
		t.Fatal("PermissionAnswered() should emit when in waiting state")
	}
	if got := statusField(ev.Fields, "phase"); got != phaseWork {
		t.Fatalf("phase = %q, want %q", got, phaseWork)
	}
	if got := statusField(ev.Fields, "label"); got != "checking auth" {
		t.Fatalf("label = %q, want %q; label was dropped across PermissionAnswered", got, "checking auth")
	}
}

// TestStatusTrackerPermissionAnsweredDoesNotLeakQuestionLabel verifies that
// the question text set by Observe(permission.request) does NOT leak into the
// working-phase label emitted by PermissionAnswered. Only labels established
// via ObserveMarker should be preserved.
func TestStatusTrackerPermissionAnsweredDoesNotLeakQuestionLabel(t *testing.T) {
	tracker := newStatusTracker("ses_1", "")

	// Enter waiting via a normal protocol event (question text set as label).
	tracker.Observe(makeEvent("permission.request", map[string]any{
		"question": "Allow file write?",
	}))

	// Resolve — the working-phase event must NOT carry the question as label.
	ev, ok := tracker.PermissionAnswered()
	if !ok {
		t.Fatal("PermissionAnswered() should emit when in waiting state")
	}
	if got := statusField(ev.Fields, "phase"); got != phaseWork {
		t.Fatalf("phase = %q, want %q", got, phaseWork)
	}
	if got := statusField(ev.Fields, "label"); got != "" {
		t.Fatalf("label = %q, want empty; permission question text leaked into working label", got)
	}
}
