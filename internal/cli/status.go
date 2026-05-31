package cli

import (
	"time"

	"github.com/sdougbrown/avenor/internal/events"
)

const (
	phaseThink = "thinking"
	phaseWork  = "working"
	phaseWait  = "waiting"
	phaseDone  = "done"
)

// statusTracker observes the ACP event stream and emits synthesized
// agent.status events on phase transitions.
type statusTracker struct {
	sessionID   string
	runID       string
	runLabel    string
	phase       string
	label       string
	markerLabel string // set only by ObserveMarker; preserved across permission gates
}

func newStatusTracker(sessionID, runID string, runLabel ...string) *statusTracker {
	tracker := &statusTracker{sessionID: sessionID, runID: runID}
	if len(runLabel) > 0 {
		tracker.runLabel = runLabel[0]
	}
	return tracker
}

// Observe returns a synthesized agent.status event and true when ev causes a
// phase transition or label change. Returns the zero Event and false otherwise.
func (s *statusTracker) Observe(ev events.Event) (events.Event, bool) {
	switch ev.Event {
	case "agent.thought_chunk":
		if s.phase == phaseThink || s.phase == phaseWait || s.phase == phaseDone {
			return events.Event{}, false
		}
		return s.set(phaseThink, "")

	case "tool.call":
		if s.phase == phaseWait || s.phase == phaseDone {
			return events.Event{}, false
		}
		label := statusField(ev.Fields, "title")
		if s.phase == phaseWork && label == s.label {
			return events.Event{}, false
		}
		return s.set(phaseWork, label)

	case "permission.request":
		if s.phase == phaseDone {
			return events.Event{}, false
		}
		label := statusField(ev.Fields, "question")
		if label == "" {
			label = statusField(ev.Fields, "tool")
		}
		return s.set(phaseWait, label)

	case "session.end":
		return s.set(phaseDone, "")
	}

	return events.Event{}, false
}

// PermissionAnswered returns a synthesized agent.status event transitioning
// back to working after a permission request is resolved.
// Only the markerLabel (set via ObserveMarker) is preserved; labels set by
// Observe(permission.request) from the protocol question/tool text are
// intentionally dropped so they do not leak into the working-phase label.
func (s *statusTracker) PermissionAnswered() (events.Event, bool) {
	if s.phase != phaseWait {
		return events.Event{}, false
	}
	return s.set(phaseWork, s.markerLabel)
}

// ObserveMarker handles an explicit [status: phase | label] marker extracted
// from agent output. It updates tracker state and returns an agent.status
// event with source="agent" instead of "avenor".
func (s *statusTracker) ObserveMarker(phase, label string) (events.Event, bool) {
	s.phase = phase
	s.label = label
	s.markerLabel = label // preserved across permission gates
	fields := map[string]any{
		"phase":  phase,
		"source": "agent",
		"ts":     time.Now().UnixMilli(),
	}
	if label != "" {
		fields["label"] = label
	}
	if s.runID != "" {
		fields["run_id"] = s.runID
	}
	if s.runLabel != "" {
		fields["run_label"] = s.runLabel
	}
	return events.Event{
		Event:     "agent.status",
		SessionID: s.sessionID,
		Fields:    fields,
	}, true
}

// chunkText extracts the plain text from an agent or user message chunk event.
func chunkText(ev events.Event) string {
	content, ok := ev.Fields["content"].(map[string]any)
	if ok {
		text, _ := content["text"].(string)
		if text != "" {
			return text
		}
	}
	text, _ := ev.Fields["delta"].(string)
	return text
}

func (s *statusTracker) set(phase, label string) (events.Event, bool) {
	s.phase = phase
	s.label = label
	fields := map[string]any{
		"phase":  phase,
		"source": "avenor",
		"ts":     time.Now().UnixMilli(),
	}
	if label != "" {
		fields["label"] = label
	}
	if s.runID != "" {
		fields["run_id"] = s.runID
	}
	if s.runLabel != "" {
		fields["run_label"] = s.runLabel
	}
	return events.Event{
		Event:     "agent.status",
		SessionID: s.sessionID,
		Fields:    fields,
	}, true
}

func statusField(fields map[string]any, key string) string {
	if fields == nil {
		return ""
	}
	v, _ := fields[key].(string)
	return v
}
