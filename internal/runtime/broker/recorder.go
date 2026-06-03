package broker

import (
	"encoding/json"

	"github.com/sdougbrown/avenor/internal/events"
)

// Recorder translates events.Event values into broker.Ingest calls so
// that any backend can mirror its event stream into the broker without
// an agent-side sidecar process.
//
// Usage:
//
//	rec := broker.NewRecorder(brk, runID)
//	for evt := range provider.Events(ctx, sessionID) {
//	    rec.Feed(evt)
//	    subscriberCh <- evt  // keep existing fanout
//	}
type Recorder struct {
	broker *Broker
	runID  string
}

// NewRecorder creates a Recorder for the given broker and run ID.
func NewRecorder(b *Broker, runID string) *Recorder {
	return &Recorder{broker: b, runID: runID}
}

// Feed translates an event into a broker.Ingest call. Recognised events:
//
//	session.end             → kind = stop_reason (e.g. "done", "failed") — stored in Finishes
//	agent.status            → kind = "status" — stored in Reports
//	agent.message_chunk     → kind = "thinking" — stored in Reports
//	permission.request      → kind = "permission_requested" — stored in Reports
//	child.question          → kind = "child_question" — stored in Reports
//
// Unknown or low-signal events are silently dropped.
func (r *Recorder) Feed(evt events.Event) {
	var kind string
	switch evt.Event {
	case "session.end":
		if r, ok := evt.Fields["stop_reason"].(string); ok && r != "" {
			kind = r
		} else {
			kind = "done"
		}
	case "agent.status":
		kind = "status"
	case "agent.message_chunk":
		kind = "thinking"
	case "permission.request":
		kind = "permission_requested"
	case "child.question":
		kind = "child_question"
	default:
		return
	}

	payload, _ := json.Marshal(evt.Fields)
	_ = r.broker.Ingest(r.runID, kind, payload)
}
