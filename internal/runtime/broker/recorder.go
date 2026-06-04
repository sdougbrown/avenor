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

// Feed translates an event into a broker.Ingest call. Events not listed
// below are silently dropped — the Recorder is a filtered mirror, not a
// complete event log.
//
//	session.end             → kind = stop_reason (e.g. "done", "failed") — stored in Finishes
//	agent.status            → kind = "status" — stored in Reports
//	agent.message_chunk     → kind = "thinking" — stored in Reports
//	permission.request      → kind = "permission_requested" — stored in Reports
//	child.question          → kind = "child_question" — stored in Reports
//
// Ingest errors (run not found, broker not started) are intentionally
// suppressed — the Recorder is best-effort and must not block or panic
// the event pump.
func (rec *Recorder) Feed(evt events.Event) {
	if rec == nil || rec.broker == nil {
		return
	}

	var kind string
	switch evt.Event {
	case "session.end":
		if stopReason, ok := evt.Fields["stop_reason"].(string); ok && stopReason != "" {
			kind = stopReason
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

	payload, _ := json.Marshal(evt.Fields) // map[string]any always marshals; ok to ignore error
	_ = rec.broker.Ingest(rec.runID, kind, payload)
}
