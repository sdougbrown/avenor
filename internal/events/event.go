package events

import "encoding/json"

type Event struct {
	Event     string         `json:"event"`
	SessionID string         `json:"session_id,omitempty"`
	Fields    map[string]any `json:"-"`
}

const MaxFinalOutputRunes = 4096

func Clone(event Event) Event {
	out := Event{Event: event.Event, SessionID: event.SessionID}
	if event.Fields != nil {
		out.Fields = make(map[string]any, len(event.Fields))
		for key, value := range event.Fields {
			out.Fields[key] = value
		}
	}
	return out
}

func Int64(value any) (int64, bool) {
	switch number := value.(type) {
	case int:
		return int64(number), true
	case int32:
		return int64(number), true
	case int64:
		return number, true
	case float64:
		return int64(number), true
	case json.Number:
		parsed, err := number.Int64()
		return parsed, err == nil
	default:
		return 0, false
	}
}

// FinalOutputTruncated reports whether a status/inspection preview will omit
// any runes from this terminal reply.
func FinalOutputTruncated(text string) bool {
	count := 0
	for range text {
		count++
		if count > MaxFinalOutputRunes {
			return true
		}
	}
	return false
}

func BoundedFinalOutput(text string) string {
	count := 0
	for index := range text {
		if count == MaxFinalOutputRunes {
			return text[:index]
		}
		count++
	}
	return text
}

func BoundFinalOutput(event Event) Event {
	if event.Event != "session.end" || event.Fields == nil {
		return event
	}
	text, _ := event.Fields["final_output"].(string)
	bounded := BoundedFinalOutput(text)
	if bounded == text {
		return event
	}
	out := Clone(event)
	out.Fields["final_output"] = bounded
	// This copy is sent to bounded presentation surfaces (status, history, and
	// subscriptions); preserve the fact that it is only a preview.
	out.Fields["final_output_truncated"] = true
	return out
}

func (e Event) MarshalJSON() ([]byte, error) {
	fields := map[string]any{}
	for k, v := range e.Fields {
		fields[k] = v
	}
	fields["event"] = e.Event
	if e.SessionID != "" {
		fields["session_id"] = e.SessionID
	}
	return json.Marshal(fields)
}

func (e *Event) UnmarshalJSON(data []byte) error {
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}

	if event, ok := fields["event"].(string); ok {
		e.Event = event
		delete(fields, "event")
	}
	if sessionID, ok := fields["session_id"].(string); ok {
		e.SessionID = sessionID
		delete(fields, "session_id")
	}
	e.Fields = fields
	return nil
}
