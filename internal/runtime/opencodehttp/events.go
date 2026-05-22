package opencodehttp

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"strings"

	"github.com/sdougbrown/avenor/internal/events"
)

// sseEvent is a raw event from the opencode SSE stream.
type sseEvent struct {
	ID         string         `json:"id"`
	Type       string         `json:"type"`
	Properties map[string]any `json:"properties"`
}

// readSSEEvents reads SSE frames from r and sends mapped avenor events to out.
// It maintains per-session state to detect session end transitions.
func readSSEEvents(ctx context.Context, r io.Reader, out chan<- events.Event) {
	defer close(out)

	sessionErrors := map[string]string{} // sessionID → last error name
	sessionBusy := map[string]bool{}     // sessionID → is busy

	emit := func(evt events.Event) {
		select {
		case out <- evt:
		case <-ctx.Done():
		}
	}

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 4096), 1<<20)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := line[6:]

		var raw sseEvent
		if err := json.Unmarshal([]byte(payload), &raw); err != nil {
			continue
		}
		sid, _ := raw.Properties["sessionID"].(string)

		switch raw.Type {
		case "message.part.delta":
			field, _ := raw.Properties["field"].(string)
			delta, _ := raw.Properties["delta"].(string)
			messageID, _ := raw.Properties["messageID"].(string)
			partID, _ := raw.Properties["partID"].(string)

			eventName := "agent.message_chunk"
			if field == "reasoning" {
				eventName = "agent.thought_chunk"
			}
			emit(events.Event{
				Event:     eventName,
				SessionID: sid,
				Fields: map[string]any{
					"delta":      delta,
					"message_id": messageID,
					"part_id":    partID,
				},
			})

		case "session.status":
			status, _ := raw.Properties["status"].(map[string]any)
			if status == nil {
				continue
			}
			statusType, _ := status["type"].(string)
			switch statusType {
			case "busy":
				sessionBusy[sid] = true
			case "idle":
				if sessionBusy[sid] {
					delete(sessionBusy, sid)
					// Emit session.end with appropriate stop_reason.
					stopReason := "end_turn"
					if errName, ok := sessionErrors[sid]; ok {
						delete(sessionErrors, sid)
						stopReason = mapErrorToStopReason(errName)
					}
					emit(events.Event{
						Event:     "session.end",
						SessionID: sid,
						Fields: map[string]any{
							"stop_reason": stopReason,
						},
					})
				}
			}

		case "session.error":
			errObj, _ := raw.Properties["error"].(map[string]any)
			if errObj != nil {
				errName, _ := errObj["name"].(string)
				sessionErrors[sid] = errName
			}

		case "permission.request":
			if sid == "" {
				continue
			}
			// Emit permission.request with normalized field names.
			fields := map[string]any{}
			for k, v := range raw.Properties {
				if k == "sessionID" {
					continue
				}
				// Map camelCase to snake_case for known fields.
				switch k {
				case "requestID":
					fields["request_id"] = v
				default:
					fields[k] = v
				}
			}
			// If request_id wasn't in properties, try the raw event id as fallback.
			if _, ok := fields["request_id"]; !ok && raw.ID != "" {
				fields["request_id"] = raw.ID
			}
			emit(events.Event{
				Event:     "permission.request",
				SessionID: sid,
				Fields:    fields,
			})

		case "permission.asked":
			if sid == "" {
				continue
			}
			fields := map[string]any{}
			for k, v := range raw.Properties {
				if k == "sessionID" {
					continue
				}
				fields[k] = v
			}
			requestID := raw.ID
			if id, _ := raw.Properties["id"].(string); id != "" {
				requestID = id
			}
			if requestID == "" {
				continue
			}
			fields["request_id"] = requestID
			fields["tool"] = "permission." + firstString(raw.Properties["permission"], "asked")
			fields["options"] = permissionAskedOptions(requestID, raw.Properties["always"])
			emit(events.Event{
				Event:     "permission.request",
				SessionID: sid,
				Fields:    fields,
			})

		case "server.connected", "server.heartbeat",
			"session.diff", "session.updated", "session.idle":
			// Known idle events — silently skip.

		default:
			// Passthrough unknown events for debugging.
			emit(events.Event{
				Event:     "http." + raw.Type,
				SessionID: sid,
				Fields:    raw.Properties,
			})
		}
	}
}

func permissionAskedOptions(requestID string, alwaysPatterns any) []any {
	options := []any{
		map[string]any{"optionId": "reject", "kind": "reject"},
		map[string]any{"optionId": "allow_once", "kind": "allow_once"},
		// Backwards-friendly alias for users who copied the permission id from
		// the raw OpenCode event before Avenor normalized it.
		map[string]any{"optionId": requestID, "kind": "allow_once"},
	}
	if patterns, ok := alwaysPatterns.([]any); ok && len(patterns) > 0 {
		options = append(options, map[string]any{"optionId": "allow_always", "kind": "allow_always"})
	}
	return options
}

func firstString(v any, fallback string) string {
	if s, _ := v.(string); s != "" {
		return s
	}
	return fallback
}

// mapErrorToStopReason converts an opencode error name to an avenor stop_reason.
func mapErrorToStopReason(errName string) string {
	switch errName {
	case "MessageAbortedError":
		return "cancelled"
	default:
		return errName
	}
}
