package acp

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/sdougbrown/avenor/internal/events"
)

var sessionUpdateEvents = map[string]string{
	"agent_message_chunk": "agent.message_chunk",
	"agent_thought_chunk": "agent.thought_chunk",
	"tool_call":           "tool.call",
	"tool_call_update":    "tool.call_update",
	"user_message_chunk":  "user.message_chunk",
	"plan":                "session.plan",
}

var knownProbeEvents = map[string]bool{
	"agent.message_chunk": true,
	"agent.thought_chunk": true,
	"tool.call":           true,
	"tool.call_update":    true,
	"user.message_chunk":  true,
	"session.plan":        true,
	"permission.request":  true,
	"session.end":          true,
}

func mapSessionUpdate(params json.RawMessage) (events.Event, error) {
	var payload map[string]any
	if err := json.Unmarshal(params, &payload); err != nil {
		return events.Event{}, err
	}

	update, ok := payload["update"].(map[string]any)
	if !ok {
		return events.Event{}, errors.New("session/update missing update object")
	}

	kind, ok := update["sessionUpdate"].(string)
	if !ok {
		return events.Event{}, errors.New("session/update missing sessionUpdate discriminator")
	}

	eventName, ok := sessionUpdateEvents[kind]
	if !ok {
		return events.Event{}, fmt.Errorf("unknown session update kind %q", kind)
	}

	sessionID, _ := payload["sessionId"].(string)
	fields := map[string]any{}
	for k, v := range update {
		if k != "sessionUpdate" {
			fields[k] = v
		}
	}
	fields["session_update"] = kind

	return events.Event{
		Event:     eventName,
		SessionID: sessionID,
		Fields:    fields,
	}, nil
}

func mapPromptResponse(sessionID string, result json.RawMessage) (events.Event, error) {
	var payload map[string]any
	if err := json.Unmarshal(result, &payload); err != nil {
		return events.Event{}, err
	}

	stopReason, _ := payload["stopReason"].(string)
	if stopReason == "" {
		stopReason, _ = payload["stop_reason"].(string)
	}
	if stopReason == "" {
		return events.Event{}, errors.New("session/prompt response missing stopReason")
	}

	fields := map[string]any{}
	for k, v := range payload {
		if k != "stopReason" && k != "stop_reason" {
			if k == "usage" {
				if usageMap, ok := v.(map[string]any); ok {
					u := map[string]any{}
					inputTokens, _ := usageMap["inputTokens"]
					if inputTokens == nil {
						inputTokens, _ = usageMap["input_tokens"]
					}
					outputTokens, _ := usageMap["outputTokens"]
					if outputTokens == nil {
						outputTokens, _ = usageMap["output_tokens"]
					}
					totalTokens, _ := usageMap["totalTokens"]
					if totalTokens == nil {
						totalTokens, _ = usageMap["total_tokens"]
					}
					cachedReadTokens, _ := usageMap["cachedReadTokens"]
					if cachedReadTokens == nil {
						cachedReadTokens, _ = usageMap["cached_read_tokens"]
					}
					if inputTokens != nil {
						u["input_tokens"] = inputTokens
					}
					if outputTokens != nil {
						u["output_tokens"] = outputTokens
					}
					if totalTokens != nil {
						u["total_tokens"] = totalTokens
					}
					if cachedReadTokens != nil {
						u["cached_read_tokens"] = cachedReadTokens
					}
					fields["usage"] = u
				}
				continue
			}
			fields[k] = v
		}
	}
	fields["stop_reason"] = stopReason

	return events.Event{
		Event:     "session.end",
		SessionID: sessionID,
		Fields:    fields,
	}, nil
}

func mapPermissionRequest(params json.RawMessage) events.Event {
	return mapPermissionRequestWithID(params, "")
}

func mapPermissionRequestWithID(params json.RawMessage, requestID string) events.Event {
	var payload map[string]any
	_ = json.Unmarshal(params, &payload)

	sessionID, _ := payload["sessionId"].(string)
	fields := map[string]any{}
	for k, v := range payload {
		if k != "sessionId" {
			fields[k] = v
		}
	}
	if requestID != "" {
		fields["request_id"] = requestID
	}

	return events.Event{
		Event:     "permission.request",
		SessionID: sessionID,
		Fields:    fields,
	}
}

func ParseTranscriptEvent(line []byte) (events.Event, error) {
	var normalized events.Event
	if err := json.Unmarshal(line, &normalized); err == nil && normalized.Event != "" {
		if !knownProbeEvents[normalized.Event] {
			return events.Event{}, fmt.Errorf("unknown event %q", normalized.Event)
		}
		return normalized, nil
	}

	var msg rpcEnvelope
	if err := json.Unmarshal(line, &msg); err != nil {
		return events.Event{}, err
	}

	switch {
	case msg.Method == "session/update":
		return mapSessionUpdate(msg.Params)
	case msg.Method == "session/request_permission":
		return mapPermissionRequest(msg.Params), nil
	case len(msg.Result) > 0:
		return mapPromptResponse("", msg.Result)
	default:
		return events.Event{}, errors.New("transcript line is not a recognized ACP event")
	}
}