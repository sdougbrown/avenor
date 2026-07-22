package agy

import (
	"encoding/json"

	"github.com/sdougbrown/avenor/internal/events"
)

// BackendID is the canonical backend identifier.
const BackendID = "agy"

// translateEvent converts a raw agy stream-json map to zero or more Avenor events.
// sessionID is the provider session ID.
// isFirstInit is true for the first init event in a session (triggers a cached
// session.start).
// initCache holds the first init metadata across calls.
// Returns events and an updated initCache.
func translateEvent(payload map[string]any, sessionID string, isFirstInit bool, initCache map[string]any) ([]events.Event, map[string]any) {
	if initCache == nil {
		initCache = make(map[string]any)
	}

	typ, _ := payload["event"].(string)

	switch typ {
	case "init":
		return translateInit(payload, sessionID, isFirstInit, initCache)
	case "step_update":
		return translateStepUpdate(payload, sessionID, initCache)
	case "result":
		return translateResult(payload, sessionID, initCache)
	default:
		return translateUnknown(payload, sessionID, initCache)
	}
}

// translateInit handles agy "init" events. On the first init it caches
// metadata and emits a session.start; subsequent inits are silently cached.
func translateInit(payload map[string]any, sessionID string, isFirstInit bool, initCache map[string]any) ([]events.Event, map[string]any) {
	conversationID, _ := payload["conversation_id"].(string)
	// agy 1.1.5 does not emit model or agy_version in the init event.
	model := ""
	agyVersion := ""

	// Already cached this conversation → no-op.
	if prev, ok := initCache["conversation_id"].(string); ok && prev == conversationID {
		return nil, initCache
	}

	// Only cache the very first init's metadata; never overwrite.
	if _, ok := initCache["conversation_id"].(string); !ok {
		initCache["conversation_id"] = conversationID
		initCache["model"] = model
		initCache["agy_version"] = agyVersion
		initCache["backend"] = BackendID
	}

	if !isFirstInit {
		return nil, initCache
	}

	return []events.Event{{
		Event:     "session.start",
		SessionID: sessionID,
		Fields: map[string]any{
			"backend":         BackendID,
			"model":           model,
			"agy_version":     agyVersion,
			"conversation_id": conversationID,
		},
	}}, initCache
}

// translateStepUpdate handles agy "step_update" events for both agent
// responses and tool calls.
func translateStepUpdate(payload map[string]any, sessionID string, initCache map[string]any) ([]events.Event, map[string]any) {
	data, ok := payload["step_update"].(map[string]any)
	if !ok {
		return translateUnknown(payload, sessionID, initCache)
	}

	stepType, _ := data["step_type"].(string)

	switch stepType {
	case "agent_response":
		return translateAgentResponse(data, sessionID), initCache
	case "tool":
		return translateToolStep(data, sessionID), initCache
	default:
		return translateUnknown(payload, sessionID, initCache)
	}
}

// translateAgentResponse emits message_chunk and delta events for agent
// response step_updates.
func translateAgentResponse(data map[string]any, sessionID string) []events.Event {
	textDelta, _ := data["text_delta"].(string)
	stepIndex := toInt64(data["step_index"])
	state, _ := data["state"].(string)

	var out []events.Event

	if state == "ACTIVE" && textDelta != "" {
		out = append(out, events.Event{
			Event:     "agent.message_chunk",
			SessionID: sessionID,
			Fields: map[string]any{
				"delta":      textDelta,
				"step_index": stepIndex,
				"step_state": state,
			},
		})
		out = append(out, events.Event{
			Event:     "avenor.message.delta",
			SessionID: sessionID,
			Fields: map[string]any{
				"delta":      textDelta,
				"step_index": stepIndex,
			},
		})
	}

	if state == "DONE" || state == "ERROR" {
		out = append(out, events.Event{
			Event:     "avenor.message.update",
			SessionID: sessionID,
			Fields: map[string]any{
				"step_index":            stepIndex,
				"step_state":            state,
				"assistantMessageEvent": map[string]any{"type": "step_done"},
			},
		})
	}

	return out
}

// translateToolStep emits tool.call and tool.call_update events.
func translateToolStep(data map[string]any, sessionID string) []events.Event {
	toolName, _ := data["tool_name"].(string)
	stepIndex := toInt64(data["step_index"])
	state, _ := data["state"].(string)

	switch state {
	case "ACTIVE":
		fields := map[string]any{
			"title":      toolName,
			"status":     "running",
			"step_index": stepIndex,
		}
		if stepInfo, ok := data["tool_info"].(map[string]any); ok {
			if params, ok := stepInfo["parameters"].(map[string]any); ok {
				fields["parameters"] = params
			}
		}
		return []events.Event{{
			Event:     "tool.call",
			SessionID: sessionID,
			Fields:    fields,
		}}

	case "DONE":
		fields := map[string]any{
			"status":     "completed",
			"step_index": stepIndex,
			"name":       toolName,
		}
		if output, ok := data["output"].(string); ok {
			fields["output"] = output
		}
		if dur, ok := data["duration_seconds"].(float64); ok {
			fields["duration_ms"] = int64(dur * 1000)
		}
		return []events.Event{{
			Event:     "tool.call_update",
			SessionID: sessionID,
			Fields:    fields,
		}}

	case "ERROR":
		fields := map[string]any{
			"status":     "error",
			"step_index": stepIndex,
			"name":       toolName,
		}
		// Error is nested under tool_info["error"] as a map {"type":"TOOL_ERROR","message":"..."}
		if stepInfo, ok := data["tool_info"].(map[string]any); ok {
			if errObj, ok := stepInfo["error"].(map[string]any); ok {
				if errText, ok := errObj["message"].(string); ok {
					fields["error"] = errText
				}
			}
		}
		return []events.Event{{
			Event:     "tool.call_update",
			SessionID: sessionID,
			Fields:    fields,
		}}

	default:
		return []events.Event{{
			Event:     "avenor.tool.update",
			SessionID: sessionID,
			Fields: map[string]any{
				"step_index": stepIndex,
				"step_state": state,
				"name":       toolName,
			},
		}}
	}
}

// translateResult handles agy "result" events, emitting session.end.
func translateResult(payload map[string]any, sessionID string, initCache map[string]any) ([]events.Event, map[string]any) {
	data, ok := payload["result"].(map[string]any)
	if !ok {
		// Handle result event with no nested data (e.g. {"event": "result"})
		return []events.Event{{
			Event:     "session.end",
			SessionID: sessionID,
			Fields: map[string]any{
				"stop_reason": "end_turn",
			},
		}}, initCache
	}

	response, _ := data["response"].(string)
	status, _ := data["status"].(string)
	errorText, _ := data["error"].(string)

	fields := map[string]any{}

	if usage, ok := data["usage"].(map[string]any); ok {
		fields["usage"] = usage
	}

	if response != "" {
		fields["final_output"] = response
	}

	hasError := status == "ERROR" || errorText != ""

	if hasError {
		fields["error"] = errorText
	}

	stopReason := "end_turn"
	if hasError {
		stopReason = "error"
	}

	allFields := map[string]any{
		"stop_reason": stopReason,
	}
	for k, v := range fields {
		allFields[k] = v
	}

	return []events.Event{{
		Event:     "session.end",
		SessionID: sessionID,
		Fields:    allFields,
	}}, initCache
}

// translateUnknown emits an avenor.agy.* telemetry event for unknown event
// types, preventing silent drops while avoiding false lifecycle events.
func translateUnknown(payload map[string]any, sessionID string, initCache map[string]any) ([]events.Event, map[string]any) {
	typ, _ := payload["event"].(string)
	return []events.Event{{
		Event:     "avenor.agy." + typ,
		SessionID: sessionID,
		Fields:    payload,
	}}, initCache
}

// toInt64 safely converts an arbitrary value to int64.
func toInt64(v any) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case int:
		return int64(val)
	case float64:
		return int64(val)
	case json.Number:
		if i, err := val.Int64(); err == nil {
			return i
		}
	}
	return 0
}
