package pi

import (
	"strings"

	"github.com/sdougbrown/avenor/internal/events"
)

func translateNotification(payload map[string]any, sessionID string) []events.Event {
	evtType, _ := payload["type"].(string)

	switch evtType {
	case "agent_start":
		return []events.Event{{Event: "session.start", SessionID: sessionID, Fields: map[string]any{}}}
	case "agent_end":
		return []events.Event{translateAgentEnd(payload, sessionID)}
	case "turn_start":
		return []events.Event{{Event: "avenor.turn.start", SessionID: sessionID, Fields: map[string]any{}}}
	case "turn_end":
		return []events.Event{{Event: "avenor.turn.end", SessionID: sessionID, Fields: map[string]any{}}}
	case "message_start":
		return []events.Event{{Event: "avenor.message.start", SessionID: sessionID, Fields: map[string]any{}}}
	case "message_end":
		return []events.Event{{Event: "avenor.message.end", SessionID: sessionID, Fields: map[string]any{}}}
	case "message_update":
		return translateMessageUpdate(payload, sessionID)
	case "tool_execution_start":
		return []events.Event{canonicalToolCall("tool.call", sessionID, payload, "running"), events.Event{Event: "avenor.tool.start", SessionID: sessionID, Fields: payload}}
	case "tool_execution_end":
		return []events.Event{canonicalToolCall("tool.call_update", sessionID, payload, "completed"), events.Event{Event: "avenor.tool.end", SessionID: sessionID, Fields: payload}}
	case "tool_execution_update":
		return []events.Event{canonicalToolCall("tool.call_update", sessionID, payload, stringValue(payload["status"])), events.Event{Event: "avenor.tool.update", SessionID: sessionID, Fields: payload}}
	case "compaction_start":
		return []events.Event{{Event: "avenor.compaction.start", SessionID: sessionID, Fields: payload}}
	case "compaction_end":
		return []events.Event{{Event: "avenor.compaction.end", SessionID: sessionID, Fields: payload}}
	case "queue_update":
		return []events.Event{{Event: "avenor.queue.update", SessionID: sessionID, Fields: payload}}
	case "auto_retry_start":
		return []events.Event{{Event: "avenor.auto_retry.start", SessionID: sessionID, Fields: payload}}
	case "auto_retry_end":
		return []events.Event{{Event: "avenor.auto_retry.end", SessionID: sessionID, Fields: payload}}
	case "extension_error":
		return []events.Event{{Event: "avenor.extension.error", SessionID: sessionID, Fields: payload}}
	default:
		return []events.Event{{Event: "avenor." + evtType, SessionID: sessionID, Fields: payload}}
	}
}

func translateAgentEnd(payload map[string]any, sessionID string) events.Event {
	fields := map[string]any{}
	if messages, ok := payload["messages"].([]any); ok && len(messages) > 0 {
		lastMsg, _ := messages[len(messages)-1].(map[string]any)
		if lastMsg != nil {
			if stopReason, ok := lastMsg["stop_reason"]; ok {
				fields["stop_reason"] = stopReason
			}
			if finalOutput := extractPiText(lastMsg); finalOutput != "" {
				fields["final_output"] = finalOutput
			}
		}
	}
	if _, ok := fields["stop_reason"]; !ok {
		if stopReason, _ := payload["stop_reason"].(string); stopReason != "" {
			fields["stop_reason"] = stopReason
		}
	}
	return events.Event{Event: "session.end", SessionID: sessionID, Fields: fields}
}

func translateMessageUpdate(payload map[string]any, sessionID string) []events.Event {
	alias := events.Event{Event: "avenor.message.update", SessionID: sessionID, Fields: payload}
	amMap, _ := payload["assistantMessageEvent"].(map[string]any)
	if amMap == nil {
		return []events.Event{alias}
	}

	evtType, _ := amMap["type"].(string)
	text := extractPiText(amMap)
	switch evtType {
	case "text_delta":
		if text == "" {
			return []events.Event{alias}
		}
		return []events.Event{
			canonicalChunk("agent.message_chunk", sessionID, text),
			events.Event{Event: "avenor.message.delta", SessionID: sessionID, Fields: map[string]any{"text": text}},
		}
	case "thinking_delta":
		if text == "" {
			return []events.Event{alias}
		}
		return []events.Event{canonicalChunk("agent.thought_chunk", sessionID, text), alias}
	case "toolcall_start":
		return []events.Event{canonicalToolCall("tool.call", sessionID, amMap, "running"), alias}
	case "toolcall_delta", "toolcall_update", "toolcall_end":
		status := "running"
		if evtType == "toolcall_end" {
			status = "completed"
		}
		return []events.Event{canonicalToolCall("tool.call_update", sessionID, amMap, status), alias}
	default:
		if text != "" {
			return []events.Event{canonicalChunk("agent.message_chunk", sessionID, text), alias}
		}
		return []events.Event{alias}
	}
}

func canonicalChunk(eventName, sessionID, text string) events.Event {
	return events.Event{
		Event:     eventName,
		SessionID: sessionID,
		Fields: map[string]any{
			"delta": text,
			"content": map[string]any{
				"type": "text",
				"text": text,
			},
		},
	}
}

func canonicalToolCall(eventName, sessionID string, payload map[string]any, fallbackStatus string) events.Event {
	fields := map[string]any{}
	for k, v := range payload {
		fields[k] = v
	}
	if kind := firstNonEmptyString(fields, "kind", "toolKind"); kind != "" {
		fields["kind"] = kind
	} else {
		fields["kind"] = "tool"
	}
	if title := firstNonEmptyString(fields, "title", "toolName", "name"); title != "" {
		fields["title"] = title
	}
	if status := firstNonEmptyString(fields, "status", "state"); status != "" {
		fields["status"] = status
	} else if fallbackStatus != "" {
		fields["status"] = fallbackStatus
	}
	return events.Event{Event: eventName, SessionID: sessionID, Fields: fields}
}

func firstNonEmptyString(fields map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, _ := fields[key].(string); value != "" {
			return value
		}
	}
	return ""
}

func stringValue(v any) string {
	s, _ := v.(string)
	return s
}

func extractPiText(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case map[string]any:
		if text, _ := x["text"].(string); text != "" {
			return text
		}
		if delta, _ := x["delta"].(string); delta != "" {
			return delta
		}
		for _, key := range []string{"data", "content", "partial", "assistantMessage", "message"} {
			if text := extractPiText(x[key]); text != "" {
				return text
			}
		}
	case []any:
		parts := make([]string, 0, len(x))
		for _, item := range x {
			if part := extractPiText(item); part != "" {
				parts = append(parts, part)
			}
		}
		return strings.Join(parts, "")
	}
	return ""
}

var dialogMethods = map[string]bool{
	"select":  true,
	"confirm": true,
	"input":   true,
	"editor":  true,
}

func translateExtensionUI(payload map[string]any, sessionID string) (*events.Event, string) {
	method, _ := payload["method"].(string)
	title, _ := payload["title"].(string)

	switch method {
	case "select":
		var rawOptions []any
		if opts := payload["options"]; opts != nil {
			optsArr, ok := opts.([]any)
			if !ok {
				return nil, method
			}
			rawOptions = optsArr
		}
		optList := make([]any, len(rawOptions))
		for i, opt := range rawOptions {
			optStr, _ := opt.(string)
			optList[i] = map[string]any{
				"optionId": optStr,
				"kind":     "allow",
			}
		}
		if len(optList) > 1 {
			for i := 1; i < len(optList); i++ {
				if m, ok := optList[i].(map[string]any); ok {
					m["kind"] = "reject"
				}
			}
		}
		return &events.Event{
			Event:     "permission.request",
			SessionID: sessionID,
			Fields: map[string]any{
				"kind":        "command",
				"description": title,
				"options":     optList,
			},
		}, method
	case "confirm":
		msg, _ := payload["message"].(string)
		if msg == "" {
			msg = title
		}
		return &events.Event{
			Event:     "permission.request",
			SessionID: sessionID,
			Fields: map[string]any{
				"kind":        "confirm",
				"description": msg,
				"options": []any{
					map[string]any{"optionId": "yes", "kind": "allow"},
					map[string]any{"optionId": "no", "kind": "reject"},
				},
			},
		}, method
	case "input", "editor":
		defVal, _ := payload["default"].(string)
		fields := map[string]any{
			"kind":        "input",
			"description": title,
		}
		if defVal != "" {
			fields["default"] = defVal
		}
		return &events.Event{
			Event:     "permission.request",
			SessionID: sessionID,
			Fields:    fields,
		}, method
	default:
		return nil, method
	}
}
