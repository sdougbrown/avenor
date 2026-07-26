package pi

import (
	"encoding/json"
	"fmt"
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
	amMap, _ := payload["assistantMessageEvent"].(map[string]any)
	if amMap == nil {
		return []events.Event{{
			Event:     "avenor.message.update",
			SessionID: sessionID,
			Fields:    map[string]any{"type": "message_update"},
		}}
	}

	// Pi's message_update payload contains both the entire cumulative message
	// and a partial snapshot on every streamed fragment. Some providers also
	// attach large opaque reasoning signatures to every snapshot. Persisting the
	// raw payload makes tool arguments grow quadratically and can duplicate the
	// same signature three times across the canonical and compatibility events.
	// Keep only the incremental data and tool identity needed by consumers.
	alias := compactMessageUpdateAlias(amMap, sessionID)
	evtType, _ := amMap["type"].(string)

	switch evtType {
	case "text_delta":
		text := extractPiDeltaText(amMap)
		if text == "" {
			return []events.Event{alias}
		}
		return []events.Event{
			canonicalChunk("agent.message_chunk", sessionID, text),
			{Event: "avenor.message.delta", SessionID: sessionID, Fields: map[string]any{"text": text}},
		}
	case "thinking_delta":
		text := extractPiDeltaText(amMap)
		if text == "" {
			return []events.Event{alias}
		}
		return []events.Event{canonicalChunk("agent.thought_chunk", sessionID, text), alias}
	case "toolcall_start":
		return []events.Event{canonicalMessageToolCall("tool.call", sessionID, amMap, "running"), alias}
	case "toolcall_delta", "toolcall_update", "toolcall_end":
		status := "running"
		if evtType == "toolcall_end" {
			status = "completed"
		}
		return []events.Event{canonicalMessageToolCall("tool.call_update", sessionID, amMap, status), alias}
	default:
		// Start/end notifications carry cumulative content, not deltas. The
		// corresponding streamed deltas already produced canonical chunks.
		return []events.Event{alias}
	}
}

func compactMessageUpdateAlias(amMap map[string]any, sessionID string) events.Event {
	return events.Event{
		Event:     "avenor.message.update",
		SessionID: sessionID,
		Fields: map[string]any{
			"type":                  "message_update",
			"assistantMessageEvent": compactAssistantMessageEvent(amMap),
		},
	}
}

func compactAssistantMessageEvent(amMap map[string]any) map[string]any {
	fields := map[string]any{}
	if evtType, _ := amMap["type"].(string); evtType != "" {
		fields["type"] = evtType
	}
	if contentIndex, ok := events.Int64(amMap["contentIndex"]); ok {
		fields["contentIndex"] = contentIndex
	}
	if delta := extractPiDeltaText(amMap); delta != "" {
		fields["delta"] = delta
	}
	for _, key := range []string{"reason", "stopReason"} {
		if value, _ := amMap[key].(string); value != "" {
			fields[key] = value
		}
	}

	toolCall := assistantToolCall(amMap)
	toolCallID := firstNonEmptyString(amMap, "toolCallId", "tool_call_id")
	toolName := firstNonEmptyString(amMap, "toolName", "name", "title")
	if toolCall != nil {
		if toolCallID == "" {
			toolCallID = firstNonEmptyString(toolCall, "id", "toolCallId", "tool_call_id")
		}
		if toolName == "" {
			toolName = firstNonEmptyString(toolCall, "name", "toolName", "title")
		}
	}
	if toolCallID != "" {
		fields["toolCallId"] = toolCallID
	}
	if toolName != "" {
		fields["toolName"] = toolName
		fields["title"] = toolName
	}
	if toolCallID != "" || toolName != "" {
		fields["kind"] = "tool"
		compactToolCall := map[string]any{}
		if toolCallID != "" {
			compactToolCall["id"] = toolCallID
		}
		if toolName != "" {
			compactToolCall["name"] = toolName
		}
		fields["toolCall"] = compactToolCall
	}
	return fields
}

func canonicalMessageToolCall(eventName, sessionID string, amMap map[string]any, status string) events.Event {
	compact := compactAssistantMessageEvent(amMap)
	fields := map[string]any{
		"kind":   "tool",
		"status": status,
	}
	for _, key := range []string{"toolCallId", "toolName", "title", "delta"} {
		if value, ok := compact[key]; ok {
			fields[key] = value
		}
	}
	if evtType, _ := amMap["type"].(string); evtType == "toolcall_end" {
		if toolCall := assistantToolCall(amMap); toolCall != nil {
			if args, ok := toolCall["arguments"]; ok {
				fields["rawInput"] = args
			}
		}
	}
	return events.Event{Event: eventName, SessionID: sessionID, Fields: fields}
}

func assistantToolCall(amMap map[string]any) map[string]any {
	if toolCall, _ := amMap["toolCall"].(map[string]any); toolCall != nil {
		return toolCall
	}
	partial, _ := amMap["partial"].(map[string]any)
	if partial == nil {
		return nil
	}
	content, _ := partial["content"].([]any)
	contentIndex, ok := events.Int64(amMap["contentIndex"])
	if !ok || contentIndex < 0 || contentIndex >= int64(len(content)) {
		return nil
	}
	toolCall, _ := content[contentIndex].(map[string]any)
	return toolCall
}

func extractPiDeltaText(amMap map[string]any) string {
	if delta, _ := amMap["delta"].(string); delta != "" {
		return delta
	}
	if text, _ := amMap["text"].(string); text != "" {
		return text
	}
	if data, _ := amMap["data"].(map[string]any); data != nil {
		if delta, _ := data["delta"].(string); delta != "" {
			return delta
		}
		if text, _ := data["text"].(string); text != "" {
			return text
		}
	}
	return ""
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

// copyPassthroughFields copies payload fields into a new map, excluding keys
// that are handled explicitly by the caller. This preserves any extra
// metadata the backend sends (e.g. command, args, cwd) so downstream
// consumers and the supervisor can surface it.
func copyPassthroughFields(payload map[string]any, exclude ...string) map[string]any {
	skip := make(map[string]bool, len(exclude))
	for _, k := range exclude {
		skip[k] = true
	}
	out := make(map[string]any, len(payload))
	for k, v := range payload {
		if skip[k] {
			continue
		}
		out[k] = v
	}
	return out
}

// copyMap creates a shallow copy of a map. Used to decouple stored tool
// payloads from event maps that are fanned out to subscribers.
func copyMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// extractToolCommand attempts to extract a command string from a
// tool_execution_start payload. The pi backend tool payloads may carry the
// command in various fields depending on the tool type (bash, exec, etc.).
func extractToolCommand(payload map[string]any) string {
	// Direct "command" field.
	if cmd, _ := payload["command"].(string); cmd != "" {
		return cmd
	}
	// "input" as a string (some tools put the raw command there).
	if cmd, _ := payload["input"].(string); cmd != "" {
		return cmd
	}
	// "input" as a map — try common sub-keys.
	if inputMap, ok := payload["input"].(map[string]any); ok {
		for _, key := range []string{"command", "cmd", "script", "expression", "code"} {
			if cmd, _ := inputMap[key].(string); cmd != "" {
				return cmd
			}
		}
	}
	// "args" as a string or string slice joined.
	if args, ok := payload["args"].(string); ok && args != "" {
		return args
	}
	if argsArr, ok := payload["args"].([]any); ok && len(argsArr) > 0 {
		parts := make([]string, 0, len(argsArr))
		for _, a := range argsArr {
			if s, _ := a.(string); s != "" {
				parts = append(parts, s)
			}
		}
		if len(parts) > 0 {
			return strings.Join(parts, " ")
		}
	}
	return ""
}

// extractToolInput returns a string representation of the tool input
// payload, useful as a fallback when a specific command field is not found.
// The output is capped at 4KB to prevent large tool inputs from bloating
// permission event metadata. Returns an error if JSON marshaling fails.
func extractToolInput(payload map[string]any) (string, error) {
	if input, ok := payload["input"].(string); ok && input != "" {
		return truncateInput(input), nil
	}
	if inputMap, ok := payload["input"].(map[string]any); ok && len(inputMap) > 0 {
		b, err := json.Marshal(inputMap)
		if err != nil {
			return "", fmt.Errorf("marshal tool input: %w", err)
		}
		return truncateInput(string(b)), nil
	}
	return "", nil
}

const maxToolInputLen = 4096

func truncateInput(s string) string {
	if len(s) <= maxToolInputLen {
		return s
	}
	return s[:maxToolInputLen] + "...[truncated]"
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
		// Start with passthrough of any extra fields the backend provides,
		// then layer normalized fields on top.
		fields := copyPassthroughFields(payload, "type", "id", "method", "title", "options")
		fields["kind"] = "command"
		fields["description"] = title
		fields["options"] = optList
		return &events.Event{
			Event:     "permission.request",
			SessionID: sessionID,
			Fields:    fields,
		}, method
	case "confirm":
		msg, _ := payload["message"].(string)
		if msg == "" {
			msg = title
		}
		fields := copyPassthroughFields(payload, "type", "id", "method", "title", "message")
		fields["kind"] = "confirm"
		fields["description"] = msg
		fields["options"] = []any{
			map[string]any{"optionId": "yes", "kind": "allow"},
			map[string]any{"optionId": "no", "kind": "reject"},
		}
		return &events.Event{
			Event:     "permission.request",
			SessionID: sessionID,
			Fields:    fields,
		}, method
	case "input", "editor":
		defVal, _ := payload["default"].(string)
		fields := copyPassthroughFields(payload, "type", "id", "method", "title", "default")
		fields["kind"] = "input"
		fields["description"] = title
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
