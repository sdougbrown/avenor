package pi

import (
	"github.com/sdougbrown/avenor/internal/events"
)

func translateNotification(payload map[string]any, sessionID string) *events.Event {
	evtType, _ := payload["type"].(string)

	switch evtType {
	case "agent_start":
		return &events.Event{Event: "session.start", SessionID: sessionID, Fields: map[string]any{}}
	case "agent_end":
		return translateAgentEnd(payload, sessionID)
	case "turn_start":
		return &events.Event{Event: "avenor.turn.start", SessionID: sessionID, Fields: map[string]any{}}
	case "turn_end":
		return &events.Event{Event: "avenor.turn.end", SessionID: sessionID, Fields: map[string]any{}}
	case "message_start":
		return &events.Event{Event: "avenor.message.start", SessionID: sessionID, Fields: map[string]any{}}
	case "message_end":
		return &events.Event{Event: "avenor.message.end", SessionID: sessionID, Fields: map[string]any{}}
	case "message_update":
		return translateMessageUpdate(payload, sessionID)
	case "tool_execution_start":
		return &events.Event{Event: "avenor.tool.start", SessionID: sessionID, Fields: payload}
	case "tool_execution_end":
		return &events.Event{Event: "avenor.tool.end", SessionID: sessionID, Fields: payload}
	case "tool_execution_update":
		return &events.Event{Event: "avenor.tool.update", SessionID: sessionID, Fields: payload}
	case "compaction_start":
		return &events.Event{Event: "avenor.compaction.start", SessionID: sessionID, Fields: payload}
	case "compaction_end":
		return &events.Event{Event: "avenor.compaction.end", SessionID: sessionID, Fields: payload}
	case "queue_update":
		return &events.Event{Event: "avenor.queue.update", SessionID: sessionID, Fields: payload}
	case "auto_retry_start":
		return &events.Event{Event: "avenor.auto_retry.start", SessionID: sessionID, Fields: payload}
	case "auto_retry_end":
		return &events.Event{Event: "avenor.auto_retry.end", SessionID: sessionID, Fields: payload}
	case "extension_error":
		return &events.Event{Event: "avenor.extension.error", SessionID: sessionID, Fields: payload}
	default:
		return &events.Event{Event: "avenor." + evtType, SessionID: sessionID, Fields: payload}
	}
}

func translateAgentEnd(payload map[string]any, sessionID string) *events.Event {
	fields := map[string]any{}

	if messages, ok := payload["messages"]; ok {
		if msgArr, ok := messages.([]any); ok && len(msgArr) > 0 {
			lastMsg, _ := msgArr[len(msgArr)-1].(map[string]any)
			if lastMsg != nil {
				if stopReason, ok := lastMsg["stop_reason"]; ok {
					fields["stop_reason"] = stopReason
				}
			}
		}
	}

	return &events.Event{Event: "session.end", SessionID: sessionID, Fields: fields}
}

func translateMessageUpdate(payload map[string]any, sessionID string) *events.Event {
	if amEvt, ok := payload["assistantMessageEvent"]; ok {
		if amMap, ok := amEvt.(map[string]any); ok {
			evtType, _ := amMap["type"].(string)
			if evtType == "text_delta" {
				fields := map[string]any{}
				if data, ok := amMap["data"]; ok {
					if dataMap, ok := data.(map[string]any); ok {
						if text, ok := dataMap["text"].(string); ok {
							fields["text"] = text
						}
					}
				}
				return &events.Event{Event: "avenor.message.delta", SessionID: sessionID, Fields: fields}
			}
		}
	}
	return &events.Event{Event: "avenor.message.update", SessionID: sessionID, Fields: payload}
}

var dialogMethods = map[string]bool{
	"select":   true,
	"confirm":  true,
	"input":    true,
	"editor":   true,
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
