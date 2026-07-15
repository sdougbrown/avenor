package codexappserver

import (
	"encoding/json"
	"strings"

	"github.com/sdougbrown/avenor/internal/events"
)

// translateNotification converts an app-server notification to Avenor events.
// Unknown or malformed notifications are silently dropped.
func translateNotification(method string, params json.RawMessage) []events.Event {
	switch method {
	case "turn/completed":
		if ev := translateTurnCompleted(params); ev != nil {
			return []events.Event{*ev}
		}
		return nil
	case "turn/started":
		if ev := informationalEvent("avenor.turn.start", params); ev != nil {
			return []events.Event{*ev}
		}
		return nil
	case "item/started":
		return translateItemNotification(params, true)
	case "item/delta", "item/updated":
		return translateItemDelta(params)
	case "item/completed":
		return translateItemNotification(params, false)
	default:
		return nil
	}
}

// translateApprovalRequest converts a server-initiated approval request to
// a permission.request event. Returns the parsed params for diagnostics.
func translateApprovalRequest(method string, params json.RawMessage) (*events.Event, *approvalRequestParams) {
	kind, ok := approvalKind(method)
	if !ok {
		return nil, nil
	}
	if params == nil {
		return nil, nil
	}
	var ar approvalRequestParams
	if err := json.Unmarshal(params, &ar); err != nil {
		return nil, nil
	}

	description := approvalDescription(kind, ar)

	fields := map[string]any{
		"kind":        kind,
		"description": description,
		"tool":        method,
		"options": []any{
			map[string]any{"optionId": "reject", "kind": "reject"},
			map[string]any{"optionId": "allow", "kind": "allow"},
		},
	}
	if ar.Command != "" {
		fields["command"] = ar.Command
	}
	if ar.Path != "" {
		fields["path"] = ar.Path
	}
	if ar.Summary != "" {
		fields["summary"] = ar.Summary
	}
	if ar.Message != "" {
		fields["message"] = ar.Message
	}

	return &events.Event{
		Event:     "permission.request",
		SessionID: ar.ThreadID,
		Fields:    fields,
	}, &ar
}

func translateTurnCompleted(params json.RawMessage) *events.Event {
	var n turnCompletedNotification
	if err := json.Unmarshal(params, &n); err != nil {
		return nil
	}

	fields := map[string]any{
		"turn_id": n.Turn.ID,
		"reason":  n.Turn.Status,
	}

	switch n.Turn.Status {
	case "completed":
		fields["stop_reason"] = "end_turn"
	case "interrupted":
		fields["stop_reason"] = "cancelled"
	case "failed":
		fields["stop_reason"] = "error"
		if n.Turn.Error != nil {
			fields["error_message"] = n.Turn.Error.Message
		}
	default:
		return nil
	}

	return &events.Event{
		Event:     "session.end",
		SessionID: n.ThreadID,
		Fields:    fields,
	}
}

func informationalEvent(eventType string, params json.RawMessage) *events.Event {
	var n itemNotification
	if err := json.Unmarshal(params, &n); err != nil {
		return nil
	}
	return &events.Event{
		Event:     eventType,
		SessionID: n.ThreadID,
		Fields: map[string]any{
			"turn_id": n.TurnID,
		},
	}
}

type itemDescriptor struct {
	threadID string
	turnID   string
	itemID   string
	kind     string
	text     string
	title    string
	status   string
	raw      map[string]any
}

func translateItemNotification(params json.RawMessage, started bool) []events.Event {
	desc := parseItemDescriptor(params)
	if desc == nil {
		return nil
	}
	var out []events.Event
	if desc.kind == "tool" {
		status := desc.status
		if status == "" {
			if started {
				status = "running"
			} else {
				status = "completed"
			}
		}
		name := "tool.call"
		if !started {
			name = "tool.call_update"
		}
		out = append(out, canonicalToolEvent(name, desc, status))
	} else if desc.text != "" && !started {
		out = append(out, canonicalTextEvent(desc))
	}
	alias := "avenor.item.start"
	if !started {
		alias = "avenor.item.end"
	}
	out = append(out, events.Event{Event: alias, SessionID: desc.threadID, Fields: map[string]any{"turn_id": desc.turnID, "item_id": desc.itemID, "item_kind": desc.kind}})
	return out
}

func translateItemDelta(params json.RawMessage) []events.Event {
	desc := parseItemDescriptor(params)
	if desc == nil || desc.text == "" {
		return nil
	}
	switch desc.kind {
	case "assistant", "message":
		return []events.Event{canonicalTextEvent(desc)}
	case "reasoning":
		return []events.Event{canonicalReasoningEvent(desc)}
	case "tool":
		return []events.Event{canonicalToolEvent("tool.call_update", desc, firstNonEmpty(desc.status, "running"))}
	default:
		return nil
	}
}

func parseItemDescriptor(params json.RawMessage) *itemDescriptor {
	var raw map[string]any
	if err := json.Unmarshal(params, &raw); err != nil {
		return nil
	}
	threadID, _ := raw["threadId"].(string)
	if threadID == "" {
		return nil
	}
	turnID, _ := raw["turnId"].(string)
	itemID, _ := raw["itemId"].(string)
	item, _ := raw["item"].(map[string]any)
	if item == nil {
		item = raw
	}
	kind := normalizeItemKind(firstNonEmpty(stringFrom(item["kind"]), stringFrom(item["type"]), stringFrom(raw["kind"]), stringFrom(raw["type"])))
	status := firstNonEmpty(stringFrom(item["status"]), stringFrom(item["state"]), stringFrom(raw["status"]), stringFrom(raw["state"]))
	title := firstNonEmpty(stringFrom(item["title"]), stringFrom(item["name"]), stringFrom(item["toolName"]), stringFrom(raw["title"]), stringFrom(raw["name"]), stringFrom(raw["toolName"]))
	text := firstNonEmpty(
		extractItemText(item["delta"]),
		extractItemText(raw["delta"]),
		extractItemText(item["content"]),
		extractItemText(item["text"]),
		extractItemText(raw["text"]),
		extractItemText(raw["content"]),
	)
	if itemID == "" {
		itemID = firstNonEmpty(stringFrom(item["id"]), stringFrom(raw["id"]))
	}
	return &itemDescriptor{threadID: threadID, turnID: turnID, itemID: itemID, kind: kind, text: text, title: title, status: status, raw: raw}
}

func canonicalTextEvent(desc *itemDescriptor) events.Event {
	return events.Event{Event: "agent.message_chunk", SessionID: desc.threadID, Fields: map[string]any{"delta": desc.text, "item_id": desc.itemID, "turn_id": desc.turnID}}
}

func canonicalReasoningEvent(desc *itemDescriptor) events.Event {
	return events.Event{Event: "agent.thought_chunk", SessionID: desc.threadID, Fields: map[string]any{"delta": desc.text, "item_id": desc.itemID, "turn_id": desc.turnID}}
}

func canonicalToolEvent(eventName string, desc *itemDescriptor, fallbackStatus string) events.Event {
	fields := map[string]any{"item_id": desc.itemID, "turn_id": desc.turnID, "kind": "tool"}
	if desc.title != "" {
		fields["title"] = desc.title
	}
	if desc.text != "" {
		fields["delta"] = desc.text
	}
	if desc.status != "" {
		fields["status"] = desc.status
	} else if fallbackStatus != "" {
		fields["status"] = fallbackStatus
	}
	return events.Event{Event: eventName, SessionID: desc.threadID, Fields: fields}
}

func normalizeItemKind(kind string) string {
	kind = strings.ToLower(kind)
	switch {
	case strings.Contains(kind, "reason"):
		return "reasoning"
	case strings.Contains(kind, "tool"), strings.Contains(kind, "function"):
		return "tool"
	case strings.Contains(kind, "assistant"), strings.Contains(kind, "message"):
		return "assistant"
	default:
		return kind
	}
}

func extractItemText(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case map[string]any:
		for _, key := range []string{"text", "delta", "content", "message", "arguments", "output"} {
			if text := extractItemText(x[key]); text != "" {
				return text
			}
		}
	case []any:
		parts := make([]string, 0, len(x))
		for _, item := range x {
			if text := extractItemText(item); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "")
	}
	return ""
}

func stringFrom(v any) string {
	s, _ := v.(string)
	return s
}

func approvalKind(method string) (string, bool) {
	switch method {
	case "item/commandExecution/requestApproval":
		return "command", true
	case "item/fileChange/requestApproval":
		return "file", true
	case "item/permissions/requestApproval":
		return "permissions", true
	default:
		return "", false
	}
}

func approvalDescription(kind string, ar approvalRequestParams) string {
	switch kind {
	case "command":
		return firstNonEmpty(ar.Command, ar.Summary, ar.Message)
	case "file":
		return firstNonEmpty(ar.Path, ar.Summary, ar.Message)
	case "permissions":
		return firstNonEmpty(ar.Summary, ar.Message)
	default:
		return ""
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
