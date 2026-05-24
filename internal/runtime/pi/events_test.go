package pi

import (
	"testing"
)

func TestTranslateBasicEvents(t *testing.T) {
	tests := []struct {
		typeStr string
		want    string
	}{
		{"agent_start", "session.start"},
		{"agent_end", "session.end"},
		{"turn_start", "avenor.turn.start"},
		{"turn_end", "avenor.turn.end"},
		{"message_start", "avenor.message.start"},
		{"message_end", "avenor.message.end"},
		{"tool_execution_start", "avenor.tool.start"},
		{"tool_execution_end", "avenor.tool.end"},
		{"tool_execution_update", "avenor.tool.update"},
		{"compaction_start", "avenor.compaction.start"},
		{"compaction_end", "avenor.compaction.end"},
		{"queue_update", "avenor.queue.update"},
		{"auto_retry_start", "avenor.auto_retry.start"},
		{"auto_retry_end", "avenor.auto_retry.end"},
		{"extension_error", "avenor.extension.error"},
		{"some_unknown_event", "avenor.some_unknown_event"},
	}
	for _, tt := range tests {
		t.Run(tt.typeStr, func(t *testing.T) {
			payload := map[string]any{"type": tt.typeStr}
			if tt.typeStr == "tool_execution_start" || tt.typeStr == "tool_execution_end" || tt.typeStr == "tool_execution_update" {
				payload["toolName"] = "cat"
			}
			ev := translateNotification(payload, "pi-s1")
			if ev == nil {
				t.Fatal("expected event")
			}
			if ev.Event != tt.want {
				t.Errorf("event = %q, want %q", ev.Event, tt.want)
			}
		if ev.SessionID != "pi-s1" {
			t.Errorf("sessionID = %q, want pi-s1", ev.SessionID)
		}
		if tt.typeStr == "tool_execution_start" || tt.typeStr == "tool_execution_end" || tt.typeStr == "tool_execution_update" {
			toolName, _ := ev.Fields["toolName"].(string)
			if toolName != "cat" {
				t.Errorf("toolName = %q, want cat", toolName)
			}
		}
	})
	}
}

func TestTranslateAgentEndWithStopReason(t *testing.T) {
	payload := map[string]any{
		"type": "agent_end",
		"messages": []any{
			map[string]any{
				"stop_reason": "end_of_turn",
			},
		},
	}
	ev := translateNotification(payload, "pi-s1")
	if ev == nil {
		t.Fatal("expected event")
	}
	if ev.Event != "session.end" {
		t.Errorf("event = %q, want session.end", ev.Event)
	}
	stopReason, _ := ev.Fields["stop_reason"].(string)
	if stopReason != "end_of_turn" {
		t.Errorf("stop_reason = %q, want end_of_turn", stopReason)
	}
}

func TestTranslateAgentEndEmptyMessages(t *testing.T) {
	payload := map[string]any{"type": "agent_end", "messages": []any{}}
	ev := translateNotification(payload, "pi-s1")
	if ev == nil {
		t.Fatal("expected event")
	}
	if ev.Event != "session.end" {
		t.Errorf("event = %q, want session.end", ev.Event)
	}
	if len(ev.Fields) != 0 {
		t.Errorf("fields should be empty for empty messages slice, got %d", len(ev.Fields))
	}
}

func TestTranslateAgentEndNoMessages(t *testing.T) {
	payload := map[string]any{"type": "agent_end"}
	ev := translateNotification(payload, "pi-s1")
	if ev == nil {
		t.Fatal("expected event")
	}
	if ev.Event != "session.end" {
		t.Errorf("event = %q, want session.end", ev.Event)
	}
}

func TestTranslateMessageUpdateTextDelta(t *testing.T) {
	payload := map[string]any{
		"type": "message_update",
		"assistantMessageEvent": map[string]any{
			"type": "text_delta",
			"data": map[string]any{
				"text": "hello",
			},
		},
	}
	ev := translateNotification(payload, "pi-s1")
	if ev == nil {
		t.Fatal("expected event")
	}
	if ev.Event != "avenor.message.delta" {
		t.Errorf("event = %q, want avenor.message.delta", ev.Event)
	}
	text, _ := ev.Fields["text"].(string)
	if text != "hello" {
		t.Errorf("text = %q, want hello", text)
	}
}

func TestTranslateMessageUpdateOtherType(t *testing.T) {
	payload := map[string]any{
		"type": "message_update",
		"assistantMessageEvent": map[string]any{
			"type": "thinking_delta",
		},
	}
	ev := translateNotification(payload, "pi-s1")
	if ev == nil {
		t.Fatal("expected event")
	}
	if ev.Event != "avenor.message.update" {
		t.Errorf("event = %q, want avenor.message.update", ev.Event)
	}
}

func TestTranslateExtensionUISelect(t *testing.T) {
	payload := map[string]any{
		"type":    "extension_ui_request",
		"id":      "ui-1",
		"method":  "select",
		"title":   "Allow command execution?",
		"options": []any{"Allow", "Deny"},
	}
	ev, method := translateExtensionUI(payload, "pi-s1")
	if ev == nil {
		t.Fatal("expected event")
	}
	if method != "select" {
		t.Errorf("method = %q, want select", method)
	}
	if ev.Event != "permission.request" {
		t.Errorf("event = %q, want permission.request", ev.Event)
	}
	kind, _ := ev.Fields["kind"].(string)
	if kind != "command" {
		t.Errorf("kind = %q, want command", kind)
	}
	desc, _ := ev.Fields["description"].(string)
	if desc != "Allow command execution?" {
		t.Errorf("description = %q", desc)
	}
}

func TestTranslateExtensionUIConfirm(t *testing.T) {
	payload := map[string]any{
		"type":    "extension_ui_request",
		"id":      "ui-2",
		"method":  "confirm",
		"title":   "Confirm?",
		"message": "Do you want to proceed?",
	}
	ev, method := translateExtensionUI(payload, "pi-s1")
	if ev == nil {
		t.Fatal("expected event")
	}
	if method != "confirm" {
		t.Errorf("method = %q, want confirm", method)
	}
	kind, _ := ev.Fields["kind"].(string)
	if kind != "confirm" {
		t.Errorf("kind = %q, want confirm", kind)
	}
	desc, _ := ev.Fields["description"].(string)
	if desc != "Do you want to proceed?" {
		t.Errorf("description = %q, want message as description", desc)
	}
}

func TestTranslateExtensionUIInput(t *testing.T) {
	payload := map[string]any{
		"type":    "extension_ui_request",
		"id":      "ui-3",
		"method":  "input",
		"title":   "Enter value",
		"default": "hello",
	}
	ev, method := translateExtensionUI(payload, "pi-s1")
	if ev == nil {
		t.Fatal("expected event")
	}
	if method != "input" {
		t.Errorf("method = %q, want input", method)
	}
	kind, _ := ev.Fields["kind"].(string)
	if kind != "input" {
		t.Errorf("kind = %q, want input", kind)
	}
}

func TestTranslateExtensionUINotify(t *testing.T) {
	payload := map[string]any{
		"type":   "extension_ui_request",
		"method": "notify",
		"title":  "Info",
	}
	ev, method := translateExtensionUI(payload, "pi-s1")
	if ev != nil {
		t.Errorf("notify method should return nil event, got %+v", ev)
	}
	if method != "notify" {
		t.Errorf("method = %q, want notify", method)
	}
}


