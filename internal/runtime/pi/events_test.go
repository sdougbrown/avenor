package pi

import "testing"

func TestTranslateBasicEvents(t *testing.T) {
	tests := []struct {
		typeStr string
		want    []string
	}{
		{"agent_start", []string{"session.start"}},
		{"agent_end", []string{"session.end"}},
		{"turn_start", []string{"avenor.turn.start"}},
		{"turn_end", []string{"avenor.turn.end"}},
		{"message_start", []string{"avenor.message.start"}},
		{"message_end", []string{"avenor.message.end"}},
		{"compaction_start", []string{"avenor.compaction.start"}},
		{"compaction_end", []string{"avenor.compaction.end"}},
		{"queue_update", []string{"avenor.queue.update"}},
		{"auto_retry_start", []string{"avenor.auto_retry.start"}},
		{"auto_retry_end", []string{"avenor.auto_retry.end"}},
		{"extension_error", []string{"avenor.extension.error"}},
		{"some_unknown_event", []string{"avenor.some_unknown_event"}},
	}
	for _, tt := range tests {
		t.Run(tt.typeStr, func(t *testing.T) {
			evs := translateNotification(map[string]any{"type": tt.typeStr}, "pi-s1")
			if len(evs) != len(tt.want) {
				t.Fatalf("len(events) = %d, want %d", len(evs), len(tt.want))
			}
			for i, want := range tt.want {
				if evs[i].Event != want {
					t.Fatalf("events[%d] = %q, want %q", i, evs[i].Event, want)
				}
				if evs[i].SessionID != "pi-s1" {
					t.Fatalf("sessionID = %q, want pi-s1", evs[i].SessionID)
				}
			}
		})
	}
}

func TestTranslateToolExecutionEmitsCanonicalAndAlias(t *testing.T) {
	tests := []struct {
		name          string
		typeName      string
		status        string
		wantCanonical string
		wantAlias     string
		wantStatus    string
	}{
		{name: "start", typeName: "tool_execution_start", wantCanonical: "tool.call", wantAlias: "avenor.tool.start", wantStatus: "running"},
		{name: "update", typeName: "tool_execution_update", status: "running", wantCanonical: "tool.call_update", wantAlias: "avenor.tool.update", wantStatus: "running"},
		{name: "end", typeName: "tool_execution_end", wantCanonical: "tool.call_update", wantAlias: "avenor.tool.end", wantStatus: "completed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := map[string]any{"type": tt.typeName, "toolName": "cat"}
			if tt.status != "" {
				payload["status"] = tt.status
			}
			evs := translateNotification(payload, "pi-s1")
			if len(evs) != 2 {
				t.Fatalf("len(events) = %d, want 2", len(evs))
			}
			if evs[0].Event != tt.wantCanonical || evs[1].Event != tt.wantAlias {
				t.Fatalf("events = %+v, want %s and %s", evs, tt.wantCanonical, tt.wantAlias)
			}
			if got, _ := evs[0].Fields["title"].(string); got != "cat" {
				t.Fatalf("title = %q, want cat", got)
			}
			if got, _ := evs[0].Fields["status"].(string); got != tt.wantStatus {
				t.Fatalf("status = %q, want %q", got, tt.wantStatus)
			}
		})
	}
}

func TestTranslateAgentEndWithStopReasonAndFinalOutput(t *testing.T) {
	evs := translateNotification(map[string]any{
		"type": "agent_end",
		"messages": []any{map[string]any{
			"stop_reason": "end_of_turn",
			"content":     []any{map[string]any{"type": "text", "text": "done"}},
		}},
	}, "pi-s1")
	if len(evs) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(evs))
	}
	ev := evs[0]
	if ev.Event != "session.end" {
		t.Fatalf("event = %q, want session.end", ev.Event)
	}
	if got, _ := ev.Fields["stop_reason"].(string); got != "end_of_turn" {
		t.Fatalf("stop_reason = %q, want end_of_turn", got)
	}
	if got, _ := ev.Fields["final_output"].(string); got != "done" {
		t.Fatalf("final_output = %q, want done", got)
	}
}

func TestTranslateAgentEndWithStopReasonAndNoFinalOutput(t *testing.T) {
	evs := translateNotification(map[string]any{
		"type": "agent_end",
		"messages": []any{map[string]any{
			"stop_reason": "cancelled",
		}},
	}, "pi-s1")
	if len(evs) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(evs))
	}
	if got, _ := evs[0].Fields["stop_reason"].(string); got != "cancelled" {
		t.Fatalf("stop_reason = %q, want cancelled", got)
	}
	if _, ok := evs[0].Fields["final_output"]; ok {
		t.Fatalf("unexpected final_output: %v", evs[0].Fields["final_output"])
	}
}

func TestTranslateMessageUpdateTextDelta(t *testing.T) {
	evs := translateNotification(map[string]any{
		"type": "message_update",
		"assistantMessageEvent": map[string]any{
			"type": "text_delta",
			"data": map[string]any{"text": "hello"},
		},
	}, "pi-s1")
	if len(evs) != 2 {
		t.Fatalf("len(events) = %d, want 2", len(evs))
	}
	if evs[0].Event != "agent.message_chunk" || evs[1].Event != "avenor.message.delta" {
		t.Fatalf("events = %+v", evs)
	}
	if got, _ := evs[0].Fields["delta"].(string); got != "hello" {
		t.Fatalf("delta = %q, want hello", got)
	}
}

func TestTranslateMessageUpdateThinkingDelta(t *testing.T) {
	evs := translateNotification(map[string]any{
		"type": "message_update",
		"assistantMessageEvent": map[string]any{
			"type": "thinking_delta",
			"data": map[string]any{"text": "hmm"},
		},
	}, "pi-s1")
	if len(evs) != 2 {
		t.Fatalf("len(events) = %d, want 2", len(evs))
	}
	if evs[0].Event != "agent.thought_chunk" || evs[1].Event != "avenor.message.update" {
		t.Fatalf("events = %+v", evs)
	}
}

func TestTranslateMessageUpdateSkipsEmptyCanonicalDelta(t *testing.T) {
	evs := translateNotification(map[string]any{
		"type":                  "message_update",
		"assistantMessageEvent": map[string]any{"type": "text_delta"},
	}, "pi-s1")
	if len(evs) != 1 || evs[0].Event != "avenor.message.update" {
		t.Fatalf("events = %+v, want only avenor.message.update", evs)
	}
}

func TestTranslateMessageUpdateFallsBackToFullMessageText(t *testing.T) {
	evs := translateNotification(map[string]any{
		"type": "message_update",
		"assistantMessageEvent": map[string]any{
			"type": "message_end",
			"partial": map[string]any{
				"content": []any{map[string]any{"type": "text", "text": "full text"}},
			},
		},
	}, "pi-s1")
	if len(evs) != 2 {
		t.Fatalf("len(events) = %d, want 2", len(evs))
	}
	if evs[0].Event != "agent.message_chunk" {
		t.Fatalf("first event = %q, want agent.message_chunk", evs[0].Event)
	}
	if got, _ := evs[0].Fields["delta"].(string); got != "full text" {
		t.Fatalf("delta = %q, want full text", got)
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
