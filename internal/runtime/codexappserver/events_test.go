package codexappserver

import (
	"encoding/json"
	"testing"
)

func TestTranslateTurnCompleted(t *testing.T) {
	tests := []struct {
		name      string
		params    string
		wantEvent string
		wantStop  string
		wantError string
	}{
		{name: "completed → end_turn", params: `{"threadId":"th1","turn":{"id":"t1","status":"completed"}}`, wantEvent: "session.end", wantStop: "end_turn"},
		{name: "interrupted → cancelled", params: `{"threadId":"th1","turn":{"id":"t1","status":"interrupted"}}`, wantEvent: "session.end", wantStop: "cancelled"},
		{name: "failed → error", params: `{"threadId":"th1","turn":{"id":"t1","status":"failed","error":{"message":"boom"}}}`, wantEvent: "session.end", wantStop: "error", wantError: "boom"},
		{name: "unknown status dropped", params: `{"threadId":"th1","turn":{"id":"t1","status":"running"}}`, wantEvent: ""},
		{name: "malformed json dropped", params: `{bad`, wantEvent: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev := translateTurnCompleted(json.RawMessage(tt.params))
			if tt.wantEvent == "" {
				if ev != nil {
					t.Fatalf("expected nil, got %+v", ev)
				}
				return
			}
			if ev == nil {
				t.Fatal("expected event, got nil")
			}
			if ev.Event != tt.wantEvent {
				t.Errorf("event = %q, want %q", ev.Event, tt.wantEvent)
			}
			if got, _ := ev.Fields["stop_reason"].(string); got != tt.wantStop {
				t.Errorf("stop_reason = %q, want %q", got, tt.wantStop)
			}
			if tt.wantError != "" {
				if got, _ := ev.Fields["error_message"].(string); got != tt.wantError {
					t.Errorf("error_message = %q, want %q", got, tt.wantError)
				}
			}
		})
	}
}

func TestTranslateNotificationDrop(t *testing.T) {
	if evs := translateNotification("unknown/method", nil); len(evs) != 0 {
		t.Fatalf("unknown method should be dropped, got %+v", evs)
	}
}

func TestTranslateNotificationDropsMalformedInformationalEvent(t *testing.T) {
	if evs := translateNotification("turn/started", json.RawMessage(`{bad`)); len(evs) != 0 {
		t.Fatalf("malformed informational event should be dropped, got %+v", evs)
	}
}

func TestTranslateNonToolItemLifecycleEmitsAliases(t *testing.T) {
	start := translateNotification("item/started", json.RawMessage(`{"threadId":"th9","turnId":"t9","itemId":"msg-1","item":{"type":"assistant_message"}}`))
	if len(start) != 1 || start[0].Event != "avenor.item.start" {
		t.Fatalf("start events = %+v, want only avenor.item.start", start)
	}
	if got, _ := start[0].Fields["item_kind"].(string); got != "assistant" {
		t.Fatalf("start item_kind = %q, want assistant", got)
	}

	completed := translateNotification("item/completed", json.RawMessage(`{"threadId":"th9","turnId":"t9","itemId":"reason-1","item":{"type":"reasoning"}}`))
	if len(completed) != 1 || completed[0].Event != "avenor.item.end" {
		t.Fatalf("completed events = %+v, want only avenor.item.end", completed)
	}
	if got, _ := completed[0].Fields["item_kind"].(string); got != "reasoning" {
		t.Fatalf("completed item_kind = %q, want reasoning", got)
	}
}

func TestTranslateApprovalCommand(t *testing.T) {
	params := json.RawMessage(`{"threadId":"th1","command":"rm -rf /","summary":"run command"}`)
	ev, ar := translateApprovalRequest("item/commandExecution/requestApproval", params)
	if ev == nil {
		t.Fatal("expected event")
	}
	if ev.Event != "permission.request" {
		t.Errorf("event = %q, want permission.request", ev.Event)
	}
	if ev.SessionID != "th1" {
		t.Errorf("sessionID = %q, want th1", ev.SessionID)
	}
	kind, _ := ev.Fields["kind"].(string)
	if kind != "command" {
		t.Errorf("kind = %q, want command", kind)
	}
	options, _ := ev.Fields["options"].([]any)
	if len(options) != 2 {
		t.Fatalf("options = %#v, want allow/reject options", ev.Fields["options"])
	}
	desc, _ := ev.Fields["description"].(string)
	if desc != "rm -rf /" {
		t.Errorf("description = %q, want 'rm -rf /'", desc)
	}
	if ar.Command != "rm -rf /" {
		t.Errorf("ar.Command = %q", ar.Command)
	}
}

func TestTranslateApprovalUnknownMethodDropped(t *testing.T) {
	params := json.RawMessage(`{"threadId":"th1","summary":"new approval type"}`)
	ev, ar := translateApprovalRequest("item/unknown/requestApproval", params)
	if ev != nil || ar != nil {
		t.Fatalf("unknown approval method should be dropped, got ev=%+v ar=%+v", ev, ar)
	}
}

func TestTranslateApprovalFile(t *testing.T) {
	params := json.RawMessage(`{"threadId":"th2","path":"/etc/hosts","summary":"modify hosts"}`)
	ev, ar := translateApprovalRequest("item/fileChange/requestApproval", params)
	if ev == nil {
		t.Fatal("expected event")
	}
	kind, _ := ev.Fields["kind"].(string)
	if kind != "file" {
		t.Errorf("kind = %q, want file", kind)
	}
	desc, _ := ev.Fields["description"].(string)
	if desc != "/etc/hosts" {
		t.Errorf("description = %q, want /etc/hosts", desc)
	}
	if ar.Path != "/etc/hosts" {
		t.Errorf("ar.Path = %q", ar.Path)
	}
}

func TestTranslateApprovalPermissions(t *testing.T) {
	params := json.RawMessage(`{"threadId":"th3","summary":"need full access","message":"allow all"}`)
	ev, ar := translateApprovalRequest("item/permissions/requestApproval", params)
	if ev == nil {
		t.Fatal("expected event")
	}
	kind, _ := ev.Fields["kind"].(string)
	if kind != "permissions" {
		t.Errorf("kind = %q, want permissions", kind)
	}
	desc, _ := ev.Fields["description"].(string)
	if desc != "need full access" {
		t.Errorf("description = %q, want 'need full access'", desc)
	}
	if ar.Summary != "need full access" {
		t.Errorf("ar.Summary = %q", ar.Summary)
	}
}

func TestTranslateApprovalMissingDescription(t *testing.T) {
	params := json.RawMessage(`{"threadId":"th4"}`)
	ev, _ := translateApprovalRequest("item/commandExecution/requestApproval", params)
	if ev == nil {
		t.Fatal("expected event")
	}
	desc, _ := ev.Fields["description"].(string)
	if desc != "" {
		t.Errorf("description = %q, want empty", desc)
	}
}

func TestTranslateApprovalNilParams(t *testing.T) {
	ev, ar := translateApprovalRequest("item/commandExecution/requestApproval", nil)
	if ev != nil || ar != nil {
		t.Fatal("expected nil for nil params")
	}
}

func TestInformationalEvents(t *testing.T) {
	params := json.RawMessage(`{"threadId":"th5","turnId":"t5"}`)
	evts := translateNotification("turn/started", params)
	if len(evts) != 1 || evts[0].Event != "avenor.turn.start" {
		t.Fatalf("events = %+v", evts)
	}
}

func TestTranslateItemAssistantDelta(t *testing.T) {
	evts := translateNotification("item/delta", json.RawMessage(`{"threadId":"th6","turnId":"t6","itemId":"i1","item":{"type":"assistant_message","delta":{"text":"hello"}}}`))
	if len(evts) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(evts))
	}
	if evts[0].Event != "agent.message_chunk" {
		t.Fatalf("event = %q, want agent.message_chunk", evts[0].Event)
	}
	if got, _ := evts[0].Fields["delta"].(string); got != "hello" {
		t.Fatalf("delta = %q, want hello", got)
	}
}

func TestTranslateItemReasoningDelta(t *testing.T) {
	evts := translateNotification("item/delta", json.RawMessage(`{"threadId":"th7","turnId":"t7","itemId":"i2","item":{"type":"reasoning","delta":{"text":"hmm"}}}`))
	if len(evts) != 1 || evts[0].Event != "agent.thought_chunk" {
		t.Fatalf("events = %+v", evts)
	}
}

func TestTranslateItemToolLifecycle(t *testing.T) {
	start := translateNotification("item/started", json.RawMessage(`{"threadId":"th8","turnId":"t8","itemId":"tool-1","item":{"type":"tool_call","name":"bash"}}`))
	if len(start) != 2 || start[0].Event != "tool.call" || start[1].Event != "avenor.item.start" {
		t.Fatalf("start events = %+v", start)
	}
	if got, _ := start[0].Fields["status"].(string); got != "running" {
		t.Fatalf("status = %q, want running", got)
	}

	update := translateNotification("item/delta", json.RawMessage(`{"threadId":"th8","turnId":"t8","itemId":"tool-1","item":{"type":"tool_call","delta":{"arguments":"ls"}}}`))
	if len(update) != 1 || update[0].Event != "tool.call_update" {
		t.Fatalf("update events = %+v", update)
	}
	if got, _ := update[0].Fields["delta"].(string); got != "ls" {
		t.Fatalf("delta = %q, want ls", got)
	}

	end := translateNotification("item/completed", json.RawMessage(`{"threadId":"th8","turnId":"t8","itemId":"tool-1","item":{"type":"tool_call","name":"bash"}}`))
	if len(end) != 2 || end[0].Event != "tool.call_update" || end[1].Event != "avenor.item.end" {
		t.Fatalf("end events = %+v", end)
	}
	if got, _ := end[0].Fields["status"].(string); got != "completed" {
		t.Fatalf("status = %q, want completed", got)
	}
}
