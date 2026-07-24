package pi

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func mustPiPayload(t *testing.T, raw string) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("unmarshal Pi RPC fixture: %v", err)
	}
	return payload
}

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
	payload := mustPiPayload(t, `{
		"type":"message_update",
		"message":{"role":"assistant","content":[{"type":"text","text":"hello"}]},
		"assistantMessageEvent":{
			"type":"text_delta","contentIndex":0,"delta":"hello",
			"partial":{"role":"assistant","content":[{"type":"text","text":"hello"}]}
		}
	}`)
	evs := translateNotification(payload, "pi-s1")
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
	payload := mustPiPayload(t, `{
		"type":"message_update",
		"message":{"role":"assistant","content":[{"type":"thinking","thinking":"hmm","thinkingSignature":"opaque"}]},
		"assistantMessageEvent":{
			"type":"thinking_delta","contentIndex":0,"delta":"hmm",
			"partial":{"role":"assistant","content":[{"type":"thinking","thinking":"hmm","thinkingSignature":"opaque"}]}
		}
	}`)
	evs := translateNotification(payload, "pi-s1")
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

func TestTranslateMessageLifecycleDoesNotReplayCumulativeText(t *testing.T) {
	payloads := []map[string]any{
		mustPiPayload(t, `{"type":"message_update","message":{"content":[{"type":"text","text":"hello"}]},"assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"hello","partial":{"content":[{"type":"text","text":"hello"}]}}}`),
		mustPiPayload(t, `{"type":"message_update","message":{"content":[{"type":"text","text":"hello world"}]},"assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":" world","partial":{"content":[{"type":"text","text":"hello world"}]}}}`),
		mustPiPayload(t, `{"type":"message_update","message":{"content":[{"type":"text","text":"hello world"}]},"assistantMessageEvent":{"type":"text_end","contentIndex":0,"content":"hello world","partial":{"content":[{"type":"text","text":"hello world"}]}}}`),
	}

	var output strings.Builder
	var endAlias map[string]any
	for _, payload := range payloads {
		for _, ev := range translateNotification(payload, "pi-s1") {
			if ev.Event == "agent.message_chunk" {
				delta, _ := ev.Fields["delta"].(string)
				output.WriteString(delta)
			}
			if ev.Event == "avenor.message.update" {
				endAlias = ev.Fields
			}
		}
	}
	if got := output.String(); got != "hello world" {
		t.Fatalf("canonical output = %q, want exactly one copy of streamed deltas", got)
	}
	if endAlias == nil {
		t.Fatal("text_end did not emit compact avenor.message.update")
	}
	if _, ok := endAlias["message"]; ok {
		t.Fatalf("compact alias retained cumulative message: %+v", endAlias)
	}
	assistantEvent, ok := endAlias["assistantMessageEvent"].(map[string]any)
	if !ok {
		t.Fatalf("compact alias missing assistantMessageEvent: %+v", endAlias)
	}
	if got := assistantEvent["type"]; got != "text_end" {
		t.Fatalf("assistant event type = %v, want text_end", got)
	}
	if got := assistantEvent["contentIndex"]; got != int64(0) {
		t.Fatalf("contentIndex = %#v, want int64(0)", got)
	}
	if _, ok := assistantEvent["partial"]; ok {
		t.Fatalf("compact alias retained cumulative partial: %+v", assistantEvent)
	}
	if _, ok := assistantEvent["content"]; ok {
		t.Fatalf("compact alias retained cumulative content: %+v", assistantEvent)
	}
}

func TestTranslateToolCallDeltaCompactsCumulativeProviderPayload(t *testing.T) {
	var translated []any
	for i, delta := range []string{`{"path":"out.txt"`, `,"content":"first"`, ` plus second"}`} {
		signature := strings.Repeat("encrypted-signature", (i+1)*1_000)
		arguments := strings.Repeat("tool argument content", (i+1)*1_000)
		payload := map[string]any{
			"type": "message_update",
			"message": map[string]any{"content": []any{
				map[string]any{"type": "thinking", "thinkingSignature": signature},
				map[string]any{"type": "toolCall", "id": "call-1", "name": "write", "arguments": map[string]any{"content": arguments}, "partialJson": arguments},
			}},
			"assistantMessageEvent": map[string]any{
				"type":         "toolcall_delta",
				"contentIndex": 1,
				"delta":        delta,
				"partial": map[string]any{"content": []any{
					map[string]any{"type": "thinking", "thinkingSignature": signature},
					map[string]any{"type": "toolCall", "id": "call-1", "name": "write", "arguments": map[string]any{"content": arguments}, "partialJson": arguments},
				}},
			},
		}

		evs := translateNotification(payload, "pi-s1")
		if len(evs) != 2 || evs[0].Event != "tool.call_update" || evs[1].Event != "avenor.message.update" {
			t.Fatalf("update %d events = %+v, want canonical and compatibility tool updates", i, evs)
		}
		if got := evs[0].Fields["toolCallId"]; got != "call-1" {
			t.Fatalf("update %d toolCallId = %v, want call-1", i, got)
		}
		if got := evs[0].Fields["title"]; got != "write" {
			t.Fatalf("update %d title = %v, want write", i, got)
		}
		if got := evs[0].Fields["delta"]; got != delta {
			t.Fatalf("update %d delta = %v, want %q", i, got, delta)
		}
		assistantEvent, ok := evs[1].Fields["assistantMessageEvent"].(map[string]any)
		if !ok {
			t.Fatalf("update %d compatibility event missing assistantMessageEvent", i)
		}
		if got := assistantEvent["type"]; got != "toolcall_delta" {
			t.Fatalf("update %d assistant type = %v", i, got)
		}
		if got := assistantEvent["contentIndex"]; got != int64(1) {
			t.Fatalf("update %d contentIndex = %#v, want int64(1)", i, got)
		}
		if got := assistantEvent["delta"]; got != delta {
			t.Fatalf("update %d compact delta = %v, want %q", i, got, delta)
		}
		if _, ok := assistantEvent["partial"]; ok {
			t.Fatalf("update %d compatibility event retained cumulative partial", i)
		}
		translated = append(translated, evs[0], evs[1])
	}

	encoded, err := json.Marshal(translated)
	if err != nil {
		t.Fatalf("marshal compact events: %v", err)
	}
	if strings.Contains(string(encoded), "encrypted-signature") {
		t.Fatal("compact events retained encrypted reasoning signature")
	}
	if strings.Contains(string(encoded), "tool argument content") {
		t.Fatal("compact delta events retained cumulative tool arguments")
	}
	if len(encoded) > 5_000 {
		t.Fatalf("compact events encoded to %d bytes, want less than 5000", len(encoded))
	}
}

func TestTranslateToolCallEndKeepsFullInputOnce(t *testing.T) {
	arguments := map[string]any{"path": "out.txt", "content": strings.Repeat("x", 10_000)}
	start := translateNotification(map[string]any{
		"type": "message_update",
		"assistantMessageEvent": map[string]any{
			"type":         "toolcall_start",
			"contentIndex": 0,
			"partial": map[string]any{"content": []any{map[string]any{
				"type": "toolCall", "id": "call-1", "name": "write", "arguments": map[string]any{},
			}}},
		},
	}, "pi-s1")
	if len(start) != 2 || start[0].Event != "tool.call" {
		t.Fatalf("tool start events = %+v", start)
	}
	if got := start[0].Fields["toolCallId"]; got != "call-1" {
		t.Fatalf("tool start id = %v, want call-1", got)
	}
	if got := start[0].Fields["title"]; got != "write" {
		t.Fatalf("tool start title = %v, want write", got)
	}

	evs := translateNotification(map[string]any{
		"type": "message_update",
		"message": map[string]any{"content": []any{map[string]any{
			"type": "toolCall", "id": "call-1", "name": "write", "arguments": arguments,
		}}},
		"assistantMessageEvent": map[string]any{
			"type": "toolcall_end",
			"toolCall": map[string]any{
				"type": "toolCall", "id": "call-1", "name": "write", "arguments": arguments,
			},
		},
	}, "pi-s1")
	if len(evs) != 2 {
		t.Fatalf("len(events) = %d, want 2", len(evs))
	}
	if got := evs[0].Fields["toolCallId"]; got != "call-1" {
		t.Fatalf("tool end id = %v, want call-1", got)
	}
	if got := evs[0].Fields["title"]; got != "write" {
		t.Fatalf("tool end title = %v, want write", got)
	}
	if got := evs[0].Fields["rawInput"]; !reflect.DeepEqual(got, arguments) {
		t.Fatalf("canonical rawInput = %#v, want %#v", got, arguments)
	}
	assistantEvent, ok := evs[1].Fields["assistantMessageEvent"].(map[string]any)
	if !ok {
		t.Fatalf("compatibility event missing assistantMessageEvent: %+v", evs[1].Fields)
	}
	compactToolCall, ok := assistantEvent["toolCall"].(map[string]any)
	if !ok {
		t.Fatalf("assistant event missing compact toolCall: %+v", assistantEvent)
	}
	if _, ok := compactToolCall["arguments"]; ok {
		t.Fatalf("compatibility alias duplicated complete arguments: %+v", compactToolCall)
	}

	encoded, err := json.Marshal(evs)
	if err != nil {
		t.Fatalf("marshal compact events: %v", err)
	}
	if count := strings.Count(string(encoded), strings.Repeat("x", 10_000)); count != 1 {
		t.Fatalf("complete tool input appears %d times, want once", count)
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
