package agy

import (
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/events"
)

func TestTranslateEvent_BasicSuccessfulTurn(t *testing.T) {
	sid := "sess-basic-1"
	cache := map[string]any{}

	evts, cache := translateEvent(newInit("conv-abc123", "", ""), sid, true, cache)
	assertEvent(t, evts[0], "session.start", sid, map[string]any{
		"backend":         BackendID,
		"model":           "",
		"agy_version":     "",
		"conversation_id": "conv-abc123",
	})

	evts, cache = translateEvent(newAgentActive(1, "Hello"), sid, false, cache)
	assertEvent(t, evts[0], "agent.message_chunk", sid, map[string]any{"delta": "Hello"})

	evts, cache = translateEvent(newAgentActive(1, "! I am Gemini."), sid, false, cache)
	assertEvent(t, evts[0], "agent.message_chunk", sid, map[string]any{"delta": "! I am Gemini."})

	evts, cache = translateEvent(newAgentDone(1), sid, false, cache)
	if len(evts) != 1 || evts[0].Event != "avenor.message.update" {
		t.Fatalf("expected avenor.message.update, got %v", evts)
	}

	evts, cache = translateEvent(newResult("conv-abc123", "Hello! I am Gemini.", map[string]any{"input_tokens": 15, "output_tokens": 12}), sid, false, cache)
	assertEvent(t, evts[0], "session.end", sid, map[string]any{
		"stop_reason":  "end_turn",
		"final_output": "Hello! I am Gemini.",
	})

	if cache["conversation_id"] != "conv-abc123" {
		t.Errorf("cache conversation_id = %v, want conv-abc123", cache["conversation_id"])
	}
	if cache["model"] != "" {
		t.Errorf("cache model = %v, want empty string", cache["model"])
	}
}

func TestTranslateEvent_ToolSuccess(t *testing.T) {
	sid := "sess-tool-1"
	cache := map[string]any{}

	translateEvent(newInit("conv-tool-1", "", ""), sid, true, cache)

	evts, _ := translateEvent(newAgentActive(1, "Let me check that."), sid, false, cache)
	assertEvent(t, evts[0], "agent.message_chunk", sid, map[string]any{"delta": "Let me check that."})

	evts, _ = translateEvent(newToolActive(2, "read", map[string]any{"path": "/tmp/test.txt"}), sid, false, cache)
	assertEvent(t, evts[0], "tool.call", sid, map[string]any{"title": "read", "status": "running"})
	if fields := evts[0].Fields; fields["parameters"].(map[string]any)["path"] != "/tmp/test.txt" {
		t.Errorf("tool.call parameters.path = %v, want /tmp/test.txt", fields["parameters"])
	}

	evts, _ = translateEvent(newToolDone(2, "read", "file contents", float64(150)), sid, false, cache)
	assertEvent(t, evts[0], "tool.call_update", sid, map[string]any{
		"status":      "completed",
		"name":        "read",
		"output":      "file contents",
		"duration_ms": int64(150000), // 150 seconds * 1000
	})

	evts, _ = translateEvent(newAgentActive(3, "The file contains: file contents"), sid, false, cache)
	assertEvent(t, evts[0], "agent.message_chunk", sid, map[string]any{"delta": "The file contains: file contents"})

	evts, _ = translateEvent(newAgentDone(3), sid, false, cache)
	if len(evts) != 1 || evts[0].Event != "avenor.message.update" {
		t.Fatalf("expected avenor.message.update, got %v", evts)
	}

	evts, _ = translateEvent(newResult("conv-tool-1", "The file contains: file contents", map[string]any{"input_tokens": 20, "output_tokens": 18}), sid, false, cache)
	assertEvent(t, evts[0], "session.end", sid, map[string]any{
		"stop_reason":  "end_turn",
		"final_output": "The file contains: file contents",
	})
}

func TestTranslateEvent_ToolDenied(t *testing.T) {
	sid := "sess-deny-1"
	cache := map[string]any{}

	translateEvent(newInit("conv-deny-1", "", ""), sid, true, cache)

	evts, _ := translateEvent(newToolActive(1, "bash", map[string]any{"command": "rm -rf /"}), sid, false, cache)
	assertEvent(t, evts[0], "tool.call", sid, map[string]any{"title": "bash", "status": "running"})

	evts, _ = translateEvent(newToolError(1, "bash", "Permission denied: command 'rm -rf /' requires interactive confirmation in headless mode"), sid, false, cache)
	assertEvent(t, evts[0], "tool.call_update", sid, map[string]any{
		"status": "error",
		"name":   "bash",
	})
	if fields := evts[0].Fields; fields["error"] != "Permission denied: command 'rm -rf /' requires interactive confirmation in headless mode" {
		t.Errorf("tool.call_update error = %v", fields["error"])
	}

	evts, _ = translateEvent(newResultWithError("conv-deny-1", "Tool execution failed: Permission denied", map[string]any{"input_tokens": 10, "output_tokens": 5}), sid, false, cache)
	assertEvent(t, evts[0], "session.end", sid, map[string]any{
		"stop_reason": "error",
		"error":       "Tool execution failed: Permission denied",
	})
}

func TestTranslateEvent_ResultError(t *testing.T) {
	sid := "sess-err-1"
	cache := map[string]any{}

	translateEvent(newInit("conv-err-1", "", ""), sid, true, cache)

	evts, _ := translateEvent(newAgentActive(1, "Processing..."), sid, false, cache)
	assertEvent(t, evts[0], "agent.message_chunk", sid, map[string]any{"delta": "Processing..."})

	evts, _ = translateEvent(newAgentDone(1), sid, false, cache)
	if len(evts) != 1 || evts[0].Event != "avenor.message.update" {
		t.Fatalf("expected avenor.message.update, got %v", evts)
	}

	evts, _ = translateEvent(newResultWithError("conv-err-1", "Internal server error", map[string]any{"input_tokens": 5, "output_tokens": 3}), sid, false, cache)
	assertEvent(t, evts[0], "session.end", sid, map[string]any{
		"stop_reason": "error",
		"error":       "Internal server error",
	})
	if evts[0].Fields["final_output"] != nil {
		t.Errorf("session.end should not have final_output on error, got %v", evts[0].Fields["final_output"])
	}
}

func TestTranslateEvent_FragmentedOutput(t *testing.T) {
	sid := "sess-frag-1"
	cache := map[string]any{}

	translateEvent(newInit("conv-frag", "", ""), sid, true, cache)

	var chunks []string
	chunks = append(chunks, collectDeltas(map[string]any{
		"event": "step_update",
		"step_update": map[string]any{
			"step_index": 1,
			"step_type":  "agent_response",
			"state":      "ACTIVE",
			"text_delta": "Hello",
		},
	}, sid, cache)...)
	chunks = append(chunks, collectDeltas(map[string]any{
		"event": "step_update",
		"step_update": map[string]any{
			"step_index": 1,
			"step_type":  "agent_response",
			"state":      "ACTIVE",
			"text_delta": " ",
		},
	}, sid, cache)...)
	chunks = append(chunks, collectDeltas(map[string]any{
		"event": "step_update",
		"step_update": map[string]any{
			"step_index": 1,
			"step_type":  "agent_response",
			"state":      "ACTIVE",
			"text_delta": "World!",
		},
	}, sid, cache)...)

	concatenated := strings.Join(chunks, "")
	if concatenated != "Hello World!" {
		t.Errorf("concatenated chunks = %q, want %q", concatenated, "Hello World!")
	}

	translateEvent(newAgentDone(1), sid, false, cache)

	evts, _ := translateEvent(newResult("conv-frag", "Hello World!", map[string]any{"input_tokens": 10, "output_tokens": 5}), sid, false, cache)
	assertEvent(t, evts[0], "session.end", sid, map[string]any{
		"stop_reason":  "end_turn",
		"final_output": "Hello World!",
	})
}

func TestTranslateEvent_EmptyAgentSteps(t *testing.T) {
	sid := "sess-empty-1"
	cache := map[string]any{}

	translateEvent(newInit("conv-empty", "", ""), sid, true, cache)

	evts, _ := translateEvent(map[string]any{
		"event": "step_update",
		"step_update": map[string]any{
			"step_index": 1,
			"step_type":  "agent_response",
			"state":      "ACTIVE",
			"text_delta": "",
		},
	}, sid, false, cache)
	for _, e := range evts {
		if e.Event == "agent.message_chunk" {
			t.Errorf("expected no agent.message_chunk for empty delta, got one")
		}
	}

	evts, _ = translateEvent(map[string]any{
		"event": "step_update",
		"step_update": map[string]any{
			"step_index": 1,
			"step_type":  "agent_response",
			"state":      "DONE",
		},
	}, sid, false, cache)
	if len(evts) != 1 || evts[0].Event != "avenor.message.update" {
		t.Errorf("expected avenor.message.update, got %v", evts)
	}
}

func TestTranslateEvent_SecondInitNoDuplicateSessionStart(t *testing.T) {
	sid := "sess-dup-1"
	cache := map[string]any{}

	evts, cache := translateEvent(newInit("conv-dup", "", ""), sid, true, cache)
	if len(evts) != 1 || evts[0].Event != "session.start" {
		t.Fatalf("first init: expected session.start, got %v", evts)
	}

	evts, _ = translateEvent(newInit("conv-dup", "", ""), sid, false, cache)
	if len(evts) != 0 {
		t.Errorf("second init with same conversation_id: expected no events, got %v", evts)
	}

	evts, _ = translateEvent(newInit("conv-dup-2", "", ""), sid, false, cache)
	if len(evts) != 0 {
		t.Errorf("second init with different conversation_id: expected no events, got %v", evts)
	}
	if cache["conversation_id"] != "conv-dup" {
		t.Errorf("cache should keep first conversation_id, got %v", cache["conversation_id"])
	}
}

func TestTranslateEvent_ResultNotDoubleRendered(t *testing.T) {
	sid := "sess-dbl-1"
	cache := map[string]any{}

	translateEvent(newInit("conv-dbl", "", ""), sid, true, cache)

	resp := "This is the full response."
	for i := 0; i < len(resp); i += 4 {
		end := i + 4
		if end > len(resp) {
			end = len(resp)
		}
		translateEvent(map[string]any{
			"event": "step_update",
			"step_update": map[string]any{
				"step_index": 1,
				"step_type":  "agent_response",
				"state":      "ACTIVE",
				"text_delta": resp[i:end],
			},
		}, sid, false, cache)
	}
	translateEvent(newAgentDone(1), sid, false, cache)

	evts, _ := translateEvent(newResult("conv-dbl", resp, map[string]any{"input_tokens": 5, "output_tokens": 3}), sid, false, cache)

	for _, e := range evts {
		if e.Event == "agent.message_chunk" {
			t.Errorf("result should not emit agent.message_chunk; got %q with delta %v", e.Event, e.Fields["delta"])
		}
	}

	if len(evts) != 1 || evts[0].Event != "session.end" {
		t.Fatalf("expected only session.end, got %d events: %v", len(evts), evts)
	}
	if evts[0].Fields["final_output"] != resp {
		t.Errorf("session.end final_output = %v, want %q", evts[0].Fields["final_output"], resp)
	}
}

func TestTranslateEvent_UnknownEventType(t *testing.T) {
	sid := "sess-unk-1"
	cache := map[string]any{}

	translateEvent(newInit("conv-unk", "", ""), sid, true, cache)

	evts, _ := translateEvent(map[string]any{
		"event": "heartbeat",
		"seq":   42,
	}, sid, false, cache)

	if len(evts) != 1 {
		t.Fatalf("expected 1 event for unknown type, got %d", len(evts))
	}
	if evts[0].Event != "avenor.agy.heartbeat" {
		t.Errorf("event = %q, want avenor.agy.heartbeat", evts[0].Event)
	}
	if evts[0].Fields["seq"] != 42 {
		t.Errorf("fields.seq = %v, want 42", evts[0].Fields["seq"])
	}
}

func TestTranslateEvent_MalformedPayload(t *testing.T) {
	sid := "sess-mal-1"
	cache := map[string]any{}

	evts, _ := translateEvent(map[string]any{}, sid, true, cache)
	if len(evts) != 1 || evts[0].Event != "avenor.agy." {
		t.Errorf("empty payload: unexpected events = %v", evts)
	}

	evts, _ = translateEvent(map[string]any{"foo": "bar"}, sid, false, cache)
	if len(evts) != 1 || evts[0].Event != "avenor.agy." {
		t.Errorf("no event key: unexpected events = %v", evts)
	}

	evts, _ = translateEvent(map[string]any{"event": "init"}, sid, true, cache)
	if len(evts) != 1 || evts[0].Event != "session.start" {
		t.Fatalf("init with no conversation_id: unexpected events = %v", evts)
	}
	if evts[0].Fields["conversation_id"] != "" {
		t.Errorf("session.start conversation_id = %v, want empty string", evts[0].Fields["conversation_id"])
	}

	evts, _ = translateEvent(map[string]any{"event": "step_update"}, sid, false, cache)
	if len(evts) != 1 {
		t.Fatalf("step_update with no step_update nested data: expected 1 event, got %d", len(evts))
	}

	evts, _ = translateEvent(map[string]any{"event": "result"}, sid, false, cache)
	if len(evts) != 1 || evts[0].Event != "session.end" {
		t.Fatalf("result with no fields: unexpected events = %v", evts)
	}
	if evts[0].Fields["stop_reason"] != "end_turn" {
		t.Errorf("stop_reason = %v, want end_turn", evts[0].Fields["stop_reason"])
	}
}

func TestTranslateEvent_InitCacheNil(t *testing.T) {
	sid := "sess-nil-1"

	evts, cache := translateEvent(newInit("conv-nil", "", ""), sid, true, nil)

	if len(evts) != 1 || evts[0].Event != "session.start" {
		t.Fatalf("expected session.start, got %v", evts)
	}
	if cache == nil {
		t.Fatal("initCache should not be nil after translateEvent")
	}
	if cache["conversation_id"] != "conv-nil" {
		t.Errorf("cache conversation_id = %v, want conv-nil", cache["conversation_id"])
	}
}

func TestTranslateEvent_ResultUsagePreserved(t *testing.T) {
	sid := "sess-usage-1"
	cache := map[string]any{}

	translateEvent(newInit("conv-usage", "", ""), sid, true, cache)

	evts, _ := translateEvent(newResult("conv-usage", "done", map[string]any{"input_tokens": float64(42), "output_tokens": float64(13)}), sid, false, cache)

	if len(evts) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evts))
	}
	usage, ok := evts[0].Fields["usage"].(map[string]any)
	if !ok {
		t.Fatalf("usage field missing or not a map")
	}
	if usage["input_tokens"] != float64(42) {
		t.Errorf("usage.input_tokens = %v, want 42", usage["input_tokens"])
	}
	if usage["output_tokens"] != float64(13) {
		t.Errorf("usage.output_tokens = %v, want 13", usage["output_tokens"])
	}
}

func TestTranslateEvent_ToolCallParameters(t *testing.T) {
	sid := "sess-params-1"
	cache := map[string]any{}

	translateEvent(newInit("conv-params", "", ""), sid, true, cache)

	evts, _ := translateEvent(map[string]any{
		"event": "step_update",
		"step_update": map[string]any{
			"step_index": 5,
			"step_type":  "tool",
			"state":      "ACTIVE",
			"tool_name":  "write",
			"tool_info": map[string]any{
				"parameters": map[string]any{
					"path":      "/tmp/out.txt",
					"content":   "hello",
					"overwrite": true,
				},
			},
		},
	}, sid, false, cache)

	if len(evts) != 1 || evts[0].Event != "tool.call" {
		t.Fatalf("expected tool.call, got %v", evts)
	}
	params, ok := evts[0].Fields["parameters"].(map[string]any)
	if !ok {
		t.Fatal("tool.call missing parameters")
	}
	if params["path"] != "/tmp/out.txt" {
		t.Errorf("parameters.path = %v, want /tmp/out.txt", params["path"])
	}
	if params["content"] != "hello" {
		t.Errorf("parameters.content = %v, want hello", params["content"])
	}
	if params["overwrite"] != true {
		t.Errorf("parameters.overwrite = %v, want true", params["overwrite"])
	}
}

func TestTranslateEvent_TelemetryNoPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("translateEvent panicked: %v", r)
		}
	}()

	sid := "sess-safe-1"
	cache := map[string]any{}

	testCases := []map[string]any{
		nil,
		{},
		{"event": nil},
		{"event": 123},
		{"event": "init", "conversation_id": nil},
		{"event": "step_update"},
		{"event": "step_update", "step_update": map[string]any{"step_type": nil}},
		{"event": "step_update", "step_update": map[string]any{"step_type": "agent_response"}},
		{"event": "step_update", "step_update": map[string]any{"step_type": "tool"}},
		{"event": "result"},
		{"event": "result", "result": map[string]any{"response": nil, "error": nil, "usage": "not-a-map"}},
		{"event": "init", "conversation_id": "conv-safe", "init": map[string]any{}},
	}

	for _, tc := range testCases {
		_, _ = translateEvent(tc, sid, true, cache)
	}
}

func TestBackendID(t *testing.T) {
	if BackendID != "agy" {
		t.Errorf("BackendID = %q, want %q", BackendID, "agy")
	}
}

func collectDeltas(payload map[string]any, sessionID string, cache map[string]any) []string {
	evts, _ := translateEvent(payload, sessionID, false, cache)
	var deltas []string
	for _, e := range evts {
		if e.Event == "agent.message_chunk" {
			if d, ok := e.Fields["delta"].(string); ok {
				deltas = append(deltas, d)
			}
		}
	}
	return deltas
}

func assertEvent(tb testing.TB, e events.Event, wantEvent, wantSessionID string, wantFields map[string]any) {
	tb.Helper()
	if e.Event != wantEvent {
		tb.Errorf("event = %q, want %q", e.Event, wantEvent)
	}
	if e.SessionID != wantSessionID {
		tb.Errorf("session_id = %q, want %q", e.SessionID, wantSessionID)
	}
	if wantFields != nil {
		for k, v := range wantFields {
			got := e.Fields[k]
			if got != v {
				tb.Errorf("fields[%q] = %v, want %v", k, got, v)
			}
		}
	}
}
