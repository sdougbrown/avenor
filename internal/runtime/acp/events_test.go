package acp

import (
	"encoding/json"
	"testing"
)

func TestMapPromptResponseNormalisesUsage(t *testing.T) {
	raw := json.RawMessage(`{ "stopReason": "end_turn", "usage": { "inputTokens": 10, "outputTokens": 5, "totalTokens": 15, "cachedReadTokens": 3, "cachedWriteTokens": 4, "thoughtTokens": 6 } }`)

	event, err := mapPromptResponse("ses_test", raw)
	if err != nil {
		t.Fatalf("mapPromptResponse() error = %v", err)
	}
	if event.Event != "session.end" {
		t.Fatalf("event.Event = %q, want %q", event.Event, "session.end")
	}
	if event.SessionID != "ses_test" {
		t.Fatalf("event.SessionID = %q, want %q", event.SessionID, "ses_test")
	}
	if event.Fields["stop_reason"] != "end_turn" {
		t.Errorf("stop_reason = %v, want end_turn", event.Fields["stop_reason"])
	}

	usage, ok := event.Fields["usage"].(map[string]any)
	if !ok {
		t.Fatal("event.Fields[\"usage\"] is missing or not a map")
	}
	if usage["input_tokens"] != float64(10) {
		t.Errorf("usage.input_tokens = %v, want 10", usage["input_tokens"])
	}
	if usage["output_tokens"] != float64(5) {
		t.Errorf("usage.output_tokens = %v, want 5", usage["output_tokens"])
	}
	if usage["total_tokens"] != float64(15) {
		t.Errorf("usage.total_tokens = %v, want 15", usage["total_tokens"])
	}
	if usage["cached_read_tokens"] != float64(3) {
		t.Errorf("usage.cached_read_tokens = %v, want 3", usage["cached_read_tokens"])
	}
	if usage["cached_write_tokens"] != float64(4) {
		t.Errorf("usage.cached_write_tokens = %v, want 4", usage["cached_write_tokens"])
	}
	if usage["thought_tokens"] != float64(6) {
		t.Errorf("usage.thought_tokens = %v, want 6", usage["thought_tokens"])
	}

	for _, topKey := range []string{"inputTokens", "outputTokens", "totalTokens", "cachedReadTokens", "cachedWriteTokens", "thoughtTokens"} {
		if _, exists := event.Fields[topKey]; exists {
			t.Errorf("event.Fields[%q] exists at top level, should be nested under usage", topKey)
		}
	}
}

func TestMapPromptResponseSnakeCaseFallback(t *testing.T) {
	raw := json.RawMessage(`{ "stopReason": "end_turn", "usage": { "input_tokens": 7, "output_tokens": 3, "total_tokens": 10, "cached_read_tokens": 2, "cached_write_tokens": 9, "thought_tokens": 11 } }`)

	event, err := mapPromptResponse("ses_snake", raw)
	if err != nil {
		t.Fatalf("mapPromptResponse() error = %v", err)
	}
	if event.Event != "session.end" {
		t.Fatalf("event.Event = %q, want %q", event.Event, "session.end")
	}

	usage, ok := event.Fields["usage"].(map[string]any)
	if !ok {
		t.Fatal("event.Fields[\"usage\"] is missing or not a map")
	}
	if usage["input_tokens"] != float64(7) {
		t.Errorf("usage.input_tokens = %v, want 7", usage["input_tokens"])
	}
	if usage["output_tokens"] != float64(3) {
		t.Errorf("usage.output_tokens = %v, want 3", usage["output_tokens"])
	}
	if usage["total_tokens"] != float64(10) {
		t.Errorf("usage.total_tokens = %v, want 10", usage["total_tokens"])
	}
	if usage["cached_read_tokens"] != float64(2) {
		t.Errorf("usage.cached_read_tokens = %v, want 2", usage["cached_read_tokens"])
	}
	if usage["cached_write_tokens"] != float64(9) {
		t.Errorf("usage.cached_write_tokens = %v, want 9", usage["cached_write_tokens"])
	}
	if usage["thought_tokens"] != float64(11) {
		t.Errorf("usage.thought_tokens = %v, want 11", usage["thought_tokens"])
	}
}

func TestMapPromptResponsePreservesExplicitZeroUsage(t *testing.T) {
	raw := json.RawMessage(`{ "stopReason": "end_turn", "usage": { "inputTokens": 0, "outputTokens": 0, "totalTokens": 0, "cachedReadTokens": 0, "cachedWriteTokens": 0, "thoughtTokens": 0 } }`)

	event, err := mapPromptResponse("ses_zero", raw)
	if err != nil {
		t.Fatalf("mapPromptResponse() error = %v", err)
	}
	usage, ok := event.Fields["usage"].(map[string]any)
	if !ok {
		t.Fatal("event.Fields[\"usage\"] is missing or not a map")
	}
	for _, key := range []string{"input_tokens", "output_tokens", "total_tokens", "cached_read_tokens", "cached_write_tokens", "thought_tokens"} {
		val, exists := usage[key]
		if !exists {
			t.Errorf("usage[%q] was omitted, want explicit 0 preserved", key)
			continue
		}
		if val != float64(0) {
			t.Errorf("usage[%q] = %v, want 0", key, val)
		}
	}
}

func TestMapPromptResponseOmitsNilUsageKeys(t *testing.T) {
	raw := json.RawMessage(`{ "stopReason": "end_turn", "usage": { "outputTokens": 5 } }`)

	event, err := mapPromptResponse("ses_partial", raw)
	if err != nil {
		t.Fatalf("mapPromptResponse() error = %v", err)
	}
	usage, ok := event.Fields["usage"].(map[string]any)
	if !ok {
		t.Fatal("event.Fields[\"usage\"] is missing or not a map")
	}
	if usage["output_tokens"] != float64(5) {
		t.Errorf("usage.output_tokens = %v, want 5", usage["output_tokens"])
	}
	for _, absentKey := range []string{"input_tokens", "total_tokens", "cached_read_tokens", "cached_write_tokens", "thought_tokens"} {
		if _, exists := usage[absentKey]; exists {
			t.Errorf("usage.%s should be omitted when absent", absentKey)
		}
	}
}

func TestMapPromptResponseNonMapUsageDropped(t *testing.T) {
	raw := json.RawMessage(`{ "stopReason": "end_turn", "usage": ["invalid"] }`)

	event, err := mapPromptResponse("ses_nonmap", raw)
	if err != nil {
		t.Fatalf("mapPromptResponse() error = %v", err)
	}
	if _, exists := event.Fields["usage"]; exists {
		t.Fatal("usage should not appear when value is not a map")
	}
}

func TestMapPromptResponseMissingStopReason(t *testing.T) {
	raw := json.RawMessage(`{ "other": "value" }`)

	_, err := mapPromptResponse("ses_nostop", raw)
	if err == nil {
		t.Fatal("expected error for missing stop_reason")
	}
}

func TestMapSessionUpdateUnknownKind(t *testing.T) {
	raw := json.RawMessage(`{"sessionId":"s1","update":{"sessionUpdate":"bogus_kind","content":{"type":"text","text":"hi"}}}`)

	_, err := mapSessionUpdate(raw)
	if err == nil {
		t.Fatal("expected error for unknown session update kind")
	}
	want := `unknown session update kind "bogus_kind"`
	if err.Error() != want {
		t.Fatalf("error = %q, want %q", err.Error(), want)
	}
}

func TestMapPromptResponseInvalidJSON(t *testing.T) {
	raw := json.RawMessage(`{ "bad"`)

	_, err := mapPromptResponse("ses_badjson", raw)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}
