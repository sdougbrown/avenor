package opencodeacp

import (
	"encoding/json"
	"testing"
)

func TestMapPromptResponseNormalisesUsage(t *testing.T) {
	raw := json.RawMessage(`{ "stopReason": "end_turn", "usage": { "inputTokens": 10, "outputTokens": 5, "totalTokens": 15, "cachedReadTokens": 3 } }`)

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

	// Top-level camelCase keys must NOT exist (they are nested under usage)
	if _, exists := event.Fields["inputTokens"]; exists {
		t.Error("event.Fields[\"inputTokens\"] exists at top level, should be nested under usage")
	}
	if _, exists := event.Fields["outputTokens"]; exists {
		t.Error("event.Fields[\"outputTokens\"] exists at top level")
	}
	if _, exists := event.Fields["totalTokens"]; exists {
		t.Error("event.Fields[\"totalTokens\"] exists at top level")
	}
	if _, exists := event.Fields["cachedReadTokens"]; exists {
		t.Error("event.Fields[\"cachedReadTokens\"] exists at top level")
	}
}

func TestMapPromptResponseSnakeCaseFallback(t *testing.T) {
	raw := json.RawMessage(`{ "stopReason": "end_turn", "usage": { "input_tokens": 7, "output_tokens": 3, "total_tokens": 10, "cached_read_tokens": 2 } }`)

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
	if usage["cached_read_tokens"] != float64(2) {
		t.Errorf("usage.cached_read_tokens = %v, want 2", usage["cached_read_tokens"])
	}
}

func TestMapPromptResponseOmitsNilUsageKeys(t *testing.T) {
	// Only two of four usage keys present — the absent ones should be omitted.
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
	if _, exists := usage["input_tokens"]; exists {
		t.Error("usage.input_tokens should be omitted when absent")
	}
	if _, exists := usage["total_tokens"]; exists {
		t.Error("usage.total_tokens should be omitted when absent")
	}
	if _, exists := usage["cached_read_tokens"]; exists {
		t.Error("usage.cached_read_tokens should be omitted when absent")
	}
}

func TestMapPromptResponseNonMapUsageDropped(t *testing.T) {
	// When usage is not a map, it should not appear in fields.
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
	// Missing stopReason should return error.
	raw := json.RawMessage(`{ "other": "value" }`)

	_, err := mapPromptResponse("ses_nostop", raw)
	if err == nil {
		t.Fatal("expected error for missing stop_reason")
	}
}

func TestMapPromptResponseInvalidJSON(t *testing.T) {
	// Invalid JSON should return error.
	raw := json.RawMessage(`{ "bad"`)

	_, err := mapPromptResponse("ses_badjson", raw)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}
