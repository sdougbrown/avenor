package events

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestCloneCopiesFields(t *testing.T) {
	original := Event{Event: "agent.status", SessionID: "ses_1", Fields: map[string]any{"phase": "working"}}
	cloned := Clone(original)
	cloned.Fields["phase"] = "done"

	if got := original.Fields["phase"]; got != "working" {
		t.Fatalf("original phase = %v, want working", got)
	}
}

func TestInt64AcceptsJSONNumber(t *testing.T) {
	got, ok := Int64(json.Number("42"))
	if !ok || got != 42 {
		t.Fatalf("Int64(json.Number(42)) = %d, %v, want 42, true", got, ok)
	}
	if _, ok := Int64(json.Number("4.2")); ok {
		t.Fatal("Int64 accepted a non-integer json.Number")
	}
}

func TestBoundFinalOutputUsesRuneLimitWithoutMutatingInput(t *testing.T) {
	text := strings.Repeat("é", MaxFinalOutputRunes+10)
	original := Event{Event: "session.end", Fields: map[string]any{"final_output": text}}
	bounded := BoundFinalOutput(original)

	got, _ := bounded.Fields["final_output"].(string)
	if count := utf8.RuneCountInString(got); count != MaxFinalOutputRunes {
		t.Fatalf("bounded rune count = %d, want %d", count, MaxFinalOutputRunes)
	}
	if original.Fields["final_output"] != text {
		t.Fatal("BoundFinalOutput mutated the input event")
	}
}
