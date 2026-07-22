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

func TestFinalOutputTruncated(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"empty", "", false},
		{"short", "hello", false},
		{"exactly MaxFinalOutputRunes", strings.Repeat("a", MaxFinalOutputRunes), false},
		{"one over MaxFinalOutputRunes", strings.Repeat("a", MaxFinalOutputRunes+1), true},
		{"multibyte at limit", strings.Repeat("é", MaxFinalOutputRunes), false},
		{"multibyte over limit", strings.Repeat("é", MaxFinalOutputRunes+1), true},
		{"mixed ascii and multibyte at limit", strings.Repeat("abé", MaxFinalOutputRunes/3) + strings.Repeat("a", MaxFinalOutputRunes%3), false},
		{"emoji over limit", strings.Repeat("🔥", MaxFinalOutputRunes+1), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FinalOutputTruncated(tt.input); got != tt.want {
				t.Errorf("FinalOutputTruncated(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
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
	if bounded.Fields["final_output_truncated"] != true {
		t.Fatalf("final_output_truncated = %v, want true", bounded.Fields["final_output_truncated"])
	}
	if original.Fields["final_output"] != text {
		t.Fatal("BoundFinalOutput mutated the input event")
	}
	if _, ok := original.Fields["final_output_truncated"]; ok {
		t.Fatal("BoundFinalOutput mutated the input metadata")
	}
}
