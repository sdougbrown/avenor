package digest

import "testing"

func TestExtractStatusMarker(t *testing.T) {
	tests := []struct {
		name      string
		text      string
		wantPhase string
		wantLabel string
		wantOK    bool
	}{
		// --- basic valid cases ---
		{
			name:      "phase only",
			text:      "[status: thinking]",
			wantPhase: "thinking",
			wantLabel: "",
			wantOK:    true,
		},
		{
			name:      "phase with label",
			text:      "[status: working | Analysing failing tests]",
			wantPhase: "working",
			wantLabel: "Analysing failing tests",
			wantOK:    true,
		},
		{
			name:      "waiting with label",
			text:      "[status: waiting | Allow file write?]",
			wantPhase: "waiting",
			wantLabel: "Allow file write?",
			wantOK:    true,
		},
		{
			name:      "done phase",
			text:      "[status: done]",
			wantPhase: "done",
			wantLabel: "",
			wantOK:    true,
		},
		{
			name:      "label is trimmed",
			text:      "[status: working |   lots of spaces   ]",
			wantPhase: "working",
			wantLabel: "lots of spaces",
			wantOK:    true,
		},
		{
			name:      "marker embedded in prose",
			text:      "Starting analysis now. [status: working | Parsing source files] This may take a moment.",
			wantPhase: "working",
			wantLabel: "Parsing source files",
			wantOK:    true,
		},

		// --- case-insensitivity ---
		{
			name:      "STATUS uppercase",
			text:      "[STATUS: thinking]",
			wantPhase: "thinking",
			wantOK:    true,
		},
		{
			name:      "phase uppercase normalised",
			text:      "[status: WORKING | Build step]",
			wantPhase: "working",
			wantLabel: "Build step",
			wantOK:    true,
		},
		{
			name:      "mixed case",
			text:      "[Status: Thinking]",
			wantPhase: "thinking",
			wantOK:    true,
		},

		// --- first match wins ---
		{
			name:      "first marker wins when multiple present",
			text:      "[status: thinking] ... [status: working | second]",
			wantPhase: "thinking",
			wantOK:    true,
		},

		// --- not-ok cases ---
		{
			name:   "unknown phase ignored",
			text:   "[status: planning]",
			wantOK: false,
		},
		{
			name:   "no marker",
			text:   "just regular text with no marker",
			wantOK: false,
		},
		{
			name:   "empty string",
			text:   "",
			wantOK: false,
		},
		{
			name:   "unclosed bracket",
			text:   "[status: working | label without close",
			wantOK: false,
		},
		{
			name:   "status without colon",
			text:   "[status working]",
			wantOK: false,
		},
		{
			name:   "unrelated bracket text",
			text:   "[finding] something important",
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			phase, label, ok := ExtractStatusMarker(tt.text)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v (phase=%q label=%q)", ok, tt.wantOK, phase, label)
			}
			if !ok {
				return
			}
			if phase != tt.wantPhase {
				t.Errorf("phase = %q, want %q", phase, tt.wantPhase)
			}
			if label != tt.wantLabel {
				t.Errorf("label = %q, want %q", label, tt.wantLabel)
			}
		})
	}
}

func TestExtractStatusAngle(t *testing.T) {
	tests := []struct {
		name      string
		text      string
		wantPhase string
		wantLabel string
		wantOK    bool
	}{
		{
			name:      "angle-token status: thinking",
			text:      "<|status: thinking|>",
			wantPhase: "thinking",
			wantLabel: "",
			wantOK:    true,
		},
		{
			name:      "angle-token status: working with label",
			text:      "<|status: working | Analysing failing tests|>",
			wantPhase: "working",
			wantLabel: "Analysing failing tests",
			wantOK:    true,
		},
		{
			name:      "angle-token status: waiting with label",
			text:      "<|status: waiting | Allow file write?|>",
			wantPhase: "waiting",
			wantLabel: "Allow file write?",
			wantOK:    true,
		},
		{
			name:      "angle-token status: done",
			text:      "<|status: done|>",
			wantPhase: "done",
			wantLabel: "",
			wantOK:    true,
		},
		{
			name:      "angle-token label is trimmed",
			text:      "<|status: working |   lots of spaces   |>",
			wantPhase: "working",
			wantLabel: "lots of spaces",
			wantOK:    true,
		},
		{
			name:      "angle-token label may contain pipe and greater-than",
			text:      "<|status: waiting | compare a |> b | c|>",
			wantPhase: "waiting",
			wantLabel: "compare a |> b | c",
			wantOK:    true,
		},
		{
			name:      "case insensitive STATUS angle",
			text:      "<|STATUS: THINKING|>",
			wantPhase: "thinking",
			wantOK:    true,
		},
		{
			name:   "angle-token inline in prose ignored",
			text:   "output says <|status: working|>",
			wantOK: false,
		},
		{
			name:      "angle-token full-line only",
			text:      "some text\n<|status: working | test|>\nmore text",
			wantPhase: "working",
			wantLabel: "test",
			wantOK:    true,
		},
		{
			name:      "first full-line angle wins",
			text:      "<|status: thinking|>\n<|status: working | second|>",
			wantPhase: "thinking",
			wantOK:    true,
		},
		{
			name:   "unknown phase rejected",
			text:   "<|status: planning|>",
			wantOK: false,
		},
		{
			name:   "no matching token",
			text:   "just regular text",
			wantOK: false,
		},
		{
			name:   "empty string",
			text:   "",
			wantOK: false,
		},
		{
			name:   "legacy bracket not matched by angle extractor",
			text:   "[status: working | legacy]",
			wantOK: false,
		},
		{
			name:      "angle wins over legacy bracket when both present",
			text:      "<|status: thinking|>\n[status: working | legacy]",
			wantPhase: "thinking",
			wantOK:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			phase, label, ok := ExtractStatusAngle(tt.text)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v (phase=%q label=%q)", ok, tt.wantOK, phase, label)
			}
			if !ok {
				return
			}
			if phase != tt.wantPhase {
				t.Errorf("phase = %q, want %q", phase, tt.wantPhase)
			}
			if label != tt.wantLabel {
				t.Errorf("label = %q, want %q", label, tt.wantLabel)
			}
		})
	}
}
