package messaging

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestRunLabelTailAndLabelWithRunTail(t *testing.T) {
	rows := []struct{ name, label, runID, want string }{
		{"the last six letters and digits", "claude-avenor", "claude-a001f5", "claude-avenor-a001f5"},
		{"a longer run id keeps its last six", "claude-avenor", "rt_child_0123456789", "claude-avenor-456789"},
		{"a run id with no tail leaves the label", "claude-avenor", "---", "claude-avenor"},
		{"a long label is cut to leave room", "claude-" + strings.Repeat("a", 57), "claude-a001f5", "claude-" + strings.Repeat("a", 50) + "-a001f5"},
		{"a dash at the cut is trimmed", strings.Repeat("a", 56) + "-bbbbbbbb", "claude-a001f5", strings.Repeat("a", 56) + "-a001f5"},
		{"nothing left of the label", "-", "claude-a001f5", ""},
		{"a multibyte label is cut by runes", strings.Repeat("日", 64), "claude-a001f5", strings.Repeat("日", 57) + "-a001f5"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			got := LabelWithRunTail(row.label, row.runID)
			if got != row.want {
				t.Fatalf("LabelWithRunTail(%q, %q) = %q, want %q", row.label, row.runID, got, row.want)
			}
			if got != "" && !ValidSessionName(got) {
				t.Fatalf("%q does not pass the name rule", got)
			}
			if n := utf8.RuneCountInString(got); n > MaxSessionNameRunes {
				t.Fatalf("%q is %d runes", got, n)
			}
		})
	}
}
