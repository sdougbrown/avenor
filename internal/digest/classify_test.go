package digest

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func parseEvent(t *testing.T, raw string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("parseEvent: %v", err)
	}
	return m
}

func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		// --- MILESTONE cases ---
		{
			name: "session.end is always MILESTONE",
			raw:  `{"event":"session.end","session_id":"s1","stop_reason":"end_turn"}`,
			want: TagMilestone,
		},
		{
			name: "session.renamed is MILESTONE",
			raw:  `{"event":"session.renamed","run_id":"rt_1","runtime_id":"rt_1","label":"main","previous_label":"supervisor"}`,
			want: TagMilestone,
		},
		{
			name: "permission.request is always MILESTONE",
			raw:  `{"event":"permission.request","session_id":"s1","question":"Allow exec?","tool":"exec"}`,
			want: TagMilestone,
		},
		{
			name: "tool.call kind=commit is MILESTONE",
			raw:  `{"event":"tool.call","session_id":"s1","kind":"commit","title":"main.go"}`,
			want: TagMilestone,
		},
		{
			name: "tool.call_update kind=commit is MILESTONE",
			raw:  `{"event":"tool.call_update","session_id":"s1","kind":"commit","status":"completed"}`,
			want: TagMilestone,
		},
		{
			name: "tool.call kind=task status=completed is MILESTONE",
			raw:  `{"event":"tool.call","session_id":"s1","kind":"task","status":"completed","title":"phase done"}`,
			want: TagMilestone,
		},
		{
			name: "tool.call title contains git commit is MILESTONE",
			raw:  `{"event":"tool.call","session_id":"s1","kind":"shell","title":"git commit -m fix","status":"running"}`,
			want: TagMilestone,
		},
		{
			name: "tool.call command contains git commit is MILESTONE",
			raw:  `{"event":"tool.call","session_id":"s1","kind":"shell","command":"git commit -am squash","title":"commit"}`,
			want: TagMilestone,
		},
		{
			name: "tool.call title git commit case-insensitive is MILESTONE",
			raw:  `{"event":"tool.call","session_id":"s1","kind":"shell","title":"Git Commit -m wip"}`,
			want: TagMilestone,
		},

		// --- FINDING cases ---
		{
			name: "agent.message_chunk with [finding] prefix is FINDING",
			raw:  `{"event":"agent.message_chunk","session_id":"s1","content":{"text":"[finding] the logic is wrong","type":"text"}}`,
			want: TagFinding,
		},
		{
			name: "[FINDING] uppercase is FINDING",
			raw:  `{"event":"agent.message_chunk","session_id":"s1","content":{"text":"[FINDING] something important","type":"text"}}`,
			want: TagFinding,
		},
		{
			name: "agent.message_chunk with reviewer flagged is FINDING",
			raw:  `{"event":"agent.message_chunk","session_id":"s1","content":{"text":"reviewer flagged this function","type":"text"}}`,
			want: TagFinding,
		},
		{
			name: "agent.message_chunk with correction needed is FINDING",
			raw:  `{"event":"agent.message_chunk","session_id":"s1","content":{"text":"correction needed in the auth flow","type":"text"}}`,
			want: TagFinding,
		},
		{
			name: "agent.message_chunk with failed test is FINDING",
			raw:  `{"event":"agent.message_chunk","session_id":"s1","content":{"text":"failed test in TestFoo","type":"text"}}`,
			want: TagFinding,
		},
		{
			name: "thought_chunk with confidence >= 60 is FINDING",
			raw:  `{"event":"agent.thought_chunk","session_id":"s1","content":{"text":"confidence: 80% this is correct","type":"text"}}`,
			want: TagFinding,
		},
		{
			name: "user.message_chunk with confidence exactly 60 is FINDING",
			raw:  `{"event":"user.message_chunk","session_id":"s1","content":{"text":"confidence: 60 this is enough","type":"text"}}`,
			want: TagFinding,
		},
		{
			name: "confidence 100% is FINDING",
			raw:  `{"event":"agent.message_chunk","session_id":"s1","content":{"text":"Confidence: 100%","type":"text"}}`,
			want: TagFinding,
		},

		// --- avenor synthetic event cases ---
		{
			name: "avenor.retry is MILESTONE",
			raw:  `{"event":"avenor.retry","session_id":"s1","attempt":2,"max_retries":3}`,
			want: TagMilestone,
		},
		{
			name: "avenor.error is MILESTONE",
			raw:  `{"event":"avenor.error","session_id":"s1","source":"permission","message":"handler timed out"}`,
			want: TagMilestone,
		},

		// --- agent.status cases ---
		{
			name: "agent.status phase=done is MILESTONE",
			raw:  `{"event":"agent.status","session_id":"s1","phase":"done","source":"avenor"}`,
			want: TagMilestone,
		},
		{
			name: "agent.status phase=waiting is MILESTONE",
			raw:  `{"event":"agent.status","session_id":"s1","phase":"waiting","label":"Allow exec?","source":"avenor"}`,
			want: TagMilestone,
		},
		{
			name: "agent.status phase=thinking is ACTIVITY",
			raw:  `{"event":"agent.status","session_id":"s1","phase":"thinking","source":"avenor"}`,
			want: TagActivity,
		},
		{
			name: "agent.status phase=working is ACTIVITY",
			raw:  `{"event":"agent.status","session_id":"s1","phase":"working","label":"go test ./...","source":"avenor"}`,
			want: TagActivity,
		},

		// --- ACTIVITY cases ---
		{
			name: "session.plan is ACTIVITY",
			raw:  `{"event":"session.plan","session_id":"s1","title":"Run tests"}`,
			want: TagActivity,
		},
		{
			name: "tool.call kind=shell is ACTIVITY",
			raw:  `{"event":"tool.call","session_id":"s1","kind":"shell","title":"go test ./...","status":"running"}`,
			want: TagActivity,
		},
		{
			name: "tool.call kind=task not completed is ACTIVITY",
			raw:  `{"event":"tool.call","session_id":"s1","kind":"task","status":"running","title":"building"}`,
			want: TagActivity,
		},
		{
			name: "agent.message_chunk routine text is ACTIVITY",
			raw:  `{"event":"agent.message_chunk","session_id":"s1","content":{"text":"reading file main.go","type":"text"}}`,
			want: TagActivity,
		},
		{
			name: "unknown event is ACTIVITY",
			raw:  `{"event":"session.started","session_id":"s1"}`,
			want: TagActivity,
		},
		{
			name: "empty content is ACTIVITY",
			raw:  `{"event":"agent.message_chunk","session_id":"s1","content":{"text":"","type":"text"}}`,
			want: TagActivity,
		},

		// --- FINDING edge cases: conservative heuristics ---
		{
			name: "confidence 59 is ACTIVITY (below threshold)",
			raw:  `{"event":"agent.message_chunk","session_id":"s1","content":{"text":"confidence: 59%","type":"text"}}`,
			want: TagActivity,
		},
		{
			name: "confidence 0 is ACTIVITY",
			raw:  `{"event":"agent.message_chunk","session_id":"s1","content":{"text":"confidence: 0%","type":"text"}}`,
			want: TagActivity,
		},
		{
			name: "single word 'error' alone is ACTIVITY (not a finding signal)",
			raw:  `{"event":"agent.message_chunk","session_id":"s1","content":{"text":"error: file not found","type":"text"}}`,
			want: TagActivity,
		},
		{
			name: "single word 'broken' alone is ACTIVITY",
			raw:  `{"event":"agent.message_chunk","session_id":"s1","content":{"text":"the build is broken","type":"text"}}`,
			want: TagActivity,
		},
		{
			name: "single word 'found' alone is ACTIVITY",
			raw:  `{"event":"agent.message_chunk","session_id":"s1","content":{"text":"found 3 files","type":"text"}}`,
			want: TagActivity,
		},
		{
			name: "array content (non-map) is ACTIVITY",
			raw:  `{"event":"user.message_chunk","session_id":"s1","content":[{"text":"[finding] array form"}]}`,
			want: TagActivity,
		},
		{
			name: "confidence without colon is ACTIVITY (no match)",
			raw:  `{"event":"agent.message_chunk","session_id":"s1","content":{"text":"high confidence at 85%","type":"text"}}`,
			want: TagActivity,
		},

		// --- regex: multi-match and word-boundary ---
		{
			// Finding 1: first match is low (40), second is high (80) — must return FINDING.
			name: "multi-confidence low-then-high returns FINDING",
			raw:  `{"event":"agent.thought_chunk","session_id":"s1","content":{"text":"confidence: 40% on retry... confidence: 80% after review","type":"text"}}`,
			want: TagFinding,
		},
		{
			// Finding 2: "60ms" must not match — the "m" is adjacent to digits.
			name: "confidence with ms suffix is ACTIVITY (word boundary prevents match)",
			raw:  `{"event":"agent.message_chunk","session_id":"s1","content":{"text":"confidence: 60ms latency observed","type":"text"}}`,
			want: TagActivity,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := parseEvent(t, tt.raw)
			got := Classify(event)
			if got != tt.want {
				t.Fatalf("Classify() = %q, want %q (input: %s)", got, tt.want, tt.raw)
			}
		})
	}
}

// TestStreamClassifyPrefix verifies that when opts.Classify is true, plain-format
// output has the tag as the first token on each line.
func TestStreamClassifyPrefix(t *testing.T) {
	input := strings.Join([]string{
		// MILESTONE: session.end
		`{"event":"session.end","session_id":"s1","stop_reason":"end_turn"}`,
		// FINDING: confidence >= 60
		`{"event":"agent.message_chunk","session_id":"s1","content":{"text":"confidence: 75% this looks right","type":"text"}}`,
		// ACTIVITY: plain tool.call
		`{"event":"tool.call","session_id":"s1","kind":"shell","title":"go test","status":"running"}`,
	}, "\n") + "\n"

	var out bytes.Buffer
	if err := Stream(strings.NewReader(input), &out, Options{Classify: true}); err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("Stream() emitted %d lines, want 3;\n%s", len(lines), out.String())
	}

	cases := []struct {
		wantPrefix string
		line       string
	}{
		{"MILESTONE EVENT session.end", lines[0]},
		{"FINDING EVENT agent.message_chunk", lines[1]},
		{"ACTIVITY EVENT tool.call", lines[2]},
	}
	for _, c := range cases {
		if !strings.HasPrefix(c.line, c.wantPrefix) {
			t.Errorf("line %q does not start with %q", c.line, c.wantPrefix)
		}
	}
}

// TestStreamClassifyOffByDefault verifies that without --classify the output
// is identical to the pre-existing plain format (no tag prefix).
func TestStreamClassifyOffByDefault(t *testing.T) {
	input := `{"event":"session.end","session_id":"s1","stop_reason":"end_turn"}` + "\n"

	var out bytes.Buffer
	if err := Stream(strings.NewReader(input), &out, Options{}); err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	line := strings.TrimRight(out.String(), "\n")
	if strings.HasPrefix(line, "MILESTONE") || strings.HasPrefix(line, "ACTIVITY") || strings.HasPrefix(line, "FINDING") {
		t.Fatalf("Stream() with Classify=false emitted tag prefix: %q", line)
	}
	if !strings.HasPrefix(line, "EVENT session.end") {
		t.Fatalf("Stream() = %q, want prefix %q", line, "EVENT session.end")
	}
}

// TestStreamClassifyJSONAddsField verifies that in JSON format, classify=true
// adds a "classify" field to each emitted object.
func TestStreamClassifyJSONAddsField(t *testing.T) {
	input := strings.Join([]string{
		`{"event":"session.end","session_id":"s1","stop_reason":"end_turn"}`,
		`{"event":"tool.call","session_id":"s1","kind":"shell","title":"go test"}`,
	}, "\n") + "\n"

	var out bytes.Buffer
	if err := Stream(strings.NewReader(input), &out, Options{Format: "json", Classify: true}); err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("Stream() emitted %d lines, want 2;\n%s", len(lines), out.String())
	}

	wantClassify := []string{TagMilestone, TagActivity}
	for i, line := range lines {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("line %d: json.Unmarshal error = %v", i, err)
		}
		got, ok := m["classify"].(string)
		if !ok {
			t.Fatalf("line %d: missing or non-string 'classify' field in %s", i, line)
		}
		if got != wantClassify[i] {
			t.Fatalf("line %d: classify=%q, want %q", i, got, wantClassify[i])
		}
		// Original "event" field must survive the round-trip.
		if _, hasEvent := m["event"]; !hasEvent {
			t.Fatalf("line %d: 'event' field missing after classify injection", i)
		}
		// Non-event fields must also survive (session_id is present on both input lines).
		if sid, ok := m["session_id"].(string); !ok || sid != "s1" {
			t.Fatalf("line %d: session_id=%q, want %q after classify injection", i, m["session_id"], "s1")
		}
	}
}

// TestStreamClassifyWithCursor verifies that when both Classify and CursorPath
// are active together, output lines are correctly tagged AND the cursor advances
// to the expected byte offset (len of input), using the same cursor-assertion
// pattern as digest_test.go.
func TestStreamClassifyWithCursor(t *testing.T) {
	input := strings.Join([]string{
		// MILESTONE
		`{"event":"session.end","session_id":"s1","stop_reason":"end_turn"}`,
		// FINDING: confidence >= 60
		`{"event":"agent.thought_chunk","session_id":"s1","content":{"text":"confidence: 75%","type":"text"}}`,
		// ACTIVITY
		`{"event":"tool.call","session_id":"s1","kind":"shell","title":"go build"}`,
	}, "\n") + "\n"

	dir := t.TempDir()
	cursorPath := filepath.Join(dir, "cursor")

	var out bytes.Buffer
	opts := Options{
		Classify:          true,
		CursorPath:        cursorPath,
		CursorStartOffset: 0,
	}
	if err := Stream(strings.NewReader(input), &out, opts); err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	// Assert tags.
	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("Stream() emitted %d lines, want 3;\n%s", len(lines), out.String())
	}
	wantPrefixes := []string{
		"MILESTONE EVENT session.end",
		"FINDING EVENT agent.thought_chunk",
		"ACTIVITY EVENT tool.call",
	}
	for i, wantPfx := range wantPrefixes {
		if !strings.HasPrefix(lines[i], wantPfx) {
			t.Errorf("line %d: %q does not start with %q", i, lines[i], wantPfx)
		}
	}

	// Assert cursor advanced to len(input).
	raw, err := os.ReadFile(cursorPath)
	if err != nil {
		t.Fatalf("readCursorFile: %v", err)
	}
	gotOffset, err := strconv.ParseInt(strings.TrimRight(string(raw), "\n"), 10, 64)
	if err != nil {
		t.Fatalf("parseCursorOffset: %v", err)
	}
	if gotOffset != int64(len(input)) {
		t.Fatalf("cursor offset = %d, want %d", gotOffset, int64(len(input)))
	}
}
