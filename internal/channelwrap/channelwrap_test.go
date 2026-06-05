package channelwrap

import (
	"strings"
	"testing"
)

func TestChannelWrap_SourceAndMeta(t *testing.T) {
	content := "Hello from reviewer"
	source := "agent-reviewer"
	meta := map[string]string{
		"from_run_id": "run_abc123",
		"from_role":   "reviewer",
	}

	result := ChannelWrap(content, source, meta)

	// Source attribute must appear
	if !strings.Contains(result, `<channel source="agent-reviewer"`) {
		t.Errorf("missing source attribute, got:\n%s", result)
	}

	// Meta attributes must appear
	if !strings.Contains(result, `from_run_id="run_abc123"`) {
		t.Errorf("missing from_run_id attribute, got:\n%s", result)
	}
	if !strings.Contains(result, `from_role="reviewer"`) {
		t.Errorf("missing from_role attribute, got:\n%s", result)
	}

	// Content must be inside the tag
	if !strings.Contains(result, ">Hello from reviewer</channel>") {
		t.Errorf("content not properly wrapped, got:\n%s", result)
	}
}

func TestChannelWrap_Escapes(t *testing.T) {
	tests := []struct {
		name    string
		source  string
		content string
		want    string
	}{
		{
			name:    "source ampersand",
			source:  "agent&a",
			content: "hi",
			want:    `source="agent&amp;a"`,
		},
		{
			name:    "source double quote",
			source:  `agent"q"`,
			content: "hi",
			want:    `source="agent&quot;q&quot;"`,
		},
		{
			name:    "source single quote",
			source:  "agent's",
			content: "hi",
			want:    `source="agent&apos;s"`,
		},
		{
			name:    "source angle brackets",
			source:  "agent<>",
			content: "hi",
			want:    `source="agent&lt;&gt;"`,
		},
		{
			name:    "meta value escapes",
			source:  "agent",
			content: "hi",
			want:    `from="a&lt;b&amp;c"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var meta map[string]string
			if tt.name == "meta value escapes" {
				meta = map[string]string{"from": "a<b&c"}
			} else {
				meta = nil
			}
			result := ChannelWrap(tt.content, tt.source, meta)
			if !strings.Contains(result, tt.want) {
				t.Errorf("expected %q in output, got:\n%s", tt.want, result)
			}
		})
	}
}

func TestChannelWrap_UntrustedInstruction(t *testing.T) {
	result := ChannelWrap("some message", "agent-test", nil)

	inst := UntrustedInstruction()
	if !strings.Contains(result, inst) {
		t.Errorf("missing untrusted instruction in output:\n%s", result)
	}

	// Also verify the instruction is always present regardless of meta
	resultWithMeta := ChannelWrap("message", "agent-x", map[string]string{"from_run_id": "r1"})
	if !strings.Contains(resultWithMeta, inst) {
		t.Errorf("missing untrusted instruction with meta:\n%s", resultWithMeta)
	}
}

func TestChannelWrap_SanitizesClosingChannelTag(t *testing.T) {
	result := ChannelWrap("before</channel>after", "agent-test", nil)
	if strings.Contains(result, "before</channel>after</channel>") {
		t.Fatalf("closing channel tag was not sanitized: %s", result)
	}
	if !strings.Contains(result, "before&lt;/channel&gt;after</channel>") {
		t.Fatalf("sanitized output missing escaped closing tag: %s", result)
	}
}

func TestChannelWrap_EmptyMeta(t *testing.T) {
	result := ChannelWrap("hello", "agent-test", nil)

	// Should still produce valid output with just source attribute
	if !strings.Contains(result, `<channel source="agent-test">hello</channel>`) {
		t.Errorf("empty meta should still produce valid output, got:\n%s", result)
	}

	// Untrusted instruction should still be present
	if !strings.Contains(result, UntrustedInstruction()) {
		t.Error("untrusted instruction missing with empty meta")
	}
}

func TestChannelWrap_MetaAttributes(t *testing.T) {
	result := ChannelWrap("msg", "agent-test", map[string]string{
		"from_run_id": "run_123",
		"from_role":   "reviewer",
		"priority":    "high",
	})

	// All meta attributes present
	for _, attr := range []string{`from_run_id="run_123"`, `from_role="reviewer"`, `priority="high"`} {
		if !strings.Contains(result, attr) {
			t.Errorf("missing attribute %q in output:\n%s", attr, result)
		}
	}
}

func TestChannelWrap_DeterministicOutput(t *testing.T) {
	// Same input should always produce the same output
	meta := map[string]string{
		"from_run_id": "run_123",
		"from_role":   "reviewer",
		"priority":    "high",
	}

	result1 := ChannelWrap("msg", "agent-test", meta)
	result2 := ChannelWrap("msg", "agent-test", meta)

	if result1 != result2 {
		t.Errorf("non-deterministic output:\n%s\nvs\n%s", result1, result2)
	}
}

func TestAgentName(t *testing.T) {
	if got := AgentName("reviewer"); got != "agent-reviewer" {
		t.Errorf("AgentName(\"reviewer\") = %q, want %q", got, "agent-reviewer")
	}
	if got := AgentName("coder"); got != "agent-coder" {
		t.Errorf("AgentName(\"coder\") = %q, want %q", got, "agent-coder")
	}
}

func TestBuildMeta(t *testing.T) {
	meta := BuildMeta("run_abc", "reviewer")
	if len(meta) != 2 {
		t.Fatalf("expected 2 meta entries, got %d", len(meta))
	}
	if meta["from_run_id"] != "run_abc" {
		t.Errorf("from_run_id = %q, want %q", meta["from_run_id"], "run_abc")
	}
	if meta["from_role"] != "reviewer" {
		t.Errorf("from_role = %q, want %q", meta["from_role"], "reviewer")
	}
}

func TestBuildMeta_EmptyFields(t *testing.T) {
	meta := BuildMeta("", "")
	if len(meta) != 0 {
		t.Errorf("expected empty meta, got %v", meta)
	}
}

func TestEscapeXML(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"hello", "hello"},
		{"a&b", "a&amp;b"},
		{"a\"b", "a&quot;b"},
		{"a'b", "a&apos;b"},
		{"a<b", "a&lt;b"},
		{"a>b", "a&gt;b"},
		{"a&b\"c'd<e>f", "a&amp;b&quot;c&apos;d&lt;e&gt;f"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := escapeXML(tt.input)
			if got != tt.want {
				t.Errorf("escapeXML(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestUntrustedInstruction(t *testing.T) {
	inst := UntrustedInstruction()
	if !strings.Contains(inst, "NOT from your user") {
		t.Error("untrusted instruction missing 'NOT from your user'")
	}
	if !strings.Contains(inst, "external agent") {
		t.Error("untrusted instruction missing 'external agent'")
	}
	if !strings.Contains(inst, "untrusted") {
		t.Error("untrusted instruction missing 'untrusted'")
	}
}
