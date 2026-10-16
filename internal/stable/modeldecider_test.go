package stable

import (
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/workflow"
)

func TestBuildModelDecisionPromptFramesUntrustedData(t *testing.T) {
	system, user := buildModelDecisionPrompt("worker-1", "clean the build dir", map[string]any{
		"tool":    "bash",
		"command": "rm -rf /tmp/build",
	}, []byte(`[{"optionId":"allow_once","kind":"allow"}]`))
	if !strings.Contains(system, `"decision"`) || !strings.Contains(system, "defer") {
		t.Fatalf("system prompt missing the decision contract:\n%s", system)
	}
	if !strings.Contains(system, "untrusted worker-supplied data") {
		t.Fatalf("system prompt must frame request data as untrusted:\n%s", system)
	}
	for _, want := range []string{"worker-1", "clean the build dir", "rm -rf /tmp/build", "allow_once"} {
		if !strings.Contains(user, want) {
			t.Fatalf("user prompt missing %q:\n%s", want, user)
		}
	}
	// A worker cannot smuggle instructions into the system prompt: the
	// request data only ever lands in the user message.
	if strings.Contains(system, "rm -rf /tmp/build") {
		t.Fatal("request data must not appear in the system prompt")
	}
}

func TestBuildModelDecisionPromptWithoutRequestMirror(t *testing.T) {
	_, user := buildModelDecisionPrompt("w", "task", nil, []byte(`[]`))
	if !strings.Contains(user, "unavailable") {
		t.Fatalf("user prompt should note the missing request mirror:\n%s", user)
	}
}

func TestParseModelDecision(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		decision string
		reason   string
		wantErr  bool
	}{
		{"plain json", `{"decision":"allow","reason":"needed"}`, "allow", "needed", false},
		{"fenced", "Here you go:\n```json\n{\"decision\": \"reject\", \"reason\": \"destructive\"}\n```", "reject", "destructive", false},
		{"surrounded", `Sure! {"decision":"defer","reason":"unclear"} hope that helps`, "defer", "unclear", false},
		{"approve synonym", `{"decision":"Approve","reason":""}`, "allow", "", false},
		{"deny synonym", `{"decision":"DENY","reason":"no"}`, "reject", "no", false},
		{"unknown decision", `{"decision":"maybe"}`, "", "", true},
		{"no json", "I cannot answer that in JSON", "", "", true},
		{"unbalanced", `{"decision":"allow"`, "", "", true},
		{"wrong shape", `{"verdict":"allow"}`, "", "", true},
		{"trailing stray brace", `{"decision":"allow","reason":"ok"} }`, "allow", "ok", false},
		{"second trailing object", `{"decision":"defer"} {"decision":"allow"}`, "defer", "", false},
		{"prose braces before", `use { or } carefully: {"decision":"allow","reason":"needed"}`, "allow", "needed", false},
	}
	for _, tc := range cases {
		decision, reason, err := parseModelDecision(tc.raw)
		if tc.wantErr {
			if err == nil {
				t.Errorf("%s: want error, got %q/%q", tc.name, decision, reason)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if decision != tc.decision || reason != tc.reason {
			t.Errorf("%s: got %q/%q, want %q/%q", tc.name, decision, reason, tc.decision, tc.reason)
		}
	}
}

func TestModelDecisionOption(t *testing.T) {
	options := []any{
		map[string]any{"optionId": "reject_once", "kind": "reject"},
		map[string]any{"optionId": "allow_once", "kind": "allow"},
		map[string]any{"optionId": "allow_always", "kind": "allow_always"},
		map[string]any{"optionId": "deny", "kind": "deny"},
	}
	if id, requiresMessage := modelDecisionOption(options, "allow"); id != "allow_once" || requiresMessage {
		t.Fatalf("allow option = %q/%v, want the once-style allow option allow_once", id, requiresMessage)
	}
	// When the backend lists the persistent grant first, the once-style
	// variant still wins (least privilege).
	if id, _ := modelDecisionOption([]any{
		map[string]any{"optionId": "allow_always", "kind": "allow_always"},
		map[string]any{"optionId": "allow_once", "kind": "allow_once"},
	}, "allow"); id != "allow_once" {
		t.Fatalf("allow option = %q, want allow_once preferred over allow_always", id)
	}
	if id, _ := modelDecisionOption(options, "reject"); id != "reject_once" {
		t.Fatalf("reject option = %q, want reject_once", id)
	}
	// Backends that spell the kind "deny" normalize to reject.
	if id, _ := modelDecisionOption([]any{map[string]any{"optionId": "deny", "kind": "deny"}}, "reject"); id != "deny" {
		t.Fatalf("deny option = %q, want deny (normalized kind)", id)
	}
	if id, _ := modelDecisionOption(options, "defer"); id != "" {
		t.Fatalf("defer option = %q, want none (defer is never applied)", id)
	}
	if id, requiresMessage := modelDecisionOption([]any{
		map[string]any{"optionId": "allow", "kind": "allow", "requiresMessage": true},
	}, "allow"); id != "allow" || !requiresMessage {
		t.Fatalf("requiresMessage option = %q/%v, want allow/true", id, requiresMessage)
	}
	// An option without an optionId cannot be answered.
	if id, _ := modelDecisionOption([]any{map[string]any{"kind": "allow"}}, "allow"); id != "" {
		t.Fatalf("optionless entry = %q, want none", id)
	}
}

func TestTruncateRunes(t *testing.T) {
	if got := truncateRunes("short", 10); got != "short" {
		t.Fatalf("truncateRunes = %q", got)
	}
	if got := truncateRunes(strings.Repeat("é", 50), 10); len([]rune(got)) != 10 {
		t.Fatalf("truncateRunes rune bound = %d runes", len([]rune(got)))
	}
}

func TestReasonSuffix(t *testing.T) {
	if got := reasonSuffix(""); got != "" {
		t.Fatalf("reasonSuffix(empty) = %q", got)
	}
	if got := reasonSuffix("because"); got != " (because)" {
		t.Fatalf("reasonSuffix = %q", got)
	}
	long := reasonSuffix(strings.Repeat("x", 10_000))
	if len([]rune(long)) > maxModelDeciderReasonRunes+3 {
		t.Fatalf("reasonSuffix not bounded: %d runes", len([]rune(long)))
	}
}

// TestResolvePermissionModelDeciderKey pins the key-resolution contract: the
// pinned api_key_env wins (the key always matches the variable the status
// surfaces), a pinned env that is unset fails fast, and a provider with a
// literal key resolves it from the registry without any environment.
func TestResolvePermissionModelDeciderKey(t *testing.T) {
	writeProviderRegistry(t, `{
		"providers": {
			"env":    {"base_url": "http://x/v1", "api_key_env": "MODELDEC_TEST_KEY"},
			"lit":    {"base_url": "http://x/v1", "api_key": "literal-secret"},
			"rotated": {"base_url": "http://x/v1", "api_key_env": "MODELDEC_ROTATED_KEY"}
		}
	}`)
	t.Setenv("MODELDEC_TEST_KEY", "from-env")

	key, err := resolvePermissionModelDeciderKey(&workflow.PermissionModelDecider{Provider: "env", APIKeyEnv: "MODELDEC_TEST_KEY"})
	if err != nil || key != "from-env" {
		t.Fatalf("pinned env key = %q, %v; want from-env", key, err)
	}
	key, err = resolvePermissionModelDeciderKey(&workflow.PermissionModelDecider{Provider: "lit"})
	if err != nil || key != "literal-secret" {
		t.Fatalf("literal key = %q, %v; want literal-secret", key, err)
	}
	if _, err := resolvePermissionModelDeciderKey(&workflow.PermissionModelDecider{Provider: "rotated", APIKeyEnv: "MODELDEC_ROTATED_KEY"}); err == nil {
		t.Fatal("a pinned env that is unset must fail fast")
	}
	// The registry entry drifting from the pin (an env var renamed between
	// dispatch and spawn) must not silently authenticate with the new
	// variable while status still shows the old name: the pinned name rules.
	if _, err := resolvePermissionModelDeciderKey(&workflow.PermissionModelDecider{Provider: "env", APIKeyEnv: "MODELDEC_ROTATED_KEY"}); err == nil {
		t.Fatal("pinned env unset even though the registry names another env must fail")
	}
}

// TestParseModelDecisionSkipsUndecidableObjects proves the scan does not
// stop at the first decodable object whose decision is outside the
// vocabulary: a verbose completion carrying an unrelated object before the
// real answer still yields the real answer instead of burning a retry.
func TestParseModelDecisionSkipsUndecidableObjects(t *testing.T) {
	raw := `Analysis: {"tool":"bash"} and then {"decision":"reject","reason":"destructive"}`
	decision, reason, err := parseModelDecision(raw)
	if err != nil || decision != "reject" || reason != "destructive" {
		t.Fatalf("parseModelDecision = %q/%q, %v; want reject/destructive", decision, reason, err)
	}
}

// TestReasonSuffixStripsControlCharacters proves model-supplied reasons
// cannot carry terminal escape sequences into stderr logs: C0/C1 control
// bytes are stripped, length still bounded.
func TestReasonSuffixStripsControlCharacters(t *testing.T) {
	for _, tc := range []struct{ name, in, want string }{
		{"ansi escape", "a\x1b[31mb", " (a[31mb)"},
		{"del", "a\x7fb", " (ab)"},
		{"c1 byte decodes to replacement", "a\x9bb", " (ab)"},
		{"lone newline", "a\nb", " (ab)"},
	} {
		if got := reasonSuffix(tc.in); got != tc.want {
			t.Errorf("%s: reasonSuffix(%q) = %q, want %q", tc.name, tc.in, got, tc.want)
		}
	}
}
