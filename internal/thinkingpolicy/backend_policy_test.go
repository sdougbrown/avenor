package thinkingpolicy

import (
	"testing"
)

func TestBackendPolicyGenerated(t *testing.T) {
	cases := []struct {
		backend string
		value   *string
		resume  bool
		fair    bool
		valid   bool
	}{
		// Empty value (nil pointer) is always fair (all backends)
		{"codex-app-server", nil, false, true, false},
		{"agy", nil, false, true, false},
		{"claude", nil, true, true, false},

		// Codex/Pi: all canonical values, start and resume
		{"codex-app-server", str("off"), false, true, true},
		{"codex-app-server", str("max"), true, true, true},
		{"pi", str("minimal"), false, true, true},
		{"pi", str("xhigh"), true, true, true},

		// Claude: low..max on start only
		{"claude", str("low"), false, true, true},
		{"claude", str("max"), false, true, true},
		{"claude", str("off"), false, false, true}, // off not supported
		{"claude", str("low"), true, false, true},   // start-only
		{"claude-channel", str("medium"), false, true, true},
		{"claude-channel", str("low"), true, false, true}, // start-only

		// Backends with no thinking support
		{"agy", str("low"), false, false, true},
		{"opencode-acp", str("off"), false, false, true},
		{"pony", str("high"), true, false, true},

		// Non-canonical values: valid=false, fair depends on backend
		{"codex-app-server", str("HIGH"), false, false, false},
		{"agy", str("auto"), false, false, false},
	}

	for _, tc := range cases {
		avail := Check(
			ThinkingPolicyFields{Thinking: tc.value},
			ThinkingPolicyConditions{Backend: tc.backend, Resume: tc.resume},
			ThinkingPolicyFields{},
		)
		if avail.Thinking.Fair != tc.fair {
			t.Errorf("Fair(%q, %v, resume=%v) = %v, want %v", tc.backend, tc.value, tc.resume, avail.Thinking.Fair, tc.fair)
		}
		if tc.value != nil {
			if avail.Thinking.Valid == nil || *avail.Thinking.Valid != tc.valid {
				t.Errorf("Valid(%q, %v, resume=%v) = %v, want %v", tc.backend, tc.value, tc.resume, avail.Thinking.Valid, tc.valid)
			}
		}
	}
}

func TestBackendPolicyActiveBranch(t *testing.T) {
	// Empty value → Empty branch
	avail := Check(
		ThinkingPolicyFields{Thinking: nil},
		ThinkingPolicyConditions{Backend: "codex-app-server", Resume: false},
		ThinkingPolicyFields{},
	)
	if avail.ActiveThinkingPolicyBranch != Empty {
		t.Errorf("empty value branch = %v, want Empty", avail.ActiveThinkingPolicyBranch)
	}

	// Codex + canonical → FullSupport
	avail = Check(
		ThinkingPolicyFields{Thinking: str("high")},
		ThinkingPolicyConditions{Backend: "codex-app-server", Resume: false},
		ThinkingPolicyFields{},
	)
	if avail.ActiveThinkingPolicyBranch != FullSupport {
		t.Errorf("codex high branch = %v, want FullSupport", avail.ActiveThinkingPolicyBranch)
	}

	// Claude + low + start → ClaudeStart
	avail = Check(
		ThinkingPolicyFields{Thinking: str("low")},
		ThinkingPolicyConditions{Backend: "claude", Resume: false},
		ThinkingPolicyFields{},
	)
	if avail.ActiveThinkingPolicyBranch != ClaudeStart {
		t.Errorf("claude low start branch = %v, want ClaudeStart", avail.ActiveThinkingPolicyBranch)
	}

	// Unknown backend + value → no branch
	avail = Check(
		ThinkingPolicyFields{Thinking: str("low")},
		ThinkingPolicyConditions{Backend: "agy", Resume: false},
		ThinkingPolicyFields{},
	)
	if avail.ActiveThinkingPolicyBranch != thinkingPolicyBranchNone {
		t.Errorf("agy low branch = %v, want None", avail.ActiveThinkingPolicyBranch)
	}
}

func str(s string) *string { return &s }