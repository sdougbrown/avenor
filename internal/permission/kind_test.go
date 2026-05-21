package permission

import "testing"

func TestNormalizeOptionKind(t *testing.T) {
	tests := map[string]string{
		"allow":         "allow",
		"allow_once":    "allow",
		"allow_always":  "allow",
		"reject":        "reject",
		"reject_once":   "reject",
		"reject_always": "reject",
		"deny":          "reject",
		"deny_once":     "reject",
		"deny_always":   "reject",
		" other ":       "other",
	}

	for input, want := range tests {
		if got := NormalizeOptionKind(input); got != want {
			t.Errorf("NormalizeOptionKind(%q) = %q, want %q", input, got, want)
		}
	}
}
