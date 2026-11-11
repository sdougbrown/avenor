package stable

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// sessionLabelSanitizeFixture mirrors the 'sanitizeCases' table of
// schemas/session-name-rule.json, the shared conformance fixture for the one
// session display-name rule. The file is not a wire schema; this half pins the
// exact labels the sanitation under DefaultSessionLabel must produce, so the
// Go derivation and the TypeScript label pipeline stay one rule apart.
type sessionLabelSanitizeFixture struct {
	DeriveCases []struct {
		Name     string `json:"name"`
		Toplevel string `json:"toplevel"`
		Branch   string `json:"branch"`
		Label    string `json:"label"`
	} `json:"deriveCases"`
	PickCases []struct {
		Name  string   `json:"name"`
		Base  string   `json:"base"`
		Taken []string `json:"taken"`
		Label string   `json:"label"`
	} `json:"pickCases"`
	SanitizeCases []struct {
		Name  string `json:"name"`
		Input string `json:"input"`
		Label string `json:"label"`
	} `json:"sanitizeCases"`
}

// loadSessionLabelSanitizeFixture reads the shared fixture relative to this
// test's own file, so the test runs the same from any working directory.
func loadSessionLabelSanitizeFixture(t *testing.T) sessionLabelSanitizeFixture {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate this test file to resolve the shared fixture")
	}
	data, err := os.ReadFile(filepath.Join(filepath.Dir(thisFile), "..", "..", "schemas", "session-name-rule.json"))
	if err != nil {
		t.Fatalf("read session-name-rule fixture: %v", err)
	}
	var f sessionLabelSanitizeFixture
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("parse session-name-rule fixture: %v", err)
	}
	if len(f.SanitizeCases) == 0 {
		t.Fatal("session-name-rule fixture has no sanitize cases")
	}
	return f
}

// TestSessionLabelSanitizeConformance drives the label sanitation behind
// DefaultSessionLabel over the shared fixture, and checks every produced label
// passes the name rule the derived label must satisfy.
func TestSessionLabelSanitizeConformance(t *testing.T) {
	for _, c := range loadSessionLabelSanitizeFixture(t).SanitizeCases {
		t.Run(c.Name, func(t *testing.T) {
			got := sanitizeSessionLabel(c.Input)
			if got != c.Label {
				t.Fatalf("sanitizeSessionLabel(%q) = %q, want %q", c.Input, got, c.Label)
			}
			if !messaging.ValidSessionName(got) {
				t.Fatalf("sanitized label %q does not pass the name rule", got)
			}
		})
	}
}

// TestSessionLabelDeriveConformance drives the work-label derivation over the
// shared fixture's top-level and branch cases, so the harness-side copies of
// the rule stay one answer apart from this one.
func TestSessionLabelDeriveConformance(t *testing.T) {
	cases := loadSessionLabelSanitizeFixture(t).DeriveCases
	if len(cases) == 0 {
		t.Fatal("session-name-rule fixture has no derive cases")
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			if got := sanitizeSessionLabel(joinWorkLabel(c.Toplevel, c.Branch)); got != c.Label {
				t.Fatalf("label for %q on %q = %q, want %q", c.Toplevel, c.Branch, got, c.Label)
			}
		})
	}
}

// TestSessionLabelPickConformance drives the collision suffixing over the
// shared fixture's base and taken-label cases.
func TestSessionLabelPickConformance(t *testing.T) {
	cases := loadSessionLabelSanitizeFixture(t).PickCases
	if len(cases) == 0 {
		t.Fatal("session-name-rule fixture has no pick cases")
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			held := map[string]bool{}
			for _, label := range c.Taken {
				held[label] = true
			}
			if got := pickSessionLabel(c.Base, func(label string) bool { return held[label] }); got != c.Label {
				t.Fatalf("pickSessionLabel(%q, taken %v) = %q, want %q", c.Base, c.Taken, got, c.Label)
			}
		})
	}
}
