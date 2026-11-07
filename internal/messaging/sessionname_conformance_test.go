package messaging

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// sessionNameRuleFixture mirrors schemas/session-name-rule.json, the shared
// conformance fixture for the one session display-name rule. The file is not a
// wire schema; it pins the rule values this package's ValidSessionName and
// StripDisallowedIdentifierRunes must reproduce, together with the label
// sanitation internal/stable applies on top of the strip path.
type sessionNameRuleFixture struct {
	Limit              int      `json:"limit"`
	ReservedReferences []string `json:"reservedReferences"`
	Cases              []struct {
		Name            string `json:"name"`
		Input           string `json:"input"`
		Valid           bool   `json:"valid"`
		DisallowedRunes bool   `json:"disallowedRunes"`
	} `json:"cases"`
}

// loadSessionNameRuleFixture reads the shared fixture relative to this test's
// own file, so the test runs the same from any working directory.
func loadSessionNameRuleFixture(t *testing.T) sessionNameRuleFixture {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate this test file to resolve the shared fixture")
	}
	data, err := os.ReadFile(filepath.Join(filepath.Dir(thisFile), "..", "..", "schemas", "session-name-rule.json"))
	if err != nil {
		t.Fatalf("read session-name-rule fixture: %v", err)
	}
	var f sessionNameRuleFixture
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("parse session-name-rule fixture: %v", err)
	}
	if len(f.Cases) == 0 {
		t.Fatal("session-name-rule fixture has no cases")
	}
	return f
}

// TestSessionNameRuleConformance drives ValidSessionName and the strip path
// over the shared fixture, so the Go rule and the TypeScript label pipeline
// accept and refuse the same names.
func TestSessionNameRuleConformance(t *testing.T) {
	f := loadSessionNameRuleFixture(t)

	if f.Limit != MaxSessionNameRunes {
		t.Fatalf("fixture limit = %d, package rule = %d", f.Limit, MaxSessionNameRunes)
	}
	reserved := map[string]bool{ReservedSelfAlias: true, SupervisorRunID: true}
	for _, ref := range f.ReservedReferences {
		if !reserved[ref] {
			t.Errorf("fixture reserved reference %q is not one of the package's reserved references", ref)
		}
	}

	for _, c := range f.Cases {
		t.Run(c.Name, func(t *testing.T) {
			if got := ValidSessionName(c.Input); got != c.Valid {
				t.Fatalf("ValidSessionName(%q) = %v, want %v", c.Input, got, c.Valid)
			}
			stripped := StripDisallowedIdentifierRunes(c.Input)
			if changed := stripped != c.Input; changed != c.DisallowedRunes {
				t.Fatalf("StripDisallowedIdentifierRunes(%q) changed = %v, want disallowedRunes = %v", c.Input, changed, c.DisallowedRunes)
			}
		})
	}
}
