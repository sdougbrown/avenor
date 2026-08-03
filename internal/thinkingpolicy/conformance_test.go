package thinkingpolicy

import (
	"encoding/json"
	"os"
	"testing"
)

type conformance struct {
	Canonical []struct {
		Name  string `json:"name"`
		Value string `json:"value"`
		Valid bool   `json:"valid"`
	} `json:"canonicalCases"`
	Backend []struct {
		Name    string `json:"name"`
		Backend string `json:"backend"`
		Value   string `json:"value"`
		Resume  bool   `json:"resume"`
		Valid   bool   `json:"valid"`
	} `json:"backendCases"`
}

func loadThinkingConformance(t *testing.T) conformance {
	t.Helper()
	data, err := os.ReadFile("../../schemas/thinking_policy.conformance.json")
	if err != nil {
		t.Fatalf("read conformance fixture: %v", err)
	}
	var c conformance
	if err := json.Unmarshal(data, &c); err != nil {
		t.Fatalf("parse conformance fixture: %v", err)
	}
	if len(c.Canonical) == 0 || len(c.Backend) == 0 {
		t.Fatal("conformance fixture missing cases")
	}
	return c
}

// TestConformanceCanonical drives the generated canonical validation and the
// hand-written Go tuple over the shared fixture.
func TestConformanceCanonical(t *testing.T) {
	for _, c := range loadThinkingConformance(t).Canonical {
		t.Run(c.Name, func(t *testing.T) {
			got := IsCanonical(c.Value)
			if got != c.Valid {
				t.Fatalf("IsCanonical(%q) = %v, want %v", c.Value, got, c.Valid)
			}
			if err := ValidateCanonical(c.Value); (err != nil) == c.Valid {
				t.Fatalf("ValidateCanonical(%q) error = %v, want valid=%v", c.Value, err, c.Valid)
			}
		})
	}
}

// TestConformanceBackendPolicy drives the static backend policy (start and
// explicit resume) over the shared fixture.
func TestConformanceBackendPolicy(t *testing.T) {
	for _, c := range loadThinkingConformance(t).Backend {
		t.Run(c.Name, func(t *testing.T) {
			got := Evaluate(c.Backend, c.Value, c.Resume) == OK
			if got != c.Valid {
				t.Fatalf("Evaluate(%q, %q, resume=%v) accepted=%v, want %v", c.Backend, c.Value, c.Resume, got, c.Valid)
			}
		})
	}
}
