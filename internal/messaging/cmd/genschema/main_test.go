package main

import (
	"strings"
	"testing"
)

// TestGenerateKeepsADeclaredZeroMaximum pins the compiled table's distinction
// between an absent maximum and a declared zero: a schema may legally bound an
// integer at zero, and dropping the constraint would let the router accept
// every value the schema refuses.
func TestGenerateKeepsADeclaredZeroMaximum(t *testing.T) {
	messageSchema := []byte(`{"oneOf":[{"properties":{"action":{"const":"ask"},"timeout_ms":{"type":"integer","minimum":0,"maximum":0}}}]}`)
	peersSchema := []byte(`{"oneOf":[]}`)
	conformance := conformanceFixture{Applicability: map[string]applicabilityEntry{
		"ask": {Permitted: []string{"action", "timeout_ms"}, Types: map[string]string{"action": "string", "timeout_ms": "integer"}},
	}}

	source, err := generate(messageSchema, peersSchema, conformance, outcomeFixture{})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if !strings.Contains(source, "Maximum: 0, HasMaximum: true") {
		t.Fatalf("generated table drops the declared zero maximum:\n%s", source)
	}
}
