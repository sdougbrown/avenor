package spawnselection

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
)

func loadConformance(t *testing.T) []ConformanceCase {
	t.Helper()
	cases, err := LoadConformanceCases("../../schemas/spawn_selection.conformance.json")
	if err != nil {
		t.Fatalf("load conformance fixture: %v", err)
	}
	if len(cases) == 0 {
		t.Fatal("conformance fixture has no cases")
	}
	return cases
}

// TestConformanceStrict drives every fixture case through the strict raw
// validator (ValidateJSON). This is the portable contract as seen at raw wire
// boundaries: contract semantics plus unknown/deferred/misspelled key
// rejection, using the exact same cases as the TypeScript evaluator.
func TestConformanceStrict(t *testing.T) {
	for _, c := range loadConformance(t) {
		t.Run(c.Name, func(t *testing.T) {
			err := ValidateJSON(c.Input, c.RosterConfigured)
			if (err != nil) == c.Valid {
				t.Fatalf("ValidateJSON() error = %v, want valid=%v", err, c.Valid)
			}
			if err != nil && c.ErrorContains != "" && !strings.Contains(err.Error(), c.ErrorContains) {
				t.Errorf("ValidateJSON() error %q does not contain %q", err.Error(), c.ErrorContains)
			}
		})
	}
}

// TestConformanceTyped drives the non-strict-only fixture cases through the
// typed Validate used at the internal stable/control boundaries. Unknown keys
// are irrelevant on the typed path because the caller has already projected
// the selector fields, so strictOnly cases are intentionally excluded.
func TestConformanceTyped(t *testing.T) {
	for _, c := range loadConformance(t) {
		if c.StrictOnly {
			continue
		}
		t.Run(c.Name, func(t *testing.T) {
			var in Input
			if err := json.Unmarshal(c.Input, &in); err != nil {
				t.Fatalf("decode input: %v", err)
			}
			err := Validate(in, c.RosterConfigured)
			if (err != nil) == c.Valid {
				t.Fatalf("Validate() error = %v, want valid=%v", err, c.Valid)
			}
			if err != nil && c.ErrorContains != "" && !strings.Contains(err.Error(), c.ErrorContains) {
				t.Errorf("Validate() error %q does not contain %q", err.Error(), c.ErrorContains)
			}
		})
	}
}

// TestSchemaFieldSet guards against required-field or field-set drift between
// the portable Umpire schema and the generated Go struct. If either side adds
// or renames a selector field, this test fails so the contract stays in one
// place.
func TestSchemaFieldSet(t *testing.T) {
	data, err := os.ReadFile("../../schemas/spawn_selection.umpire.json")
	if err != nil {
		t.Fatalf("read umpire schema: %v", err)
	}
	var schema struct {
		Fields     map[string]map[string]any `json:"fields"`
		Conditions map[string]map[string]any `json:"conditions"`
	}
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatalf("parse umpire schema: %v", err)
	}

	if len(schema.Fields) == 0 || len(schema.Conditions) == 0 {
		t.Fatalf("schema missing fields or conditions: fields=%d conditions=%d", len(schema.Fields), len(schema.Conditions))
	}

	// The generated struct must expose exactly the schema's selector fields.
	// The umpire generator maps snake_case schema names to Go field names, so
	// compare against struct field names to detect field-set drift.
	fieldType := reflect.TypeOf(SpawnSelectionFields{})
	generated := make(map[string]bool)
	for i := 0; i < fieldType.NumField(); i++ {
		generated[fieldType.Field(i).Name] = true
	}

	for name := range schema.Fields {
		if !generated[goFieldName(name)] {
			t.Errorf("generated struct missing schema field %q (expected field %q)", name, goFieldName(name))
		}
	}
	if len(generated) != len(schema.Fields) {
		t.Fatalf("generated field count %d != schema field count %d", len(generated), len(schema.Fields))
	}

	// The generated conditions struct must carry a matching boolean per
	// declared condition.
	condType := reflect.TypeOf(SpawnSelectionConditions{})
	generatedConds := make(map[string]bool)
	for i := 0; i < condType.NumField(); i++ {
		generatedConds[condType.Field(i).Name] = true
	}
	for name := range schema.Conditions {
		if !generatedConds[goFieldName(name)] {
			t.Errorf("generated conditions struct missing condition %q (expected field %q)", name, goFieldName(name))
		}
	}
}

// goFieldName converts a snake_case schema name to the Go struct field name
// the umpire generator emits (each underscore-separated segment capitalized).
func goFieldName(s string) string {
	var b strings.Builder
	for _, part := range strings.Split(s, "_") {
		if part == "" {
			continue
		}
		b.WriteString(strings.ToUpper(part[:1]))
		b.WriteString(part[1:])
	}
	return b.String()
}
