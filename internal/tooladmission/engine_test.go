package tooladmission

import (
	"encoding/json"
	"strings"
	"testing"
)

// testRenderer renders the shared sentences the tests pin, from the same
// templates the conformance fixtures declare.
func testRenderer(code string, facts map[string]string) string {
	templates := map[string]string{
		"integer_too_large":  "property {name} must be at most {max}",
		"integer_too_small":  "property {name} must be at least {min}",
		"wrong_type_integer": "property {name} must be an integer",
	}
	template, ok := templates[code]
	if !ok {
		return code
	}
	out := template
	for name, value := range facts {
		out = strings.ReplaceAll(out, "{"+name+"}", value)
	}
	return out
}

// TestIntegerWireTypeAcceptsMathematicalIntegers pins the integer verdict the
// schema and the table share: 1.0, 1e3 and values beyond int64 are integers,
// 1.5 and "1" are not. An out-of-int64 value is refused by the constraint
// checks, which name the bound.
func TestIntegerWireTypeAcceptsMathematicalIntegers(t *testing.T) {
	for _, raw := range []string{"1", "1.0", "1e3", "-2.0", "999999999999999999999"} {
		if !hasWireType(json.RawMessage(raw), "integer") {
			t.Fatalf("%s should be an integer", raw)
		}
	}
	for _, raw := range []string{"1.5", "1e-1", `"1"`} {
		if hasWireType(json.RawMessage(raw), "integer") {
			t.Fatalf("%s should not be an integer", raw)
		}
	}
}

// TestIntegerConstraintNamesTheInt64Bound pins the sentence for an integer the
// declared bound cannot name: where the schema declares no bound, the int64
// limit is named literally, because 2^63 does not fit an int64 constant.
func TestIntegerConstraintNamesTheInt64Bound(t *testing.T) {
	engine := New(testRenderer)
	raw := json.RawMessage("999999999999999999999")
	if detail := engine.validateIntegerConstraint("after_seq", raw, PropertyConstraints{}); detail != `property "after_seq" must be at most 9223372036854775807` {
		t.Fatalf("detail = %q, want the int64 limit named", detail)
	}
	negative := json.RawMessage("-999999999999999999999")
	if detail := engine.validateIntegerConstraint("after_seq", negative, PropertyConstraints{}); detail != `property "after_seq" must be at least -9223372036854775808` {
		t.Fatalf("detail = %q, want the int64 floor named", detail)
	}
	// A declared bound wins: the schema's own limit is the actionable one.
	declared := PropertyConstraints{Maximum: 600000, HasMaximum: true}
	if detail := engine.validateIntegerConstraint("timeout_ms", raw, declared); detail != `property "timeout_ms" must be at most 600000` {
		t.Fatalf("detail = %q, want the declared maximum named", detail)
	}
}

// TestValidateIntegerConstraintEnforcesAZeroMaximum pins the declared bound's
// presence, not its truthiness: a schema may legally cap an integer at zero,
// and the table must still refuse everything above it.
func TestValidateIntegerConstraintEnforcesAZeroMaximum(t *testing.T) {
	engine := New(testRenderer)
	constraint := PropertyConstraints{Maximum: 0, HasMaximum: true}
	if detail := engine.validateIntegerConstraint("timeout_ms", json.RawMessage(`1`), constraint); detail != `property "timeout_ms" must be at most 0` {
		t.Fatalf("detail = %q, want the zero maximum to refuse 1", detail)
	}
	if detail := engine.validateIntegerConstraint("timeout_ms", json.RawMessage(`0`), constraint); detail != "" {
		t.Fatalf("detail = %q, want the zero maximum to accept 0", detail)
	}
}

// TestValidateFieldsOrderPinsUnknownBeforeRequired pins the fixed admission
// order: an unknown property is reported before a missing one.
func TestValidateFieldsOrderPinsUnknownBeforeRequired(t *testing.T) {
	engine := New(testRenderer)
	entry := ActionApplicability{
		Permitted: []string{"action", "name"},
		Required:  []string{"name"},
		Types:     map[string]string{"action": "string", "name": "string"},
	}
	fields := map[string]json.RawMessage{"action": json.RawMessage(`"x"`), "bogus": json.RawMessage(`"y"`)}
	if detail := engine.ValidateFields(fields, entry); detail != "unknown_property" {
		t.Fatalf("detail = %q, want unknown_property before the missing required property", detail)
	}
}

// TestExactIntegerConvertsAdmittedValues pins the conversion routers use after
// admission: an integer-valued exponential converts exactly.
func TestExactIntegerConvertsAdmittedValues(t *testing.T) {
	for raw, want := range map[string]int64{"1": 1, "1.0": 1, "1e3": 1000, "-2.0": -2} {
		rational, ok := ExactInteger(json.RawMessage(raw))
		if !ok {
			t.Fatalf("%s should convert", raw)
		}
		if got := rational.Num().Int64(); got != want {
			t.Fatalf("%s converted to %d, want %d", raw, got, want)
		}
	}
	if _, ok := ExactInteger(json.RawMessage(`1.5`)); ok {
		t.Fatal("1.5 should not convert")
	}
	if _, ok := ExactInteger(json.RawMessage(`"1"`)); ok {
		t.Fatal(`"1" should not convert`)
	}
}
