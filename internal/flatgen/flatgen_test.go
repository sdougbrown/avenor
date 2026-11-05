package flatgen

import (
	"strings"
	"testing"
)

// TestFlattenKeepsInt64BoundsExact proves the decoder keeps a declared integer
// bound at the int64 limit exact. A float64 decode rounds it before the schema
// is re-encoded, so both the generated constant and the written file would
// carry the rounded value and no other test would notice.
func TestFlattenKeepsInt64BoundsExact(t *testing.T) {
	union := []byte(`{
	  "oneOf": [
	    {
	      "properties": {
	        "action": {"const": "events"},
	        "after_seq": {"type": "integer", "minimum": -9223372036854775808, "maximum": 9223372036854775807}
	      },
	      "required": ["action"]
	    }
	  ]
	}`)
	out, err := Flatten(union, Meta{Title: "t", ID: "https://example.test/t", Description: "d"})
	if err != nil {
		t.Fatalf("Flatten: %v", err)
	}
	for _, want := range []string{"-9223372036854775808", "9223372036854775807"} {
		if !strings.Contains(string(out), want) {
			t.Fatalf("flat schema lost the exact bound %s:\n%s", want, out)
		}
	}
	for _, rounded := range []string{"-9223372036854776000", "9223372036854776000"} {
		if strings.Contains(string(out), rounded) {
			t.Fatalf("flat schema carries the rounded bound %s:\n%s", rounded, out)
		}
	}
}

// TestFlattenRejectsTrailingContent proves the decoder refuses a schema file
// with content after the top-level object. json.Decoder.Decode alone accepts it,
// so the flat file would be written from a partially read input.
func TestFlattenRejectsTrailingContent(t *testing.T) {
	union := []byte(`{"oneOf":[{"properties":{"action":{"const":"tell"}}}]}{"oneOf":[]}`)
	if _, err := Flatten(union, Meta{}); err == nil {
		t.Fatal("Flatten accepted trailing content after the top-level object")
	}
}

// TestFlattenDropsPortableUnsafePattern proves a published flat schema carries
// no regex from the union. A provider's tool-schema validator implements its
// own regex dialect, and OpenAI's rejects Unicode property classes: a published
// `\p{Cc}` pattern fails every turn with "Invalid schema for function" before
// the model runs. The union keeps the pattern for the route table, which the
// router enforces; the flat schema is publication only.
func TestFlattenDropsPortableUnsafePattern(t *testing.T) {
	union := []byte(`{
	  "oneOf": [
	    {
	      "properties": {
	        "action": {"const": "rename"},
	        "name": {"type": "string", "minLength": 1, "maxLength": 64, "pattern": "^[^\\p{Cc}\\p{Cf}]+$"}
	      },
	      "required": ["action", "name"]
	    }
	  ]
	}`)
	out, err := Flatten(union, Meta{Title: "t", ID: "https://example.test/t", Description: "d"})
	if err != nil {
		t.Fatalf("Flatten: %v", err)
	}
	if strings.Contains(string(out), "pattern") {
		t.Fatalf("flat schema publishes a pattern:\n%s", out)
	}
	if strings.Contains(string(out), `\p{`) {
		t.Fatalf("flat schema carries a Unicode property class:\n%s", out)
	}
	for _, want := range []string{`"minLength": 1`, `"maxLength": 64`} {
		if !strings.Contains(string(out), want) {
			t.Fatalf("flat schema lost the bound %s:\n%s", want, out)
		}
	}
}
