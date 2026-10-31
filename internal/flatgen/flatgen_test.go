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
