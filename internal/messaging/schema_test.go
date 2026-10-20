package messaging

import (
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
)

func readSchemaFile(t *testing.T, path string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var schema map[string]any
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return schema
}

func validateAgainst(t *testing.T, schema map[string]any, input json.RawMessage) error {
	t.Helper()
	raw, err := json.Marshal(schema)
	if err != nil {
		t.Fatalf("encode schema: %v", err)
	}
	var compiled jsonschema.Schema
	if err := json.Unmarshal(raw, &compiled); err != nil {
		t.Fatalf("decode schema: %v", err)
	}
	resolved, err := compiled.Resolve(nil)
	if err != nil {
		t.Fatalf("resolve schema: %v", err)
	}
	var instance any
	if err := json.Unmarshal(input, &instance); err != nil {
		t.Fatalf("decode input: %v", err)
	}
	return resolved.Validate(instance)
}

// TestConformanceSchemaCases asserts that the JSON Schema rejects every case
// the fixture marks as a schema violation, so the schema and the applicability
// table reject the same cross-variant fields.
func TestConformanceSchemaCases(t *testing.T) {
	for _, fixture := range []struct {
		path   string
		schema func() map[string]any
		load   func(string) ([]ConformanceCase, error)
	}{
		{"../../schemas/message_route.conformance.json", MessageInputSchema, LoadConformanceCases},
		{"../../schemas/message_route.conformance.json", PeersInputSchema, LoadPeersConformanceCases},
	} {
		cases, err := fixture.load(fixture.path)
		if err != nil {
			t.Fatalf("load fixture: %v", err)
		}
		checked := 0
		for _, c := range cases {
			if !c.Schema {
				continue
			}
			checked++
			t.Run(c.Name, func(t *testing.T) {
				if err := validateAgainst(t, fixture.schema(), c.Input); err == nil {
					t.Fatalf("schema accepted %s", c.Input)
				}
			})
		}
		if checked == 0 {
			t.Fatal("fixture marks no case as a schema violation")
		}
	}
}

// TestSchemaMatchesRouteTable proves each schema variant and its applicability
// entry admit the same set: same properties, same required keys, and no
// additional properties. Both schemas are covered.
func TestSchemaMatchesRouteTable(t *testing.T) {
	for _, fixture := range []struct {
		name   string
		schema map[string]any
		table  map[string]ActionApplicability
	}{
		{"message", MessageInputSchema(), ApplicabilityTable()},
		{"peers", PeersInputSchema(), PeersApplicabilityTable()},
	} {
		variants, ok := fixture.schema["oneOf"].([]any)
		if !ok || len(variants) != len(fixture.table) {
			t.Fatalf("%s: %d variants, %d applicability entries", fixture.name, len(variants), len(fixture.table))
		}
		seen := map[string]bool{}
		for _, rawVariant := range variants {
			variant, ok := rawVariant.(map[string]any)
			if !ok {
				t.Fatalf("%s: variant is not an object", fixture.name)
			}
			properties, _ := variant["properties"].(map[string]any)
			action := ""
			if discriminator, ok := properties["action"].(map[string]any); ok {
				action, _ = discriminator["const"].(string)
			}
			entry, known := fixture.table[action]
			if !known {
				t.Fatalf("%s: schema variant %q has no applicability entry", fixture.name, action)
			}
			seen[action] = true

			names := make([]string, 0, len(properties))
			for name := range properties {
				names = append(names, name)
			}
			sort.Strings(names)
			permitted := append([]string(nil), entry.Permitted...)
			sort.Strings(permitted)
			if !reflect.DeepEqual(names, permitted) {
				t.Fatalf("%s/%s: properties = %v, permitted = %v", fixture.name, action, names, permitted)
			}
			required, _ := variant["required"].([]any)
			fromSchema := make([]string, 0, len(required))
			for _, name := range required {
				fromSchema = append(fromSchema, name.(string))
			}
			sort.Strings(fromSchema)
			fromTable := append([]string(nil), entry.Required...)
			sort.Strings(fromTable)
			if !reflect.DeepEqual(fromSchema, fromTable) {
				t.Fatalf("%s/%s: required = %v, table = %v", fixture.name, action, fromSchema, fromTable)
			}
			if additional, ok := variant["additionalProperties"].(bool); !ok || additional {
				t.Fatalf("%s/%s: additionalProperties must be false", fixture.name, action)
			}
			for name := range properties {
				if entry.Types[name] == "" {
					t.Fatalf("%s/%s: property %q declares no wire type", fixture.name, action, name)
				}
			}
		}
		for action := range fixture.table {
			if !seen[action] {
				t.Fatalf("%s: applicability entry %q has no schema variant", fixture.name, action)
			}
		}
	}
}

// TestSchemaFilesCompiled catches a stale schema.gen.go without running go
// generate: the compiled constant and the checked-in file decode identically.
func TestSchemaFilesCompiled(t *testing.T) {
	if got, want := MessageInputSchema(), readSchemaFile(t, "../../schemas/message.input.json"); !reflect.DeepEqual(got, want) {
		t.Fatal("message.input.json and schema.gen.go disagree; run go generate ./internal/messaging/...")
	}
	if got, want := PeersInputSchema(), readSchemaFile(t, "../../schemas/peers.input.json"); !reflect.DeepEqual(got, want) {
		t.Fatal("peers.input.json and schema.gen.go disagree; run go generate ./internal/messaging/...")
	}
}

// TestSurfaceVocabulary pins the two declarations of the surface vocabulary: the
// Surface constants and the surface values the conformance fixture passes
// through to a provider. A typo such as SurfacePiHost = "pi_host" would
// otherwise make controlReplySupported silently wrong with no failing test.
func TestSurfaceVocabulary(t *testing.T) {
	constants := map[string]bool{
		string(SurfaceMCP): true, string(SurfaceChanneltools): true,
		string(SurfaceSidecar): true, string(SurfacePiHost): true,
		string(SurfacePiChild): true, string(SurfaceOpenCode): true,
		string(SurfaceMCPTS): true,
	}
	cases, err := LoadConformanceCases("../../schemas/message_route.conformance.json")
	if err != nil {
		t.Fatalf("load fixture: %v", err)
	}
	peersCases, err := LoadPeersConformanceCases("../../schemas/message_route.conformance.json")
	if err != nil {
		t.Fatalf("load peers fixture: %v", err)
	}
	seen := map[string]bool{}
	for _, c := range append(cases, peersCases...) {
		surface, ok := c.Conditions["surface"].(string)
		if !ok {
			t.Fatalf("case %q carries no surface", c.Name)
		}
		if !constants[surface] {
			t.Fatalf("case %q names surface %q, which has no constant", c.Name, surface)
		}
		seen[surface] = true
	}
	for surface := range constants {
		if !seen[surface] {
			t.Fatalf("surface %q appears in no conformance case", surface)
		}
	}
}
