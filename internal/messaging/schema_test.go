package messaging

import (
	"encoding/json"
	"os"
	"reflect"
	"slices"
	"sort"
	"strings"
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
// the fixture marks as a schema violation and accepts every valid case, so a
// type typo cannot make the schema reject the whole contract silently.
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
		valid := 0
		for _, c := range cases {
			if c.Schema || c.Expect.Code != "" {
				continue
			}
			valid++
			t.Run("valid/"+c.Name, func(t *testing.T) {
				if err := validateAgainst(t, fixture.schema(), c.Input); err != nil {
					t.Fatalf("schema rejected the valid input %s: %v", c.Input, err)
				}
			})
		}
		if valid == 0 {
			t.Fatal("fixture carries no valid case")
		}
	}
}

// TestSchemaMatchesRouteTable proves each schema variant and its applicability
// entry admit the same set: same properties, same required keys, same declared
// types and the same string constraints. The checked-in fixture maps are
// compared with the compiled tables too, so a fixture edit that is not
// regenerated fails here.
func TestSchemaMatchesRouteTable(t *testing.T) {
	fixtureMessages, fixturePeers, err := LoadApplicabilityTables("../../schemas/message_route.conformance.json")
	if err != nil {
		t.Fatalf("load applicability fixture: %v", err)
	}
	for _, fixture := range []struct {
		name    string
		schema  map[string]any
		table   map[string]ActionApplicability
		fixture map[string]ActionApplicability
	}{
		{"message", MessageInputSchema(), ApplicabilityTable(), fixtureMessages},
		{"peers", PeersInputSchema(), PeersApplicabilityTable(), fixturePeers},
	} {
		if len(fixture.fixture) != len(fixture.table) {
			t.Fatalf("%s: fixture has %d applicability entries, table has %d", fixture.name, len(fixture.fixture), len(fixture.table))
		}
		for action, want := range fixture.fixture {
			got, ok := fixture.table[action]
			if !ok {
				t.Fatalf("%s: fixture action %q has no table entry", fixture.name, action)
			}
			if !reflect.DeepEqual(want.Permitted, got.Permitted) || !slices.Equal(want.Required, got.Required) || !reflect.DeepEqual(want.Types, got.Types) {
				t.Fatalf("%s/%s: fixture = %+v, table = %+v; run go generate", fixture.name, action, want, got)
			}
		}
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
			if !slices.Equal(fromSchema, fromTable) {
				t.Fatalf("%s/%s: required = %v, table = %v", fixture.name, action, fromSchema, fromTable)
			}
			if additional, ok := variant["additionalProperties"].(bool); !ok || additional {
				t.Fatalf("%s/%s: additionalProperties must be false", fixture.name, action)
			}
			for name, rawProperty := range properties {
				property, _ := rawProperty.(map[string]any)
				kind := entry.Types[name]
				switch kind {
				case "string", "integer", "object":
				default:
					t.Fatalf("%s/%s: property %q declares type %q", fixture.name, action, name, kind)
				}
				if _, discriminator := property["const"]; discriminator {
					continue
				}
				if schemaType, _ := property["type"].(string); schemaType != kind {
					t.Fatalf("%s/%s: property %q schema type = %q, table type = %q", fixture.name, action, name, schemaType, kind)
				}
				constraint := entry.Constraints[name]
				if got := schemaInt(property["minLength"]); got != constraint.MinLength {
					t.Fatalf("%s/%s: property %q schema minLength = %d, table = %d", fixture.name, action, name, got, constraint.MinLength)
				}
				if got := schemaInt(property["maxLength"]); got != constraint.MaxLength {
					t.Fatalf("%s/%s: property %q schema maxLength = %d, table = %d", fixture.name, action, name, got, constraint.MaxLength)
				}
				if got, _ := property["pattern"].(string); got != constraint.Pattern {
					t.Fatalf("%s/%s: property %q schema pattern = %q, table = %q", fixture.name, action, name, got, constraint.Pattern)
				}
				if got, declared := schemaInt(property["minimum"]), property["minimum"] != nil; declared != constraint.HasMinimum {
					t.Fatalf("%s/%s: property %q schema minimum present = %t, table = %t", fixture.name, action, name, declared, constraint.HasMinimum)
				} else if int64(got) != constraint.Minimum {
					t.Fatalf("%s/%s: property %q schema minimum = %d, table = %d", fixture.name, action, name, got, constraint.Minimum)
				}
				if got, declared := schemaInt(property["maximum"]), property["maximum"] != nil; declared != constraint.HasMaximum {
					t.Fatalf("%s/%s: property %q schema maximum present = %t, table = %t", fixture.name, action, name, declared, constraint.HasMaximum)
				} else if int64(got) != constraint.Maximum {
					t.Fatalf("%s/%s: property %q schema maximum = %d, table = %d", fixture.name, action, name, got, constraint.Maximum)
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

func schemaInt(value any) int {
	number, _ := value.(float64)
	return int(number)
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

// TestTimeoutMaximumMatchesWaitCap pins the schema's timeout_ms upper bound to
// the wait cap askclient enforces, which also keeps the duration conversion in
// range.
func TestTimeoutMaximumMatchesWaitCap(t *testing.T) {
	schema := MessageInputSchema()
	variants, _ := schema["oneOf"].([]any)
	for _, rawVariant := range variants {
		variant, _ := rawVariant.(map[string]any)
		properties, _ := variant["properties"].(map[string]any)
		action, _ := properties["action"].(map[string]any)
		if action["const"] != "ask" {
			continue
		}
		timeout, _ := properties["timeout_ms"].(map[string]any)
		if got, want := schemaInt(timeout["maximum"]), int(MaxWait.Milliseconds()); got != want {
			t.Fatalf("timeout_ms maximum = %d, want the wait cap %d", got, want)
		}
		return
	}
	t.Fatal("ask variant has no timeout_ms property")
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

// TestRuleMetaMatchesUmpireSpec pins the generated rule metadata to the Umpire
// spec, and enforces the one-slot-per-rule-kind invariant the generator silently
// violates by overwriting a second rule on the same field and kind.
func TestRuleMetaMatchesUmpireSpec(t *testing.T) {
	var spec struct {
		Rules []struct {
			Type   string `json:"type"`
			Field  string `json:"field"`
			Reason string `json:"reason"`
		} `json:"rules"`
	}
	if err := decodeFixture("../../schemas/message_route.umpire.json", &spec); err != nil {
		t.Fatalf("decode umpire spec: %v", err)
	}
	type ruleKey struct {
		Field    string
		RuleType string
		Reason   string
	}
	seen := map[string]bool{}
	want := make([]ruleKey, 0, len(spec.Rules))
	for _, rule := range spec.Rules {
		key := rule.Field + "\x00" + rule.Type
		if seen[key] {
			t.Fatalf("field %q carries two %q rules; the generator keeps only one", rule.Field, rule.Type)
		}
		seen[key] = true
		want = append(want, ruleKey{Field: capitalize(rule.Field), RuleType: rule.Type, Reason: rule.Reason})
	}
	sort.Slice(want, func(i, j int) bool {
		if want[i].Field != want[j].Field {
			return want[i].Field < want[j].Field
		}
		return want[i].RuleType < want[j].RuleType
	})
	got := make([]ruleKey, 0, len(ruleMeta))
	for _, entry := range ruleMeta {
		got = append(got, ruleKey{Field: entry.Field, RuleType: entry.RuleType, Reason: entry.Reason})
	}
	sort.Slice(got, func(i, j int) bool {
		if got[i].Field != got[j].Field {
			return got[i].Field < got[j].Field
		}
		return got[i].RuleType < got[j].RuleType
	})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("generated ruleMeta = %+v, spec = %+v; run go generate", got, want)
	}
}

func capitalize(value string) string {
	if value == "" {
		return value
	}
	return strings.ToUpper(value[:1]) + value[1:]
}
