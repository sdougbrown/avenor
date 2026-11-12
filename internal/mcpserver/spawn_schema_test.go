package mcpserver

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sdougbrown/avenor/internal/runtime/factory"
)

// TestGeneratedSpawnSchemaIsCurrent fails when the checked-in schema file and
// the copy compiled into the binary disagree, which is what an edit without
// `go generate ./internal/mcpserver` leaves behind. Every other test in this
// file reads the generated copy, so without this one a reworded field could sit
// in the repository while the server keeps publishing the old text.
func TestGeneratedSpawnSchemaIsCurrent(t *testing.T) {
	onDisk, err := os.ReadFile("../../schemas/spawn.input.json")
	if err != nil {
		t.Fatalf("read schemas/spawn.input.json: %v", err)
	}
	if string(onDisk) != spawnInputSchemaJSON {
		t.Fatal("spawn_schema.gen.go is stale: run go generate ./internal/mcpserver")
	}
}

// TestSpawnToolPublishesSharedSchema drives the registered tool over the real
// protocol, because every other guard here tests a function. Without this,
// deleting the InputSchema line from the registration would leave the server
// publishing the SDK's reflection-derived schema — eighteen properties, no
// descriptions, no additionalProperties — with every other test green.
func TestSpawnToolPublishesSharedSchema(t *testing.T) {
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: &fakeClient{}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	serverTransport, clientTransport := mcpsdk.NewInMemoryTransports()
	serverSession, err := s.mcpServer.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatalf("connect server: %v", err)
	}
	defer serverSession.Close()
	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test-client", Version: "dev"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("connect client: %v", err)
	}
	defer clientSession.Close()

	tools, err := clientSession.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}

	var published map[string]any
	for _, tool := range tools.Tools {
		if tool.Name == "spawn" {
			published, _ = tool.InputSchema.(map[string]any)
		}
	}
	if published == nil {
		t.Fatal("spawn tool not published with an object schema")
	}
	if got := published["type"]; got != "object" {
		t.Errorf("spawn schema type = %v, want object", got)
	}
	if got, ok := published["additionalProperties"].(bool); !ok || got {
		t.Errorf("spawn schema additionalProperties = %v, want false", published["additionalProperties"])
	}
	var surfaceDescriptions struct {
		Descriptions map[string]string `json:"x-avenor-surface-descriptions"`
	}
	if err := json.Unmarshal([]byte(spawnInputSchemaJSON), &surfaceDescriptions); err != nil {
		t.Fatal(err)
	}
	if got, want := published["description"], surfaceDescriptions.Descriptions["go"]; got != want {
		t.Errorf("published description = %v, want the schema's go-surface description", got)
	}

	required, ok := published["required"].([]any)
	if !ok || len(required) != 1 || required[0] != "dir" {
		t.Errorf("spawn schema required = %v, want [dir]", published["required"])
	}

	properties, ok := published["properties"].(map[string]any)
	if !ok {
		t.Fatal("spawn schema has no properties object")
	}
	for name := range properties {
		if name == "wait" {
			t.Errorf("spawn schema publishes %q, which this server does not accept", name)
		}
	}

	var source struct {
		Properties map[string]struct {
			Description string `json:"description"`
			Type        string `json:"type"`
			Items       struct {
				Type string `json:"type"`
			} `json:"items"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(spawnInputSchemaJSON), &source); err != nil {
		t.Fatal(err)
	}
	for name, property := range properties {
		entry, ok := property.(map[string]any)
		if !ok {
			t.Fatalf("spawn property %q is not an object", name)
		}
		if got, want := entry["description"], source.Properties[name].Description; got != want {
			t.Errorf("published %q description = %v, want %v", name, got, want)
		}
		if got, want := entry["type"], source.Properties[name].Type; got != want {
			t.Errorf("published %q type = %v, want %v", name, got, want)
		}
		for key := range entry {
			if strings.HasPrefix(key, "x-avenor-") {
				t.Errorf("published property %q carries the internal %q annotation", name, key)
			}
		}
		if name == "launch_wrapper" {
			items, _ := entry["items"].(map[string]any)
			if items["type"] != "string" {
				t.Errorf("launch_wrapper items.type = %v, want string", items["type"])
			}
		}
	}
}

// TestSpawnSchemaBackendEnumMatchesRegistry holds the schema's backend enum to
// the backends this build registers. A backend added to the factory without a
// schema edit would otherwise be refused by every client that validates the
// published schema first.
func TestSpawnSchemaBackendEnumMatchesRegistry(t *testing.T) {
	var schema struct {
		Properties map[string]struct {
			Enum []string `json:"enum"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(spawnInputSchemaJSON), &schema); err != nil {
		t.Fatalf("parse generated schema: %v", err)
	}
	got := schema.Properties["backend"].Enum
	want := factory.KnownBackends()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("backend enum = %v, want the registered backends %v; edit schemas/spawn.input.json and regenerate", got, want)
	}
}

// TestSpawnSchemaMatchesStruct holds spawnArgs to the input schema the spawn
// tool publishes. schemas/spawn.input.json is the single source for the
// field's name, type, and description, so a field added, renamed, or
// reclassified on one side without the other has to fail here rather than at a
// client.
func TestSpawnSchemaMatchesStruct(t *testing.T) {
	var schema struct {
		Properties map[string]struct {
			Type     string   `json:"type"`
			Surfaces []string `json:"x-avenor-surfaces"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(spawnInputSchemaJSON), &schema); err != nil {
		t.Fatalf("parse generated schema: %v", err)
	}
	if len(schema.Properties) < 10 {
		t.Fatalf("generated schema declares %d properties; the artifact is stale or malformed", len(schema.Properties))
	}

	structFields := map[string]string{}
	typ := reflect.TypeOf(spawnArgs{})
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		name := strings.Split(field.Tag.Get("json"), ",")[0]
		if name == "" || name == "-" {
			continue
		}
		structFields[name] = schemaTypeFor(field.Type)
	}

	// Every Go field is a schema property this server publishes.
	names := make([]string, 0, len(structFields))
	for name := range structFields {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		property, ok := schema.Properties[name]
		if !ok {
			t.Errorf("spawnArgs.%s is published but missing from schemas/spawn.input.json", name)
			continue
		}
		if !containsSurface(property.Surfaces, "go") {
			t.Errorf("schemas/spawn.input.json property %q does not list the go surface", name)
		}
		if property.Type != structFields[name] {
			t.Errorf("property %q is %q in the schema and %q in spawnArgs", name, property.Type, structFields[name])
		}
	}

	// Every schema property that names this server has a struct field to decode
	// into, and every property that does not is a JS-only field.
	for name, property := range schema.Properties {
		_, isGo := structFields[name]
		namesGo := containsSurface(property.Surfaces, "go")
		if namesGo && !isGo {
			t.Errorf("schemas/spawn.input.json property %q names the go surface but spawnArgs has no such field", name)
		}
		if isGo && !namesGo {
			t.Errorf("spawnArgs.%s is published but the schema does not name the go surface for it", name)
		}
		if !namesGo {
			continue
		}
		switch property.Type {
		case "string", "array", "boolean":
		default:
			t.Errorf("property %q declares unsupported type %q", name, property.Type)
		}
	}
}

// TestPublishedSpawnSchemaIsTheGoFieldSet pins the published schema to what this
// server actually decodes: a JS-only property (dir, wait) must not reach the
// schema, because the decoder would refuse it after the schema admitted it.
func TestPublishedSpawnSchemaIsTheGoFieldSet(t *testing.T) {
	var published struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	encoded := publishedSpawnInputSchema()
	if err := json.Unmarshal(encoded, &published); err != nil {
		t.Fatalf("parse published schema: %v", err)
	}
	var args spawnArgs
	_ = args
	typ := reflect.TypeOf(spawnArgs{})
	want := map[string]bool{}
	for i := 0; i < typ.NumField(); i++ {
		name := strings.Split(typ.Field(i).Tag.Get("json"), ",")[0]
		if name != "" && name != "-" {
			want[name] = true
		}
	}
	for name := range published.Properties {
		if !want[name] {
			t.Errorf("published schema advertises %q, which spawnArgs cannot decode", name)
		}
	}
	for name := range want {
		if _, ok := published.Properties[name]; !ok {
			t.Errorf("published schema omits %q, which spawnArgs decodes", name)
		}
	}
}

// TestSpawnSchemaRequiresOnlyWhatTheDecoderRequires holds the published schema's
// required list to spawnArgs: the fields the decoder cannot default are exactly
// the ones a client must send.
func TestSpawnSchemaRequiresOnlyWhatTheDecoderRequires(t *testing.T) {
	var published struct {
		Required []string `json:"required"`
	}
	if err := json.Unmarshal(publishedSpawnInputSchema(), &published); err != nil {
		t.Fatalf("parse published schema: %v", err)
	}
	want := requiredSpawnFields()
	if !reflect.DeepEqual(published.Required, want) {
		t.Fatalf("published required = %v, want %v", published.Required, want)
	}
	if !reflect.DeepEqual(want, []string{"dir"}) {
		t.Fatalf("requiredSpawnFields = %v, want [dir]; a new non-omitempty field changes the published contract", want)
	}
}

// schemaTypeFor maps a Go field type to the JSON Schema type the spawn input
// schema must declare for it.
func schemaTypeFor(t reflect.Type) string {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.String:
		return "string"
	case reflect.Bool:
		return "boolean"
	case reflect.Slice:
		return "array"
	default:
		return t.Kind().String()
	}
}

func containsSurface(surfaces []string, want string) bool {
	for _, surface := range surfaces {
		if surface == want {
			return true
		}
	}
	return false
}
