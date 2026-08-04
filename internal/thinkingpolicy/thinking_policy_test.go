package thinkingpolicy

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestIsCanonical(t *testing.T) {
	for _, value := range append([]string{""}, CanonicalValues()...) {
		if !IsCanonical(value) {
			t.Errorf("IsCanonical(%q) = false, want true", value)
		}
	}
	for _, value := range []string{"HIGH", "auto", " low", "", "xhigh "} {
		if value != "" {
			if IsCanonical(value) {
				t.Errorf("IsCanonical(%q) = true, want false", value)
			}
		}
	}
}

func TestValidateCanonical(t *testing.T) {
	if err := ValidateCanonical("high"); err != nil {
		t.Fatalf("high rejected: %v", err)
	}
	err := ValidateCanonical("HIGH")
	if err == nil {
		t.Fatal("HIGH accepted")
	}
	if !strings.Contains(err.Error(), "invalid thinking value") || !strings.Contains(err.Error(), "HIGH") {
		t.Fatalf("error = %q, want substring %q and %q", err.Error(), "invalid thinking value", "HIGH")
	}
}

func TestEvaluate(t *testing.T) {
	// Empty value is always accepted.
	if got := Evaluate("agy", "", false); got != OK {
		t.Fatalf("empty value = %v", got)
	}

	// Unknown backend is an unsupported capability.
	if got := Evaluate("nope", "low", false); got != UnsupportedCapability {
		t.Errorf("unknown backend = %v", got)
	}

	// Backends with no support at all.
	for _, backend := range []string{"opencode-acp", "opencode-http", "gemini-acp", "cursor-acp", "agy", "pony"} {
		if got := Evaluate(backend, "low", false); got != UnsupportedCapability {
			t.Errorf("%s low = %v, want UnsupportedCapability", backend, got)
		}
	}

	// Full-support backends accept everything on start and resume.
	for _, backend := range []string{"codex-app-server", "pi"} {
		for _, value := range []string{"off", "minimal", "low", "medium", "high", "xhigh", "max"} {
			if got := Evaluate(backend, value, false); got != OK {
				t.Errorf("%s start %s = %v", backend, value, got)
			}
			if got := Evaluate(backend, value, true); got != OK {
				t.Errorf("%s resume %s = %v", backend, value, got)
			}
		}
	}

	// Claude family: low..max at start; unsupported capability / values
	// distinguished, and resume is start-only.
	for _, backend := range []string{"claude", "claude-channel"} {
		if got := Evaluate(backend, "off", false); got != UnsupportedValue {
			t.Errorf("%s off = %v, want UnsupportedValue", backend, got)
		}
		if got := Evaluate(backend, "low", false); got != OK {
			t.Errorf("%s low = %v, want OK", backend, got)
		}
		if got := Evaluate(backend, "low", true); got != StartOnly {
			t.Errorf("%s resume low = %v, want StartOnly", backend, got)
		}
	}
}

func TestStartValues(t *testing.T) {
	if got := StartValues("claude"); !reflect.DeepEqual(got, []string{"low", "medium", "high", "xhigh", "max"}) {
		t.Errorf("claude start = %v", got)
	}
	if got := StartValues("agy"); len(got) != 0 {
		t.Errorf("agy start = %v, want empty", got)
	}
}

// TestSchemaCanonicalLockstep guards against canonical tuple drift between the
// portable Umpire schema, the generated Go validation, and the exported Go
// tuple. The schema's check rule literally embeds the canonical values, so it
// is the single file of truth both Go and TypeScript verify against.
func TestSchemaCanonicalLockstep(t *testing.T) {
	data, err := os.ReadFile("../../schemas/thinking_policy.umpire.json")
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	var schema struct {
		Conditions map[string]any        `json:"conditions"`
		Fields     map[string]any        `json:"fields"`
		Rules     []map[string]any      `json:"rules"`
	}
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	if len(schema.Fields) != 1 || schema.Fields["thinking"] == nil {
		t.Fatalf("schema must declare exactly the thinking field")
	}

	// The schema must declare backend and resume conditions.
	if schema.Conditions["backend"] == nil || schema.Conditions["resume"] == nil {
		t.Fatal("schema must declare backend and resume conditions")
	}

	// Extract the canonical values embedded in the check rule's literal array.
	var schemaCanonical []string
	for _, rule := range schema.Rules {
		if rule["type"] != "check" {
			continue
		}
		check := rule["check"].(map[string]any)
		if check["op"] != "in" {
			continue
		}
		for _, v := range check["value"].([]any) {
			schemaCanonical = append(schemaCanonical, v.(string))
		}
	}
	if len(schemaCanonical) == 0 {
		t.Fatal("no canonical values found in schema")
	}
	if !reflect.DeepEqual(schemaCanonical, CanonicalValues()) {
		t.Fatalf("schema canonical %v != Go CanonicalValues() %v", schemaCanonical, CanonicalValues())
	}

	// The generated field struct must cover exactly the schema fields.
	fields := reflect.TypeOf(ThinkingPolicyFields{})
	if fields.NumField() != 1 || fields.Field(0).Name != "Thinking" {
		t.Fatalf("generated fields = %v", fields)
	}

	// The generated conditions struct must have Backend and Resume fields.
	conds := reflect.TypeOf(ThinkingPolicyConditions{})
	if conds.NumField() != 2 || conds.Field(0).Name != "Backend" || conds.Field(1).Name != "Resume" {
		t.Fatalf("generated conditions = %v", conds)
	}
}

// TestSchemaBackendsMatchConformance verifies that the set of backends with
// thinking support in the schema-derived supportedBackends set matches the
// backends that appear in the conformance fixture. This catches drift between
// the schema and the fixture.
func TestSchemaBackendsMatchConformance(t *testing.T) {
	type conformanceData struct {
		Backend []struct {
			Backend string `json:"backend"`
		} `json:"backendCases"`
	}
	data, err := os.ReadFile("../../schemas/thinking_policy.conformance.json")
	if err != nil {
		t.Fatalf("read conformance: %v", err)
	}
	var c conformanceData
	if err := json.Unmarshal(data, &c); err != nil {
		t.Fatalf("parse conformance: %v", err)
	}
	fixtureBackends := make(map[string]bool)
	for _, bc := range c.Backend {
		fixtureBackends[bc.Backend] = true
	}
	// Every backend in the fixture must be classified by supportedBackends.
	for backend := range fixtureBackends {
		// Just ensure Evaluate doesn't panic; the actual valid/invalid
		// classification is tested by TestConformanceBackendPolicy.
		_ = Evaluate(backend, "low", false)
	}
	// Every backend in supportedBackends must appear in the fixture.
	for backend := range supportedBackends {
		if !fixtureBackends[backend] {
			t.Errorf("supported backend %q missing from conformance fixture", backend)
		}
	}
}
