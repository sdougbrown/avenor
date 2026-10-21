package main

import (
	"encoding/json"
	"reflect"
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

	source, err := generate(messageSchema, peersSchema, nil, nil, conformance, outcomeFixture{})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if !strings.Contains(source, "Maximum: 0, HasMaximum: true") {
		t.Fatalf("generated table drops the declared zero maximum:\n%s", source)
	}
}

// TestFlattenReportsAPropertyTypeDisagreement pins the generator's stop rule:
// one flat property cannot carry two actions' declared types, so the generator
// must fail loudly instead of publishing the first type and silently widening
// or narrowing the other action's input.
func TestFlattenReportsAPropertyTypeDisagreement(t *testing.T) {
	union := []byte(`{"oneOf":[
		{"properties":{"action":{"const":"tell"},"payload":{"type":"object"}}},
		{"properties":{"action":{"const":"reply"},"payload":{"type":"string"}}}
	]}`)
	_, err := flattenInput(union, flatSchemaMeta{})
	if err == nil {
		t.Fatal("flattenInput accepted two declarations of one property")
	}
	for _, want := range []string{`property "payload"`, "type object", "type string"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not name %q", err, want)
		}
	}
}

// TestFlattenKeepsTheActionDeclarations pins the two action declarations one
// flat property still has to carry: the union's action default, which a client
// prefills from, and every action's description, which cannot stay attached to
// a per-action const once the variants are gone. Dropping them loses the peers
// default and the only in-schema warning that an action is backend-only.
func TestFlattenKeepsTheActionDeclarations(t *testing.T) {
	union := []byte(`{"oneOf":[
		{"properties":{"action":{"const":"list","default":"list","description":"List the sessions visible to the caller."}}},
		{"properties":{"action":{"const":"rename","description":"Rename a run's display name."},"name":{"type":"string"}},"required":["action","name"]},
		{"properties":{"action":{"const":"last","description":"Read a peer's last agent message."},"target":{"type":"string"}},"required":["action","target"]}
	]}`)
	flat, err := flattenInput(union, flatSchemaMeta{Title: "t", ID: "id", Description: "d"})
	if err != nil {
		t.Fatalf("flattenInput: %v", err)
	}
	var doc struct {
		Properties map[string]map[string]any `json:"properties"`
	}
	if err := json.Unmarshal(flat, &doc); err != nil {
		t.Fatalf("decode flat schema: %v", err)
	}
	action := doc.Properties["action"]
	if got := action["default"]; got != "list" {
		t.Fatalf("action default = %v, want the union's list", got)
	}
	description, _ := action["description"].(string)
	for _, want := range []string{
		"list — List the sessions visible to the caller (the default)",
		"rename — Rename a run's display name",
		"last — Read a peer's last agent message",
	} {
		if !strings.Contains(description, want) {
			t.Fatalf("action description %q does not carry %q", description, want)
		}
	}
}

// TestFlattenNamesEveryActionWithoutDescriptions pins the summary's fallback: a
// union that describes no action still publishes a description naming each one,
// so the flat action property is never left undocumented.
func TestFlattenNamesEveryActionWithoutDescriptions(t *testing.T) {
	union := []byte(`{"oneOf":[
		{"properties":{"action":{"const":"tell"}}},
		{"properties":{"action":{"const":"ask"}}}
	]}`)
	flat, err := flattenInput(union, flatSchemaMeta{Title: "t", ID: "id", Description: "d"})
	if err != nil {
		t.Fatalf("flattenInput: %v", err)
	}
	var doc struct {
		Properties map[string]map[string]any `json:"properties"`
	}
	if err := json.Unmarshal(flat, &doc); err != nil {
		t.Fatalf("decode flat schema: %v", err)
	}
	if got, want := doc.Properties["action"]["description"], "Action to perform: tell, ask."; got != want {
		t.Fatalf("action description = %v, want %q", got, want)
	}
}

// TestFlattenRequiresOnlyTheIntersection pins the published shape the MCP
// surfaces depend on: one object, an action enum, no union, and no required key
// beyond the ones every action requires, so a client cannot refuse an otherwise
// valid call before the router sees it. The intersection is the action here; for
// peers, whose list action takes no arguments, it is empty.
func TestFlattenRequiresOnlyTheIntersection(t *testing.T) {
	union := []byte(`{"oneOf":[
		{"properties":{"action":{"const":"ask"},"target":{"type":"string","description":"Recipient."},"timeout_ms":{"type":"integer","minimum":0,"maximum":600000}},"required":["action","target"]},
		{"properties":{"action":{"const":"resume"},"target":{"type":"string","description":"Prior run."}},"required":["action"]}
	]}`)
	flat, err := flattenInput(union, flatSchemaMeta{Title: "t", ID: "id", Description: "d"})
	if err != nil {
		t.Fatalf("flattenInput: %v", err)
	}
	for _, banned := range []string{`"oneOf"`, `"anyOf"`} {
		if strings.Contains(string(flat), banned) {
			t.Fatalf("flat schema carries %s:\n%s", banned, flat)
		}
	}
	var doc struct {
		Type       string                    `json:"type"`
		Required   []string                  `json:"required"`
		Properties map[string]map[string]any `json:"properties"`
	}
	if err := json.Unmarshal(flat, &doc); err != nil {
		t.Fatalf("decode flat schema: %v", err)
	}
	if doc.Type != "object" {
		t.Fatalf("flat type = %q, want object", doc.Type)
	}
	if !reflect.DeepEqual(doc.Required, []string{"action"}) {
		t.Fatalf("flat required = %v, want only action", doc.Required)
	}
	if got, want := doc.Properties["action"]["enum"], []any{"ask", "resume"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("action enum = %v, want %v", got, want)
	}
	if got := doc.Properties["timeout_ms"]["maximum"]; got != float64(600000) {
		t.Fatalf("timeout_ms maximum = %v, want the declared 600000", got)
	}
	if got, want := doc.Properties["target"]["description"], "Recipient. Prior run."; got != want {
		t.Fatalf("target description = %v, want %q", got, want)
	}
}
