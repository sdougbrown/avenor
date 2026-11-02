package messaging

import (
	"encoding/json"
	"os"

	"github.com/sdougbrown/avenor/internal/tooladmission"
	"github.com/sdougbrown/avenor/internal/toolerr"
)

// ActionApplicability and PropertyConstraints are the shared contract types
// from internal/tooladmission, spelled the way this package's tables and tests
// have always named them. The definitions live in one place; the messaging and
// workflow tables are the same Go type.
type (
	ActionApplicability = tooladmission.ActionApplicability
	PropertyConstraints = tooladmission.PropertyConstraints
)

// ConformanceCase is one language-neutral conformance case from
// schemas/message_route.conformance.json. Input is kept as raw JSON so the
// routers and the JSON Schema test validate the exact on-wire keys.
type ConformanceCase struct {
	Name       string          `json:"name"`
	Input      json.RawMessage `json:"input"`
	Conditions map[string]any  `json:"conditions"`
	Provider   map[string]any  `json:"provider"`
	Schema     bool            `json:"schema"`
	Expect     struct {
		Code    string            `json:"code"`
		Facts   map[string]string `json:"facts"`
		Message string            `json:"message"`
		Outcome string            `json:"outcome"`
	} `json:"expect"`
}

// OutcomeRow is one outcome of the shared contract: its vocabulary columns, the
// result fields it carries, and its render template per action.
type OutcomeRow struct {
	Outcome                string
	Source                 string // messaging | tool
	MayHaveBeenTransmitted bool
	Terminal               bool
	ReuseMessageID         bool
	Fields                 []string
	Render                 map[string]string // action -> template
}

// OutcomeSpec is the whole outcome contract: the messaging vocabulary, the
// tool-level successes, and the peers `last` templates, which report no
// outcome.
type OutcomeSpec struct {
	Messaging []OutcomeRow
	Tool      []OutcomeRow
	PeersLast map[string]string
}

// MessageInputSchema returns the avenor_message input schema as a JSON value.
func MessageInputSchema() map[string]any { return decodeSchema(messageInputSchemaJSON) }

// PeersInputSchema returns the avenor_peers input schema as a JSON value.
func PeersInputSchema() map[string]any { return decodeSchema(peersInputSchemaJSON) }

// MessageInputFlatSchema returns the flat object schema MCP surfaces publish for
// avenor_message: it is compiled from the per-action union, keeps every action's
// properties and their declared types and limits, and requires only the keys
// every action requires — the action itself.
func MessageInputFlatSchema() map[string]any { return decodeSchema(messageInputFlatSchemaJSON) }

// PeersInputFlatSchema returns the flat object schema MCP surfaces publish for
// avenor_peers, compiled from the per-action union the same way. Its required set
// is the union's intersection, which is empty: the list action takes no
// arguments, so a peers call that omits the action is valid.
func PeersInputFlatSchema() map[string]any { return decodeSchema(peersInputFlatSchemaJSON) }

func decodeSchema(raw string) map[string]any {
	var schema map[string]any
	if err := json.Unmarshal([]byte(raw), &schema); err != nil {
		return nil
	}
	return schema
}

// ApplicabilityTable returns the message tool's per-action input contract,
// keyed by action.
func ApplicabilityTable() map[string]ActionApplicability {
	return cloneApplicability(messageApplicability)
}

// PeersApplicabilityTable returns the peers tool's per-action input contract,
// keyed by action.
func PeersApplicabilityTable() map[string]ActionApplicability {
	return cloneApplicability(peersApplicability)
}

func cloneApplicability(table map[string]ActionApplicability) map[string]ActionApplicability {
	out := make(map[string]ActionApplicability, len(table))
	for action, entry := range table {
		types := make(map[string]string, len(entry.Types))
		for name, kind := range entry.Types {
			types[name] = kind
		}
		enums := make(map[string][]string, len(entry.Enums))
		for name, values := range entry.Enums {
			enums[name] = append([]string(nil), values...)
		}
		constraints := make(map[string]PropertyConstraints, len(entry.Constraints))
		for name, constraint := range entry.Constraints {
			constraints[name] = constraint
		}
		out[action] = ActionApplicability{
			Access:      entry.Access,
			Permitted:   append([]string(nil), entry.Permitted...),
			Required:    append([]string(nil), entry.Required...),
			Types:       types,
			Enums:       enums,
			Constraints: constraints,
		}
	}
	return out
}

// LoadOutcomeSpec returns the shared outcome contract. The loader carries the
// Load prefix because Go cannot declare a type and a function with one name;
// the contract type keeps the name the contract documents.
func LoadOutcomeSpec() OutcomeSpec {
	spec := OutcomeSpec{PeersLast: make(map[string]string, len(peersLastTemplates))}
	for _, row := range outcomeRows {
		row.Render = cloneTemplates(row.Render)
		row.Fields = append([]string(nil), row.Fields...)
		if row.Source == "tool" {
			spec.Tool = append(spec.Tool, row)
			continue
		}
		spec.Messaging = append(spec.Messaging, row)
	}
	for key, template := range peersLastTemplates {
		spec.PeersLast[key] = template
	}
	return spec
}

func cloneTemplates(templates map[string]string) map[string]string {
	out := make(map[string]string, len(templates))
	for action, template := range templates {
		out[action] = template
	}
	return out
}

// LoadConformanceCases reads the message-tool cases from the shared conformance
// fixture at path. Tests pass ../../schemas/message_route.conformance.json.
func LoadConformanceCases(path string) ([]ConformanceCase, error) {
	fixture := struct {
		Cases []ConformanceCase `json:"cases"`
	}{}
	if err := decodeFixture(path, &fixture); err != nil {
		return nil, err
	}
	return fixture.Cases, nil
}

// LoadPeersConformanceCases reads the peers-tool cases from the same fixture.
func LoadPeersConformanceCases(path string) ([]ConformanceCase, error) {
	fixture := struct {
		Cases []ConformanceCase `json:"peers_cases"`
	}{}
	if err := decodeFixture(path, &fixture); err != nil {
		return nil, err
	}
	return fixture.Cases, nil
}

// LoadApplicabilityTables reads the fixture's applicability maps, which are the
// source of the permitted/required/type contract the routers validate against,
// so a test can compare the checked-in fixture with the compiled artifact.
func LoadApplicabilityTables(path string) (messages, peers map[string]ActionApplicability, err error) {
	fixture := struct {
		Applicability      map[string]ActionApplicability `json:"applicability"`
		PeersApplicability map[string]ActionApplicability `json:"peers_applicability"`
	}{}
	if err := decodeFixture(path, &fixture); err != nil {
		return nil, nil, err
	}
	return fixture.Applicability, fixture.PeersApplicability, nil
}

func decodeFixture(path string, fixture any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, fixture)
}

// Repair renders the template for an error code with the supplied pre-quoted
// facts. A placeholder with no fact is left intact so a missing fact fails the
// fixture comparison loudly instead of rendering half a sentence.
func Repair(code string, facts map[string]string) string {
	return toolerr.Substitute(repairsTemplates[code], facts)
}

// RenderOutcome renders the (action, outcome) template with the supplied
// pre-quoted facts. It is the only producer of a result's text, so a missing
// template is a generated-artifact error, caught by TestOutcomeRenderTotal.
func RenderOutcome(action string, outcome Outcome, facts map[string]string) string {
	return toolerr.Substitute(outcomeRenderTemplates[action+":"+string(outcome)], facts)
}

// LastRender renders one of the peers `last` templates with the supplied
// pre-quoted facts.
func LastRender(key string, facts map[string]string) string {
	return toolerr.Substitute(peersLastTemplates[key], facts)
}
