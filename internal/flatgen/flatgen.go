// Package flatgen compiles a per-action `oneOf` input schema into the single
// flat object schema an MCP surface publishes. The messaging and workflow
// genschema commands both call Flatten, so a change to the compile rules
// cannot leave one published schema behind the other.
package flatgen

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// Meta is the header of one generated flat input schema.
type Meta struct {
	Title       string
	ID          string
	Description string
}

// flattenInput compiles a per-action `oneOf` input schema into the single flat
// object schema the MCP surfaces publish. It keeps every action's properties
// with their declared types and limits, requires only the keys every action
// requires (the action for message, nothing for peers), and stops when two
// actions declare one property differently, because one flat property cannot
// carry both declarations. The union's `action` declarations are kept too: its
// default verbatim, and its per-action descriptions as one summary naming each
// action.
func Flatten(raw []byte, meta Meta) ([]byte, error) {
	var union struct {
		OneOf []struct {
			Properties map[string]map[string]any `json:"properties"`
			Required   []string                  `json:"required"`
		} `json:"oneOf"`
	}
	// UseNumber keeps a declared integer bound at the int64 limit exact: a
	// float64 decode rounds it before the schema is re-encoded.
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&union); err != nil {
		return nil, fmt.Errorf("decode input schema: %w", err)
	}
	if len(union.OneOf) == 0 {
		return nil, errors.New("input schema declares no action variant")
	}
	properties := make(map[string]any, len(union.OneOf)+4)
	declarations := map[string]map[string]any{}
	declaredBy := map[string]string{}
	actions := make([]string, 0, len(union.OneOf))
	// A flat schema can express only one required set, so it keeps the
	// intersection: a key any chosen action may omit must stay optional, or the
	// published schema would refuse a call the router admits.
	required := append([]string{}, union.OneOf[0].Required...)
	var actionDeclarations []actionDeclaration
	actionDefault := ""
	for i, variant := range union.OneOf {
		action, _ := variant.Properties["action"]["const"].(string)
		if action == "" {
			return nil, errors.New("an input schema variant declares no action const")
		}
		actions = append(actions, action)
		actionDeclarations = append(actionDeclarations, actionDeclaration{
			name:        action,
			description: stringValue(variant.Properties["action"]["description"]),
		})
		if declared, ok := variant.Properties["action"]["default"]; ok {
			value := stringValue(declared)
			if actionDefault != "" && actionDefault != value {
				return nil, fmt.Errorf("actions declare different defaults (%q and %q); the flat schema cannot keep both", actionDefault, value)
			}
			actionDefault = value
		}
		if i > 0 {
			required = intersectRequired(required, variant.Required)
		}
		for name, spec := range variant.Properties {
			if name == "action" {
				continue
			}
			declared := withoutDescription(spec)
			existing, seen := properties[name].(map[string]any)
			if !seen {
				properties[name] = copyMap(spec)
				declarations[name] = declared
				declaredBy[name] = action
				continue
			}
			if err := compareDeclaration(name, declaredBy[name], action, declarations[name], declared); err != nil {
				return nil, err
			}
			existing["description"] = mergeDescription(existing["description"], spec["description"])
		}
	}
	properties["action"] = map[string]any{
		"type":        "string",
		"enum":        actions,
		"description": describeActions(actionDeclarations, actionDefault),
	}
	if actionDefault != "" {
		properties["action"].(map[string]any)["default"] = actionDefault
	}
	required = intersectRequired(required, []string{"action"})
	doc := map[string]any{
		"$schema":              "https://json-schema.org/draft/2020-12/schema",
		"$id":                  meta.ID,
		"title":                meta.Title,
		"description":          meta.Description,
		"type":                 "object",
		"properties":           properties,
		"required":             required,
		"additionalProperties": false,
	}
	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode flat schema: %w", err)
	}
	return append(out, '\n'), nil
}

// actionDeclaration is one action's own declaration on the union's `action`
// property: the action and the description attached to its variant.
type actionDeclaration struct {
	name        string
	description string
}

// describeActions renders the flat `action` property's description. The union
// carries one description per action and one flat property cannot keep them
// separately, so each action is named with what it does; the action declaring
// the union's default says so, because a client prefills from it. A union that
// describes no action keeps the bare list.
func describeActions(declarations []actionDeclaration, defaultAction string) string {
	parts := make([]string, 0, len(declarations))
	described := 0
	for _, declaration := range declarations {
		part := declaration.name
		if summary := strings.TrimSuffix(strings.TrimSpace(declaration.description), "."); summary != "" {
			part += " — " + summary
			described++
		}
		if declaration.name == defaultAction {
			part += " (the default)"
		}
		parts = append(parts, part)
	}
	separator := ", "
	if described > 0 {
		separator = "; "
	}
	return "Action to perform: " + strings.Join(parts, separator) + "."
}

// compareDeclaration reports why one flat property cannot carry two actions'
// declarations. The type disagreement is named as such, because that is the
// case a surface would have to resolve with the owner rather than the generator.
func compareDeclaration(name, firstAction, action string, declared, incoming map[string]any) error {
	if reflect.DeepEqual(declared, incoming) {
		return nil
	}
	if !reflect.DeepEqual(declared["type"], incoming["type"]) {
		return fmt.Errorf("property %q is declared as type %v by action %q and as type %v by action %q; the flat schema cannot keep both", name, declared["type"], firstAction, incoming["type"], action)
	}
	return fmt.Errorf("property %q carries different constraints for action %q and action %q; the flat schema cannot keep both", name, firstAction, action)
}

// intersectRequired keeps the keys both sets require, in the first set's order.
func intersectRequired(first, second []string) []string {
	out := make([]string, 0, len(first))
	for _, name := range first {
		for _, other := range second {
			if name == other {
				out = append(out, name)
				break
			}
		}
	}
	return out
}

func withoutDescription(spec map[string]any) map[string]any {
	declared := copyMap(spec)
	delete(declared, "description")
	return declared
}

func copyMap(spec map[string]any) map[string]any {
	out := make(map[string]any, len(spec))
	for key, value := range spec {
		out[key] = value
	}
	return out
}

// stringValue reads a JSON string keyword, treating any other value as absent.
func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

func mergeDescription(existing, incoming any) string {
	first, _ := existing.(string)
	next, _ := incoming.(string)
	switch {
	case first == "":
		return next
	case next == "" || next == first:
		return first
	}
	return first + " " + next
}
