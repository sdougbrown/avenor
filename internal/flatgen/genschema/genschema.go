// Package genschema holds the scaffolding the messaging and workflow
// genschema commands share: the fixture types they decode, the applicability
// table writer, and the small file and formatting helpers. One copy means a
// change to the applicability-table format cannot leave one generated table
// behind the other.
package genschema

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

// ApplicabilityEntry is one action's row in a conformance fixture's
// applicability map.
type ApplicabilityEntry struct {
	Access    string            `json:"access"`
	Permitted []string          `json:"permitted"`
	Required  []string          `json:"required"`
	Types     map[string]string `json:"types"`
}

// SchemaProperty is the subset of a JSON Schema property the generators read.
type SchemaProperty struct {
	Const     string          `json:"const"`
	Type      string          `json:"type"`
	MinLength int             `json:"minLength"`
	MaxLength int             `json:"maxLength"`
	Pattern   string          `json:"pattern"`
	Minimum   *int64          `json:"minimum"`
	Maximum   *int64          `json:"maximum"`
	Enum      []string        `json:"enum"`
	Items     *SchemaProperty `json:"items"`
}

// InputSchema is a per-action `oneOf` input schema.
type InputSchema struct {
	OneOf []struct {
		Properties map[string]SchemaProperty `json:"properties"`
		Required   []string                  `json:"required"`
	} `json:"oneOf"`
}

// PropertyConstraints is one property's declared limits, compiled from the
// published JSON Schema so the router and the schema enforce the same rule from
// one declaration.
type PropertyConstraints struct {
	MinLength  int
	MaxLength  int
	Pattern    string
	Minimum    int64
	HasMinimum bool
	Maximum    int64
	HasMaximum bool
	ItemType   string
}

// ConformanceFixture is the language-neutral fixture both generators decode.
// A tool that declares no peers applicability leaves that map nil.
type ConformanceFixture struct {
	Repairs            map[string]string             `json:"repairs"`
	Applicability      map[string]ApplicabilityEntry `json:"applicability"`
	PeersApplicability map[string]ApplicabilityEntry `json:"peers_applicability"`
}

// ConstraintsFromSchema reads each variant's string length, pattern, integer
// bounds, enum and array item type, so the compiled applicability table
// enforces the published schema's rules instead of restating them.
func ConstraintsFromSchema(raw []byte) (map[string]map[string]PropertyConstraints, map[string]map[string][]string, error) {
	var schema InputSchema
	if err := json.Unmarshal(raw, &schema); err != nil {
		return nil, nil, fmt.Errorf("decode input schema: %w", err)
	}
	constraints := map[string]map[string]PropertyConstraints{}
	enums := map[string]map[string][]string{}
	for _, variant := range schema.OneOf {
		action := variant.Properties["action"].Const
		if action == "" {
			continue
		}
		for property, spec := range variant.Properties {
			if property == "action" {
				continue
			}
			if len(spec.Enum) > 0 {
				if enums[action] == nil {
					enums[action] = map[string][]string{}
				}
				enums[action][property] = spec.Enum
			}
			if spec.MinLength == 0 && spec.MaxLength == 0 && spec.Pattern == "" && spec.Minimum == nil && spec.Maximum == nil && spec.Items == nil {
				continue
			}
			if constraints[action] == nil {
				constraints[action] = map[string]PropertyConstraints{}
			}
			constraint := PropertyConstraints{MinLength: spec.MinLength, MaxLength: spec.MaxLength, Pattern: spec.Pattern}
			if spec.Minimum != nil {
				constraint.Minimum = *spec.Minimum
				constraint.HasMinimum = true
			}
			if spec.Maximum != nil {
				constraint.Maximum = *spec.Maximum
				constraint.HasMaximum = true
			}
			if spec.Items != nil {
				constraint.ItemType = spec.Items.Type
			}
			constraints[action][property] = constraint
		}
	}
	return constraints, enums, nil
}

// WriteApplicability writes one generated applicability table. name is the
// generated variable and description completes its doc sentence.
func WriteApplicability(b *strings.Builder, name, description string, table map[string]ApplicabilityEntry, constraints map[string]map[string]PropertyConstraints, enums map[string]map[string][]string) {
	fmt.Fprintf(b, "// %s is %s.\nvar %s = map[string]ActionApplicability{\n", name, description, name)
	for _, action := range SortedKeys(table) {
		entry := table[action]
		for property, kind := range entry.Types {
			switch kind {
			case "string", "integer", "object", "array", "json":
			default:
				panic(fmt.Sprintf("%s: property %q declares unsupported type %q", action, property, kind))
			}
		}
		fmt.Fprintf(b, "\t%s: {", strconv.Quote(action))
		if entry.Access != "" {
			fmt.Fprintf(b, "Access: %s, ", strconv.Quote(entry.Access))
		}
		fmt.Fprintf(b, "Permitted: %s, Required: %s, Types: map[string]string{", StringSlice(entry.Permitted), StringSlice(entry.Required))
		for _, property := range SortedKeys(entry.Types) {
			fmt.Fprintf(b, "%s: %s, ", strconv.Quote(property), strconv.Quote(entry.Types[property]))
		}
		b.WriteString("}")
		if len(enums[action]) > 0 {
			b.WriteString(", Enums: map[string][]string{")
			for _, property := range SortedKeys(enums[action]) {
				fmt.Fprintf(b, "%s: %s, ", strconv.Quote(property), StringSlice(enums[action][property]))
			}
			b.WriteString("}")
		}
		if len(constraints[action]) > 0 {
			b.WriteString(", Constraints: map[string]PropertyConstraints{")
			for _, property := range SortedKeys(constraints[action]) {
				constraint := constraints[action][property]
				fmt.Fprintf(b, "%s: {", strconv.Quote(property))
				if constraint.MinLength > 0 {
					fmt.Fprintf(b, "MinLength: %d, ", constraint.MinLength)
				}
				if constraint.MaxLength > 0 {
					fmt.Fprintf(b, "MaxLength: %d, ", constraint.MaxLength)
				}
				if constraint.Pattern != "" {
					fmt.Fprintf(b, "Pattern: %s, ", strconv.Quote(constraint.Pattern))
				}
				if constraint.HasMinimum {
					fmt.Fprintf(b, "Minimum: %d, HasMinimum: true, ", constraint.Minimum)
				}
				if constraint.HasMaximum {
					fmt.Fprintf(b, "Maximum: %d, HasMaximum: true, ", constraint.Maximum)
				}
				if constraint.ItemType != "" {
					fmt.Fprintf(b, "ItemType: %s, ", strconv.Quote(constraint.ItemType))
				}
				b.WriteString("}, ")
			}
			b.WriteString("}")
		}
		b.WriteString("},\n")
	}
	b.WriteString("}\n")
}

// StringSlice renders a string slice as a Go literal, or nil when empty.
func StringSlice(values []string) string {
	if len(values) == 0 {
		return "nil"
	}
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, strconv.Quote(value))
	}
	return "[]string{" + strings.Join(quoted, ", ") + "}"
}

// SortedKeys returns a map's keys in sorted order, so generated output does not
// depend on map iteration.
func SortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// ReadFile reads a generator input, exiting on failure.
func ReadFile(path string) []byte {
	data, err := os.ReadFile(path)
	if err != nil {
		Fail("read %s: %v", path, err)
	}
	return data
}

// MustDecode decodes a generator input, exiting on failure.
func MustDecode(path string, value any) {
	if err := json.Unmarshal(ReadFile(path), value); err != nil {
		Fail("decode %s: %v", path, err)
	}
}

// Fail reports a generator error and exits.
func Fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "genschema: "+format+"\n", args...)
	os.Exit(1)
}
