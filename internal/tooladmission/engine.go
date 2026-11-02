package tooladmission

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"sort"
	"sync"
	"unicode/utf8"
)

// Renderer renders one repair sentence for an error code with its pre-quoted
// facts. Each router supplies its own renderer over its own repairs table, so
// the shared engine composes no sentence itself.
type Renderer func(code string, facts map[string]string) string

// Engine validates tool inputs against an applicability table. The zero value
// is not usable; build one with New.
type Engine struct {
	repair Renderer
}

// New builds an engine whose denials are rendered through repair.
func New(repair Renderer) *Engine {
	return &Engine{repair: repair}
}

// ValidateFields applies one action's applicability entry in the fixed order
// admission uses: a property outside Permitted, then a missing Required
// property, then a value whose wire type, enum or declared constraint is not
// met. The returned sentence is the invalid_request template's {detail} fact,
// rendered through the engine's renderer so no failure sentence is composed
// here.
func (e *Engine) ValidateFields(fields map[string]json.RawMessage, entry ActionApplicability) string {
	permitted := make(map[string]bool, len(entry.Permitted))
	for _, name := range entry.Permitted {
		permitted[name] = true
	}
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if !permitted[name] {
			return e.repair("unknown_property", map[string]string{"name": Quote(name)})
		}
	}
	for _, name := range entry.Required {
		if _, ok := fields[name]; !ok {
			return e.repair("missing_required_property", map[string]string{"name": Quote(name)})
		}
	}
	for _, name := range entry.Permitted {
		raw, ok := fields[name]
		if !ok {
			continue
		}
		kind := entry.Types[name]
		if !hasWireType(raw, kind) {
			switch kind {
			case "integer":
				return e.repair("wrong_type_integer", map[string]string{"name": Quote(name)})
			case "object":
				return e.repair("wrong_type_object", map[string]string{"name": Quote(name)})
			case "array":
				return e.repair("wrong_type_array", map[string]string{"name": Quote(name)})
			default:
				return e.repair("wrong_type_string", map[string]string{"name": Quote(name)})
			}
		}
		constraint := entry.Constraints[name]
		if kind == "array" && constraint.ItemType != "" {
			if detail := e.validateArrayItems(name, raw, constraint.ItemType); detail != "" {
				return detail
			}
		}
		if values, ok := entry.Enums[name]; ok {
			value, _ := FieldString(fields, name)
			if !contains(values, value) {
				return e.repair("invalid_enum", map[string]string{
					"name":   Quote(name),
					"values": QuoteList(values),
				})
			}
		}
		switch kind {
		case "string":
			value, _ := FieldString(fields, name)
			if detail := e.validateStringConstraint(name, value, constraint); detail != "" {
				return detail
			}
		case "integer":
			if detail := e.validateIntegerConstraint(name, raw, constraint); detail != "" {
				return detail
			}
		}
	}
	return ""
}

func (e *Engine) validateArrayItems(name string, raw json.RawMessage, itemType string) string {
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return e.repair("wrong_type_array", map[string]string{"name": Quote(name)})
	}
	for _, item := range items {
		if !hasWireType(item, itemType) {
			return e.repair("array_item_type", map[string]string{"name": Quote(name)})
		}
	}
	return ""
}

// validateStringConstraint applies the string limits the published JSON Schema
// declares. It reads the compiled constraints, so the schema and the router
// cannot disagree about length or allowed characters.
func (e *Engine) validateStringConstraint(name, value string, constraint PropertyConstraints) string {
	length := utf8.RuneCountInString(value)
	if constraint.MinLength > 0 && length < constraint.MinLength {
		return e.repair("string_too_short", map[string]string{
			"name": Quote(name),
			"min":  fmt.Sprint(constraint.MinLength),
		})
	}
	if constraint.MaxLength > 0 && length > constraint.MaxLength {
		return e.repair("string_too_long", map[string]string{
			"name": Quote(name),
			"max":  fmt.Sprint(constraint.MaxLength),
		})
	}
	if constraint.Pattern != "" {
		// A pattern that does not compile matches nothing, so every value is
		// refused rather than the check being skipped.
		if pattern, ok := compiledPattern(constraint.Pattern); !ok || !pattern.MatchString(value) {
			return e.repair("string_pattern", map[string]string{"name": Quote(name)})
		}
	}
	return ""
}

// validateIntegerConstraint applies the integer limits the published JSON
// Schema declares. The declared bound is the int64 limit the schema publishes,
// so a value outside int64 names the same bound the schema rejects with, and a
// declared bound wins where the schema declares one. Both bounds are read from
// their declared-presence flags, so a bound of zero is a limit and not an
// omission.
func (e *Engine) validateIntegerConstraint(name string, raw json.RawMessage, constraint PropertyConstraints) string {
	rational, ok := ExactInteger(raw)
	if !ok {
		return e.repair("wrong_type_integer", map[string]string{"name": Quote(name)})
	}
	if !rational.Num().IsInt64() {
		if rational.Sign() < 0 {
			return e.repair("integer_too_small", map[string]string{"name": Quote(name), "min": minIntegerBound(constraint)})
		}
		return e.repair("integer_too_large", map[string]string{"name": Quote(name), "max": maxIntegerBound(constraint)})
	}
	value := rational.Num().Int64()
	if constraint.HasMinimum && value < constraint.Minimum {
		return e.repair("integer_too_small", map[string]string{
			"name": Quote(name),
			"min":  fmt.Sprint(constraint.Minimum),
		})
	}
	if constraint.HasMaximum && value > constraint.Maximum {
		return e.repair("integer_too_large", map[string]string{
			"name": Quote(name),
			"max":  fmt.Sprint(constraint.Maximum),
		})
	}
	return ""
}

// minIntegerBound and maxIntegerBound render the bound a property declares, or
// the int64 limit as text when the schema declares none — the wire type's
// range, written literally because 2^63 does not fit an int64 constant.
func minIntegerBound(constraint PropertyConstraints) string {
	if constraint.HasMinimum {
		return fmt.Sprint(constraint.Minimum)
	}
	return "-9223372036854775808"
}

func maxIntegerBound(constraint PropertyConstraints) string {
	if constraint.HasMaximum {
		return fmt.Sprint(constraint.Maximum)
	}
	return "9223372036854775807"
}

// hasWireType reports whether raw carries the declared wire type. An integer
// means a mathematical integer, matching the JSON Schema: 1.0, 1e3 and values
// beyond int64 are integers; 1.5 and "1" are not. A value outside int64 is
// refused by the constraint checks, which name the bound.
func hasWireType(raw json.RawMessage, kind string) bool {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return false
	}
	switch kind {
	case "json":
		return true
	case "string":
		_, ok := value.(string)
		return ok
	case "integer":
		_, ok := ExactInteger(raw)
		return ok
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	}
	return false
}

// ExactInteger returns raw's exact rational value when it is a mathematical
// integer, so an integer-valued exponential such as 1e3 is admitted. Routers
// call it after admission to convert a validated integer exactly.
func ExactInteger(raw json.RawMessage) (*big.Rat, bool) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, false
	}
	number, ok := value.(json.Number)
	if !ok {
		return nil, false
	}
	rational, ok := new(big.Rat).SetString(number.String())
	if !ok || !rational.IsInt() {
		return nil, false
	}
	return rational, true
}

// FieldString returns the named property's string value, or false when the
// property is absent or not a string. Routers use it to extract the action
// name and the engine reads enum candidates with it.
func FieldString(fields map[string]json.RawMessage, name string) (string, bool) {
	raw, ok := fields[name]
	if !ok {
		return "", false
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", false
	}
	return value, true
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

// constraintPatterns caches every pattern the tables declare, compiled once. A
// pattern that failed to compile is cached as nil so it is not retried per call.
var constraintPatterns sync.Map

func compiledPattern(pattern string) (*regexp.Regexp, bool) {
	if cached, ok := constraintPatterns.Load(pattern); ok {
		compiled, _ := cached.(*regexp.Regexp)
		return compiled, compiled != nil
	}
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		constraintPatterns.Store(pattern, (*regexp.Regexp)(nil))
		return nil, false
	}
	constraintPatterns.Store(pattern, compiled)
	return compiled, true
}
