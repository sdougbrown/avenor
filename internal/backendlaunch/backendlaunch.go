// Package backendlaunch declares the roster-entry fields a backend owns and
// resolves them into the executable identity recorded for a session.
//
// Every backend starts with no extra roster fields: strict decoding rejects
// them and resolution is the zero Spec. A backend that launches a configured
// binary registers the fields it reads and the resolver that turns them into a
// Spec, so roster validation and the direct CLI and supervisor paths never name
// a backend.
package backendlaunch

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
)

// Spec is the executable identity a backend resolves from one roster entry
// before a provider exists. It is recorded per session, so a resume that
// resolves to a different executable identity is rejected.
type Spec struct {
	Command  string
	Args     []string
	Settings string
}

// IsZero reports whether the spec carries no launch identity at all. An
// explicit empty Args is present, not absent: the launch honors it as "no
// arguments", which is a different identity from an entry that named none.
func (s Spec) IsZero() bool {
	return s.Command == "" && s.Args == nil && s.Settings == ""
}

// Equal reports whether two specs describe the same launch identity.
func (s Spec) Equal(other Spec) bool {
	return Diff(s, other) == ""
}

// Diff returns the first ownership field ("command", "args" or "settings")
// whose value differs between two specs, or "" when they are identical. The
// name is the roster field, so a conflict message can name what changed. Args
// compare by presence and value, like IsZero: an explicit empty list is a
// different launch identity from an absent one.
func Diff(a, b Spec) string {
	switch {
	case a.Command != b.Command:
		return "command"
	case (a.Args == nil) != (b.Args == nil) || !slices.Equal(a.Args, b.Args):
		return "args"
	case a.Settings != b.Settings:
		return "settings"
	default:
		return ""
	}
}

// Value renders one ownership field for a conflict message. An absent args
// list renders as "(none)", so a presence-only difference between two specs
// stays visible in the message that reports it.
func (s Spec) Value(field string) string {
	switch field {
	case "command":
		return s.Command
	case "args":
		if s.Args == nil {
			return "(none)"
		}
		return strings.Join(s.Args, " ")
	case "settings":
		return s.Settings
	default:
		return ""
	}
}

// Input carries the roster entry values a backend declared as its own. A
// backend that registered no fields always receives the zero Input.
//
// Its fields are the launch-field vocabulary: each JSON key is the roster key an
// entry uses, and the table shared code validates against is derived from them,
// so adding a launch field is one edit here.
type Input struct {
	Command  string   `json:"command,omitempty"`
	Args     []string `json:"args,omitempty"`
	Settings string   `json:"settings,omitempty"`
}

// Resolver turns one roster entry's declared fields into a Spec. entryName is
// the roster key, empty when the launch selected no entry, so a resolver's
// errors can name what the operator has to fix.
type Resolver func(entryName string, input Input) (Spec, error)

type registration struct {
	fields   []string
	resolver Resolver
}

var (
	mu            sync.RWMutex
	registrations = map[string]registration{}
)

type inputField struct {
	name    string
	present func(Input) bool
}

// inputFields is the launch field vocabulary Input carries, in the order
// Validate reports it. It is derived from Input's fields, so a field added there
// is validated and advertised without a second edit. It is the only place a
// field's presence is decided, so a backend's registered names and the fields
// shared code can see cannot drift.
var inputFields = inputFieldTable()

func inputFieldTable() []inputField {
	typ := reflect.TypeFor[Input]()
	fields := make([]inputField, 0, typ.NumField())
	keys := make(map[string]string, typ.NumField())
	for i := range typ.NumField() {
		structField := typ.Field(i)
		key, _, _ := strings.Cut(structField.Tag.Get("json"), ",")
		if key == "" || key == "-" {
			panic(fmt.Sprintf("backendlaunch: Input field %q has no roster key", structField.Name))
		}
		if other, duplicate := keys[key]; duplicate {
			panic(fmt.Sprintf("backendlaunch: Input fields %q and %q share roster key %q", other, structField.Name, key))
		}
		keys[key] = structField.Name
		fields = append(fields, inputField{name: key, present: launchFieldPresent(i)})
	}
	return fields
}

// launchFieldPresent reports whether the Input field at index is set. A slice is
// present when it is not nil, so an explicit empty args list stays a launch
// decision; every other field is present when it carries a value.
func launchFieldPresent(index int) func(Input) bool {
	return func(in Input) bool {
		value := reflect.ValueOf(in).Field(index)
		if value.Kind() == reflect.Slice {
			return !value.IsNil()
		}
		return !value.IsZero()
	}
}

// Register declares the extra roster fields backend owns and the resolver that
// turns them into a Spec. A backend calls it from its package init, so a binary
// that does not link the backend cannot accept its fields. Every name must be a
// field of Input; Register panics otherwise, because a field no resolver can be
// handed would be advertised by Fields yet never validated.
func Register(backend string, fields []string, resolver Resolver) {
	if strings.TrimSpace(backend) == "" {
		panic("backendlaunch: Register requires a backend name")
	}
	if len(fields) == 0 {
		panic("backendlaunch: Register requires at least one field")
	}
	if resolver == nil {
		panic("backendlaunch: Register requires a resolver")
	}
	for _, field := range fields {
		if !slices.ContainsFunc(inputFields, func(f inputField) bool { return f.name == field }) {
			panic(fmt.Sprintf("backendlaunch: backend %q registered unknown launch field %q", backend, field))
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if _, exists := registrations[backend]; exists {
		panic(fmt.Sprintf("backendlaunch: backend %q already registered", backend))
	}
	registrations[backend] = registration{fields: slices.Clone(fields), resolver: resolver}
}

// Fields returns the roster fields backend declared, or nil when it declared
// none.
func Fields(backend string) []string {
	mu.RLock()
	defer mu.RUnlock()
	return slices.Clone(registrations[backend].fields)
}

// Validate reports the first field in input that backend does not declare.
// Only a backend that registered the field may carry it in a roster entry.
func Validate(backend string, input Input) error {
	fields := Fields(backend)
	for _, field := range inputFields {
		if field.present(input) && !slices.Contains(fields, field.name) {
			return fmt.Errorf("field %q is not supported by backend %q", field.name, backend)
		}
	}
	return nil
}

// Declared returns the first launch field input sets in field order, or "" when
// it sets none. Presence follows the rule Validate uses, so an explicit empty
// args list is a declared launch.
func Declared(input Input) string {
	for _, field := range inputFields {
		if field.present(input) {
			return field.name
		}
	}
	return ""
}

// Resolve turns one entry's declared fields into a Spec. An unregistered
// backend resolves to the zero Spec: its roster entries cannot carry fields and
// start no configured binary.
func Resolve(backend, entryName string, input Input) (Spec, error) {
	mu.RLock()
	reg, ok := registrations[backend]
	mu.RUnlock()
	if !ok {
		return Spec{}, nil
	}
	return reg.resolver(entryName, input)
}
