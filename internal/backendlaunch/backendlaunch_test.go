package backendlaunch

import (
	"slices"
	"strings"
	"testing"
)

// Test registration runs once per test binary (Register panics on a duplicate
// name, and -count=N re-runs tests in the same process).
func init() {
	Register("testlaunch", []string{"command", "args", "settings"}, func(entryName string, in Input) (Spec, error) {
		return Spec{Command: entryName + ":" + in.Command, Args: in.Args, Settings: in.Settings}, nil
	})
}

func TestValidateAcceptsDeclaredFields(t *testing.T) {
	if err := Validate("testlaunch", Input{Command: "/opt/x", Args: []string{"serve"}, Settings: "/etc/x.yaml"}); err != nil {
		t.Fatalf("Validate(testlaunch) = %v, want nil", err)
	}
}

func TestValidateRejectsUndeclaredFields(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input Input
	}{
		{name: "command", input: Input{Command: "/opt/x"}},
		{name: "args", input: Input{Args: []string{"serve"}}},
		{name: "settings", input: Input{Settings: "/etc/x.yaml"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate("unregistered", tc.input)
			if err == nil {
				t.Fatalf("Validate(unregistered, %s) = nil, want rejection", tc.name)
			}
			if !slices.Contains([]string{"command", "args", "settings"}, tc.name) || !strings.Contains(err.Error(), tc.name) || !strings.Contains(err.Error(), "unregistered") {
				t.Fatalf("Validate() error = %q, want field %q and backend named", err, tc.name)
			}
		})
	}
}

func TestFieldsAndResolveDispatch(t *testing.T) {
	if got := Fields("testlaunch"); !slices.Equal(got, []string{"command", "args", "settings"}) {
		t.Fatalf("Fields(testlaunch) = %v", got)
	}
	if got := Fields("unregistered"); got != nil {
		t.Fatalf("Fields(unregistered) = %v, want nil", got)
	}
	spec, err := Resolve("testlaunch", "reviewer", Input{Command: "/opt/x", Settings: "/etc/x.yaml"})
	if err != nil {
		t.Fatalf("Resolve(testlaunch) = %v", err)
	}
	if spec.Command != "reviewer:/opt/x" || spec.Settings != "/etc/x.yaml" {
		t.Fatalf("Resolve(testlaunch) = %+v, want the registered resolver's spec", spec)
	}
	if zero, err := Resolve("unregistered", "reviewer", Input{Command: "/opt/x"}); err != nil || !zero.IsZero() {
		t.Fatalf("Resolve(unregistered) = %+v, %v; want zero spec and no error", zero, err)
	}
}

func TestRegisterRejectsDuplicateAndIncompleteRegistrations(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func()
	}{
		{name: "duplicate", call: func() {
			Register("testlaunch", []string{"command"}, func(string, Input) (Spec, error) { return Spec{}, nil })
		}},
		{name: "empty backend", call: func() { Register("", []string{"command"}, func(string, Input) (Spec, error) { return Spec{}, nil }) }},
		{name: "no fields", call: func() { Register("nofields", nil, func(string, Input) (Spec, error) { return Spec{}, nil }) }},
		{name: "no resolver", call: func() { Register("noresolver", []string{"command"}, nil) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("Register(%s) did not panic", tc.name)
				}
			}()
			tc.call()
		})
	}
}

func TestSpecEqualDiffAndValue(t *testing.T) {
	base := Spec{Command: "/opt/x", Args: []string{"serve"}, Settings: "/etc/x.yaml"}
	if !base.Equal(Spec{Command: "/opt/x", Args: []string{"serve"}, Settings: "/etc/x.yaml"}) {
		t.Fatal("Equal(same spec) = false")
	}
	for _, tc := range []struct {
		field  string
		change Spec
	}{
		{field: "command", change: Spec{Command: "/opt/y", Args: []string{"serve"}, Settings: "/etc/x.yaml"}},
		{field: "args", change: Spec{Command: "/opt/x", Args: []string{"serve", "--verbose"}, Settings: "/etc/x.yaml"}},
		{field: "settings", change: Spec{Command: "/opt/x", Args: []string{"serve"}, Settings: "/etc/y.yaml"}},
	} {
		t.Run(tc.field, func(t *testing.T) {
			if got := Diff(base, tc.change); got != tc.field {
				t.Fatalf("Diff() = %q, want %q", got, tc.field)
			}
			if got := tc.change.Value(tc.field); got == "" {
				t.Fatalf("Value(%q) = empty, want the changed value", tc.field)
			}
		})
	}
	if !(Spec{}).IsZero() {
		t.Fatal("zero Spec.IsZero() = false")
	}

	// An explicit empty args list is honored at launch, so it is a different
	// launch identity from an absent one: IsZero and Diff hold the same rule.
	explicitEmpty := Spec{Args: []string{}}
	absent := Spec{}
	if explicitEmpty.IsZero() {
		t.Fatal("a spec with an explicit empty args list is zero")
	}
	if got := Diff(explicitEmpty, absent); got != "args" {
		t.Fatalf("Diff(explicit empty args, absent args) = %q, want args", got)
	}
	// A conflict message is read by an operator deciding what to change, so the
	// two sides of a presence-only difference must not render identically.
	if got := absent.Value("args"); got != "(none)" {
		t.Fatalf("absent args Value() = %q, want (none)", got)
	}
	if got := explicitEmpty.Value("args"); got == absent.Value("args") {
		t.Fatalf("explicit empty and absent args both render %q, want distinct messages", got)
	}
	if explicitEmpty.Equal(absent) {
		t.Fatal("Equal(explicit empty args, absent args) = true, want distinct identities")
	}
	if !(Spec{Args: []string{}}).Equal(Spec{Args: []string{}}) {
		t.Fatal("Equal(two explicit empty args specs) = false")
	}
}
