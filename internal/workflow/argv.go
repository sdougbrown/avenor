package workflow

import (
	"fmt"
	"path/filepath"
	"strings"
)

// argv.go holds the one declared-argv vocabulary shared by launch wrappers and
// completion commands. A template of literal and instance-param elements
// resolves to an exact argv slice, and template-side validation runs before any
// element reaches a process.

// ResolveArgv resolves one declared argv template into an exact argv slice.
// Literal elements pass through verbatim, and instance-param elements resolve
// from the named parameter's recorded value. No element is ever split, joined,
// or expanded. what names the declaration in errors.
//
// Templates are normally validated before they reach this resolver, but the
// resolver fails closed anyway. An empty declaration, an unsupplied or empty
// value, or a NUL in any element is an error. So is a first element that is not
// a literal absolute path. None is ever a silently degraded argv.
func ResolveArgv(what string, declared []ArgvElement, params map[string]string) ([]string, error) {
	if len(declared) == 0 {
		return nil, fmt.Errorf("%s: argv must not be empty", what)
	}
	argv := make([]string, 0, len(declared))
	for index, arg := range declared {
		value, err := argvElementValue(what, index, arg, params)
		if err != nil {
			return nil, err
		}
		argv = append(argv, value)
	}
	if declared[0].Type != ArgvElementKindLiteral || !filepath.IsAbs(argv[0]) {
		return nil, fmt.Errorf("%s: argv[0] must be a literal absolute executable path", what)
	}
	return argv, nil
}

// argvElementValue resolves one argv element, rejecting an empty or NUL result
// regardless of the element's declared form.
func argvElementValue(what string, index int, arg ArgvElement, params map[string]string) (string, error) {
	var value string
	switch arg.Type {
	case ArgvElementKindLiteral:
		value = arg.Value
	case ArgvElementKindInstanceParam:
		value = params[arg.FromInstanceParam]
		if _, ok := params[arg.FromInstanceParam]; !ok {
			return "", fmt.Errorf("%s: argv[%d] instance param %q is not supplied", what, index, arg.FromInstanceParam)
		}
	default:
		return "", fmt.Errorf("%s: argv[%d] type %q is not an argv element type", what, index, arg.Type)
	}
	if value == "" {
		return "", fmt.Errorf("%s: argv[%d] resolved to an empty value", what, index)
	}
	if strings.ContainsRune(value, '\x00') {
		return "", fmt.Errorf("%s: argv[%d] must not contain NUL", what, index)
	}
	return value, nil
}

// validateArgv checks tagged elements and requires a literal absolute
// executable path first. what names the declaration in errors; parameter
// references must name declared, required instance parameters so an argv
// element is never empty.
func validateArgv(what string, argv []ArgvElement, declared map[string]struct{}, required map[string]bool) error {
	if len(argv) == 0 {
		return fmt.Errorf("%s argv must not be empty", what)
	}
	for index, arg := range argv {
		switch arg.Type {
		case ArgvElementKindLiteral:
			if arg.FromInstanceParam != "" {
				return fmt.Errorf("%s argv[%d] must not declare from_instance_param", what, index)
			}
			if arg.Value == "" {
				return fmt.Errorf("%s argv[%d] literal value must not be empty", what, index)
			}
			if strings.ContainsRune(arg.Value, '\x00') {
				return fmt.Errorf("%s argv[%d] literal value must not contain NUL", what, index)
			}
		case ArgvElementKindInstanceParam:
			if arg.Value != "" {
				return fmt.Errorf("%s argv[%d] must not declare value", what, index)
			}
			if strings.TrimSpace(arg.FromInstanceParam) == "" {
				return fmt.Errorf("%s argv[%d].from_instance_param is required", what, index)
			}
			if _, ok := declared[arg.FromInstanceParam]; !ok {
				return fmt.Errorf("%s argv[%d] names undeclared instance param %q", what, index, arg.FromInstanceParam)
			}
			// An unsupplied optional param would resolve to an empty argv
			// element, so the named param must be declared required.
			if !required[arg.FromInstanceParam] {
				return fmt.Errorf("%s argv[%d] names instance param %q, which must be declared required", what, index, arg.FromInstanceParam)
			}
		default:
			return fmt.Errorf("%s argv[%d] type %q is not an argv element type", what, index, arg.Type)
		}
	}
	if argv[0].Type != ArgvElementKindLiteral || !filepath.IsAbs(argv[0].Value) {
		return fmt.Errorf("%s argv[0] must be a literal absolute executable path", what)
	}
	return nil
}
