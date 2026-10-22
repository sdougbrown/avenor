package workflow

import (
	"fmt"
	"path/filepath"
	"strings"
)

// ResolveNodeLaunchWrapper resolves one node's launch-wrapper selection into
// an argv slice: every literal element is passed through exactly as declared
// (no shell expansion, no interpolation, no trimming), and every
// instance-param element is replaced with the named parameter's recorded
// value. A node without a selection resolves to nil with no error.
//
// Templates are normally validated before they reach this resolver, but the
// resolver fails closed anyway: a missing definition, an unsupplied or empty
// resolved value, a NUL in any argv element, or a first element that is not
// a literal absolute path is an error, never a silently degraded argv.
func ResolveNodeLaunchWrapper(template Template, node NodeDefinition, params map[string]string) ([]string, error) {
	if node.LaunchWrapper == "" {
		return nil, nil
	}
	var definition *LaunchWrapperDefinition
	for index := range template.LaunchWrappers {
		if template.LaunchWrappers[index].ID == node.LaunchWrapper {
			definition = &template.LaunchWrappers[index]
			break
		}
	}
	if definition == nil {
		return nil, fmt.Errorf("launch wrapper %q: unknown launch wrapper", node.LaunchWrapper)
	}
	argv := make([]string, 0, len(definition.Argv))
	for index, arg := range definition.Argv {
		value, err := launchWrapperArgValue(node.LaunchWrapper, index, arg, params)
		if err != nil {
			return nil, err
		}
		argv = append(argv, value)
	}
	if len(argv) == 0 || definition.Argv[0].Type != LaunchWrapperArgKindLiteral || !filepath.IsAbs(argv[0]) {
		return nil, fmt.Errorf("launch wrapper %q: argv[0] must be a literal absolute executable path", node.LaunchWrapper)
	}
	return argv, nil
}

// launchWrapperArgValue resolves one argv element, rejecting an empty or NUL
// result regardless of the element's declared form.
func launchWrapperArgValue(wrapperID string, index int, arg LaunchWrapperArg, params map[string]string) (string, error) {
	var value string
	switch arg.Type {
	case LaunchWrapperArgKindLiteral:
		value = arg.Value
	case LaunchWrapperArgKindInstanceParam:
		value = params[arg.FromInstanceParam]
		if _, ok := params[arg.FromInstanceParam]; !ok {
			return "", fmt.Errorf("launch wrapper %q: argv[%d] instance param %q is not supplied", wrapperID, index, arg.FromInstanceParam)
		}
	default:
		return "", fmt.Errorf("launch wrapper %q: argv[%d] type %q is not a launch_wrapper_arg type", wrapperID, index, arg.Type)
	}
	if value == "" {
		return "", fmt.Errorf("launch wrapper %q: argv[%d] resolved to an empty value", wrapperID, index)
	}
	if strings.ContainsRune(value, '\x00') {
		return "", fmt.Errorf("launch wrapper %q: argv[%d] must not contain NUL", wrapperID, index)
	}
	return value, nil
}
