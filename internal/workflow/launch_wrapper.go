package workflow

import "fmt"

// ResolveNodeLaunchWrapper resolves one node's launch-wrapper selection into
// an argv slice. Every literal element is passed through exactly as declared,
// with no shell expansion, interpolation, or trimming. Every instance-param
// element is replaced with the named parameter's recorded value. A node without
// a selection resolves to nil with no error; the resolved elements and their
// fail-closed checks are ResolveArgv's.
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
	return ResolveArgv(fmt.Sprintf("launch wrapper %q", node.LaunchWrapper), definition.Argv, params)
}
