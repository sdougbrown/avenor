package tools

import (
	"context"
	"encoding/json"
	"fmt"
)

// Tool is the interface every pony tool implements.
type Tool interface {
	// Name returns the tool name used in function_call / tool_use blocks.
	Name() string

	// Description returns a human-readable description of what the tool does.
	// Sent to the model as part of the tool definition.
	Description() string

	// Schema returns the JSON Schema for the tool's arguments.
	Schema() json.RawMessage

	// Execute runs the tool with the given JSON arguments and returns
	// a string result, or an error.
	Execute(ctx context.Context, workingDir string, args json.RawMessage) (string, error)
}

// Registry holds all available tools and dispatches calls by name.
type Registry struct {
	tools map[string]Tool
}

// NewRegistry creates a registry from the given tools.
func NewRegistry(tools []Tool) *Registry {
	r := &Registry{tools: make(map[string]Tool, len(tools))}
	for _, t := range tools {
		r.tools[t.Name()] = t
	}
	return r
}

// Dispatch looks up a tool by name and executes it with the given JSON args.
// Returns the tool's output string, or an error if the tool is not found
// or execution fails.
func (r *Registry) Dispatch(ctx context.Context, workingDir string, name string, args json.RawMessage) (string, error) {
	t, ok := r.tools[name]
	if !ok {
		return "", fmt.Errorf("unknown tool: %s", name)
	}
	return t.Execute(ctx, workingDir, args)
}

// AllTools returns all registered tools.
func (r *Registry) AllTools() []Tool {
	result := make([]Tool, 0, len(r.tools))
	for _, t := range r.tools {
		result = append(result, t)
	}
	return result
}
