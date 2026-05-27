package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

// safeResolvePath resolves a path relative to workingDir, follows symlinks,
// and verifies the result is within workingDir. Returns the resolved absolute path.
func safeResolvePath(workingDir, target string) (string, error) {
	absPath := target
	if !filepath.IsAbs(target) {
		absPath = filepath.Join(workingDir, target)
	}
	absPath, err := filepath.Abs(absPath)
	if err != nil {
		return "", err
	}
	// Resolve symlinks to prevent symlink-based escapes.
	// For paths that don't exist yet (new file writes), resolve the parent instead.
	safePath, err := filepath.EvalSymlinks(absPath)
	if err != nil {
		if os.IsNotExist(err) {
			parent := filepath.Dir(absPath)
			parentSafe, err := filepath.EvalSymlinks(parent)
			if err != nil {
				return "", err
			}
			safePath = filepath.Join(parentSafe, filepath.Base(absPath))
		} else {
			return "", err
		}
	}
	wdAbs, err := filepath.Abs(workingDir)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(wdAbs, safePath)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("path %q is outside the working directory", target)
	}
	return safePath, nil
}

// AllTools returns all registered tools.
func (r *Registry) AllTools() []Tool {
	names := make([]string, 0, len(r.tools))
	for n := range r.tools {
		names = append(names, n)
	}
	sort.Strings(names)
	result := make([]Tool, 0, len(names))
	for _, n := range names {
		result = append(result, r.tools[n])
	}
	return result
}
