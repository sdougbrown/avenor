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

// contextKey is used for context-scoped values in tool dispatch.
type contextKey string

const (
	// AllowedReadDirsKey carries additional directories read-only tools may access beyond workingDir.
	AllowedReadDirsKey contextKey = "allowed_read_dirs"
	// AllowedWriteDirsKey carries additional directories write tools may access beyond workingDir.
	AllowedWriteDirsKey contextKey = "allowed_write_dirs"
)

// AllowedReadDirsFromContext extracts additional read-allowed directories from context.
func AllowedReadDirsFromContext(ctx context.Context) []string {
	dirs, _ := ctx.Value(AllowedReadDirsKey).([]string)
	return dirs
}

// AllowedWriteDirsFromContext extracts additional write-allowed directories from context.
func AllowedWriteDirsFromContext(ctx context.Context) []string {
	dirs, _ := ctx.Value(AllowedWriteDirsKey).([]string)
	return dirs
}

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
	tools            map[string]Tool
	allowedReadDirs  []string // additional directories read tools may access
	allowedWriteDirs []string // additional directories write tools may access
}

// NewRegistry creates a registry from the given tools.
func NewRegistry(tools []Tool) *Registry {
	r := &Registry{
		tools: make(map[string]Tool, len(tools)),
	}
	for _, t := range tools {
		r.tools[t.Name()] = t
	}
	return r
}

// SetAllowedDirs sets additional directories that tools may access
// beyond the working directory for both read and write operations.
func (r *Registry) SetAllowedDirs(dirs []string) {
	r.allowedReadDirs = dirs
	r.allowedWriteDirs = dirs
}

// SetAllowedReadDirs sets additional directories read-only tools may access.
func (r *Registry) SetAllowedReadDirs(dirs []string) {
	r.allowedReadDirs = dirs
}

// SetAllowedWriteDirs sets additional directories write tools may access.
func (r *Registry) SetAllowedWriteDirs(dirs []string) {
	r.allowedWriteDirs = dirs
}

// Dispatch looks up a tool by name and executes it with the given JSON args.
// Returns the tool's output string, or an error if the tool is not found
// or execution fails.
func (r *Registry) Dispatch(ctx context.Context, workingDir string, name string, args json.RawMessage) (string, error) {
	t, ok := r.tools[name]
	if !ok {
		return "", fmt.Errorf("unknown tool: %s", name)
	}
	ctx = context.WithValue(ctx, AllowedReadDirsKey, r.allowedReadDirs)
	ctx = context.WithValue(ctx, AllowedWriteDirsKey, r.allowedWriteDirs)
	return t.Execute(ctx, workingDir, args)
}

// safeResolvePath resolves a path relative to workingDir, follows symlinks,
// and verifies the result is within workingDir or any additional allowed dir.
func safeResolvePath(workingDir string, additionalDirs []string, target string) (string, error) {
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

	// Check against workingDir first, then any additional allowed dirs
	baseDirs := append([]string{workingDir}, additionalDirs...)
	for _, base := range baseDirs {
		wdAbs, err := filepath.Abs(base)
		if err != nil {
			continue
		}
		rel, err := filepath.Rel(wdAbs, safePath)
		if err == nil && !strings.HasPrefix(rel, "..") {
			return safePath, nil
		}
	}

	return "", fmt.Errorf("path %q is outside the working directory and configured allowed directories", target)
}

// IsPathInAllowedDirs checks if a resolved path is within any of the allowed directories.
// Returns the relative path if found, empty string otherwise.
func IsPathInAllowedDirs(path string, additionalDirs []string) string {
	for _, dir := range additionalDirs {
		dirAbs, err := filepath.Abs(dir)
		if err != nil {
			continue
		}
		rel, err := filepath.Rel(dirAbs, path)
		if err == nil && !strings.HasPrefix(rel, "..") {
			return rel
		}
	}
	return ""
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
