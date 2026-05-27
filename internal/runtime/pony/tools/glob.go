package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
)

func NewGlobTool() Tool {
	return &GlobTool{}
}

type GlobTool struct{}

type globInput struct {
	Pattern string `json:"pattern"`
}

func (t *GlobTool) Name() string { return "glob" }

func (t *GlobTool) Description() string {
	return "List files matching a glob pattern. Patterns use filepath.Match syntax (?, *, [...]) and are relative to the working directory."
}

func (t *GlobTool) Schema() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {
			"pattern": {
				"type": "string",
				"description": "Glob pattern to match (e.g. *.go, src/**/*.ts)"
			}
		},
		"required": ["pattern"]
	}`)
}

func (t *GlobTool) Execute(ctx context.Context, workingDir string, args json.RawMessage) (string, error) {
	var input globInput
	if err := json.Unmarshal(args, &input); err != nil {
		return "", fmt.Errorf("glob: invalid args: %w", err)
	}
	if input.Pattern == "" {
		return "", fmt.Errorf("glob: pattern is required")
	}

	pattern := input.Pattern
	if !filepath.IsAbs(input.Pattern) {
		pattern = filepath.Join(workingDir, input.Pattern)
	}
	pattern, err := filepath.Abs(pattern)
	if err != nil {
		return "", fmt.Errorf("glob: resolve pattern: %w", err)
	}

	wdAbs, err := filepath.Abs(workingDir)
	if err != nil {
		return "", fmt.Errorf("glob: resolve working dir: %w", err)
	}

	// Validate pattern resolves within working directory
	patternRel, err := filepath.Rel(wdAbs, pattern)
	if err != nil || strings.HasPrefix(patternRel, "..") {
		return "", fmt.Errorf("glob: pattern %q is outside the working directory", input.Pattern)
	}

	matches, err := filepath.Glob(pattern)
	if err != nil {
		return "", fmt.Errorf("glob: %w", err)
	}

	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("glob: %w", err)
	}

	var relPaths []string
	for _, m := range matches {
		relPath, err := filepath.Rel(wdAbs, m)
		if err != nil {
			continue
		}
		relPaths = append(relPaths, relPath)
	}

	return strings.Join(relPaths, "\n"), nil
}
