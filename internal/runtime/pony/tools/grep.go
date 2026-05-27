package tools

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

func NewGrepTool() Tool {
	return &GrepTool{}
}

type GrepTool struct{}

type grepInput struct {
	Pattern string `json:"pattern"`
	Glob    string `json:"glob"`
}

func (t *GrepTool) Name() string { return "grep" }

func (t *GrepTool) Description() string {
	return "Search for a regular expression pattern in files matching a glob. Returns matching lines with line numbers. Supports file path filter (glob pattern) and regex text search."
}

func (t *GrepTool) Schema() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {
			"pattern": {
				"type": "string",
				"description": "Regex pattern to search for"
			},
			"glob": {
				"type": "string",
				"description": "File glob pattern to search in (e.g. *.go)"
			}
		},
		"required": ["pattern", "glob"]
	}`)
}

func (t *GrepTool) Execute(ctx context.Context, workingDir string, args json.RawMessage) (string, error) {
	var input grepInput
	if err := json.Unmarshal(args, &input); err != nil {
		return "", fmt.Errorf("grep: invalid args: %w", err)
	}
	if input.Pattern == "" {
		return "", fmt.Errorf("grep: pattern is required")
	}
	if input.Glob == "" {
		return "", fmt.Errorf("grep: glob is required")
	}

	re, err := regexp.Compile(input.Pattern)
	if err != nil {
		return "", fmt.Errorf("grep: invalid regex: %w", err)
	}

	globPattern := input.Glob
	if !filepath.IsAbs(input.Glob) {
		globPattern = filepath.Join(workingDir, input.Glob)
	}
	globPattern, err = filepath.Abs(globPattern)
	if err != nil {
		return "", fmt.Errorf("grep: resolve glob: %w", err)
	}

	wdAbs, err := filepath.Abs(workingDir)
	if err != nil {
		return "", fmt.Errorf("grep: resolve working dir: %w", err)
	}

	globPatternRel, err := filepath.Rel(wdAbs, globPattern)
	if err != nil || strings.HasPrefix(globPatternRel, "..") {
		return "", fmt.Errorf("grep: glob pattern %q is outside the working directory", input.Glob)
	}

	matches, err := filepath.Glob(globPattern)
	if err != nil {
		return "", fmt.Errorf("grep: glob: %w", err)
	}

	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("grep: %w", err)
	}

	var results []string
	for _, matchFile := range matches {
		info, err := os.Stat(matchFile)
		if err != nil || info.IsDir() {
			continue
		}
		f, err := os.Open(matchFile)
		if err != nil {
			continue
		}

		relPath, _ := filepath.Rel(wdAbs, matchFile)

		scanner := bufio.NewScanner(f)
		lineNum := 0
		for scanner.Scan() {
			lineNum++
			if re.MatchString(scanner.Text()) {
				results = append(results, fmt.Sprintf("%s:%d:%s", relPath, lineNum, scanner.Text()))
			}
			if len(results) >= 100 {
				f.Close()
				goto done
			}
		}
		f.Close()
	}

done:
	return strings.Join(results, "\n"), nil
}
