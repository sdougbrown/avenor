package tools

import (
	"context"
	"encoding/json"
	"fmt"
)

// ReportFindingToolName is the canonical name for the report_finding tool.
const ReportFindingToolName = "report_finding"

func NewReportFindingTool() Tool {
	return &ReportFindingTool{}
}

type ReportFindingTool struct{}

type findingInput struct {
	File       string `json:"file"`
	Line       int    `json:"line"`
	Severity   string `json:"severity"`
	Message    string `json:"message"`
	Suggestion string `json:"suggestion,omitempty"`
}

func (t *ReportFindingTool) Name() string { return "report_finding" }

func (t *ReportFindingTool) Description() string {
	return "Report a structured finding. Call this for each issue you identify. Severity must be one of: error, warning, info."
}

func (t *ReportFindingTool) Schema() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {
			"file": {"type": "string", "description": "Path to the file with the finding"},
			"line": {"type": "integer", "description": "Line number of the finding"},
			"severity": {"type": "string", "description": "Severity level: error, warning, or info"},
			"message": {"type": "string", "description": "Description of the finding"},
			"suggestion": {"type": "string", "description": "Suggested fix or next step"}
		},
		"required": ["file", "line", "severity", "message"]
	}`)
}

func (t *ReportFindingTool) Execute(ctx context.Context, workingDir string, args json.RawMessage) (string, error) {
	var input findingInput
	if err := json.Unmarshal(args, &input); err != nil {
		return "", fmt.Errorf("report_finding: invalid args: %w", err)
	}
	if input.File == "" {
		return "", fmt.Errorf("report_finding: file is required")
	}
	if input.Severity == "" {
		return "", fmt.Errorf("report_finding: severity is required")
	}
	if input.Message == "" {
		return "", fmt.Errorf("report_finding: message is required")
	}

	// Validate severity
	switch input.Severity {
	case "error", "warning", "info":
	default:
		return "", fmt.Errorf("report_finding: invalid severity %q (must be error, warning, or info)", input.Severity)
	}

	// In a structured flow, this result would be parsed by the supervisor.
	// For now, return a human-readable summary.
	result := fmt.Sprintf("[%s] %s:%d — %s", input.Severity, input.File, input.Line, input.Message)
	if input.Suggestion != "" {
		result += "\nSuggestion: " + input.Suggestion
	}
	return result, nil
}

// FindingResult returns a structured representation of the finding for event emission.
func FindingResult(input json.RawMessage) (map[string]any, error) {
	var f findingInput
	if err := json.Unmarshal(input, &f); err != nil {
		return nil, err
	}
	return map[string]any{
		"file":       f.File,
		"line":       f.Line,
		"severity":   f.Severity,
		"message":    f.Message,
		"suggestion": f.Suggestion,
	}, nil
}
