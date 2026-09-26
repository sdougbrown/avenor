package workflow

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestValidateOutputSourceShape pins the typed-Go output-source rules the
// JSON Schema cannot express: exactly one of artifact or git, pointer
// requires an artifact, git can only name head, and the artifact path must
// be declared in the node's files completion contract.
func TestValidateOutputSourceShape(t *testing.T) {
	tests := []struct {
		name    string
		source  map[string]any
		wantErr string
	}{
		{
			name:   "whole artifact source",
			source: map[string]any{"artifact": "result.md"},
		},
		{
			name:   "pointer source into declared artifact",
			source: map[string]any{"artifact": "result.md", "pointer": "/name"},
		},
		{
			name:   "git head source",
			source: map[string]any{"git": "head"},
		},
		{
			name:    "artifact and git together",
			source:  map[string]any{"artifact": "result.md", "git": "head"},
			wantErr: `output "summary" declares both artifact and git source`,
		},
		{
			name:    "empty source",
			source:  map[string]any{},
			wantErr: `output "summary" declares an empty source`,
		},
		{
			name:    "pointer without artifact",
			source:  map[string]any{"pointer": "/name"},
			wantErr: `output "summary": pointer requires an artifact source`,
		},
		{
			name:    "git source other than head",
			source:  map[string]any{"git": "tree"},
			wantErr: `output "summary": git source "tree" must be "head"`,
		},
		{
			name:    "artifact not declared in the contract",
			source:  map[string]any{"artifact": "other.md"},
			wantErr: `output "summary" artifact source "other.md" is not declared in the node's files completion contract`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tmpl := Template{
				SchemaVersion:    1,
				TemplateID:       "source-shape",
				TemplateVersion:  "1",
				EntryNodes:       []NodeID{"produce"},
				TerminalOutcomes: []OutcomeName{"done"},
				Nodes: []NodeDefinition{{
					ID:       "produce",
					Action:   Action{Kind: ActionRun, Run: &RunAction{Prompt: "work"}},
					Dispatch: &DispatchPolicy{Mode: DispatchAuto, ControllerID: "c1"},
					Outputs: []OutputDefinition{{
						ID:     "summary",
						Name:   "Summary",
						Type:   OutputString,
						Source: decodeOutputSource(t, test.source),
					}},
					Completion: &CompletionContract{
						Kind:      CompletionFiles,
						Artifacts: []ArtifactRequirement{{Path: "result.md"}},
					},
				}},
			}
			err := ValidateTemplate(tmpl)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateTemplate() rejected a valid template: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("ValidateTemplate() error = %v, want containing %q", err, test.wantErr)
			}
		})
	}
}

// decodeOutputSource round-trips a raw source object through the typed model
// so the tests declare sources in their wire shape.
func decodeOutputSource(t *testing.T, raw map[string]any) *OutputSource {
	t.Helper()
	data, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("marshal source: %v", err)
	}
	var src OutputSource
	if err := json.Unmarshal(data, &src); err != nil {
		t.Fatalf("decode source: %v", err)
	}
	return &src
}
