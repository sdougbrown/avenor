package stable

import (
	"strings"
	"testing"
)

func TestDerivePermissionDetail(t *testing.T) {
	long := strings.Repeat("x", permissionDetailLimit+50)

	tests := []struct {
		name       string
		permission map[string]any
		wantDesc   string
		wantCmd    string
		wantChange bool
	}{
		{
			name: "nested tool call supplies title and command",
			permission: map[string]any{
				"request_id": "0",
				"toolCall": map[string]any{
					"title":    "mkdir -p /tmp/probe",
					"rawInput": map[string]any{"command": "mkdir -p /tmp/probe"},
				},
			},
			wantDesc:   "mkdir -p /tmp/probe",
			wantCmd:    "mkdir -p /tmp/probe",
			wantChange: true,
		},
		{
			name: "existing description wins over the tool call",
			permission: map[string]any{
				"description": "Allow bash to execute?",
				"toolCall":    map[string]any{"title": "mkdir -p /tmp/probe"},
			},
			wantDesc:   "Allow bash to execute?",
			wantCmd:    "",
			wantChange: false,
		},
		{
			name: "a request that already explains itself is untouched",
			permission: map[string]any{
				"question": "Allow bash to execute?",
			},
			wantDesc:   "",
			wantCmd:    "",
			wantChange: false,
		},
		{
			name: "top-level command becomes the description",
			permission: map[string]any{
				"command": "rm -rf build",
			},
			wantDesc:   "rm -rf build",
			wantCmd:    "rm -rf build",
			wantChange: true,
		},
		{
			name: "multi-line titles collapse to one line",
			permission: map[string]any{
				"toolCall": map[string]any{"title": "line one\n\tline two"},
			},
			wantDesc:   "line one line two",
			wantChange: true,
		},
		{
			name: "long titles are bounded",
			permission: map[string]any{
				"toolCall": map[string]any{"title": long},
			},
			wantDesc:   long[:permissionDetailLimit],
			wantChange: true,
		},
		{
			name:       "tool call kind is the last resort",
			permission: map[string]any{"toolCall": map[string]any{"kind": "other"}},
			wantDesc:   "other",
			wantChange: true,
		},
		{
			name:       "nothing to derive",
			permission: map[string]any{"request_id": "0"},
			wantChange: false,
		},
		{
			name:       "nil map",
			permission: nil,
			wantChange: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := derivePermissionDetail(test.permission)
			if changed != test.wantChange {
				t.Fatalf("changed = %v, want %v", changed, test.wantChange)
			}
			if got, _ := test.permission["description"].(string); got != test.wantDesc {
				t.Errorf("description = %q, want %q", got, test.wantDesc)
			}
			if got, _ := test.permission["command"].(string); got != test.wantCmd {
				t.Errorf("command = %q, want %q", got, test.wantCmd)
			}
		})
	}
}
