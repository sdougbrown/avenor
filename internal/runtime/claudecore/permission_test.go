package claudecore

import (
	"reflect"
	"testing"
)

func TestParseTerminalPermissionTakesDialogOptions(t *testing.T) {
	bash := "Bash command\n  rm -rf build\nDo you want to proceed?\n" +
		"❯ 1. Yes\n" +
		"  2. Yes, and don't ask again for rm commands in\n     /Users/me/project\n" +
		"  3. No, and tell Claude what to do differently (esc)\n"
	bashOptions := []PermissionOption{
		{ID: "1", Label: "Yes", Kind: "allow"},
		{ID: "2", Label: "Yes, and don't ask again for rm commands in", Kind: "allow"},
		{ID: "3", Label: "No, and tell Claude what to do differently (esc)", Kind: "reject"},
	}
	tests := []struct {
		name string
		pane string
		want []PermissionOption
	}{
		{name: "wrapped label", pane: bash, want: bashOptions},
		{name: "numbered prose above", pane: "● Plan:\n  1. Clean the build\n  2. Rebuild\n  3. Run tests\n\n" + bash, want: bashOptions},
		{name: "cut-off list above", pane: "  2. Rebuild\n  3. Run tests\n\n" + bash, want: bashOptions},
		{
			name: "plan approval",
			pane: "Would you like to proceed?\n❯ 1. Yes, and auto-accept edits\n  2. Yes, and manually approve edits\n  3. No, keep planning\n",
			want: []PermissionOption{
				{ID: "1", Label: "Yes, and auto-accept edits", Kind: "allow"},
				{ID: "2", Label: "Yes, and manually approve edits", Kind: "allow"},
				{ID: "3", Label: "No, keep planning", Kind: "reject"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			perm := ParseTerminalPermission(tt.pane)
			if perm == nil {
				t.Fatal("no permission parsed")
			}
			if !reflect.DeepEqual(perm.Options, tt.want) {
				t.Fatalf("options = %+v, want %+v", perm.Options, tt.want)
			}
		})
	}
}
