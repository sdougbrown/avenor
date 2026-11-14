package claudecore

import (
	"reflect"
	"strings"
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
	const bashPrompt = "Bash command rm -rf build Do you want to proceed?"
	tests := []struct {
		name       string
		pane       string
		want       []PermissionOption
		wantPrompt string
	}{
		{name: "wrapped label", pane: bash, want: bashOptions, wantPrompt: bashPrompt},
		{name: "numbered prose above", pane: "● Plan:\n  1. Clean the build\n  2. Rebuild\n\n" + bash, want: bashOptions, wantPrompt: "1. Clean the build 2. Rebuild " + bashPrompt},
		{name: "cut-off list above", pane: "  2. Rebuild\n  3. Run tests\n\n" + bash, want: bashOptions, wantPrompt: "2. Rebuild 3. Run tests " + bashPrompt},
		{
			name:       "numbered line in command preview",
			pane:       "Bash command\n  rm -rf build && cat <<'EOF'\n  9. example\n  EOF\nDo you want to proceed?\n❯ 1. Yes\n  2. No\n",
			want:       []PermissionOption{{ID: "1", Label: "Yes", Kind: "allow"}, {ID: "2", Label: "No", Kind: "reject"}},
			wantPrompt: "Bash command rm -rf build && cat <<'EOF' 9. example EOF Do you want to proceed?",
		},
		{
			name:       "plan approval",
			pane:       "Would you like to proceed?\n❯ 1. Yes, and auto-accept edits\n  2. Yes, and manually approve edits\n  3. No, keep planning\n",
			wantPrompt: "Would you like to proceed?",
			want: []PermissionOption{
				{ID: "1", Label: "Yes, and auto-accept edits", Kind: "allow"},
				{ID: "2", Label: "Yes, and manually approve edits", Kind: "allow"},
				{ID: "3", Label: "No, keep planning", Kind: "reject"},
			},
		},
		{
			name:       "unrecognized label",
			pane:       "Do you want to proceed?\n❯ 1. Yes\n  2. Skip this step\n  3. Yesterday's build\n",
			wantPrompt: "Do you want to proceed?",
			want: []PermissionOption{
				{ID: "1", Label: "Yes", Kind: "allow"},
				{ID: "2", Label: "Skip this step", Kind: ""},
				{ID: "3", Label: "Yesterday's build", Kind: ""},
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
			if perm.Prompt != tt.wantPrompt {
				t.Fatalf("prompt = %q, want %q", perm.Prompt, tt.wantPrompt)
			}
		})
	}
}

// The tool dialogs carry their question and the file path in the pane text
// above the option list, which is what the brokered description shows.
func TestParseTerminalPermissionFileDialogs(t *testing.T) {
	fileOptions := []PermissionOption{
		{ID: "1", Label: "Yes", Kind: "allow"},
		{ID: "2", Label: "Yes, and switch to accept edits (auto-approve file edits and common file commands) for this session", Kind: "allow"},
		{ID: "3", Label: "No", Kind: "reject"},
	}
	tests := []struct {
		name     string
		pane     string
		wantPath string
		wantAsk  string
	}{
		{name: "create file", pane: paneCaptureCreateFile, wantPath: "Create file hello.txt", wantAsk: "Do you want to create hello.txt?"},
		{name: "edit file", pane: paneCaptureEditFile, wantPath: "Edit file hello.txt", wantAsk: "Do you want to make this edit to hello.txt?"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			perm := ParseTerminalPermission(tt.pane)
			if perm == nil {
				t.Fatal("no permission parsed")
			}
			if !reflect.DeepEqual(perm.Options, fileOptions) {
				t.Fatalf("options = %+v, want %+v", perm.Options, fileOptions)
			}
			if !strings.Contains(perm.Prompt, tt.wantPath) || !strings.Contains(perm.Prompt, tt.wantAsk) {
				t.Fatalf("prompt = %q, want it to carry %q and %q", perm.Prompt, tt.wantPath, tt.wantAsk)
			}
		})
	}
}
