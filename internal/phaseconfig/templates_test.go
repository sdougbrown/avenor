package phaseconfig

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestTemplateRenderPrompt(t *testing.T) {
	tests := []struct {
		name    string
		tmpl    string
		ctx     TemplateContext
		want    string
		wantErr bool
	}{
		{
			name: "phase and iteration",
			tmpl: "Phase {{.Phase}}, iteration {{.Iteration}}",
			ctx: TemplateContext{
				Phase:     "test",
				Iteration: 2,
			},
			want: "Phase test, iteration 2",
		},
		{
			name: "run id",
			tmpl: "{{.RunID}}",
			ctx: TemplateContext{
				RunID: "abc123",
			},
			want: "abc123",
		},
		{
			name: "changed files present",
			tmpl: "{{if .ChangedFiles}}changed: {{.ChangedFiles}}{{else}}no changes{{end}}",
			ctx: TemplateContext{
				ChangedFiles: "foo.go\nbar.go",
			},
			want: "changed: foo.go\nbar.go",
		},
		{
			name: "changed files absent",
			tmpl: "{{if .ChangedFiles}}changed: {{.ChangedFiles}}{{else}}no changes{{end}}",
			ctx: TemplateContext{
				ChangedFiles: "",
			},
			want: "no changes",
		},
		{
			name:    "malformed template",
			tmpl:    "{{.NonexistentField",
			ctx:     TemplateContext{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := RenderPrompt(tt.tmpl, tt.ctx)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("RenderPrompt() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestTemplateCaptureGitDelta(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	workDir := t.TempDir()
	runGitForTest(t, workDir, "init")
	runGitForTest(t, workDir, "config", "user.email", "test@example.com")
	runGitForTest(t, workDir, "config", "user.name", "Test User")
	if err := os.WriteFile(workDir+"/tracked.txt", []byte("before\n"), 0o600); err != nil {
		t.Fatalf("write tracked file: %v", err)
	}
	runGitForTest(t, workDir, "add", "tracked.txt")
	runGitForTest(t, workDir, "commit", "-m", "initial")
	prevCommit := strings.TrimSpace(runGitForTest(t, workDir, "rev-parse", "HEAD"))
	if err := os.WriteFile(workDir+"/tracked.txt", []byte("after\n"), 0o600); err != nil {
		t.Fatalf("modify tracked file: %v", err)
	}

	t.Run("empty prevCommit", func(t *testing.T) {
		diffStat, changedFiles, err := CaptureGitDelta(workDir, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if diffStat != "" {
			t.Errorf("diffStat = %q, want empty", diffStat)
		}
		if changedFiles != "" {
			t.Errorf("changedFiles = %q, want empty", changedFiles)
		}
	})

	t.Run("valid commit SHA", func(t *testing.T) {
		diffStat, changedFiles, err := CaptureGitDelta(workDir, prevCommit)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strings.Contains(diffStat, "tracked.txt") {
			t.Fatalf("diffStat = %q, want tracked.txt", diffStat)
		}
		if strings.TrimSpace(changedFiles) != "tracked.txt" {
			t.Fatalf("changedFiles = %q, want tracked.txt", changedFiles)
		}
	})
}

func runGitForTest(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return string(out)
}