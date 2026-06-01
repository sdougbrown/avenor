package phaseconfig

import (
	"bytes"
	"os/exec"
	"strings"
	"text/template"
)

type TemplateContext struct {
	RunID           string
	Phase           string
	Iteration       int
	MaxIterations   int
	WorkDir         string
	PrevPhaseCommit string
	DiffStat        string
	ChangedFiles    string
	TeamOutput      string
	TeamFinalOutput string
}

func RenderPrompt(tmpl string, ctx TemplateContext) (string, error) {
	t, err := template.New("prompt").Parse(tmpl)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, ctx); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func CaptureGitDelta(workDir, prevCommit string) (diffStat, changedFiles string, err error) {
	if prevCommit == "" {
		return "", "", nil
	}

	diffStat, err = runGit(workDir, "diff", "--stat", prevCommit)
	if err != nil {
		return "", "", nil
	}

	changedFiles, err = runGit(workDir, "diff", "--name-only", prevCommit)
	if err != nil {
		return "", "", nil
	}

	return diffStat, changedFiles, nil
}

func runGit(workDir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = workDir
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return string(bytes.TrimRight(out, "\n")), nil
}

func CaptureHeadCommit(workDir string) string {
	cmd := exec.Command("git", "-C", workDir, "rev-parse", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}
