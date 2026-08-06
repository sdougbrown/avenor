//go:build linux || darwin || freebsd

package terminal

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// printMarkerCmd reports whether the child can still see the parent's
// child-session marker. Claude Code disables transcript persistence when it
// inherits that marker, and the transcript is the only end-of-turn signal the
// PTY backend has, so a leaked marker hangs the run forever.
const printMarkerCmd = `printf 'MARKER=[%s]\n' "$CLAUDE_CODE_CHILD_SESSION"; sleep 5`

func captureUntil(t *testing.T, session Session, want string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	deadline := time.Now().Add(4 * time.Second)
	var out string
	for time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		got, err := session.Capture(ctx)
		if err != nil {
			t.Fatalf("Capture: %v", err)
		}
		out = got
		if strings.Contains(out, want) {
			return out
		}
	}
	t.Fatalf("never captured %q; last screen:\n%s", want, out)
	return out
}

func TestPTYLauncherDoesNotLeakParentClaudeSession(t *testing.T) {
	t.Setenv("CLAUDE_CODE_CHILD_SESSION", "leaked-from-parent")

	session, err := (PTYLauncher{}).Start(context.Background(), StartOptions{
		Name:    "env-scrub-pty",
		Command: printMarkerCmd,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer session.Kill(context.Background())

	out := captureUntil(t, session, "MARKER=[")
	if strings.Contains(out, "leaked-from-parent") {
		t.Errorf("child inherited CLAUDE_CODE_CHILD_SESSION; screen:\n%s", out)
	}
	if !strings.Contains(out, "MARKER=[]") {
		t.Errorf("want MARKER=[] in child output, got:\n%s", out)
	}
}

// A tmux pane inherits the tmux *server's* environment, not this process's, so
// t.Setenv cannot reach the child and an end-to-end assertion here would pass
// whether or not the scrub exists. Assert on the rendered command instead: it is
// what strips the marker no matter which environment the server was started
// with, including a server that baked the marker in long before this run.
func TestTmuxNewSessionArgsUnsetParentClaudeSession(t *testing.T) {
	args := tmuxNewSessionArgs(StartOptions{
		Name:    "avenor-test",
		Dir:     "/tmp/x",
		Cols:    220,
		Rows:    50,
		Command: "exec claude --session-id abc",
	})

	shellCmd := args[len(args)-1]
	if !strings.HasPrefix(shellCmd, UnsetParentClaudeEnvPrefix()) {
		t.Fatalf("tmux command = %q, want it prefixed with %q", shellCmd, UnsetParentClaudeEnvPrefix())
	}
	if !strings.HasSuffix(shellCmd, "exec claude --session-id abc") {
		t.Fatalf("tmux command = %q, want the original command preserved at the end", shellCmd)
	}
}

// The prefix composes with a real `exec` command under a real shell. This is the
// check that rules out `env -u`, which would have sh hand env a builtin name and
// fail with "env: exec: No such file or directory".
func TestUnsetParentClaudeEnvPrefixRunsUnderShellWithExec(t *testing.T) {
	cmd := exec.Command("sh", "-c", UnsetParentClaudeEnvPrefix()+`exec printf 'MARKER=[%s]' "$CLAUDE_CODE_CHILD_SESSION"`)
	cmd.Env = append(cmd.Environ(), "CLAUDE_CODE_CHILD_SESSION=leaked-from-parent")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("prefixed command failed: %v; output: %s", err, out)
	}
	if got := string(out); got != "MARKER=[]" {
		t.Fatalf("output = %q, want \"MARKER=[]\"", got)
	}
}

func TestTmuxLauncherStartRejectsEmptyCommand(t *testing.T) {
	if _, err := exec.LookPath("tmux"); err != nil {
		t.Skip("tmux not installed")
	}
	if _, err := (TmuxLauncher{}).Start(context.Background(), StartOptions{Name: "avenor-test"}); err == nil {
		t.Fatal("Start succeeded with no Command")
	}
}
