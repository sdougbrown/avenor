//go:build linux || darwin || freebsd

package terminal

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// printMarkerCmd reports whether the child can still see any of the parent's
// Claude session identity variables. Claude Code disables transcript
// persistence when the child inherits the session marker, and the transcript
// is the only end-of-turn signal the PTY backend has, so a leaked marker hangs
// the run forever. The other two are dropped so a hosted session cannot be
// mistaken for its launcher.
const printMarkerCmd = `printf 'CHILD=[%s] SID=[%s] PID=[%s]\n' "$CLAUDE_CODE_CHILD_SESSION" "$CLAUDE_CODE_SESSION_ID" "$CLAUDE_PID"; sleep 5`

func captureUntil(t *testing.T, session Session, want string) string {
	t.Helper()
	// The child writes the marker immediately, so the token arrives fast once the
	// PTY is spawned. It is the spawn plus first capture that can stall under
	// load, so poll for the whole context window rather than a short wall-clock
	// deadline — keeping the timeout from being a flakiness vector.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out string
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("never captured %q; last screen:\n%s", want, out)
			return out
		default:
		}
		got, err := session.Capture(ctx)
		if err != nil {
			t.Fatalf("Capture: %v", err)
		}
		out = got
		if strings.Contains(out, want) {
			return out
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// The PTY path must also scrub identity that a caller passes in via Env — the
// parent marker is not the only source of the leak, and scrubbing only
// os.Environ would still reintroduce it on append.
func TestPTYLauncherScrubsCallerProvidedEnv(t *testing.T) {
	session, err := (PTYLauncher{Env: []string{
		"CLAUDE_CODE_CHILD_SESSION=leaked-via-env",
		"CLAUDE_CODE_SESSION_ID=leaked-sid-via-env",
		"CLAUDE_PID=leaked-pid-via-env",
	}}).Start(context.Background(), StartOptions{
		Name:    "env-scrub-pty-env",
		Command: printMarkerCmd,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer session.Kill(context.Background())

	out := captureUntil(t, session, "CHILD=[")
	if !strings.Contains(out, "CHILD=[] SID=[] PID=[]") {
		t.Errorf("want all scrub targets cleared in child output, got:\n%s", out)
	}
	if strings.Contains(out, "leaked-via-env") || strings.Contains(out, "leaked-sid-via-env") || strings.Contains(out, "leaked-pid-via-env") {
		t.Errorf("child inherited a scrub target via Env; screen:\n%s", out)
	}
}

func TestPTYLauncherDoesNotLeakParentClaudeSession(t *testing.T) {
	t.Setenv("CLAUDE_CODE_CHILD_SESSION", "leaked-from-parent")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "leaked-sid-from-parent")
	t.Setenv("CLAUDE_PID", "leaked-pid-from-parent")

	session, err := (PTYLauncher{}).Start(context.Background(), StartOptions{
		Name:    "env-scrub-pty",
		Command: printMarkerCmd,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer session.Kill(context.Background())

	out := captureUntil(t, session, "CHILD=[")
	if !strings.Contains(out, "CHILD=[] SID=[] PID=[]") {
		t.Errorf("want all scrub targets cleared in child output, got:\n%s", out)
	}
	for _, sentinel := range []string{"leaked-from-parent", "leaked-sid-from-parent", "leaked-pid-from-parent"} {
		if strings.Contains(out, sentinel) {
			t.Errorf("child inherited %q; screen:\n%s", sentinel, out)
		}
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
	cmd := exec.Command("sh", "-c", UnsetParentClaudeEnvPrefix()+`exec printf 'CHILD=[%s] SID=[%s] PID=[%s]' "$CLAUDE_CODE_CHILD_SESSION" "$CLAUDE_CODE_SESSION_ID" "$CLAUDE_PID"`)
	cmd.Env = append(cmd.Environ(),
		"CLAUDE_CODE_CHILD_SESSION=leaked-child",
		"CLAUDE_CODE_SESSION_ID=leaked-sid",
		"CLAUDE_PID=leaked-pid",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("prefixed command failed: %v; output: %s", err, out)
	}
	if got := string(out); got != "CHILD=[] SID=[] PID=[]" {
		t.Fatalf("output = %q, want \"CHILD=[] SID=[] PID=[]\"", got)
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

func TestTmuxLauncherStartAppliesParentClaudeScrub(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "tmux.log")
	fakeDir := filepath.Join(tmpDir, "fakebin")
	if err := os.Mkdir(fakeDir, 0o755); err != nil {
		t.Fatalf("mkdir fake bin: %v", err)
	}
	fakePath := filepath.Join(fakeDir, "tmux")

	script := `#!/bin/sh
printf '%s\n' "$@" >> "$FAKE_TMUX_LOG"
case "$1" in
  new-session)
    exit 0
    ;;
  list-panes)
    printf '4242\n'
    exit 0
    ;;
esac
exit 1
`
	if err := os.WriteFile(fakePath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake tmux: %v", err)
	}

	t.Setenv("PATH", fakeDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FAKE_TMUX_LOG", logPath)

	session, err := (TmuxLauncher{}).Start(context.Background(), StartOptions{
		Name:    "avenor-test",
		Dir:     "/tmp/x",
		Cols:    220,
		Rows:    50,
		Command: "exec claude --session-id abc",
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	_ = session // fake never creates a real session; no Kill needed

	linesRaw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read tmux log: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(linesRaw)), "\n")

	// Find the index of the "new-session" line.
	nsIdx := -1
	for i, l := range lines {
		if l == "new-session" {
			nsIdx = i
			break
		}
	}
	if nsIdx < 0 {
		t.Fatalf("no new-session line found in log:\n%s", strings.Join(lines, "\n"))
	}

	// The last element before list-panes is the scrubbed command.
	lastCmdIdx := nsIdx
	for i := nsIdx + 1; i < len(lines); i++ {
		if lines[i] == "list-panes" {
			break
		}
		lastCmdIdx = i
	}
	cmdLine := lines[lastCmdIdx]

	if !strings.HasPrefix(cmdLine, UnsetParentClaudeEnvPrefix()) {
		t.Errorf("scrub prefix not found in new-session argv element at index %d (%q), want it to start with %q. Full log:\n%s",
			lastCmdIdx, cmdLine, UnsetParentClaudeEnvPrefix(), strings.Join(lines, "\n"))
	}
	if !strings.Contains(cmdLine, "exec claude --session-id abc") {
		t.Errorf("command not found in new-session argv element at index %d (%q). Full log:\n%s",
			lastCmdIdx, cmdLine, strings.Join(lines, "\n"))
	}
}
