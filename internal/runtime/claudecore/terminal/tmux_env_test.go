package terminal

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A launch's environment file holds the broker credential it provisions, so it
// must not outlive the attempt that wrote it. A `tmux new-session` that fails
// hands no session back for teardown, so Start removes the file itself; this
// drive is the only one that reaches that path.
func TestTmuxFailedNewSessionRemovesTheLaunchEnvFile(t *testing.T) {
	tmp := t.TempDir()
	fakeDir := filepath.Join(tmp, "fakebin")
	if err := os.Mkdir(fakeDir, 0o755); err != nil {
		t.Fatalf("mkdir fake bin: %v", err)
	}
	logPath := filepath.Join(tmp, "tmux.log")
	// The pane command is the last argv element; it sources the launch env file
	// from a single-quoted path, which the fake records while the file exists.
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$FAKE_TMUX_LOG"
command=""
for arg in "$@"; do
  case "$arg" in
    *avenor-launch-env-*) command="$arg" ;;
  esac
done
file=$(printf '%s' "$command" | sed -n "s/.*\. '\([^']*\)';.*/\1/p")
if [ -n "$file" ] && [ -f "$file" ]; then printf 'present %s\n' "$file" >> "$FAKE_TMUX_LOG"; fi
if [ "$1" = new-session ]; then exit 1; fi
exit 0
`
	if err := os.WriteFile(filepath.Join(fakeDir, "tmux"), []byte(script), 0o755); err != nil {
		t.Fatalf("write fake tmux: %v", err)
	}
	t.Setenv("PATH", fakeDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FAKE_TMUX_LOG", logPath)
	t.Setenv("TMPDIR", tmp)

	_, err := (TmuxLauncher{}).Start(context.Background(), StartOptions{
		Name:    "avenor-test",
		Dir:     tmp,
		Cols:    220,
		Rows:    50,
		Command: "exec claude",
		Env:     []string{"AVENOR_BROKER_TOKEN=tok-that-must-not-outlive-the-attempt"},
	})
	if err == nil {
		t.Fatal("Start = nil error, want the failed new-session error")
	}

	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read fake tmux log: %v", err)
	}
	var envFile string
	for _, line := range strings.Split(string(raw), "\n") {
		if path, ok := strings.CutPrefix(line, "present "); ok {
			envFile = path
		}
	}
	if envFile == "" {
		t.Fatalf("fake tmux log = %q, want the launch env file present when new-session ran", raw)
	}
	if _, err := os.Stat(envFile); !os.IsNotExist(err) {
		t.Errorf("stat %s = %v, want the env file removed with the failed attempt", envFile, err)
	}
	if _, err := os.Stat(filepath.Dir(envFile)); !os.IsNotExist(err) {
		t.Errorf("stat %s = %v, want the 0700 env directory removed too", filepath.Dir(envFile), err)
	}
	leftover, err := filepath.Glob(filepath.Join(tmp, "avenor-launch-env-*"))
	if err != nil {
		t.Fatalf("glob launch env dirs: %v", err)
	}
	if len(leftover) != 0 {
		t.Errorf("leftover launch env dirs = %v, want none", leftover)
	}
}
