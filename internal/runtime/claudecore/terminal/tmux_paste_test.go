package terminal

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestTmuxPasteAndEnterFramesThePromptItself(t *testing.T) {
	cases := map[string]struct {
		pasteSErr string
		wantErr   bool
		want      []string
	}{
		"tmux with -S": {want: []string{
			"load-buffer -b avenor-prompt-avenor-test -",
			"paste-buffer -S -t avenor-test -b avenor-prompt-avenor-test",
			"send-keys -t avenor-test Enter",
		}},
		// tmux before 3.7 has no -S and does not sanitize a paste.
		"tmux without -S": {pasteSErr: "command paste-buffer: unknown flag -S", want: []string{
			"load-buffer -b avenor-prompt-avenor-test -",
			"paste-buffer -S -t avenor-test -b avenor-prompt-avenor-test",
			"paste-buffer -t avenor-test -b avenor-prompt-avenor-test",
			"send-keys -t avenor-test Enter",
		}},
		// Any other failure would fail an unsanitised paste too.
		"pane gone": {pasteSErr: "can't find pane: avenor-test", wantErr: true, want: []string{
			"load-buffer -b avenor-prompt-avenor-test -",
			"paste-buffer -S -t avenor-test -b avenor-prompt-avenor-test",
		}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tmpDir := t.TempDir()
			logPath := filepath.Join(tmpDir, "tmux.log")
			bufPath := filepath.Join(tmpDir, "buffer")
			fakeDir := filepath.Join(tmpDir, "fakebin")
			if err := os.Mkdir(fakeDir, 0o755); err != nil {
				t.Fatalf("mkdir fake bin: %v", err)
			}
			script := `#!/bin/sh
printf '%s\n' "$*" >> "$FAKE_TMUX_LOG"
if [ "$1" = load-buffer ]; then cat > "$FAKE_TMUX_BUF"; fi
if [ "$1" = paste-buffer ] && [ "$2" = -S ] && [ -n "$FAKE_TMUX_PASTE_S_ERR" ]; then echo "$FAKE_TMUX_PASTE_S_ERR" >&2; exit 1; fi
exit 0
`
			if err := os.WriteFile(filepath.Join(fakeDir, "tmux"), []byte(script), 0o755); err != nil {
				t.Fatalf("write fake tmux: %v", err)
			}
			t.Setenv("PATH", fakeDir+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv("FAKE_TMUX_LOG", logPath)
			t.Setenv("FAKE_TMUX_BUF", bufPath)
			t.Setenv("FAKE_TMUX_PASTE_S_ERR", tc.pasteSErr)

			err := (&TmuxSession{name: "avenor-test"}).PasteAndEnter(context.Background(), "line one\nline \x1b[201~two")
			if (err != nil) != tc.wantErr {
				t.Fatalf("PasteAndEnter error = %v, want error %v", err, tc.wantErr)
			}

			buf, err := os.ReadFile(bufPath)
			if err != nil {
				t.Fatalf("read buffer: %v", err)
			}
			if want := pasteStart + "line one\nline [201~two" + pasteEnd; string(buf) != want {
				t.Fatalf("buffer = %q, want %q", buf, want)
			}
			log, err := os.ReadFile(logPath)
			if err != nil {
				t.Fatalf("read tmux log: %v", err)
			}
			if got := strings.Split(strings.TrimSpace(string(log)), "\n"); strings.Join(got, "\n") != strings.Join(tc.want, "\n") {
				t.Fatalf("tmux commands =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(tc.want, "\n"))
			}
		})
	}
}

// TestTmuxPasteAndEnterDeliversRawMarkers pastes into a real pane on a private
// tmux server and checks the bytes the program reads.
func TestTmuxPasteAndEnterDeliversRawMarkers(t *testing.T) {
	if _, err := exec.LookPath("tmux"); err != nil {
		t.Skip("tmux not installed")
	}
	// A short socket directory: the socket path has a length limit.
	sockDir, err := os.MkdirTemp("", "avtmux")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	t.Setenv("TMUX_TMPDIR", sockDir)
	t.Setenv("TMUX", "")
	os.Unsetenv("TMUX")
	t.Cleanup(func() { _ = exec.Command("tmux", "kill-server").Run() })

	out := filepath.Join(sockDir, "read")
	ready := filepath.Join(sockDir, "ready")
	want := pasteStart + "one\rtwo" + pasteEnd + "\r"
	pane := "stty raw -echo && : > " + ready + "; head -c " + strconv.Itoa(len(want)) + " > " + out
	// -f /dev/null keeps the user's tmux.conf (plugins, key settings) out.
	if err := exec.Command("tmux", "-f", "/dev/null", "new-session", "-d", "-s", "avenor-paste-test", "-x", "80", "-y", "10", pane).Run(); err != nil {
		t.Fatalf("new-session: %v", err)
	}
	// A paste that arrives before raw mode has its CRs turned into LFs.
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pane never switched to raw mode")
		}
	}

	if err := (&TmuxSession{name: "avenor-paste-test"}).PasteAndEnter(context.Background(), "one\ntwo"); err != nil {
		t.Fatalf("PasteAndEnter: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, _ := os.ReadFile(out)
		if string(got) == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("pane read %q, want %q", got, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
