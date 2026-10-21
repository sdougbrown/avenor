package terminal

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTmuxPasteAndEnterFramesThePromptItself(t *testing.T) {
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
exit 0
`
	if err := os.WriteFile(filepath.Join(fakeDir, "tmux"), []byte(script), 0o755); err != nil {
		t.Fatalf("write fake tmux: %v", err)
	}
	t.Setenv("PATH", fakeDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FAKE_TMUX_LOG", logPath)
	t.Setenv("FAKE_TMUX_BUF", bufPath)

	if err := (&TmuxSession{name: "avenor-test"}).PasteAndEnter(context.Background(), "line one\nline \x1b[201~two"); err != nil {
		t.Fatalf("PasteAndEnter: %v", err)
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
	for _, line := range strings.Split(strings.TrimSpace(string(log)), "\n") {
		if strings.HasPrefix(line, "paste-buffer") && strings.Contains(" "+line+" ", " -p ") {
			t.Fatalf("paste-buffer %q adds a second pair of markers when the pane enabled bracketed paste", line)
		}
	}
}
