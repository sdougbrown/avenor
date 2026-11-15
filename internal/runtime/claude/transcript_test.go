package claude

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"
)

// TestStartWatchesTheTranscriptUnderTheClaudeConfigDir pins which file Start's
// transcript reader follows. Claude Code writes under
// $CLAUDE_CONFIG_DIR/projects when the variable is set, and the transcript is
// the only end-of-turn signal when OSC 7501 reports are off or unanswered, so a
// reader pinned to ~/.claude/projects reads nothing and the turn never ends.
func TestStartWatchesTheTranscriptUnderTheClaudeConfigDir(t *testing.T) {
	fakeClaudeOnPath(t)
	configDir := os.Getenv("CLAUDE_CONFIG_DIR")
	home := os.Getenv("HOME")
	p := &Provider{
		sessions:    make(map[string]*claudecore.Session),
		launcher:    &captureLauncher{term: terminal.NewFakeSession("avenor-test", 4242, "ready")},
		cancelGrace: defaultCancelGrace,
	}
	runDir := t.TempDir()

	sess, err := p.Start(context.Background(), runtime.StartOptions{Dir: runDir})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	started := p.sessions[sess.SessionID]
	defer started.CancelFn()
	if started.Transcript == nil {
		t.Fatal("Start built no transcript reader")
	}

	want := claudecore.TranscriptPath(claudecore.TranscriptRoot(home), runDir, sess.SessionID)
	got := started.Transcript.Path()
	if got != want {
		t.Errorf("transcript reader follows %q, want %q", got, want)
	}
	if !strings.HasPrefix(got, configDir+string(filepath.Separator)) {
		t.Errorf("transcript reader follows %q, outside CLAUDE_CONFIG_DIR %q", got, configDir)
	}
	if legacy := filepath.Join(home, ".claude"); strings.HasPrefix(got, legacy+string(filepath.Separator)) {
		t.Errorf("transcript reader follows %q, under %q", got, legacy)
	}
}
