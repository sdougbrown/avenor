package claude

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"
)

// captureLauncher is a terminal.Launcher that records the command the provider
// asked it to run and whether Start had been called, and returns a fake
// session.
type captureLauncher struct {
	command string
	started bool
	term    *terminal.FakeSession
}

func (l *captureLauncher) Start(_ context.Context, opts terminal.StartOptions) (terminal.Session, error) {
	l.command = opts.Command
	l.started = true
	return l.term, nil
}

// fakeClaudeOnPath puts a stub `claude` binary on PATH so Start's LookPath and
// --version preflight succeed without a real Claude Code install. It also
// points HOME and CLAUDE_CONFIG_DIR at empty temp directories and clears the
// override that moves Claude Code's config name, so a test that runs through
// New() can never read or edit a real config file. It returns the stub
// directory, for a test that needs another fake binary beside it.
func fakeClaudeOnPath(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	home := filepath.Join(root, "home")
	configDir := filepath.Join(root, "claude-config")
	for _, dir := range []string{bin, home, configDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
	script := "#!/bin/sh\necho 'Claude Code 1.0.0 (fake)'\n"
	if err := os.WriteFile(filepath.Join(bin, "claude"), []byte(script), 0o755); err != nil {
		t.Fatalf("write fake claude: %v", err)
	}
	t.Setenv("PATH", bin)
	t.Setenv("HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", configDir)
	t.Setenv("CLAUDE_CODE_CUSTOM_OAUTH_URL", "")
	return bin
}

// TestProviderStartWrapperPrefixesCommand proves the provider-level handoff:
// StartOptions.LaunchWrapper must reach claudecore.LaunchCommand, so the
// terminal command the provider launches is the wrapper followed by claude.
func TestProviderStartWrapperPrefixesCommand(t *testing.T) {
	fakeClaudeOnPath(t)
	l := &captureLauncher{term: terminal.NewFakeSession("avenor-test", 4242, "ready")}
	p := New().(*Provider)
	p.launcher = l

	sess, err := p.Start(context.Background(), runtime.StartOptions{
		Dir:           t.TempDir(),
		LaunchWrapper: []string{"nice", "-n", "5"},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	wantPrefix := "exec 'nice' '-n' '5' claude "
	if !strings.HasPrefix(l.command, wantPrefix) {
		t.Fatalf("terminal command = %q, want prefix %q", l.command, wantPrefix)
	}

	// Stop the supervision goroutine started by Start.
	p.mu.Lock()
	if s := p.sessions[sess.SessionID]; s != nil {
		s.CancelFn()
	}
	p.mu.Unlock()
}
