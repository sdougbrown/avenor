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
// asked it to run and returns a fake session.
type captureLauncher struct {
	command string
	opts    terminal.StartOptions
	term    *terminal.FakeSession
}

func (l *captureLauncher) Start(_ context.Context, opts terminal.StartOptions) (terminal.Session, error) {
	l.command = opts.Command
	l.opts = opts
	return l.term, nil
}

// fakeClaudeOnPath puts a stub `claude` binary on PATH so Start's LookPath and
// --version preflight succeed without a real Claude Code install.
func fakeClaudeOnPath(t *testing.T) {
	t.Helper()
	bin := t.TempDir()
	script := "#!/bin/sh\necho 'Claude Code 1.0.0 (fake)'\n"
	if err := os.WriteFile(filepath.Join(bin, "claude"), []byte(script), 0o755); err != nil {
		t.Fatalf("write fake claude: %v", err)
	}
	t.Setenv("PATH", bin)
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

// TestProviderCloseKillsTerminal proves Close ends the hosted terminal. The
// supervisor closes a runtime's provider when a parked runtime times out, and
// Claude Code sessions live only as long as their terminal process.
func TestProviderCloseKillsTerminal(t *testing.T) {
	fakeClaudeOnPath(t)
	term := terminal.NewFakeSession("avenor-test", 4242, "ready")
	p := New().(*Provider)
	p.launcher = &captureLauncher{term: term}

	if _, err := p.Start(context.Background(), runtime.StartOptions{Dir: t.TempDir()}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := term.KillCalls(); got < 1 {
		t.Fatalf("Kill calls after Close = %d, want at least 1", got)
	}
}

// Claude's terminal is the one that gets graceful teardown. Forgetting the
// option puts a wrapped runtime back on leader-only kills, which leaves the
// wrapper's VM or container behind.
func TestProviderStartRequestsHangupGrace(t *testing.T) {
	fakeClaudeOnPath(t)
	l := &captureLauncher{term: terminal.NewFakeSession("avenor-test", 4242, "ready")}
	p := New().(*Provider)
	p.launcher = l

	sess, err := p.Start(context.Background(), runtime.StartOptions{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	p.mu.Lock()
	if s := p.sessions[sess.SessionID]; s != nil {
		s.CancelFn()
	}
	p.mu.Unlock()
	if l.opts.HangupGrace <= 0 {
		t.Fatalf("terminal HangupGrace = %s, want a graceful teardown", l.opts.HangupGrace)
	}
}
