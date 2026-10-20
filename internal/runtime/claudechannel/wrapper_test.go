package claudechannel

import (
	"context"
	"encoding/json"
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
	term    *terminal.FakeSession
}

func (l *captureLauncher) Start(_ context.Context, opts terminal.StartOptions) (terminal.Session, error) {
	l.command = opts.Command
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
// The generated MCP args are pinned here too: the sidecar registers under the
// session's label, and dropping --sender silently falls back to the run ID.
func TestProviderStartWrapperPrefixesCommand(t *testing.T) {
	fakeClaudeOnPath(t)
	l := &captureLauncher{term: terminal.NewFakeSession("avenor-channel-test", 4242, "ready")}
	p := New().(*Provider)
	p.launcher = l

	dir := t.TempDir()
	sess, err := p.Start(context.Background(), runtime.StartOptions{
		Dir:           dir,
		Label:         "worker",
		LaunchWrapper: []string{"nice", "-n", "5"},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	wantPrefix := "exec 'nice' '-n' '5' claude "
	if !strings.HasPrefix(l.command, wantPrefix) {
		t.Fatalf("terminal command = %q, want prefix %q", l.command, wantPrefix)
	}

	raw, err := os.ReadFile(filepath.Join(dir, ".mcp.json"))
	if err != nil {
		t.Fatalf("read project mcp config: %v", err)
	}
	var cfg struct {
		MCPServers map[string]struct {
			Args []string `json:"args"`
		} `json:"mcpServers"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("decode project mcp config: %v", err)
	}
	found := false
	for name, server := range cfg.MCPServers {
		if !strings.HasPrefix(name, "avenor-channel-") {
			continue
		}
		found = true
		if got := strings.Join(server.Args, " "); !strings.Contains(got, "--sender worker") {
			t.Fatalf("channel sidecar args = %q, want --sender worker", got)
		}
	}
	if !found {
		t.Fatalf("project mcp config has no channel server: %v", cfg.MCPServers)
	}

	// Stop the supervision goroutine started by Start.
	p.mu.Lock()
	s := p.sessions[sess.SessionID]
	b := p.broker
	p.mu.Unlock()
	if s != nil {
		s.CancelFn()
	}
	if b != nil {
		_ = b.Stop()
	}
}
