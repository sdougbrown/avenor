package claudechannel

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/internal/identitytest"
)

// channelObservationExtras is the parent Claude Code session marker the
// observer also records: a channel pane must not inherit it either.
var channelObservationExtras = []string{"CLAUDE_CODE_CHILD_SESSION"}

func poisonParentIdentity(t *testing.T) {
	t.Helper()
	identitytest.PoisonParent(t)
	t.Setenv("CLAUDE_CODE_CHILD_SESSION", "parent-claude-marker")
}

func fakeClaudeIdentityChild(t *testing.T, obsPath string) {
	t.Helper()
	binDir := t.TempDir()
	body := "#!/bin/sh\n" +
		"if [ \"$1\" = \"--version\" ]; then\n  echo 'Claude Code 1.0.0 (fake)'\n  exit 0\nfi\n" +
		identitytest.Body(obsPath, channelObservationExtras...) +
		"sleep 30\n"
	if err := os.WriteFile(filepath.Join(binDir, "claude"), []byte(body), 0o755); err != nil {
		t.Fatalf("write fake claude: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func isolateTmuxServer(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("tmux"); err != nil {
		t.Skip("tmux not installed")
	}
	t.Setenv("TMUX_TMPDIR", t.TempDir())
	prior, had := os.LookupEnv("TMUX")
	if err := os.Unsetenv("TMUX"); err != nil {
		t.Fatalf("unset TMUX: %v", err)
	}
	t.Cleanup(func() {
		_ = exec.Command("tmux", "kill-server").Run()
		if had {
			_ = os.Setenv("TMUX", prior)
		}
	})
}

// startIdentityChannelProvider launches one channel session against the fake
// claude child and returns the path its pane wrote.
func startIdentityChannelProvider(t *testing.T, opts runtime.StartOptions) string {
	t.Helper()
	obsPath := filepath.Join(t.TempDir(), "observed.env")
	fakeClaudeIdentityChild(t, obsPath)

	p := New().(*Provider)
	session, err := p.Start(context.Background(), opts)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		p.mu.Lock()
		s := p.sessions[session.SessionID]
		b := p.broker
		p.mu.Unlock()
		if s != nil {
			s.CancelFn()
			_ = s.Term.Kill(context.Background())
		}
		if b != nil {
			_ = b.Stop()
		}
	})
	return obsPath
}

// TestIdentityChildEnv drives the channel backend through its tmux pane seam
// and asserts what the real pane child saw, including that the sidecar MCP
// bootstrap and channel credentials are untouched.
func TestIdentityChildEnv(t *testing.T) {
	t.Run("identity reaches the pane", func(t *testing.T) {
		isolateTmuxServer(t)
		poisonParentIdentity(t)

		provenance := identitytest.Provenance("rt_child")
		obsPath := startIdentityChannelProvider(t, runtime.StartOptions{
			Dir:        t.TempDir(),
			Provenance: provenance,
		})

		observed := identitytest.Read(t, obsPath)
		identitytest.Assert(t, observed, provenance, channelObservationExtras...)
		identitytest.AssertBrokerPreserved(t, observed)
	})

	t.Run("nil identity clears inherited values", func(t *testing.T) {
		isolateTmuxServer(t)
		poisonParentIdentity(t)

		obsPath := startIdentityChannelProvider(t, runtime.StartOptions{Dir: t.TempDir()})

		observed := identitytest.Read(t, obsPath)
		identitytest.AssertCleared(t, observed, channelObservationExtras...)
		identitytest.AssertBrokerPreserved(t, observed)
	})
}
