package claude

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/internal/identitytest"
)

// claudeObservationExtras is the parent Claude Code session marker the observer
// also records: a hosted terminal must not inherit it.
var claudeObservationExtras = []string{"CLAUDE_CODE_CHILD_SESSION"}

func poisonParentIdentity(t *testing.T) {
	t.Helper()
	identitytest.PoisonParent(t)
	t.Setenv("AVENOR_CLAUDE_TERMINAL", "")
	t.Setenv("CLAUDE_CODE_CHILD_SESSION", "parent-claude-marker")
}

// fakeClaudeIdentityChild installs a real `claude` child that answers the
// version preflight and records the allowlisted environment when launched.
func fakeClaudeIdentityChild(t *testing.T, obsPath string) {
	t.Helper()
	binDir := t.TempDir()
	body := "#!/bin/sh\n" +
		"if [ \"$1\" = \"--version\" ]; then\n  echo 'Claude Code 1.0.0 (fake)'\n  exit 0\nfi\n" +
		identitytest.Body(obsPath, claudeObservationExtras...) +
		"sleep 30\n"
	if err := os.WriteFile(filepath.Join(binDir, "claude"), []byte(body), 0o755); err != nil {
		t.Fatalf("write fake claude: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func stopClaudeSession(t *testing.T, p *Provider, sessionID string) {
	t.Helper()
	p.mu.Lock()
	s := p.sessions[sessionID]
	p.mu.Unlock()
	if s == nil {
		return
	}
	s.CancelFn()
	_ = s.Term.Kill(context.Background())
}

func TestIdentityChildEnv(t *testing.T) {
	t.Run("pty", func(t *testing.T) {
		poisonParentIdentity(t)
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		fakeClaudeIdentityChild(t, obsPath)

		provenance := identitytest.Provenance("rt_child")
		p := New().(*Provider)
		session, err := p.Start(context.Background(), runtime.StartOptions{
			Dir:        t.TempDir(),
			Provenance: provenance,
		})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer stopClaudeSession(t, p, session.SessionID)

		observed := identitytest.Read(t, obsPath)
		identitytest.Assert(t, observed, provenance, claudeObservationExtras...)
		identitytest.AssertBrokerPreserved(t, observed)
	})

	t.Run("pty nil identity clears inherited values", func(t *testing.T) {
		poisonParentIdentity(t)
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		fakeClaudeIdentityChild(t, obsPath)

		p := New().(*Provider)
		session, err := p.Start(context.Background(), runtime.StartOptions{Dir: t.TempDir()})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer stopClaudeSession(t, p, session.SessionID)

		observed := identitytest.Read(t, obsPath)
		identitytest.AssertCleared(t, observed, claudeObservationExtras...)
		identitytest.AssertBrokerPreserved(t, observed)
	})

	t.Run("tmux", func(t *testing.T) {
		identitytest.IsolateTmuxServer(t)
		poisonParentIdentity(t)
		t.Setenv("AVENOR_CLAUDE_TERMINAL", "tmux")
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		fakeClaudeIdentityChild(t, obsPath)

		provenance := identitytest.Provenance("rt_child")
		p := New().(*Provider)
		session, err := p.Start(context.Background(), runtime.StartOptions{
			Dir:        t.TempDir(),
			Provenance: provenance,
		})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer stopClaudeSession(t, p, session.SessionID)

		observed := identitytest.Read(t, obsPath)
		identitytest.Assert(t, observed, provenance, claudeObservationExtras...)
		identitytest.AssertBrokerPreserved(t, observed)
	})

	t.Run("tmux nil identity clears inherited values", func(t *testing.T) {
		identitytest.IsolateTmuxServer(t)
		poisonParentIdentity(t)
		t.Setenv("AVENOR_CLAUDE_TERMINAL", "tmux")
		obsPath := filepath.Join(t.TempDir(), "observed.env")
		fakeClaudeIdentityChild(t, obsPath)

		p := New().(*Provider)
		session, err := p.Start(context.Background(), runtime.StartOptions{Dir: t.TempDir()})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer stopClaudeSession(t, p, session.SessionID)

		observed := identitytest.Read(t, obsPath)
		identitytest.AssertCleared(t, observed, claudeObservationExtras...)
		identitytest.AssertBrokerPreserved(t, observed)
	})
}
