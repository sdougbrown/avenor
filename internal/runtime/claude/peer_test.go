package claude

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"
)

// withFakeClaude installs a `claude` that answers --version, so Start reaches
// the launcher without a real Claude Code session behind it.
func withFakeClaude(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	body := "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 'Claude Code 2.1.311'; exit 0; fi\nexec sleep 300\n"
	if err := os.WriteFile(filepath.Join(dir, "claude"), []byte(body), 0o755); err != nil {
		t.Fatalf("write fake claude: %v", err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// recordingLauncher captures the options the provider hands the terminal.
type recordingLauncher struct {
	mu      sync.Mutex
	opts    terminal.StartOptions
	session terminal.Session
}

func (l *recordingLauncher) Start(_ context.Context, opts terminal.StartOptions) (terminal.Session, error) {
	l.mu.Lock()
	l.opts = opts
	l.mu.Unlock()
	return l.session, nil
}

func (l *recordingLauncher) captured() terminal.StartOptions {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.opts
}

func peerEnvOf(entries []string) map[string]string {
	env := make(map[string]string, len(entries))
	for _, entry := range entries {
		if key, value, ok := strings.Cut(entry, "="); ok {
			env[key] = value
		}
	}
	return env
}

func startTestProvider(t *testing.T, opts runtime.StartOptions) (*Provider, *recordingLauncher) {
	t.Helper()
	withFakeClaude(t)
	term := terminal.NewFakeSession("test-term", 1, "")
	term.SetAlive(false) // the hosted process is already gone: runSession reaps and returns
	launcher := &recordingLauncher{session: term}
	p := &Provider{
		sessions:    make(map[string]*claudecore.Session),
		launcher:    launcher,
		cancelGrace: time.Millisecond,
	}
	opts.Dir = t.TempDir()
	if _, err := p.Start(context.Background(), opts); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return p, launcher
}

// A spawned claude child joins the broker as the run its supervisor already
// provisioned for it: the launch carries that run id and token, and the label
// peers address it by. Minting a second run here would give one child two
// records under one label — the ambiguity that makes `avenor:<label>` fail.
func TestStartProvisionsTheSupervisorsBrokerRun(t *testing.T) {
	b := broker.New("")
	if err := b.Start(); err != nil {
		t.Fatalf("broker.Start: %v", err)
	}
	defer b.Stop()
	token, _ := b.EnsureRun("rt_7", "child-a")

	_, launcher := startTestProvider(t, runtime.StartOptions{
		Label:       "child-a",
		Broker:      b,
		RuntimeID:   "rt_7",
		BrokerToken: token,
	})

	env := peerEnvOf(launcher.captured().Env)
	if want := "http://" + b.Addr(); env["AVENOR_BROKER_URL"] != want {
		t.Errorf("AVENOR_BROKER_URL = %q, want %q", env["AVENOR_BROKER_URL"], want)
	}
	if env["AVENOR_RUN_ID"] != "rt_7" {
		t.Errorf("AVENOR_RUN_ID = %q, want rt_7", env["AVENOR_RUN_ID"])
	}
	if env["AVENOR_BROKER_TOKEN"] != token {
		t.Errorf("AVENOR_BROKER_TOKEN = %q, want %q", env["AVENOR_BROKER_TOKEN"], token)
	}
	if env["AVENOR_SESSION_NAME"] != "child-a" {
		t.Errorf("AVENOR_SESSION_NAME = %q, want child-a", env["AVENOR_SESSION_NAME"])
	}
	if n := b.RunCount(); n != 1 {
		t.Errorf("broker holds %d runs, want the one provisioned run", n)
	}
}

// A broker without a run for this child must not half-configure it: URL, run id
// and token travel together or not at all.
func TestStartOmitsPeerCredentialsWithoutAProvisionedRun(t *testing.T) {
	b := broker.New("")
	if err := b.Start(); err != nil {
		t.Fatalf("broker.Start: %v", err)
	}
	defer b.Stop()

	_, launcher := startTestProvider(t, runtime.StartOptions{
		Label:  "child-a",
		Broker: b,
		// RuntimeID without a token: nothing to join as.
		RuntimeID: "rt_7",
	})

	if env := launcher.captured().Env; len(env) != 0 {
		t.Fatalf("launch env = %q, want none", env)
	}
	if n := b.RunCount(); n != 0 {
		t.Errorf("broker holds %d runs, want none minted", n)
	}
}
