package claude

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
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
// provisioned for it, and the credential it is handed is one-shot: the launch
// carries an attach credential the mods layer trades for the run's own, so a
// nested session that inherits this environment cannot join the same run.
// Minting a second run here would give one child two records under one label —
// the ambiguity that makes `avenor:<label>` fail.
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
	attach := env["AVENOR_BROKER_TOKEN"]
	if attach == "" || attach == token {
		t.Fatalf("AVENOR_BROKER_TOKEN = %q, want a one-shot attach credential, not the run token", attach)
	}
	if env["AVENOR_SESSION_NAME"] != "" {
		// The label reaches the child once, as the launch identity's AVENOR_LABEL.
		// A second spelling could disagree with the record's label.
		t.Errorf("AVENOR_SESSION_NAME = %q, want the peer set only", env["AVENOR_SESSION_NAME"])
	}
	if n := len(env); n != len(runtime.PeerEnvNames()) {
		t.Errorf("launch env = %q, want exactly the peer vocabulary", launcher.captured().Env)
	}
	if n := b.RunCount(); n != 1 {
		t.Errorf("broker holds %d runs, want the one provisioned run", n)
	}

	// The credential the launch handed over is the one the broker will spend, and
	// only once: that is the single inbound consumer.
	first, status, body := attachTo(t, b.Addr(), "rt_7", attach)
	if status != 200 || first != token {
		t.Fatalf("attach = (%q, %d, %v), want the run token", first, status, body)
	}
	if _, status, body := attachTo(t, b.Addr(), "rt_7", attach); status != 409 {
		t.Fatalf("second attach = (%d, %v), want 409", status, body)
	}
}

// A provisioned run with no run record behind it must not half-configure the
// child: URL, run id and a spendable credential travel together or not at all.
func TestStartOmitsPeerCredentialsWithNoBearerForTheRunToken(t *testing.T) {
	b := broker.New("")
	if err := b.Start(); err != nil {
		t.Fatalf("broker.Start: %v", err)
	}
	defer b.Stop()

	_, launcher := startTestProvider(t, runtime.StartOptions{
		Label:  "child-a",
		Broker: b,
		// RuntimeID and a token with no run behind them: nothing to join as, and
		// no attach credential the broker would honour.
		RuntimeID:   "rt_7",
		BrokerToken: "loose-token",
	})

	if env := launcher.captured().Env; len(env) != 0 {
		t.Fatalf("launch env = %q, want none", env)
	}
	if n := b.RunCount(); n != 0 {
		t.Errorf("broker holds %d runs, want none minted", n)
	}
}

// attachTo posts one attach handshake at the broker's open route.
func attachTo(t *testing.T, addr, runID, credential string) (string, int, string) {
	t.Helper()
	body := `{"run_id":"` + runID + `","token":"` + credential + `"}`
	resp, err := http.Post("http://"+addr+"/attach", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST /attach: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read attach body: %v", err)
	}
	var answer struct {
		Token string `json:"token"`
	}
	_ = json.Unmarshal(raw, &answer)
	return answer.Token, resp.StatusCode, string(raw)
}
