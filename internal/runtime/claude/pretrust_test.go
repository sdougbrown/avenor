package claude

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"
)

// TestStartPreAcceptsRunDirTrust checks Start hands the run directory and its
// ctx to the pre-trust step, and that the step runs before the terminal
// launcher's Start is called.
func TestStartPreAcceptsRunDirTrust(t *testing.T) {
	fakeClaudeOnPath(t)
	term := terminal.NewFakeSession("avenor-test", 4242, "ready")
	launcher := &captureLauncher{term: term}
	p := &Provider{
		sessions: make(map[string]*claudecore.Session),
		launcher: launcher,
	}
	var gotDir string
	var gotCtx context.Context
	var launchedBefore bool
	p.preTrustDir = func(ctx context.Context, dir string) error {
		gotCtx = ctx
		gotDir = dir
		launchedBefore = launcher.started
		return nil
	}
	runDir := t.TempDir()
	ctx := context.WithValue(context.Background(), ctxKey{}, "start")

	sess, err := p.Start(ctx, runtime.StartOptions{Dir: runDir})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.sessions[sess.SessionID].CancelFn()

	if gotDir != runDir {
		t.Errorf("pre-trust directory = %q, want %q", gotDir, runDir)
	}
	if gotCtx == nil || gotCtx.Value(ctxKey{}) != "start" {
		t.Error("pre-trust did not receive Start's ctx")
	}
	if launchedBefore {
		t.Error("pre-trust ran after the terminal launch")
	}
	if !launcher.started {
		t.Error("the terminal was not launched")
	}
}

type ctxKey struct{}

// TestStartSurvivesPreTrustFailure keeps the failure non-fatal: the trust
// dialog handler is the fallback, so the session still launches.
func TestStartSurvivesPreTrustFailure(t *testing.T) {
	fakeClaudeOnPath(t)
	term := terminal.NewFakeSession("avenor-test", 4242, "ready")
	launcher := &captureLauncher{term: term}
	p := &Provider{
		sessions:    make(map[string]*claudecore.Session),
		launcher:    launcher,
		cancelGrace: defaultCancelGrace,
	}
	p.preTrustDir = func(context.Context, string) error { return errors.New("config is not writable") }

	sess, err := p.Start(context.Background(), runtime.StartOptions{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Start returned an error on a failed pre-trust: %v", err)
	}
	defer p.sessions[sess.SessionID].CancelFn()

	if launcher.command == "" {
		t.Error("the terminal was not launched after a failed pre-trust")
	}
}

// TestStartSkipsPreTrustForWrappedRuns checks a wrapped launch never edits the
// host config: the wrapper may not read it, so the trust dialog handler
// answers there instead.
func TestStartSkipsPreTrustForWrappedRuns(t *testing.T) {
	fakeClaudeOnPath(t)
	launcher := &captureLauncher{term: terminal.NewFakeSession("avenor-test", 4242, "ready")}
	p := &Provider{
		sessions:    make(map[string]*claudecore.Session),
		launcher:    launcher,
		cancelGrace: defaultCancelGrace,
	}
	called := false
	p.preTrustDir = func(context.Context, string) error {
		called = true
		return nil
	}

	sess, err := p.Start(context.Background(), runtime.StartOptions{
		Dir:           t.TempDir(),
		LaunchWrapper: []string{"nice", "-n", "5"},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.sessions[sess.SessionID].CancelFn()

	if called {
		t.Error("pre-trust ran for a wrapped launch")
	}
	if launcher.command == "" {
		t.Error("the wrapped terminal was not launched")
	}
}

// TestStartSkipsPreTrustForTheTmuxLauncher checks the tmux launcher never edits
// the host config: a pane inherits the tmux server's environment rather than
// avenor's, so the child may read a different CLAUDE_CONFIG_DIR or HOME than
// the file avenor would edit.
func TestStartSkipsPreTrustForTheTmuxLauncher(t *testing.T) {
	bin := fakeClaudeOnPath(t)
	fakeTmuxOnPath(t, bin)
	p := &Provider{
		sessions:    make(map[string]*claudecore.Session),
		launcher:    terminal.TmuxLauncher{},
		cancelGrace: defaultCancelGrace,
	}
	called := false
	p.preTrustDir = func(context.Context, string) error {
		called = true
		return nil
	}

	sess, err := p.Start(context.Background(), runtime.StartOptions{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.sessions[sess.SessionID].CancelFn()

	if called {
		t.Error("pre-trust ran for a tmux launch")
	}
}

// fakeTmuxOnPath puts a stub `tmux` binary beside the fake claude, so a tmux
// launch runs without a real tmux server.
func fakeTmuxOnPath(t *testing.T, bin string) {
	t.Helper()
	script := "#!/bin/sh\ncase \"$1\" in\nlist-panes) echo 4242 ;;\nesac\nexit 0\n"
	if err := os.WriteFile(filepath.Join(bin, "tmux"), []byte(script), 0o755); err != nil {
		t.Fatalf("write fake tmux: %v", err)
	}
}

// TestProviderLiteralWithNoPreTrustDirWritesNoConfig checks a Provider built
// without the field (as tests do) skips pre-trust entirely, so it can never
// touch a real config file.
func TestProviderLiteralWithNoPreTrustDirWritesNoConfig(t *testing.T) {
	fakeClaudeOnPath(t)
	home := t.TempDir()
	configDir := t.TempDir()
	homeConfig := filepath.Join(home, ".claude.json")
	dirConfig := filepath.Join(configDir, ".claude.json")
	for _, path := range []string{homeConfig, dirConfig} {
		if err := os.WriteFile(path, []byte(`{"projects": {}}`), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}
	}
	t.Setenv("HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", configDir)

	p := &Provider{
		sessions:    make(map[string]*claudecore.Session),
		launcher:    &captureLauncher{term: terminal.NewFakeSession("avenor-test", 4242, "ready")},
		cancelGrace: defaultCancelGrace,
	}
	sess, err := p.Start(context.Background(), runtime.StartOptions{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.sessions[sess.SessionID].CancelFn()

	for _, path := range []string{homeConfig, dirConfig} {
		if got := string(readFile(t, path)); got != `{"projects": {}}` {
			t.Errorf("%s changed: %s", path, got)
		}
	}
}

// TestStartPreTrustsThroughNew covers the production wiring end to end: New()
// must carry claudecore.PreAcceptTrust, so a plain Start marks the run
// directory trusted in the config the run's Claude Code reads. Removing the
// hook from NewWithOptions leaves the config untouched and fails here.
func TestStartPreTrustsThroughNew(t *testing.T) {
	fakeClaudeOnPath(t)
	configPath := filepath.Join(os.Getenv("CLAUDE_CONFIG_DIR"), ".claude.json")
	if err := os.WriteFile(configPath, []byte(`{"projects": {}}`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	runDir := t.TempDir()
	key, err := filepath.EvalSymlinks(runDir)
	if err != nil {
		t.Fatalf("resolve run dir: %v", err)
	}

	p := New().(*Provider)
	p.launcher = &captureLauncher{term: terminal.NewFakeSession("avenor-test", 4242, "ready")}
	sess, err := p.Start(context.Background(), runtime.StartOptions{Dir: runDir})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.sessions[sess.SessionID].CancelFn()

	var doc map[string]json.RawMessage
	if err := json.Unmarshal(readFile(t, configPath), &doc); err != nil {
		t.Fatalf("config is not a JSON object: %v", err)
	}
	var projects map[string]json.RawMessage
	if err := json.Unmarshal(doc["projects"], &projects); err != nil {
		t.Fatalf("projects: %v", err)
	}
	var entry map[string]json.RawMessage
	if err := json.Unmarshal(projects[filepath.Clean(key)], &entry); err != nil {
		t.Fatalf("projects[%q]: %v (keys: %v)", key, err, projects)
	}
	var trusted bool
	if err := json.Unmarshal(entry["hasTrustDialogAccepted"], &trusted); err != nil || !trusted {
		t.Fatalf("hasTrustDialogAccepted = %s, want true", entry["hasTrustDialogAccepted"])
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return raw
}
