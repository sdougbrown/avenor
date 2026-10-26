package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/admission"
	"github.com/sdougbrown/avenor/internal/stable"
	"github.com/sdougbrown/avenor/internal/token"
)

// stableTestEnv isolates HOME (runtime state and sockets) and TMPDIR (the
// supervisor's artifact root) for a test that runs a real supervisor in
// process, and returns the scratch directory.
func stableTestEnv(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("TMPDIR", dir)
	return dir
}

func TestRunStableJoinsInheritedTreeBudget(t *testing.T) {
	tmpDir := stableTestEnv(t)
	root, err := admission.CreateRootInRuntimeState(2)
	if err != nil {
		t.Fatalf("create root tree budget: %v", err)
	}
	defer root.Close()
	t.Setenv(admission.EnvTreeBudget, root.Path())

	// An overlong Unix-socket path fails in Listen without probing an existing
	// path. A directory at the socket path can make Unix Dial block on Linux.
	socketPath := filepath.Join(tmpDir, strings.Repeat("s", 200))
	if code := runStable([]string{"--control-socket", socketPath}); code != 1 {
		t.Fatalf("runStable() = %d, want 1", code)
	}
	if _, err := os.Stat(root.Path()); err != nil {
		t.Fatalf("inherited root tree budget was removed: %v", err)
	}
}

func TestRunStableWritesTombstoneOnStartFailure(t *testing.T) {
	tmpDir := stableTestEnv(t)
	socketPath := filepath.Join(tmpDir, "socket-path-is-dir")
	if err := os.Mkdir(socketPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(socketPath, "keep-dir-nonempty"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	tombstonePath := socketPath + ".dead"
	if err := os.WriteFile(tombstonePath, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}

	code := runStable([]string{"--control-socket", socketPath})
	if code != 1 {
		t.Fatalf("runStable() = %d, want 1", code)
	}

	data, err := os.ReadFile(tombstonePath)
	if err != nil {
		t.Fatalf("tombstone not written: %v", err)
	}
	content := string(data)
	if strings.Contains(content, "stale") {
		t.Fatalf("tombstone = %q, was not overwritten", content)
	}
	if !strings.Contains(content, "reason=start_failed") {
		t.Fatalf("tombstone = %q, want reason=start_failed", content)
	}

	budgetFiles, err := filepath.Glob(filepath.Join(tmpDir, ".avenor", "sockets", "*.tree-budget"))
	if err != nil {
		t.Fatalf("glob tree budget files: %v", err)
	}
	if len(budgetFiles) != 0 {
		t.Fatalf("tree budget files remain after start failure: %v", budgetFiles)
	}
}

func TestRunStableWorkflowRootFlagParses(t *testing.T) {
	tmpDir := stableTestEnv(t)
	socketPath := filepath.Join(tmpDir, strings.Repeat("s", 200)) // fails fast in Listen
	dir := filepath.Join(tmpDir, "wfroot")
	if code := runStable([]string{"--control-socket", socketPath, "--workflow-root", dir}); code != 1 {
		t.Fatalf("runStable() = %d, want 1 (start failure on bad socket)", code)
	}
	// Reaching a start failure is only possible after the flag parsed; the
	// tombstone confirms Run() executed.
	data, err := os.ReadFile(socketPath + ".dead")
	if err != nil {
		t.Fatalf("tombstone not written: %v", err)
	}
	if !strings.Contains(string(data), "reason=start_failed") {
		t.Fatalf("tombstone = %q, want reason=start_failed", data)
	}
	// The workflow root stays uncreated: the manager is lazy and never
	// constructed on a start failure.
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("workflow root dir created on start failure: %v", err)
	}
}

func TestRunStableIdempotencyCapacityGuard(t *testing.T) {
	tmpDir := stableTestEnv(t)
	// An overlong Unix-socket path fails fast in Listen, so a valid capacity
	// proceeds past flag validation into Run and fails at the socket rather
	// than the guard.
	socketPath := filepath.Join(tmpDir, strings.Repeat("s", 200))
	tombstonePath := socketPath + ".dead"

	// The guard rejects non-positive capacity before Run: a pre-existing
	// tombstone is left untouched (Run would overwrite it, and runStable only
	// removes it after the guard passes).
	marker := "pre-existing"
	if err := os.WriteFile(tombstonePath, []byte(marker), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, cap := range []string{"0", "-5"} {
		if code := runStable([]string{"--control-socket", socketPath, "--idempotency-capacity", cap}); code != 1 {
			t.Fatalf("runStable(capacity %s) = %d, want 1", cap, code)
		}
		data, err := os.ReadFile(tombstonePath)
		if err != nil {
			t.Fatalf("capacity %s: tombstone missing: %v", cap, err)
		}
		if string(data) != marker {
			t.Fatalf("capacity %s: tombstone = %q, want the pre-existing marker (guard must fire before Run)", cap, data)
		}
	}

	// A valid capacity passes flag validation: Run executes and fails at the
	// control socket, overwriting the tombstone with reason=start_failed.
	if code := runStable([]string{"--control-socket", socketPath, "--idempotency-capacity", "64"}); code != 1 {
		t.Fatalf("runStable(capacity 64) = %d, want 1 (start failure)", code)
	}
	data, err := os.ReadFile(tombstonePath)
	if err != nil {
		t.Fatalf("capacity 64: tombstone missing: %v", err)
	}
	if !strings.Contains(string(data), "reason=start_failed") {
		t.Fatalf("capacity 64: tombstone = %q, want reason=start_failed (guard must not fire)", data)
	}
}

// TestRunStableIdempotencyCapacityFlagPropagation: --idempotency-capacity 7
// propagates to Config.IdempotencyCapacity at the point the supervisor is
// constructed.
func TestRunStableIdempotencyCapacityFlagPropagation(t *testing.T) {
	tmpDir := stableTestEnv(t)
	// An overlong Unix-socket path makes Run fail fast, so the constructor
	// seam is hit and the config captured before Run's side effects matter.
	socketPath := filepath.Join(tmpDir, strings.Repeat("s", 200))

	var captured *stable.Config
	orig := newSupervisorFn
	newSupervisorFn = func(cfg stable.Config) *stable.Supervisor {
		c := cfg
		captured = &c
		return orig(cfg)
	}
	t.Cleanup(func() { newSupervisorFn = orig })

	_ = runStable([]string{"--control-socket", socketPath, "--idempotency-capacity", "7"})
	if captured == nil {
		t.Fatal("config was not captured; the constructor seam was not hit")
	}
	if captured.IdempotencyCapacity != 7 {
		t.Fatalf("Config.IdempotencyCapacity = %d, want 7", captured.IdempotencyCapacity)
	}
}

// TestRunStableBrokerParentFlagPropagation: --broker-parent-url and
// $AVENOR_PARENT_BROKER_TOKEN propagate to the Config fields the parent link
// reads. The token travels by environment variable, not argv.
func TestRunStableBrokerParentFlagPropagation(t *testing.T) {
	tmpDir := stableTestEnv(t)
	socketPath := filepath.Join(tmpDir, strings.Repeat("s", 200))

	var captured *stable.Config
	orig := newSupervisorFn
	newSupervisorFn = func(cfg stable.Config) *stable.Supervisor {
		c := cfg
		captured = &c
		return orig(cfg)
	}
	t.Cleanup(func() { newSupervisorFn = orig })

	t.Setenv(token.EnvBrokerParentToken, "tok")
	_ = runStable([]string{
		"--control-socket", socketPath,
		"--broker-parent-url", "http://127.0.0.1:9",
	})
	if captured == nil {
		t.Fatal("config was not captured; the constructor seam was not hit")
	}
	if captured.BrokerParentURL != "http://127.0.0.1:9" || captured.BrokerParentToken != "tok" {
		t.Fatalf("Config parent link = (%q, %q), want the flag and env values", captured.BrokerParentURL, captured.BrokerParentToken)
	}
}

// TestRunStableBrokerParentTokenWithoutURL: a credential
// ($AVENOR_PARENT_BROKER_TOKEN) with no parent URL runs this broker as the
// tree's parent broker. The credential is preserved on the Config, the URL
// stays empty, and the credential guard does not fire (the constructor is
// reached).
func TestRunStableBrokerParentTokenWithoutURL(t *testing.T) {
	tmpDir := stableTestEnv(t)
	// An overlong Unix-socket path makes Run fail fast, so the constructor
	// seam is hit and the config captured before Run's side effects matter.
	socketPath := filepath.Join(tmpDir, strings.Repeat("s", 200))

	var captured *stable.Config
	orig := newSupervisorFn
	newSupervisorFn = func(cfg stable.Config) *stable.Supervisor {
		c := cfg
		captured = &c
		return orig(cfg)
	}
	t.Cleanup(func() { newSupervisorFn = orig })

	// Nonempty env credential; no parent URL (the flag stays at its empty
	// default).
	t.Setenv(token.EnvBrokerParentToken, "tok")
	_ = runStable([]string{"--control-socket", socketPath})
	if captured == nil {
		t.Fatal("config was not captured; the constructor seam was not hit (the credential guard must not fire when the URL is empty)")
	}
	if captured.BrokerParentURL != "" {
		t.Fatalf("Config.BrokerParentURL = %q, want empty (no parent URL)", captured.BrokerParentURL)
	}
	if captured.BrokerParentToken != "tok" {
		t.Fatalf("Config.BrokerParentToken = %q, want the env credential preserved", captured.BrokerParentToken)
	}
}

// TestRunStableBrokerParentURLRequiresToken: --broker-parent-url without a
// credential (neither $AVENOR_PARENT_BROKER_TOKEN nor --broker-parent-token)
// is rejected at the CLI boundary with exit 1 and the guard's error message.
func TestRunStableBrokerParentURLRequiresToken(t *testing.T) {
	tmpDir := stableTestEnv(t)
	socketPath := filepath.Join(tmpDir, strings.Repeat("s", 200))

	orig := newSupervisorFn
	newSupervisorFn = func(cfg stable.Config) *stable.Supervisor {
		return nil
	}
	t.Cleanup(func() { newSupervisorFn = orig })

	oldStderr := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stderr = w
	defer func() {
		os.Stderr = oldStderr
		_ = r.Close()
	}()

	t.Setenv(token.EnvBrokerParentToken, "")
	code := runStable([]string{"--control-socket", socketPath, "--broker-parent-url", "http://127.0.0.1:9"})
	_ = w.Close()
	os.Stderr = oldStderr
	var stderr bytes.Buffer
	if _, err := io.Copy(&stderr, r); err != nil {
		t.Fatalf("read captured stderr: %v", err)
	}
	if code != 1 {
		t.Fatalf("runStable(url without token) = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), token.EnvBrokerParentToken) {
		t.Fatalf("stderr = %q, want the %s guard error", stderr.String(), token.EnvBrokerParentToken)
	}
}

// TestRunStableBrokerParentTokenFlagOverridesEnv: an explicit
// --broker-parent-token wins over $AVENOR_PARENT_BROKER_TOKEN.
func TestRunStableBrokerParentTokenFlagOverridesEnv(t *testing.T) {
	tmpDir := stableTestEnv(t)
	socketPath := filepath.Join(tmpDir, strings.Repeat("s", 200))

	var captured *stable.Config
	orig := newSupervisorFn
	newSupervisorFn = func(cfg stable.Config) *stable.Supervisor {
		captured = &cfg
		return orig(cfg)
	}
	t.Cleanup(func() { newSupervisorFn = orig })

	t.Setenv(token.EnvBrokerParentToken, "env-token")
	_ = runStable([]string{
		"--control-socket", socketPath,
		"--broker-parent-url", "http://127.0.0.1:9",
		"--broker-parent-token", "flag-token",
	})
	if captured == nil {
		t.Fatal("config was not captured; the constructor seam was not hit")
	}
	if captured.BrokerParentToken != "flag-token" {
		t.Fatalf("Config parent token = %q, want the explicit flag value", captured.BrokerParentToken)
	}
}
