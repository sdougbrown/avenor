package main

import (
	"bytes"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/admission"
	"github.com/sdougbrown/avenor/internal/stable"
	"github.com/sdougbrown/avenor/internal/token"
)

// stableTestEnv isolates runtime state and parent-link configuration so tests
// cannot join the operator's broker.
func stableTestEnv(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("TMPDIR", dir)
	t.Setenv(token.EnvBrokerParentURL, "")
	t.Setenv(token.EnvBrokerParentToken, "")
	t.Setenv(stable.EnvMCPLaunchWrapperPolicy, "")
	t.Setenv(token.EnvBrokerAdvertiseURL, "")
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

func TestRunStableBrokerParentURLFromEnv(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  string
		want string
	}{
		{"clean", "http://127.0.0.1:9", "http://127.0.0.1:9"},
		{"padded", "\t http://127.0.0.1:9 \n", "http://127.0.0.1:9"},
		{"blank", " \t\n", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := stableTestEnv(t)
			socketPath := filepath.Join(tmpDir, strings.Repeat("s", 200))

			var captured *stable.Config
			orig := newSupervisorFn
			newSupervisorFn = func(cfg stable.Config) *stable.Supervisor {
				captured = &cfg
				return orig(cfg)
			}
			t.Cleanup(func() { newSupervisorFn = orig })

			t.Setenv(token.EnvBrokerParentURL, tc.env)
			t.Setenv(token.EnvBrokerParentToken, "env-token")
			_ = runStable([]string{"--control-socket", socketPath})
			if captured == nil {
				t.Fatal("config was not captured; the constructor seam was not hit")
			}
			if captured.BrokerParentURL != tc.want {
				t.Fatalf("Config.BrokerParentURL = %q, want %q", captured.BrokerParentURL, tc.want)
			}
			if captured.BrokerParentToken != "env-token" {
				t.Fatalf("Config.BrokerParentToken = %q, want the environment credential", captured.BrokerParentToken)
			}
		})
	}
}

// TestRunStableBrokerParentURLFlagOverridesEnv: an explicit --broker-parent-url
// wins over $AVENOR_PARENT_BROKER_URL.
func TestRunStableBrokerParentURLFlagOverridesEnv(t *testing.T) {
	tmpDir := stableTestEnv(t)
	socketPath := filepath.Join(tmpDir, strings.Repeat("s", 200))

	var captured *stable.Config
	orig := newSupervisorFn
	newSupervisorFn = func(cfg stable.Config) *stable.Supervisor {
		captured = &cfg
		return orig(cfg)
	}
	t.Cleanup(func() { newSupervisorFn = orig })

	t.Setenv(token.EnvBrokerParentURL, "http://127.0.0.1:8")
	t.Setenv(token.EnvBrokerParentToken, "tok")
	_ = runStable([]string{
		"--control-socket", socketPath,
		"--broker-parent-url", "http://127.0.0.1:9",
	})
	if captured == nil {
		t.Fatal("config was not captured; the constructor seam was not hit")
	}
	if captured.BrokerParentURL != "http://127.0.0.1:9" {
		t.Fatalf("Config.BrokerParentURL = %q, want the explicit flag value", captured.BrokerParentURL)
	}
}

func TestRunStableBrokerParentEnvURLRequiresToken(t *testing.T) {
	tmpDir := stableTestEnv(t)
	socketPath := filepath.Join(tmpDir, strings.Repeat("s", 200))

	orig := newSupervisorFn
	newSupervisorFn = func(cfg stable.Config) *stable.Supervisor {
		t.Fatal("a supervisor was constructed despite the missing credential")
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
		_ = w.Close()
		_ = r.Close()
	}()

	t.Setenv(token.EnvBrokerParentURL, "http://127.0.0.1:9")
	code := runStable([]string{"--control-socket", socketPath})
	_ = w.Close()
	os.Stderr = oldStderr
	var stderr bytes.Buffer
	if _, err := io.Copy(&stderr, r); err != nil {
		t.Fatalf("read captured stderr: %v", err)
	}
	if code != 1 {
		t.Fatalf("runStable(env url without token) = %d, want 1", code)
	}
	for _, want := range []string{token.EnvBrokerParentToken, token.EnvBrokerParentURL} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr = %q, want the guard error naming %s", stderr.String(), want)
		}
	}
}

// TestRunStableBrokerAdvertiseURLPropagation: the advertised broker URL reaches
// the supervisor config from the flag or, when the flag is empty, from
// $AVENOR_BROKER_ADVERTISE_URL.
func TestRunStableBrokerAdvertiseURLPropagation(t *testing.T) {
	for _, tc := range []struct {
		name string
		flag string
		env  string
		want string
	}{
		{name: "flag", flag: "https://flag.example.com:8443", env: "https://env.example.com", want: "https://flag.example.com:8443"},
		{name: "env", env: "https://env.example.com", want: "https://env.example.com"},
		{name: "neither", want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := stableTestEnv(t)
			socketPath := filepath.Join(tmpDir, strings.Repeat("s", 200))

			var captured *stable.Config
			orig := newSupervisorFn
			newSupervisorFn = func(cfg stable.Config) *stable.Supervisor {
				captured = &cfg
				return orig(cfg)
			}
			t.Cleanup(func() { newSupervisorFn = orig })

			t.Setenv(token.EnvBrokerAdvertiseURL, tc.env)
			args := []string{"--control-socket", socketPath}
			if tc.flag != "" {
				args = append(args, "--broker-advertise-url", tc.flag)
			}
			_ = runStable(args)
			if captured == nil {
				t.Fatal("config was not captured; the constructor seam was not hit")
			}
			if captured.BrokerAdvertiseURL != tc.want {
				t.Fatalf("Config.BrokerAdvertiseURL = %q, want %q", captured.BrokerAdvertiseURL, tc.want)
			}
		})
	}
}

// TestRunStableInvalidBrokerAdvertiseURLExits pins that a value a peer could not
// post to stops the process before the control socket is bound, so nothing
// publishes or registers an address that would never answer.
func TestRunStableInvalidBrokerAdvertiseURLExits(t *testing.T) {
	tmpDir := stableTestEnv(t)
	socketPath := filepath.Join(tmpDir, "advertise-invalid.sock")

	oldStderr := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stderr = w
	defer func() {
		os.Stderr = oldStderr
		_ = w.Close()
		_ = r.Close()
	}()

	// runStable parks in the supervisor's run loop when it does not exit, so a
	// regression that stops the rejected URL from short-circuiting startup
	// would hang this test until the package timeout. Bound the wait so the
	// regression is reported while the suite is still usable.
	exited := make(chan int, 1)
	go func() {
		exited <- runStable([]string{"--control-socket", socketPath, "--broker-advertise-url", "hub.example.com:8787"})
	}()
	var code int
	select {
	case code = <-exited:
	case <-time.After(30 * time.Second):
		t.Fatalf("runStable did not exit on the rejected advertise URL within 30s; startup no longer stops on invalid --broker-advertise-url")
	}
	_ = w.Close()
	os.Stderr = oldStderr
	var stderr bytes.Buffer
	if _, err := io.Copy(&stderr, r); err != nil {
		t.Fatalf("read captured stderr: %v", err)
	}
	if code != 1 {
		t.Fatalf("runStable(invalid advertise url) = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "broker advertise URL") || !strings.Contains(stderr.String(), "hub.example.com:8787") {
		t.Fatalf("stderr = %q, want it to name the rejected advertised URL", stderr.String())
	}
	if _, err := os.Stat(socketPath); !os.IsNotExist(err) {
		t.Fatalf("control socket bound despite the rejected advertised URL: %v", err)
	}
	if _, err := os.Stat(socketPath + ".broker.url"); !os.IsNotExist(err) {
		t.Fatalf("broker URL record published despite the rejected advertised URL: %v", err)
	}
}

// TestRunStableBrokerAddrFlagPropagation: --broker-addr reaches the supervisor
// config so the broker binds (and publishes) that address.
func TestRunStableBrokerAddrFlagPropagation(t *testing.T) {
	tmpDir := stableTestEnv(t)
	socketPath := filepath.Join(tmpDir, strings.Repeat("s", 200))

	var captured *stable.Config
	orig := newSupervisorFn
	newSupervisorFn = func(cfg stable.Config) *stable.Supervisor {
		captured = &cfg
		return orig(cfg)
	}
	t.Cleanup(func() { newSupervisorFn = orig })

	_ = runStable([]string{"--control-socket", socketPath, "--broker-addr", "127.0.0.1:0"})
	if captured == nil {
		t.Fatal("config was not captured; the constructor seam was not hit")
	}
	if captured.BrokerAddr != "127.0.0.1:0" {
		t.Fatalf("Config.BrokerAddr = %q, want the flag value", captured.BrokerAddr)
	}
}

// TestRunStableWildcardBrokerAddrRejected: a wildcard bind is refused before
// the control socket is bound, because the address is what children and parent
// branches are told to reach.
func TestRunStableWildcardBrokerAddrRejected(t *testing.T) {
	tmpDir := stableTestEnv(t)
	socketPath := filepath.Join(tmpDir, "wildcard.sock")

	oldStderr := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stderr = w
	defer func() {
		os.Stderr = oldStderr
		_ = w.Close()
		_ = r.Close()
	}()

	code := runStable([]string{"--control-socket", socketPath, "--broker-addr", "0.0.0.0:0"})
	_ = w.Close()
	os.Stderr = oldStderr
	var stderr bytes.Buffer
	if _, err := io.Copy(&stderr, r); err != nil {
		t.Fatalf("read captured stderr: %v", err)
	}
	if code != 1 {
		t.Fatalf("runStable(wildcard broker addr) = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "wildcard") {
		t.Fatalf("stderr = %q, want the wildcard-bind refusal", stderr.String())
	}
	if _, err := os.Stat(socketPath); !os.IsNotExist(err) {
		t.Fatalf("control socket bound despite the wildcard broker address: %v", err)
	}
}

// TestRunStableOccupiedBrokerAddrFailsStartup: an explicit --broker-addr that
// cannot be bound exits non-zero and names the address instead of running
// without a broker.
func TestRunStableOccupiedBrokerAddrFailsStartup(t *testing.T) {
	tmpDir := stableTestEnv(t)
	socketPath := filepath.Join(tmpDir, "occupied.sock")
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("occupy port: %v", err)
	}
	defer occupied.Close()
	addr := occupied.Addr().String()

	oldStderr := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stderr = w
	defer func() {
		os.Stderr = oldStderr
		_ = w.Close()
		_ = r.Close()
	}()

	code := runStable([]string{"--control-socket", socketPath, "--broker-addr", addr})
	_ = w.Close()
	os.Stderr = oldStderr
	var stderr bytes.Buffer
	if _, err := io.Copy(&stderr, r); err != nil {
		t.Fatalf("read captured stderr: %v", err)
	}
	if code != 1 {
		t.Fatalf("runStable(occupied broker addr) = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), addr) {
		t.Fatalf("stderr = %q, want it to name %q", stderr.String(), addr)
	}
	if _, err := os.Stat(socketPath); !os.IsNotExist(err) {
		t.Fatalf("control socket bound despite the broker bind failure: %v", err)
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
