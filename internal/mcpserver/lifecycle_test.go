package mcpserver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/token"
)

func TestStartSupervisorLeavesStaleSocketForChild(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "avenor-mcp-test-stale.sock")

	// Create a plain file (not a real socket) to simulate a stale socket.
	if err := os.WriteFile(socketPath, []byte("not a socket"), 0600); err != nil {
		t.Fatalf("write stale file: %v", err)
	}
	defer os.Remove(socketPath)

	// The child ControlServer.Start owns stale-path removal under its control
	// lock. A failed parent spawn must leave that path untouched.
	origExec := execCommand
	execCommand = func(name string, args ...string) *exec.Cmd {
		return exec.Command("/nonexistent/avenor-test-stub")
	}
	defer func() { execCommand = origExec }()

	if _, err := startSupervisor(socketPath, 5*time.Second, ""); err == nil {
		t.Fatal("expected error from failed spawn")
	}
	if _, err := os.Stat(socketPath); err != nil {
		t.Fatalf("parent removed stale socket instead of leaving it for child Start: %v", err)
	}
}

// TestStartSupervisorForwardsLaunchWrapperPolicy proves the policy path is
// passed to the autostarted supervisor's command line.
func TestStartSupervisorForwardsLaunchWrapperPolicy(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	var capturedArgs []string
	origExec := execCommand
	execCommand = func(name string, args ...string) *exec.Cmd {
		capturedArgs = args
		return exec.Command("/nonexistent/avenor-test-stub")
	}
	defer func() { execCommand = origExec }()
	_, _ = startSupervisor(filepath.Join(t.TempDir(), "s.sock"), 0, "/etc/avenor/policy.json")
	joined := strings.Join(capturedArgs, " ")
	if !strings.Contains(joined, "--mcp-launch-wrapper-policy /etc/avenor/policy.json") {
		t.Fatalf("args = %v, want policy flag forwarded", capturedArgs)
	}
}

// TestStartSupervisorOmitsLaunchWrapperPolicyByDefault proves no policy flag is
// emitted when the path is empty.
func TestStartSupervisorOmitsLaunchWrapperPolicyByDefault(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	var capturedArgs []string
	origExec := execCommand
	execCommand = func(name string, args ...string) *exec.Cmd {
		capturedArgs = args
		return exec.Command("/nonexistent/avenor-test-stub")
	}
	defer func() { execCommand = origExec }()
	_, _ = startSupervisor(filepath.Join(t.TempDir(), "s.sock"), 0, "")
	if strings.Contains(strings.Join(capturedArgs, " "), "mcp-launch-wrapper-policy") {
		t.Fatalf("args = %v, want no policy flag", capturedArgs)
	}
}

// TestSupervisorScrubChildEnv proves the autostarted
// supervisor child never inherits secrets it must not carry: the control test
// hooks and the parent-link credential.
func TestSupervisorScrubChildEnv(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(t *testing.T)
		scrubbed []string
	}{
		{
			name: "hooks",
			setup: func(t *testing.T) {
				t.Setenv(runtime.EnvTestHooks, "1")
				t.Setenv(runtime.EnvTestOwnerPublishDelay, "5s")
			},
			scrubbed: []string{runtime.EnvTestHooks, runtime.EnvTestOwnerPublishDelay},
		},
		{
			name: "peer-credential",
			setup: func(t *testing.T) {
				// What a claude session started from a provisioned child exports
				// into everything it spawns, including this autostarted supervisor.
				t.Setenv(runtime.EnvBrokerURL, "http://127.0.0.1:9")
				t.Setenv(runtime.EnvBrokerRunID, "rt_parent")
				t.Setenv(runtime.EnvBrokerToken, "inherited-launch-grant")
			},
			scrubbed: runtime.PeerEnvNames(),
		},
		{
			name: "broker-advertise-url",
			setup: func(t *testing.T) {
				// A nested supervisor runs its own broker, so its ancestor's
				// published address would be a lie about where relays arrive.
				t.Setenv(token.EnvBrokerAdvertiseURL, "https://public.example.com")
			},
			scrubbed: []string{token.EnvBrokerAdvertiseURL},
		},
		{
			name: "parent-token",
			setup: func(t *testing.T) {
				// A parent URL would intentionally forward this credential.
				t.Setenv(token.EnvBrokerParentURL, "")
				t.Setenv(token.EnvBrokerParentToken, "leaked-credential")
			},
			scrubbed: []string{token.EnvBrokerParentToken},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			t.Setenv("AVENOR_TEST_UNRELATED_MARKER", "inherited-value")
			tt.setup(t)
			var child *exec.Cmd
			origExec := execCommand
			execCommand = func(string, ...string) *exec.Cmd {
				child = exec.Command("/nonexistent/avenor-test-stub")
				return child
			}
			t.Cleanup(func() { execCommand = origExec })

			_, _ = startSupervisor(filepath.Join(t.TempDir(), "s.sock"), 0, "")
			if child == nil {
				t.Fatal("startSupervisor never built a child command")
			}
			foundMarker := false
			for _, entry := range child.Env {
				for _, name := range tt.scrubbed {
					if strings.HasPrefix(entry, name+"=") {
						t.Fatalf("%q reached the supervisor child environment", entry)
					}
				}
				if entry == "AVENOR_TEST_UNRELATED_MARKER=inherited-value" {
					foundMarker = true
				}
			}
			if !foundMarker {
				t.Fatal("the unrelated inherited marker was not preserved in the child environment")
			}
		})
	}
}

// TestSupervisorChildStripsEveryBrokerEnvName pins that the nested strip list
// is sourced from runtime.BrokerEnvNames: a name added there is stripped from
// the autostarted supervisor's environment without editing this launcher. The
// parent URL is deliberately forwarded, and the parent credential's
// conditional forwarding is pinned by
// TestSupervisorForwardsParentLinkWhenURLInEnv, so both are skipped here.
func TestSupervisorChildStripsEveryBrokerEnvName(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	// An inherited parent URL keeps the credential in the child's environment,
	// so clear it to test the no-parent-link path whatever the invoking shell set.
	t.Setenv(token.EnvBrokerParentURL, "")
	t.Setenv("AVENOR_TEST_UNRELATED_MARKER", "inherited-value")
	forwarded := map[string]bool{token.EnvBrokerParentURL: true}
	var stripped []string
	for _, name := range runtime.BrokerEnvNames() {
		if forwarded[name] {
			continue
		}
		t.Setenv(name, "must-not-reach-the-child")
		stripped = append(stripped, name)
	}
	var child *exec.Cmd
	origExec := execCommand
	execCommand = func(string, ...string) *exec.Cmd {
		child = exec.Command("/nonexistent/avenor-test-stub")
		return child
	}
	t.Cleanup(func() { execCommand = origExec })

	_, _ = startSupervisor(filepath.Join(t.TempDir(), "s.sock"), 0, "")
	if child == nil {
		t.Fatal("startSupervisor never built a child command")
	}
	for _, entry := range child.Env {
		for _, name := range stripped {
			if strings.HasPrefix(entry, name+"=") {
				t.Fatalf("%q reached the supervisor child environment", entry)
			}
		}
	}
	foundMarker := false
	for _, entry := range child.Env {
		if entry == "AVENOR_TEST_UNRELATED_MARKER=inherited-value" {
			foundMarker = true
		}
	}
	if !foundMarker {
		t.Fatal("the unrelated inherited marker was not preserved in the child environment")
	}
}

func TestSupervisorForwardsParentLinkWhenURLInEnv(t *testing.T) {
	tests := []struct {
		name      string
		parentURL string
		forwarded bool
	}{
		{name: "url-in-env", parentURL: "http://127.0.0.1:9", forwarded: true},
		{name: "no-url", parentURL: "", forwarded: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			t.Setenv(token.EnvBrokerParentURL, tt.parentURL)
			t.Setenv(token.EnvBrokerParentToken, "shared-link-credential")
			var child *exec.Cmd
			origExec := execCommand
			execCommand = func(string, ...string) *exec.Cmd {
				child = exec.Command("/nonexistent/avenor-test-stub")
				return child
			}
			t.Cleanup(func() { execCommand = origExec })

			_, _ = startSupervisor(filepath.Join(t.TempDir(), "s.sock"), 0, "")
			if child == nil {
				t.Fatal("startSupervisor never built a child command")
			}
			var forwardedToken, forwardedURL string
			for _, entry := range child.Env {
				if value, ok := strings.CutPrefix(entry, token.EnvBrokerParentToken+"="); ok {
					forwardedToken = value
				}
				if value, ok := strings.CutPrefix(entry, token.EnvBrokerParentURL+"="); ok {
					forwardedURL = value
				}
			}
			if forwardedURL != tt.parentURL {
				t.Fatalf("forwarded parent URL = %q, want %q", forwardedURL, tt.parentURL)
			}
			wantToken := ""
			if tt.forwarded {
				wantToken = "shared-link-credential"
			}
			if forwardedToken != wantToken {
				t.Fatalf("forwarded parent credential = %q, want %q", forwardedToken, wantToken)
			}
		})
	}
}

func TestStartSupervisorDefaultSocketPath(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)

	expectedDir := filepath.Join(tmpDir, ".avenor", "sockets")
	expectedPrefix := filepath.Join(expectedDir, "avenor-mcp-")
	expectedSuffix := ".sock"

	// Mock execCommand to capture the socket path
	var capturedSocket string
	origExec := execCommand
	execCommand = func(name string, args ...string) *exec.Cmd {
		for i, a := range args {
			if a == "--control-socket" && i+1 < len(args) {
				capturedSocket = args[i+1]
			}
		}
		return exec.Command("/nonexistent/avenor-test-stub")
	}
	defer func() { execCommand = origExec }()

	// Clean up any stale socket from the test
	defer func() {
		if capturedSocket != "" {
			os.Remove(capturedSocket)
		}
	}()

	_, _ = startSupervisor("", 5*time.Second, "")

	if capturedSocket == "" {
		t.Fatal("captured socket path is empty")
	}

	if !strings.HasPrefix(capturedSocket, expectedPrefix) {
		t.Errorf("socket path %q should have prefix %q", capturedSocket, expectedPrefix)
	}
	if !strings.HasSuffix(capturedSocket, expectedSuffix) {
		t.Errorf("socket path %q should have suffix %q", capturedSocket, expectedSuffix)
	}

	// Verify sockets directory was created with 0700
	info, err := os.Stat(expectedDir)
	if err != nil {
		t.Fatalf("sockets directory not created: %v", err)
	}
	if !info.IsDir() {
		t.Error("expected sockets dir to be a directory")
	}
}

func TestStartSupervisorZeroIdleTimeoutOmitsFlag(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test-zero-idle.sock")

	var capturedArgs []string
	origExec := execCommand
	execCommand = func(name string, args ...string) *exec.Cmd {
		capturedArgs = args
		return exec.Command("/nonexistent/avenor-test-stub")
	}
	defer func() { execCommand = origExec }()

	_, _ = startSupervisor(socketPath, 0, "")

	for _, a := range capturedArgs {
		if a == "--idle-timeout" {
			t.Error("--idle-timeout flag should be omitted when idleTimeout is zero")
		}
	}
}

func TestStartSupervisorNonZeroIdleTimeoutIncludesFlag(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test-idle.sock")
	os.WriteFile(socketPath, []byte("fake"), 0600)
	defer os.Remove(socketPath)

	var capturedArgs []string
	origExec := execCommand
	execCommand = func(name string, args ...string) *exec.Cmd {
		capturedArgs = args
		return exec.Command("/nonexistent/avenor-test-stub")
	}
	defer func() { execCommand = origExec }()

	_, _ = startSupervisor(socketPath, 30*time.Minute, "")

	found := false
	for i, a := range capturedArgs {
		if a == "--idle-timeout" && i+1 < len(capturedArgs) {
			found = true
			if capturedArgs[i+1] != "30m0s" {
				t.Errorf("expected --idle-timeout 30m0s, got %s", capturedArgs[i+1])
			}
		}
	}
	if !found {
		t.Error("--idle-timeout flag should be present when idleTimeout is non-zero")
	}
}

func TestNewServerNoAutostartError(t *testing.T) {
	_, err := NewServer(Options{
		Transport:   "stdio",
		NoAutostart: true,
	})
	if err == nil {
		t.Fatal("expected error for no-autostart without supervisor socket")
	}
	if !strings.Contains(err.Error(), "no-autostart requires") {
		t.Fatalf("expected error to contain 'no-autostart requires', got: %v", err)
	}
}

func TestNewServerAutostartFailsLazily(t *testing.T) {
	// Autostart is lazy: construction succeeds, and the first control
	// operation starts the supervisor and reports the startup failure.
	origExec := execCommand
	var execAttempts atomic.Int32
	execCommand = func(name string, args ...string) *exec.Cmd {
		execAttempts.Add(1)
		return exec.Command("/nonexistent/avenor-test-stub")
	}
	defer func() { execCommand = origExec }()

	s, err := NewServer(Options{
		Transport: "stdio",
	})
	if err != nil {
		t.Fatalf("NewServer should not start lazily: %v", err)
	}
	defer s.Close()
	if got := execAttempts.Load(); got != 0 {
		t.Fatalf("NewServer exec attempts = %d, want 0", got)
	}

	_, _, err = s.handleAvenorStatus(context.Background(), nil, statusArgs{})
	if err == nil {
		t.Fatal("expected lazy autostart to fail (spawn can't succeed in test)")
	}
	if !strings.Contains(err.Error(), "autostart supervisor") {
		t.Fatalf("expected error to mention autostart supervisor, got: %v", err)
	}
	if got := execAttempts.Load(); got != 1 {
		t.Fatalf("first control operation exec attempts = %d, want 1", got)
	}
}

func TestLifecycleShutdownNoProcess(t *testing.T) {
	// Test Shutdown with a lifecycle that has no running process
	// (e.g., when an existing socket was reused)
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "avenor-mcp-test-noproc.sock")

	lc := &supervisorLifecycle{
		socketPath: socketPath,
		cmd:        nil,
		client:     nil,
	}

	if err := lc.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestOwnedLifecycleShutdownDoesNotUnlinkReplacement(t *testing.T) {
	// Keep the Unix-socket path below macOS's pathname limit.
	tmpDir, err := os.MkdirTemp("/tmp", "mcp-replace-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	socketPath := filepath.Join(tmpDir, "test.sock")
	if err := os.WriteFile(socketPath, []byte("former child socket"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(socketPath); err != nil {
		t.Fatal(err)
	}

	winner, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer winner.Close()

	// Model an owned child that has already exited. The lifecycle parent must
	// not inspect or unlink the path: on Linux the winner can reuse the child's
	// inode before that inspection.
	cmd := exec.Command("true")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	lc := &supervisorLifecycle{socketPath: socketPath, cmd: cmd, exited: exited}

	if err := lc.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if _, err := os.Stat(socketPath); err != nil {
		t.Fatalf("owned lifecycle parent removed replacement socket: %v", err)
	}
}

func TestStartSupervisorStartupTimeout(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "timeout-test.sock")

	origTimeout := startupTimeout
	startupTimeout = 100 * time.Millisecond
	defer func() { startupTimeout = origTimeout }()

	origExec := execCommand
	execCommand = func(name string, args ...string) *exec.Cmd {
		// Sleep long enough to trigger the timeout, but short enough
		// that the test doesn't hang when Kill() is called.
		return exec.Command("sleep", "10")
	}
	defer func() { execCommand = origExec }()

	_, err := startSupervisor(socketPath, 0, "")
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Errorf("expected error to contain 'timed out', got: %v", err)
	}
}

func TestStartSupervisorReuseExistingSocket(t *testing.T) {
	tmpDir, err := os.MkdirTemp("/tmp", "s")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(tmpDir) })
	socketPath := filepath.Join(tmpDir, "r.sock")

	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	cap := publishOwnerRecordForStub(t, socketPath)

	// Accept multiple connections; each connection handles multiple
	// requests (the Client maintains a persistent connection).
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				scanner := bufio.NewScanner(c)
				for scanner.Scan() {
					var req client.Request
					if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
						continue
					}
					resp := client.Response{JSONRPC: "2.0", ID: req.ID}
					if req.Method == "authenticate_owner" {
						resp = ownerAuthResponse(req, cap)
					} else {
						resp.Result, _ = json.Marshal(map[string]any{"session_id": "ses_test", "phase": "working"})
					}
					data, _ := json.Marshal(resp)
					data = append(data, '\n')
					c.Write(data)
				}
			}(conn)
		}
	}()

	// startSupervisor should detect the live socket and reuse it
	// without spawning a new process (no execCommand call).
	origExec := execCommand
	execCalled := false
	execCommand = func(name string, args ...string) *exec.Cmd {
		execCalled = true
		return exec.Command("/nonexistent/avenor-test-stub")
	}
	defer func() { execCommand = origExec }()

	lc, err := startSupervisor(socketPath, 0, "")
	if err != nil {
		t.Fatalf("startSupervisor: %v", err)
	}
	defer lc.Shutdown()

	if execCalled {
		t.Error("expected existing socket to be reused, but execCommand was called")
	}

	if lc.client == nil {
		t.Fatal("expected non-nil client in lifecycle")
	}

	// Verify the client actually works
	status, err := lc.client.Status("")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status["session_id"] != "ses_test" {
		t.Errorf("session_id = %v, want ses_test", status["session_id"])
	}
}

func signalMCPStartupReady() {
	fdValue := os.Getenv("AVENOR_CONTROL_READY_FD")
	_ = os.Unsetenv("AVENOR_CONTROL_READY_FD")
	fd, err := strconv.Atoi(fdValue)
	if err != nil || fd < 3 {
		return
	}
	ready := os.NewFile(uintptr(fd), "avenor-control-ready")
	if ready != nil {
		_, _ = ready.Write([]byte{1})
		_ = ready.Close()
	}
}

func TestMCPSupervisorChild(t *testing.T) {
	if os.Getenv("AVENOR_MCP_SUPERVISOR_CHILD") != "1" {
		return
	}
	path := os.Getenv("AVENOR_MCP_SUPERVISOR_SOCKET")
	identityToken := os.Getenv(token.EnvControlIdentityToken)
	_ = os.Unsetenv(token.EnvControlIdentityToken)
	// A real child removes a proven-stale path under the control lock before
	// binding; the stub does the same so a predecessor's leftover socket does
	// not block it.
	if _, err := os.Stat(path); err == nil {
		if c, dialErr := net.DialTimeout("unix", path, 250*time.Millisecond); dialErr == nil {
			_ = c.Close()
			t.Fatalf("child socket %s is live", path)
		}
		_ = os.Remove(path)
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("child listen: %v", err)
	}
	defer ln.Close()
	cap := publishOwnerRecordForStub(t, path)
	signalMCPStartupReady()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go func(c net.Conn) {
			defer c.Close()
			scanner := bufio.NewScanner(c)
			for scanner.Scan() {
				var req client.Request
				if json.Unmarshal(scanner.Bytes(), &req) != nil {
					continue
				}
				resp := client.Response{JSONRPC: "2.0", ID: req.ID}
				if req.Method == "authenticate_owner" {
					resp = ownerAuthResponse(req, cap)
				} else if req.Method == "identity" {
					resp.Result, _ = json.Marshal(map[string]any{"token": identityToken})
				} else {
					resp.Result, _ = json.Marshal(map[string]any{"phase": "working"})
				}
				data, _ := json.Marshal(resp)
				_, _ = c.Write(append(data, '\n'))
				if req.Method == "shutdown" {
					os.Exit(0)
				}
			}
		}(conn)
	}
}

func TestPreReadyWinnerIsSharedAndSurvivesLoserClose(t *testing.T) {
	// Keep the path under macOS's short Unix-socket pathname limit.
	tmpDir, err := os.MkdirTemp("/tmp", "mcp-winner-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	socketPath := filepath.Join(tmpDir, "winner.sock")

	var winner net.Listener
	origExec := execCommand
	execCommand = func(name string, args ...string) *exec.Cmd {
		var err error
		winner, err = net.Listen("unix", socketPath)
		if err != nil {
			t.Fatalf("start competing winner: %v", err)
		}
		go serveHealthySupervisor(winner, publishOwnerRecordForStub(t, socketPath))
		// The child has not bound the socket and therefore cannot send the
		// readiness acknowledgement. It exits after the winner is healthy.
		return exec.Command("sleep", "0.1")
	}
	defer func() {
		execCommand = origExec
		if winner != nil {
			_ = winner.Close()
		}
	}()

	lifecycle, err := startSupervisor(socketPath, 0, "")
	if err != nil {
		t.Fatalf("startSupervisor: %v", err)
	}
	if !lifecycle.shared {
		t.Fatal("starter classified a pre-ready competing winner as its child")
	}
	if err := lifecycle.Close(); err != nil {
		t.Fatalf("close loser lifecycle: %v", err)
	}
	if cl, state, err := attachSupervisor(socketPath); state != supervisorLive {
		t.Fatalf("loser close shut down or removed the competing winner: %v", err)
	} else {
		_ = cl.Close()
	}
}

func TestMCPPostReadyReplacementChild(t *testing.T) {
	if os.Getenv("AVENOR_MCP_POST_READY_REPLACEMENT_CHILD") != "1" {
		return
	}

	path := os.Getenv("AVENOR_MCP_SUPERVISOR_SOCKET")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("child listen: %v", err)
	}
	signalMCPStartupReady()
	if err := ln.Close(); err != nil {
		t.Fatalf("child close: %v", err)
	}
	// Be explicit even on platforms where closing a Unix listener does not
	// unlink its pathname by default.
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		t.Fatalf("child unlink: %v", err)
	}

	closedFD, err := strconv.Atoi(os.Getenv("AVENOR_MCP_CHILD_CLOSED_FD"))
	if err != nil || closedFD < 3 {
		t.Fatal("child closed notification fd is unavailable")
	}
	closed := os.NewFile(uintptr(closedFD), "avenor-mcp-child-closed")
	if _, err := closed.Write([]byte{1}); err != nil {
		t.Fatalf("notify child socket closed: %v", err)
	}
	_ = closed.Close()

	holdFD, err := strconv.Atoi(os.Getenv("AVENOR_MCP_CHILD_HOLD_FD"))
	if err != nil || holdFD < 3 {
		t.Fatal("child hold fd is unavailable")
	}
	hold := os.NewFile(uintptr(holdFD), "avenor-mcp-child-hold")
	_, _ = hold.Read(make([]byte, 1))
	_ = hold.Close()
}

func TestPostReadyReplacementIsShared(t *testing.T) {
	tmpDir, err := os.MkdirTemp("/tmp", "mcp-replace-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	socketPath := filepath.Join(tmpDir, "winner.sock")

	childClosedReader, childClosedWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer childClosedReader.Close()
	defer childClosedWriter.Close()
	childHoldReader, childHoldWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer childHoldReader.Close()
	defer childHoldWriter.Close()

	origExec := execCommand
	origAfterReady := afterSupervisorReady
	var spawnedChild *exec.Cmd
	defer func() {
		execCommand = origExec
		afterSupervisorReady = origAfterReady
	}()

	parentReady := make(chan struct{})
	releaseProbe := make(chan struct{})
	var releaseProbeOnce sync.Once
	release := func() { releaseProbeOnce.Do(func() { close(releaseProbe) }) }
	defer release()
	afterSupervisorReady = func() {
		close(parentReady)
		<-releaseProbe
	}
	execCommand = func(name string, args ...string) *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=^TestMCPPostReadyReplacementChild$")
		spawnedChild = cmd
		cmd.ExtraFiles = []*os.File{childClosedWriter, childHoldReader}
		cmd.Env = append(os.Environ(),
			"AVENOR_MCP_POST_READY_REPLACEMENT_CHILD=1",
			"AVENOR_MCP_SUPERVISOR_SOCKET="+socketPath,
			"AVENOR_MCP_CHILD_CLOSED_FD=3",
			"AVENOR_MCP_CHILD_HOLD_FD=4",
		)
		return cmd
	}

	type startResult struct {
		lifecycle *supervisorLifecycle
		err       error
	}
	started := make(chan startResult, 1)
	go func() {
		lifecycle, err := startSupervisor(socketPath, 0, "")
		started <- startResult{lifecycle: lifecycle, err: err}
	}()

	select {
	case <-parentReady:
	case <-time.After(time.Second):
		t.Fatal("parent did not receive child readiness")
	}
	var signal [1]byte
	childClosed := make(chan error, 1)
	go func() {
		_, err := childClosedReader.Read(signal[:])
		childClosed <- err
	}()
	select {
	case err := <-childClosed:
		if err != nil {
			t.Fatalf("read child socket-closed notification: %v", err)
		}
		if signal[0] != 1 {
			t.Fatalf("child socket-closed signal = %d, want 1", signal[0])
		}
	case <-time.After(time.Second):
		t.Fatal("child did not close and unlink its socket")
	}

	winner, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("start replacement supervisor: %v", err)
	}
	defer winner.Close()
	// Its identity response has an empty token.
	go serveHealthySupervisor(winner, publishOwnerRecordForStub(t, socketPath))

	// The replacement is bound before the parent can make its first probe.
	release()
	result := <-started
	if result.err != nil {
		t.Fatalf("startSupervisor: %v", result.err)
	}
	if !result.lifecycle.shared {
		t.Fatal("starter classified a post-ready replacement as its child")
	}
	if spawnedChild == nil || spawnedChild.ProcessState == nil {
		t.Fatal("starter did not reap its replaced child process")
	}
	if err := result.lifecycle.Close(); err != nil {
		t.Fatalf("close shared lifecycle: %v", err)
	}
	cl, err := client.Dial(socketPath)
	if err != nil {
		t.Fatalf("dial replacement after shared close: %v", err)
	}
	defer cl.Close()
	identity, err := cl.Identity()
	if err != nil || identity != "" {
		t.Fatalf("replacement identity = %q, %v; want empty token, nil", identity, err)
	}
}

func serveHealthySupervisor(ln net.Listener, cap control.OwnerCapability) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go func(c net.Conn) {
			defer c.Close()
			scanner := bufio.NewScanner(c)
			for scanner.Scan() {
				var req client.Request
				if json.Unmarshal(scanner.Bytes(), &req) != nil {
					continue
				}
				resp := client.Response{JSONRPC: "2.0", ID: req.ID}
				if req.Method == "authenticate_owner" {
					resp = ownerAuthResponse(req, cap)
				} else {
					resp.Result, _ = json.Marshal(map[string]any{"phase": "working"})
				}
				data, _ := json.Marshal(resp)
				_, _ = c.Write(append(data, '\n'))
			}
		}(conn)
	}
}

func TestCompetingLifecycleStartersConvergeOnWinner(t *testing.T) {
	tmpDir, err := os.MkdirTemp("/tmp", "mcp-start-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	socketPath := filepath.Join(tmpDir, "winner.sock")

	origExec := execCommand
	var execCalls atomic.Int32
	execCommand = func(name string, args ...string) *exec.Cmd {
		execCalls.Add(1)
		cmd := exec.Command(os.Args[0], "-test.run=TestMCPSupervisorChild")
		cmd.Env = append(os.Environ(),
			"AVENOR_MCP_SUPERVISOR_CHILD=1",
			"AVENOR_MCP_SUPERVISOR_SOCKET="+socketPath,
		)
		return cmd
	}
	defer func() { execCommand = origExec }()

	type result struct {
		lifecycle *supervisorLifecycle
		err       error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			lifecycle, err := startSupervisor(socketPath, 0, "")
			results <- result{lifecycle: lifecycle, err: err}
		}()
	}
	wg.Wait()
	close(results)

	var lifecycles []*supervisorLifecycle
	for result := range results {
		if result.err != nil {
			t.Fatalf("startSupervisor: %v", result.err)
		}
		lifecycles = append(lifecycles, result.lifecycle)
	}
	if got := execCalls.Load(); got != 1 {
		t.Fatalf("child starts = %d, want 1", got)
	}
	if lifecycles[0].shared == lifecycles[1].shared {
		t.Fatal("expected one owner and one shared lifecycle")
	}

	var owner, shared *supervisorLifecycle
	for _, lifecycle := range lifecycles {
		if lifecycle.shared {
			shared = lifecycle
		} else {
			owner = lifecycle
		}
	}
	if err := shared.Close(); err != nil {
		t.Fatalf("close shared lifecycle: %v", err)
	}
	if winnerClient, state, err := attachSupervisor(socketPath); state != supervisorLive {
		t.Fatalf("shared cleanup removed the winner socket: %v", err)
	} else {
		_ = winnerClient.Close()
	}
	if err := owner.Close(); err != nil {
		t.Fatalf("close owner lifecycle: %v", err)
	}
}

func TestCompetingLifecycleAttachmentsDoNotRemoveWinner(t *testing.T) {
	tmpDir, err := os.MkdirTemp("/tmp", "mcp-race-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	socketPath := filepath.Join(tmpDir, "winner.sock")
	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	cap := publishOwnerRecordForStub(t, socketPath)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				scanner := bufio.NewScanner(c)
				for scanner.Scan() {
					var req client.Request
					if json.Unmarshal(scanner.Bytes(), &req) != nil {
						continue
					}
					resp := client.Response{JSONRPC: "2.0", ID: req.ID}
					if req.Method == "authenticate_owner" {
						resp = ownerAuthResponse(req, cap)
					} else {
						resp.Result, _ = json.Marshal(map[string]any{"phase": "working"})
					}
					data, _ := json.Marshal(resp)
					_, _ = c.Write(append(data, '\n'))
				}
			}(conn)
		}
	}()

	first, err := startSupervisor(socketPath, 0, "")
	if err != nil {
		t.Fatalf("first startSupervisor: %v", err)
	}
	second, err := startSupervisor(socketPath, 0, "")
	if err != nil {
		t.Fatalf("second startSupervisor: %v", err)
	}
	if !first.shared || !second.shared {
		t.Fatal("expected both starters to attach to the existing supervisor")
	}

	// Closing the losing MCP server must only close its connection. The
	// winner's socket remains live for the other attached server.
	if err := first.Close(); err != nil {
		t.Fatalf("close first attachment: %v", err)
	}
	if _, err := os.Stat(socketPath); err != nil {
		t.Fatalf("winner socket removed by competing cleanup: %v", err)
	}
	if winnerClient, state, err := attachSupervisor(socketPath); state != supervisorLive {
		t.Fatalf("winner supervisor is no longer healthy: %v", err)
	} else {
		_ = winnerClient.Close()
	}
	_ = second.Close()
}

func TestLifecycleShutdownIsIdempotentAfterChildExit(t *testing.T) {
	cmd := exec.Command("true")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	lc := &supervisorLifecycle{cmd: cmd, exited: exited, client: &fakeClient{}}

	for i := 0; i < 2; i++ {
		done := make(chan error, 1)
		go func() { done <- lc.Shutdown() }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("shutdown %d: %v", i+1, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("shutdown %d blocked after child exit", i+1)
		}
	}
}

// TestAuthenticateOwnChildRefusesAnotherOwnerRecord proves another owner's
// record never reaches the capability handshake: the mismatch stays in the
// retryable replacement window, so the peer that would accept that capability
// receives nothing.
func TestAuthenticateOwnChildRefusesAnotherOwnerRecord(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "own.sock")
	_, authCount, _, stop := startStubSupervisorListener(t, socket)
	defer stop()
	record, err := control.LoadOwnerRecord(socket)
	if err != nil {
		t.Fatalf("load record: %v", err)
	}
	writeOwnerRecord(t, socket, control.OwnerCapability{ID: record.CapabilityID, Secret: record.Secret}, os.Getpid()+1)

	cl, err := client.Dial(socket)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cl.Close()
	if err := authenticateOwnChild(cl, socket, os.Getpid()); !isOwnerRecordError(err) {
		t.Fatalf("authenticateOwnChild = %v, want a retryable record error", err)
	}
	if n := authCount(); n != 0 {
		t.Fatalf("the peer that holds the socket answered %d authenticate_owner requests, want 0", n)
	}
}

func TestStartSupervisorExitsDuringStartup(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "exit-test.sock")

	origExec := execCommand
	execCommand = func(name string, args ...string) *exec.Cmd {
		// Command that exits immediately without creating a socket
		return exec.Command("true")
	}
	defer func() { execCommand = origExec }()

	_, err := startSupervisor(socketPath, 0, "")
	if err == nil {
		t.Fatal("expected error for supervisor exiting during startup")
	}
	if !strings.Contains(err.Error(), "exited during startup") {
		t.Errorf("expected error to contain 'exited during startup', got: %v", err)
	}
}

// TestMCPAttachAfterSIGKILLUsesTypedOutcomes proves the launcher's replacement
// decision is driven by the probe outcome, not by a boolean health check: only
// ECONNREFUSED or an absent path authorizes a replacement.
func TestMCPAttachAfterSIGKILLUsesTypedOutcomes(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "mcp-typed-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	t.Run("absent path is dead", func(t *testing.T) {
		if _, state, err := attachSupervisor(filepath.Join(dir, "absent.sock")); state != supervisorDead || err == nil {
			t.Fatalf("state = %v err = %v, want dead", state, err)
		}
	})

	t.Run("leftover socket is dead", func(t *testing.T) {
		socket := filepath.Join(dir, "leftover.sock")
		ln, err := net.Listen("unix", socket)
		if err != nil {
			t.Fatal(err)
		}
		ln.(*net.UnixListener).SetUnlinkOnClose(false)
		_ = ln.Close()
		if _, state, _ := attachSupervisor(socket); state != supervisorDead {
			t.Fatalf("state = %v, want dead", state)
		}
	})

	t.Run("live socket without a record rejects attachment", func(t *testing.T) {
		socket := filepath.Join(dir, "norecord.sock")
		ln, err := net.Listen("unix", socket)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		go func() {
			for {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				go func(c net.Conn) { _ = c.Close() }(conn)
			}
		}()
		if _, state, err := attachSupervisor(socket); state != supervisorAuthRejected || !errors.Is(err, control.ErrOwnerRecordMissing) {
			t.Fatalf("state = %v err = %v, want auth-rejected on the missing record", state, err)
		}
	})

	t.Run("rejected capability is auth-rejected", func(t *testing.T) {
		socket := filepath.Join(dir, "wrongcap.sock")
		ln, err := net.Listen("unix", socket)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		publishOwnerRecordForStub(t, socket)
		other, err := control.GenerateOwnerCapability()
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			for {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				go serveOwnerAuthOnly(conn, other, nil)
			}
		}()
		if _, state, err := attachSupervisor(socket); state != supervisorAuthRejected || !errors.Is(err, client.ErrOwnerUnauthenticated) {
			t.Fatalf("state = %v err = %v, want auth-rejected on the rejected capability", state, err)
		}
	})

	t.Run("uncertain probe never spawns a replacement", func(t *testing.T) {
		origDial := supervisorProbeDial
		supervisorProbeDial = func(string) (net.Conn, error) {
			return nil, &net.OpError{Op: "dial", Err: syscall.ETIMEDOUT}
		}
		t.Cleanup(func() { supervisorProbeDial = origDial })

		origExec := execCommand
		execCalled := false
		execCommand = func(string, ...string) *exec.Cmd {
			execCalled = true
			return exec.Command("true")
		}
		t.Cleanup(func() { execCommand = origExec })

		_, err := startSupervisor(filepath.Join(dir, "uncertain.sock"), 0, "")
		if err == nil {
			t.Fatal("uncertain probe started a replacement")
		}
		if execCalled {
			t.Fatal("uncertain probe reached the child spawn")
		}
	})
}

// TestReplacementNotBlockedByMissingTombstone proves the tombstone neither
// authorizes nor blocks a replacement: its absence and presence both produce
// the same decision and the launcher never touches it.
func TestReplacementNotBlockedByMissingTombstone(t *testing.T) {
	cases := []struct {
		name     string
		tombston string
	}{
		{"absent", ""},
		{"present", "STOPPED reason=signal pid=2 at=2026-10-19T00:00:00Z\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir, err := os.MkdirTemp("/tmp", "mcp-tomb-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			socket := filepath.Join(dir, "tomb.sock")
			// A SIGKILLed predecessor leaves the socket path behind with no
			// listener, and no tombstone was written for it.
			ln, err := net.Listen("unix", socket)
			if err != nil {
				t.Fatal(err)
			}
			ln.(*net.UnixListener).SetUnlinkOnClose(false)
			_ = ln.Close()
			tombstone := socket + ".dead"
			if tc.tombston != "" {
				if err := os.WriteFile(tombstone, []byte(tc.tombston), 0o600); err != nil {
					t.Fatal(err)
				}
			}

			origExec := execCommand
			t.Cleanup(func() { execCommand = origExec })
			execCommand = func(string, ...string) *exec.Cmd {
				cmd := exec.Command(os.Args[0], "-test.run=TestMCPSupervisorChild")
				cmd.Env = append(os.Environ(),
					"AVENOR_MCP_SUPERVISOR_CHILD=1",
					"AVENOR_MCP_SUPERVISOR_SOCKET="+socket,
				)
				return cmd
			}

			lifecycle, err := startSupervisor(socket, 0, "")
			if err != nil {
				t.Fatalf("startSupervisor: %v", err)
			}
			if lifecycle.shared {
				t.Fatal("dead owner was attached instead of replaced")
			}
			if err := lifecycle.Close(); err != nil {
				t.Fatalf("close replacement: %v", err)
			}
			got, err := os.ReadFile(tombstone)
			if tc.tombston == "" {
				if err == nil {
					t.Fatalf("launcher created a tombstone: %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("read tombstone: %v", err)
			}
			if string(got) != tc.tombston {
				t.Fatalf("tombstone = %q, want %q", got, tc.tombston)
			}
		})
	}
}

// TestNoLauncherUnlinksOwnerArtifacts proves no launcher failure path removes
// the socket, its owner record, its control lock or its tombstone.
func TestNoLauncherUnlinksOwnerArtifacts(t *testing.T) {
	type scenario struct {
		name  string
		setup func(t *testing.T, socket string)
	}
	// leftovers models a dead owner's artifacts: a bound-then-closed socket, a
	// published record, a control lock and a tombstone.
	leftovers := func(t *testing.T, socket string) {
		t.Helper()
		ln, err := net.Listen("unix", socket)
		if err != nil {
			t.Fatal(err)
		}
		ln.(*net.UnixListener).SetUnlinkOnClose(false)
		_ = ln.Close()
		publishOwnerRecordForStub(t, socket)
		if err := os.WriteFile(socket+".control.lock", []byte("lock"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(socket+".dead", []byte("STOPPED reason=signal pid=1 at=2026-10-19T00:00:00Z\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// liveRejectingOwner models a live supervisor this process cannot
	// authenticate to.
	liveRejectingOwner := func(t *testing.T, socket string) {
		t.Helper()
		ln, err := net.Listen("unix", socket)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		publishOwnerRecordForStub(t, socket)
		other, err := control.GenerateOwnerCapability()
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			for {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				go serveOwnerAuthOnly(conn, other, nil)
			}
		}()
		if err := os.WriteFile(socket+".control.lock", []byte("lock"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(socket+".dead", []byte("STOPPED reason=signal pid=1 at=2026-10-19T00:00:00Z\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cases := []scenario{
		{"spawn failure", leftovers},
		{"child exits during startup", leftovers},
		{"startup timeout", leftovers},
		{"auth rejected", liveRejectingOwner},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir, err := os.MkdirTemp("/tmp", "mcp-keep-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			socket := filepath.Join(dir, "keep.sock")
			tc.setup(t, socket)
			before := snapshotLauncherArtifacts(t, socket)

			origExec := execCommand
			origTimeout := startupTimeout
			t.Cleanup(func() {
				execCommand = origExec
				startupTimeout = origTimeout
			})
			switch tc.name {
			case "spawn failure":
				execCommand = func(string, ...string) *exec.Cmd {
					return exec.Command("/nonexistent/avenor-test-stub")
				}
			case "child exits during startup":
				execCommand = func(string, ...string) *exec.Cmd { return exec.Command("true") }
			case "startup timeout":
				startupTimeout = 100 * time.Millisecond
				execCommand = func(string, ...string) *exec.Cmd { return exec.Command("sleep", "10") }
			}

			if _, err := startSupervisor(socket, 0, ""); err == nil {
				t.Fatal("startSupervisor succeeded, want a failure")
			}
			after := snapshotLauncherArtifacts(t, socket)
			for path, want := range before {
				if got, ok := after[path]; !ok {
					t.Fatalf("%s was removed by a failing launcher", path)
				} else if got != want {
					t.Fatalf("%s changed: %q -> %q", path, want, got)
				}
			}
		})
	}
}

// snapshotLauncherArtifacts captures the artifacts a launcher must never
// mutate: the socket's identity, the record, the control lock and the
// tombstone.
func snapshotLauncherArtifacts(t *testing.T, socket string) map[string]string {
	t.Helper()
	info, err := os.Lstat(socket)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}
	snapshot := map[string]string{socket: fmt.Sprintf("%v:%v", info.Mode(), info.Size())}
	for _, path := range []string{socket + ".owner.json", socket + ".control.lock", socket + ".dead"} {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		snapshot[path] = string(body)
	}
	return snapshot
}
