package mcpserver

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/client"
)

func TestStartSupervisorStaleSocket(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "avenor-mcp-test-stale.sock")

	// Clean up from previous runs
	os.Remove(socketPath)

	// Create a plain file (not a real socket) to simulate stale socket
	if err := os.WriteFile(socketPath, []byte("not a socket"), 0600); err != nil {
		t.Fatalf("write stale file: %v", err)
	}
	defer os.Remove(socketPath)

	// Mock execCommand to verify spawn was attempted after stale removal
	execCalled := false
	origExec := execCommand
	execCommand = func(name string, args ...string) *exec.Cmd {
		execCalled = true

		found := false
		for i, a := range args {
			if a == "--control-socket" && i+1 < len(args) && args[i+1] == socketPath {
				found = true
			}
		}
		if !found {
			t.Errorf("expected --control-socket %s in args, got %v", socketPath, args)
		}

		return exec.Command("/nonexistent/avenor-test-stub")
	}
	defer func() { execCommand = origExec }()

	// startSupervisor should detect the stale file, remove it, then try to spawn
	_, err := startSupervisor(socketPath, 5*time.Second)

	if !execCalled {
		t.Error("execCommand was never called — spawn was not attempted after stale removal")
	}

	// The spawn should fail since we returned a false command
	if err == nil {
		t.Error("expected error from failed spawn")
	}

	// Verify the stale file was removed
	if _, statErr := os.Stat(socketPath); !os.IsNotExist(statErr) {
		t.Error("stale socket was not removed")
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

	_, _ = startSupervisor("", 5*time.Second)

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

	_, _ = startSupervisor(socketPath, 0)

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

	_, _ = startSupervisor(socketPath, 30*time.Minute)

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

func TestNewServerAutostartFails(t *testing.T) {
	// Autostart should try to spawn the supervisor, which will fail
	// because the test binary is not avenor with a "stable" subcommand.
	// We mock execCommand to ensure it doesn't actually try.
	origExec := execCommand
	execCommand = func(name string, args ...string) *exec.Cmd {
		return exec.Command("/nonexistent/avenor-test-stub")
	}
	defer func() { execCommand = origExec }()

	_, err := NewServer(Options{
		Transport: "stdio",
	})
	if err == nil {
		t.Fatal("expected autostart to fail (spawn can't succeed in test)")
	}
	if !strings.Contains(err.Error(), "autostart supervisor") {
		t.Fatalf("expected error to mention autostart supervisor, got: %v", err)
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

func TestSupervisorLifecycleSocketCleanup(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test.sock")

	// Create a file that exists so we can verify removal
	if err := os.WriteFile(socketPath, []byte("fake"), 0600); err != nil {
		t.Fatal(err)
	}

	lc := &supervisorLifecycle{
		socketPath: socketPath,
		cmd:        nil,
		client:     nil,
	}

	if err := lc.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	if _, err := os.Stat(socketPath); !os.IsNotExist(err) {
		t.Error("socket file was not removed during shutdown")
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

	_, err := startSupervisor(socketPath, 0)
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
					snap := map[string]any{"session_id": "ses_test", "phase": "working"}
					resp.Result, _ = json.Marshal(snap)
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

	lc, err := startSupervisor(socketPath, 0)
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

func TestStartSupervisorExitsDuringStartup(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "exit-test.sock")

	origExec := execCommand
	execCommand = func(name string, args ...string) *exec.Cmd {
		// Command that exits immediately without creating a socket
		return exec.Command("true")
	}
	defer func() { execCommand = origExec }()

	_, err := startSupervisor(socketPath, 0)
	if err == nil {
		t.Fatal("expected error for supervisor exiting during startup")
	}
	if !strings.Contains(err.Error(), "exited during startup") {
		t.Errorf("expected error to contain 'exited during startup', got: %v", err)
	}
}
