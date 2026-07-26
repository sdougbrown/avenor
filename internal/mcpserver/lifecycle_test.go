package mcpserver

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/client"
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

	if _, err := startSupervisor(socketPath, 5*time.Second); err == nil {
		t.Fatal("expected error from failed spawn")
	}
	if _, err := os.Stat(socketPath); err != nil {
		t.Fatalf("parent removed stale socket instead of leaving it for child Start: %v", err)
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
	identityToken := os.Getenv(controlIdentityTokenEnv)
	_ = os.Unsetenv(controlIdentityTokenEnv)
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("child listen: %v", err)
	}
	defer ln.Close()
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
				if req.Method == "identity" {
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
		go serveHealthySupervisor(winner)
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

	lifecycle, err := startSupervisor(socketPath, 0)
	if err != nil {
		t.Fatalf("startSupervisor: %v", err)
	}
	if !lifecycle.shared {
		t.Fatal("starter classified a pre-ready competing winner as its child")
	}
	if err := lifecycle.Close(); err != nil {
		t.Fatalf("close loser lifecycle: %v", err)
	}
	if cl, ok := dialHealthySupervisor(socketPath); !ok {
		t.Fatal("loser close shut down or removed the competing winner")
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
		lifecycle, err := startSupervisor(socketPath, 0)
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
	go serveHealthySupervisor(winner) // Its identity response has an empty token.

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

func serveHealthySupervisor(ln net.Listener) {
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
				resp.Result, _ = json.Marshal(map[string]any{"phase": "working"})
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
			lifecycle, err := startSupervisor(socketPath, 0)
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
	if winnerClient, ok := dialHealthySupervisor(socketPath); !ok {
		t.Fatal("shared cleanup removed the winner socket")
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
					resp.Result, _ = json.Marshal(map[string]any{"phase": "working"})
					data, _ := json.Marshal(resp)
					_, _ = c.Write(append(data, '\n'))
				}
			}(conn)
		}
	}()

	first, err := startSupervisor(socketPath, 0)
	if err != nil {
		t.Fatalf("first startSupervisor: %v", err)
	}
	second, err := startSupervisor(socketPath, 0)
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
	if winnerClient, ok := dialHealthySupervisor(socketPath); !ok {
		t.Fatal("winner supervisor is no longer healthy")
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
