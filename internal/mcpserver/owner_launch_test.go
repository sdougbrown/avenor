package mcpserver

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/control"
)

// requireAvenorBinary returns the real avenor binary. Launcher evidence is a
// gate, so a missing binary fails the test instead of skipping it.
func requireAvenorBinary(t *testing.T) string {
	t.Helper()
	candidates := []string{os.Getenv("AVENOR_BINARY"), filepath.Join("..", "..", "avenor")}
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		path, err := filepath.Abs(candidate)
		if err != nil {
			continue
		}
		info, err := os.Stat(path)
		if err != nil || info.IsDir() || info.Mode().Perm()&0o111 == 0 {
			continue
		}
		return path
	}
	t.Fatal("avenor binary not found; run `mise run go-build` or set AVENOR_BINARY")
	return ""
}

// startRealSupervisorServer returns an MCP server whose autostart launches the
// real avenor binary, plus the socket path it will use.
func startRealSupervisorServer(t *testing.T, socketPath string) *Server {
	t.Helper()
	binary := requireAvenorBinary(t)
	origExec := execCommand
	execCommand = func(_ string, args ...string) *exec.Cmd {
		return exec.Command(binary, args...)
	}
	t.Cleanup(func() { execCommand = origExec })

	s, err := NewServer(Options{
		Transport:     "stdio",
		ControlSocket: socketPath,
		IdleTimeout:   30 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return s
}

// shortHome returns a short-lived HOME and TMPDIR whose socket and artifact
// paths stay inside the unix socket pathname limit and out of a developer's
// real tree. The autostarted supervisor inherits both, so its artifact
// retention never reads or prunes a real $TMPDIR/avenor-stable-<euid> root.
func shortHome(t *testing.T) string {
	t.Helper()
	home, err := os.MkdirTemp("/tmp", "mcp-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	tmp := filepath.Join(home, "tmp")
	if err := os.Mkdir(tmp, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("TMPDIR", tmp)
	return home
}

// supervisorSocketPath is the socket an autostarted supervisor binds.
func supervisorSocketPath(home string) string {
	return filepath.Join(home, ".avenor", "sockets", "mcp.sock")
}

// launcherClient returns the connection the launcher attached as the socket's
// owner, which is the connection its gated calls travel on.
func launcherClient(t *testing.T, cl ControlClient) *client.Client {
	t.Helper()
	conn, ok := cl.(*client.Client)
	if !ok {
		t.Fatalf("launcher client is %T, want *client.Client", cl)
	}
	return conn
}

// TestMCPAutostartAuthenticatedAttachment proves MCP autostart launches a real
// supervisor, attaches with its published capability and delivers an
// owner-gated broker_send to the supervisor's registered run.
func TestMCPAutostartAuthenticatedAttachment(t *testing.T) {
	home := shortHome(t)
	socketPath := supervisorSocketPath(home)
	s := startRealSupervisorServer(t, socketPath)
	t.Cleanup(func() { _ = s.Close() })

	launcher, _, err := s.getClientForSupervisor("")
	if err != nil {
		t.Fatalf("autostart attach: %v", err)
	}
	cl := launcherClient(t, launcher)

	// broker_receive registers the supervisor's own named run, so the delivery
	// below has a recipient.
	var drained []map[string]any
	if err := cl.Call("broker_receive", nil, &drained); err != nil {
		t.Fatalf("broker_receive before send: %v", err)
	}
	params := map[string]any{
		"from_run_id": "supervisor",
		"to_run_id":   "supervisor",
		"message":     "hello supervisor",
	}
	var sent map[string]any
	if err := cl.Call("broker_send", params, &sent); err != nil {
		t.Fatalf("broker_send: %v", err)
	}
	var received []map[string]any
	if err := cl.Call("broker_receive", nil, &received); err != nil {
		t.Fatalf("broker_receive after send: %v", err)
	}
	if len(received) != 1 {
		t.Fatalf("received %d messages, want 1: %v", len(received), received)
	}
	if received[0]["message"] != "hello supervisor" || received[0]["from_run_id"] != "supervisor" {
		t.Fatalf("received = %v", received[0])
	}

	// The recipient is the supervisor's run, registered under its name.
	var peers map[string]any
	if err := cl.Call("broker_peers", nil, &peers); err != nil {
		t.Fatalf("broker_peers: %v", err)
	}
	sessions, _ := peers["sessions"].([]any)
	found := false
	for _, entry := range sessions {
		session, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		if session["run_id"] == "supervisor" && session["label"] == "supervisor" {
			found = true
		}
	}
	if !found {
		t.Fatalf("supervisor run missing from peers: %v", peers)
	}

	if err := s.lifecycle.ShutdownWithMode("graceful"); err != nil {
		t.Fatalf("owner shutdown: %v", err)
	}
}

// TestWrongCapabilityAttachRejected proves a wrong capability is refused by a
// real supervisor while its record, socket and owner access survive.
func TestWrongCapabilityAttachRejected(t *testing.T) {
	home := shortHome(t)
	socketPath := supervisorSocketPath(home)
	s := startRealSupervisorServer(t, socketPath)
	t.Cleanup(func() { _ = s.Close() })
	if _, _, err := s.getClientForSupervisor(""); err != nil {
		t.Fatalf("autostart attach: %v", err)
	}

	record, err := control.LoadOwnerRecord(socketPath)
	if err != nil {
		t.Fatalf("load record: %v", err)
	}
	raw, err := client.Dial(socketPath)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer raw.Close()
	wrong := control.OwnerCapability{ID: record.CapabilityID, Secret: record.Secret + "00"}
	authErr := raw.AuthenticateOwner(wrong)
	if !errors.Is(authErr, client.ErrOwnerUnauthenticated) {
		t.Fatalf("wrong capability error = %v, want unauthenticated", authErr)
	}
	var result map[string]any
	var rpcErr *client.RPCError
	if err := raw.Call("cancel", nil, &result); !errors.As(err, &rpcErr) || rpcErr.Code != control.CodePermissionDenied {
		t.Fatalf("cancel on a rejected connection = %v, want permission denied", err)
	}

	// The owner principal and its artifacts are intact for the real owner.
	after, err := control.LoadOwnerRecord(socketPath)
	if err != nil || after.CapabilityID != record.CapabilityID || after.Secret != record.Secret {
		t.Fatalf("record changed after a rejected attach: %v", err)
	}
	owner, err := client.AttachOwner(socketPath)
	if err != nil {
		t.Fatalf("owner attach after a rejected attach: %v", err)
	}
	defer owner.Close()
	if _, err := owner.Status(""); err != nil {
		t.Fatalf("owner lost access after a rejected attach: %v", err)
	}
}

// TestMCPAttachReloadsRotatedOwnerRecord proves a fresh launcher reloads the
// replacement's record instead of caching the dead capability.
func TestMCPAttachReloadsRotatedOwnerRecord(t *testing.T) {
	home := shortHome(t)
	socketPath := supervisorSocketPath(home)

	first := startRealSupervisorServer(t, socketPath)
	if _, _, err := first.getClientForSupervisor(""); err != nil {
		t.Fatalf("first autostart attach: %v", err)
	}
	dead, err := control.LoadOwnerRecord(socketPath)
	if err != nil {
		t.Fatalf("load first record: %v", err)
	}
	pid := first.lifecycle.cmd.Process.Pid
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatalf("kill supervisor: %v", err)
	}
	<-first.lifecycle.exited
	first.lifecycle.waited = true
	_ = first.Close()
	if _, err := os.Stat(socketPath + ".dead"); err == nil {
		t.Fatal("SIGKILL wrote a tombstone; the replacement case would be masked")
	}

	second := startRealSupervisorServer(t, socketPath)
	t.Cleanup(func() { _ = second.Close() })
	launcher, _, err := second.getClientForSupervisor("")
	if err != nil {
		t.Fatalf("replacement autostart attach: %v", err)
	}
	live, err := control.LoadOwnerRecord(socketPath)
	if err != nil {
		t.Fatalf("load replacement record: %v", err)
	}
	if live.CapabilityID == dead.CapabilityID || live.Secret == dead.Secret {
		t.Fatal("replacement reused the dead capability")
	}

	cl := launcherClient(t, launcher)
	var received []map[string]any
	if err := cl.Call("broker_receive", nil, &received); err != nil {
		t.Fatalf("broker_receive: %v", err)
	}
	var sent map[string]any
	if err := cl.Call("broker_send", map[string]any{"from_run_id": "supervisor", "to_run_id": "supervisor", "message": "post-rotation"}, &sent); err != nil {
		t.Fatalf("broker_send: %v", err)
	}
	if err := cl.Call("broker_receive", nil, &received); err != nil {
		t.Fatalf("broker_receive after send: %v", err)
	}
	if len(received) != 1 || received[0]["message"] != "post-rotation" {
		t.Fatalf("received = %v, want the delivered message", received)
	}
}

// TestFreshLauncherRespawnsAfterSIGKILL proves a launcher that never captured
// the killed child still replaces it and completes an owner-gated delivery.
func TestFreshLauncherRespawnsAfterSIGKILL(t *testing.T) {
	home := shortHome(t)
	socketPath := supervisorSocketPath(home)

	first := startRealSupervisorServer(t, socketPath)
	if _, _, err := first.getClientForSupervisor(""); err != nil {
		t.Fatalf("first autostart attach: %v", err)
	}
	pid := first.lifecycle.cmd.Process.Pid
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatalf("kill supervisor: %v", err)
	}
	<-first.lifecycle.exited
	first.lifecycle.waited = true
	_ = first.Close()

	// A fresh server holds no handles to the dead child: its replacement
	// decision comes from the socket probe alone.
	fresh := startRealSupervisorServer(t, socketPath)
	t.Cleanup(func() { _ = fresh.Close() })
	launcher, _, err := fresh.getClientForSupervisor("")
	if err != nil {
		t.Fatalf("fresh launcher attach: %v", err)
	}
	cl := launcherClient(t, launcher)
	var received []map[string]any
	if err := cl.Call("broker_receive", nil, &received); err != nil {
		t.Fatalf("broker_receive: %v", err)
	}
	var sent map[string]any
	if err := cl.Call("broker_send", map[string]any{"from_run_id": "supervisor", "to_run_id": "supervisor", "message": "after sigkill"}, &sent); err != nil {
		t.Fatalf("broker_send: %v", err)
	}
	if err := cl.Call("broker_receive", nil, &received); err != nil {
		t.Fatalf("broker_receive after send: %v", err)
	}
	if len(received) != 1 || received[0]["message"] != "after sigkill" {
		t.Fatalf("received = %v, want the delivered message", received)
	}
}
