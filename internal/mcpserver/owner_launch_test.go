package mcpserver

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/messaging"
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

func controlGeneration(t *testing.T, cl *client.Client) string {
	t.Helper()
	pre, err := cl.MessagePreflight()
	if err != nil {
		t.Fatalf("message_preflight: %v", err)
	}
	if pre.Generation == "" {
		t.Fatal("message_preflight reported no generation")
	}
	return pre.Generation
}

func receiveInbound(t *testing.T, cl *client.Client) []messaging.InboundMessage {
	t.Helper()
	res, err := cl.MessageReceive(messaging.ReceiveRequest{Generation: controlGeneration(t, cl)})
	if err != nil {
		t.Fatalf("message_receive: %v", err)
	}
	return res.Messages
}

func sendSupervisorMessage(t *testing.T, cl *client.Client, message string) {
	t.Helper()
	id, err := messaging.NewMsgID(time.Now(), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	body, err := messaging.SendBody("supervisor", "supervisor", message, "host")
	if err != nil {
		t.Fatal(err)
	}
	res, err := cl.MessageSend(messaging.SendRequest{
		From:       messaging.Identity{RunID: "supervisor"},
		To:         messaging.Identity{RunID: "supervisor"},
		MessageID:  id,
		Generation: controlGeneration(t, cl),
		Body:       string(body),
		NoReply:    true,
	})
	if err != nil {
		t.Fatalf("message_send: %v", err)
	}
	if res.Outcome != messaging.OutcomeSent {
		t.Fatalf("message_send outcome = %q, want sent", res.Outcome)
	}
}

func inboundMessageText(t *testing.T, msg messaging.InboundMessage) string {
	t.Helper()
	var payload struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		t.Fatalf("decode inbound payload: %v", err)
	}
	return payload.Message
}

// TestMCPAutostartAuthenticatedAttachment proves MCP autostart launches a real
// supervisor, attaches with its published capability and delivers an
// owner-gated message_send to the supervisor's registered run.
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

	// message_receive registers the supervisor's own named run, so the delivery
	// below has a recipient.
	_ = receiveInbound(t, cl)
	sendSupervisorMessage(t, cl, "hello supervisor")
	received := receiveInbound(t, cl)
	if len(received) != 1 {
		t.Fatalf("received %d messages, want 1: %v", len(received), received)
	}
	if inboundMessageText(t, received[0]) != "hello supervisor" || received[0].FromRunID != "supervisor" {
		t.Fatalf("received = %v", received[0])
	}

	// The recipient is the supervisor's run, registered under its name.
	peers, err := cl.MessagePeers(messaging.PeersRequest{Generation: controlGeneration(t, cl)})
	if err != nil {
		t.Fatalf("message_peers: %v", err)
	}
	found := false
	for _, entry := range peers.Peers {
		var session map[string]any
		if len(entry.Session) > 0 {
			_ = json.Unmarshal(entry.Session, &session)
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
	_ = receiveInbound(t, cl)
	sendSupervisorMessage(t, cl, "post-rotation")
	received := receiveInbound(t, cl)
	if len(received) != 1 || inboundMessageText(t, received[0]) != "post-rotation" {
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
	_ = receiveInbound(t, cl)
	sendSupervisorMessage(t, cl, "after sigkill")
	received := receiveInbound(t, cl)
	if len(received) != 1 || inboundMessageText(t, received[0]) != "after sigkill" {
		t.Fatalf("received = %v, want the delivered message", received)
	}
}
