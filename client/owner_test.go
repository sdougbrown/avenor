package client

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/control/controltest"
)

func testOwnerSocket(t *testing.T) (string, *control.ControlServer) {
	t.Helper()
	srv, _ := controltest.NewServer(control.NewState("run_1", "", 0))
	path := filepath.Join(t.TempDir(), "control.sock")
	if err := srv.Start(path); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop() })
	return path, srv
}

// TestOwnerAttachSocketSwapLeaksNoSecret proves no credential byte reaches a
// peer of another uid, even when the socket is replaced after the record was
// validated.
func TestOwnerAttachSocketSwapLeaksNoSecret(t *testing.T) {
	path, _ := testOwnerSocket(t)

	origHook, origPeerUID := beforeOwnerAttachDial, peerUIDFunc
	t.Cleanup(func() {
		beforeOwnerAttachDial = origHook
		peerUIDFunc = origPeerUID
	})

	const foreignUID = 4242
	var attacker net.Listener
	beforeOwnerAttachDial = func() {
		if err := os.Remove(path); err != nil {
			t.Fatalf("unlink owner socket: %v", err)
		}
		l, err := net.Listen("unix", path)
		if err != nil {
			t.Fatalf("bind impostor socket: %v", err)
		}
		attacker = l
	}
	peerUIDFunc = func(*net.UnixConn) (uint32, error) { return foreignUID, nil }

	_, err := AttachOwner(path)

	// The attacker must have received nothing at all.
	unixAttacker, ok := attacker.(*net.UnixListener)
	if !ok {
		t.Fatalf("attacker listener type = %T", attacker)
	}
	_ = unixAttacker.SetDeadline(time.Now().Add(2 * time.Second))
	impostor, acceptErr := unixAttacker.Accept()
	if acceptErr != nil {
		t.Fatalf("attacker accept: %v", acceptErr)
	}
	defer impostor.Close()
	buf := make([]byte, 128)
	_ = impostor.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	n, _ := impostor.Read(buf)
	if n != 0 {
		t.Fatalf("attach sent %d credential bytes to a foreign peer: %q", n, buf[:n])
	}

	var foreign *ForeignPeerError
	if !errors.As(err, &foreign) {
		t.Fatalf("attach error = %v, want ForeignPeerError", err)
	}
	if foreign.SocketPath != path || foreign.PeerUID != foreignUID || foreign.EUID != uint32(os.Geteuid()) {
		t.Fatalf("ForeignPeerError = %+v, want the socket path and peer uid", foreign)
	}
	if !strings.Contains(err.Error(), path) {
		t.Fatalf("error %q does not name the socket path", err)
	}

	// A foreign peer is terminal: no retry, no re-registration.
	_ = unixAttacker.SetDeadline(time.Now().Add(150 * time.Millisecond))
	if extra, err := unixAttacker.Accept(); err == nil {
		_ = extra.Close()
		t.Fatal("attach retried against a foreign peer")
	}
}

// TestOwnerAttachAuthenticates proves the happy path: record, peer check and
// handshake produce a working owner connection.
func TestOwnerAttachAuthenticates(t *testing.T) {
	path, _ := testOwnerSocket(t)
	owner, err := AttachOwner(path)
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	defer owner.Close()
	identity, err := owner.Identity()
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	if identity != "" {
		t.Fatalf("identity = %q, want an empty attempt token", identity)
	}
	if err := owner.Cancel(""); err != nil {
		t.Fatalf("owner cancel: %v", err)
	}
}

// TestOwnerAttachRequiresRecord proves attach refuses a socket with no record.
func TestOwnerAttachRequiresRecord(t *testing.T) {
	path, _ := testOwnerSocket(t)
	if err := os.Remove(path + ".owner.json"); err != nil {
		t.Fatalf("remove record: %v", err)
	}
	if _, err := AttachOwner(path); !errors.Is(err, control.ErrOwnerRecordMissing) {
		t.Fatalf("attach without a record = %v, want owner-record-missing", err)
	}
}

// TestOwnerAttachRevalidatesAfterDial proves a socket rebuilt inside the dial
// window is refused, so the predecessor's secret is never presented to the
// peer that replaced it.
func TestOwnerAttachRevalidatesAfterDial(t *testing.T) {
	path, _ := testOwnerSocket(t)
	origHook := beforeOwnerAttachDial
	t.Cleanup(func() { beforeOwnerAttachDial = origHook })
	beforeOwnerAttachDial = func() {
		if err := os.Remove(path); err != nil {
			t.Fatalf("unlink owner socket: %v", err)
		}
		rebuilt, err := net.Listen("unix", path)
		if err != nil {
			t.Fatalf("rebind socket: %v", err)
		}
		t.Cleanup(func() { _ = rebuilt.Close() })
	}

	if _, err := AttachOwner(path); !errors.Is(err, control.ErrStaleOwnerRecord) {
		t.Fatalf("attach after a mid-dial socket rebuild = %v, want stale-owner-record", err)
	}
}

// TestAuthenticateOwnerBindsThePeer proves the bare handshake refuses a foreign
// peer too, so no exported path can write credentials to one.
func TestAuthenticateOwnerBindsThePeer(t *testing.T) {
	path, _ := testOwnerSocket(t)
	origPeerUID := peerUIDFunc
	t.Cleanup(func() { peerUIDFunc = origPeerUID })
	peerUIDFunc = func(*net.UnixConn) (uint32, error) { return 4242, nil }

	c, err := Dial(path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if err := c.AuthenticateOwner(control.OwnerCapability{ID: "id", Secret: "secret"}); !errors.As(err, new(*ForeignPeerError)) {
		t.Fatalf("AuthenticateOwner = %v, want ForeignPeerError", err)
	}
}

// TestOwnerAttachRejectsStaleRecord proves a record that does not describe the
// live socket is rejected before anything is dialed.
func TestOwnerAttachRejectsStaleRecord(t *testing.T) {
	path, _ := testOwnerSocket(t)
	if err := os.Remove(path); err != nil {
		t.Fatalf("unlink socket: %v", err)
	}
	impostor, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("bind impostor: %v", err)
	}
	defer impostor.Close()

	if _, err := AttachOwner(path); !errors.Is(err, control.ErrStaleOwnerRecord) {
		t.Fatalf("attach over a replaced socket = %v, want stale-owner-record", err)
	}
	_ = impostor.(*net.UnixListener).SetDeadline(time.Now().Add(150 * time.Millisecond))
	if conn, err := impostor.Accept(); err == nil {
		_ = conn.Close()
		t.Fatal("a stale record still reached the replaced socket")
	}

	// A regular file in the socket's place is not attachable either.
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove impostor socket: %v", err)
	}
	if err := os.WriteFile(path, []byte("not a socket"), 0o600); err != nil {
		t.Fatalf("write non-socket: %v", err)
	}
	if _, err := AttachOwner(path); !errors.Is(err, control.ErrStaleOwnerRecord) {
		t.Fatalf("attach over a regular file = %v, want stale-owner-record", err)
	}
}

// TestOwnerAuthWireFixtures pins the owner-auth wire codes and the mapping each
// client applies to them.
func TestOwnerAuthWireFixtures(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "internal", "messaging", "testdata", "ownerauth_*.json"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("owner-auth fixtures: %v (%d found)", err, len(paths))
	}
	for _, path := range paths {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("%s: read: %v", path, err)
		}
		var envelope struct {
			Result json.RawMessage `json:"result"`
			Error  *struct {
				Code    int             `json:"code"`
				Message string          `json:"message"`
				Data    json.RawMessage `json:"data"`
			} `json:"error"`
		}
		if err := json.Unmarshal(body, &envelope); err != nil {
			t.Fatalf("%s: decode: %v", path, err)
		}
		name := filepath.Base(path)
		if envelope.Error == nil {
			var result control.OwnerAuthResult
			if err := json.Unmarshal(envelope.Result, &result); err != nil {
				t.Fatalf("%s: decode result: %v", name, err)
			}
			if !result.Authenticated || result.Version != control.OwnerAuthVersion || result.CapabilityID == "" {
				t.Fatalf("%s: result = %+v", name, result)
			}
			continue
		}
		mapped := classifyOwnerAuthError(&RPCError{Code: envelope.Error.Code, Message: envelope.Error.Message, Data: envelope.Error.Data})
		wantClass := ownerAuthFixtureClass(t, name)
		if !errors.Is(mapped, wantClass) {
			t.Fatalf("%s: class = %v, want %v", name, mapped, wantClass)
		}
		var authErr *OwnerAuthError
		if !errors.As(mapped, &authErr) {
			t.Fatalf("%s: mapped error is not an OwnerAuthError", name)
		}
		var data control.OwnerAuthErrorData
		if err := json.Unmarshal(envelope.Error.Data, &data); err != nil {
			t.Fatalf("%s: decode data: %v", name, err)
		}
		if authErr.Reason != data.Reason || data.Version != control.OwnerAuthVersion {
			t.Fatalf("%s: mapped error = %+v, want reason %q", name, authErr, data.Reason)
		}
	}
}

func ownerAuthFixtureClass(t *testing.T, name string) error {
	t.Helper()
	switch {
	case strings.Contains(name, "unauthenticated"):
		return ErrOwnerUnauthenticated
	case strings.Contains(name, "unsupported"):
		return ErrUnsupportedOwnerAuth
	case strings.Contains(name, "timeout"):
		return ErrOwnerAuthTimeout
	default:
		t.Fatalf("fixture %s has no expected class", name)
		return nil
	}
}

// TestOwnerRecordPublishesUnderWorldWritableDir proves a record published
// beside a socket in a world-writable, non-euid-owned directory still
// attaches.
func TestOwnerRecordPublishesUnderWorldWritableDir(t *testing.T) {
	dir := os.TempDir()
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat temp dir: %v", err)
	}
	if info.Mode().Perm()&0o002 == 0 {
		t.Skipf("temp dir %s is not world-writable on this host", dir)
	}

	s, _ := controltest.NewServer(control.NewState("run_1", "", 0))
	path := filepath.Join(os.TempDir(), "avc-ww-"+time.Now().Format("150405.000000000")+".sock")
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	recordPath := path + ".owner.json"
	recordInfo, err := os.Lstat(recordPath)
	if err != nil {
		t.Fatalf("stat record: %v", err)
	}
	if !recordInfo.Mode().IsRegular() {
		t.Fatalf("record mode = %v, want a regular file", recordInfo.Mode())
	}
	if recordInfo.Mode().Perm() != 0o600 {
		t.Fatalf("record mode = %04o, want 0600", recordInfo.Mode().Perm())
	}
	if got := recordInfo.Sys().(*syscall.Stat_t).Uid; got != uint32(os.Geteuid()) {
		t.Fatalf("record uid = %d, want %d", got, os.Geteuid())
	}
	if filepath.Dir(recordPath) != filepath.Dir(path) {
		t.Fatalf("record %s is not beside socket %s", recordPath, path)
	}

	cl, err := AttachOwner(path)
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	defer cl.Close()
	if err := cl.Cancel(""); err != nil {
		t.Fatalf("owner cancel: %v", err)
	}
}
