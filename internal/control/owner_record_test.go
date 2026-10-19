package control

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
)

func ownerRecordPath(socketPath string) string { return socketPath + ".owner.json" }

// dropListener abandons the bind without running cleanup, leaving the socket
// and record behind exactly as an unclean owner death would.
func dropListener(t *testing.T, s *ControlServer) {
	t.Helper()
	s.mu.Lock()
	listener := s.listener
	s.listener = nil
	s.mu.Unlock()
	if listener == nil {
		return
	}
	if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("close abandoned listener: %v", err)
	}
}

func publishTestServer(t *testing.T, socketPath string) (*ControlServer, OwnerCapability) {
	t.Helper()
	s, cap := NewTestServer(NewState("run_1", "", 0))
	if err := s.Start(socketPath); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop() })
	return s, cap
}

func socketIdentityOf(t *testing.T, path string) SocketIdentity {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("stat socket: unexpected stat type %T", info.Sys())
	}
	return SocketIdentity{Dev: strconv.FormatUint(uint64(st.Dev), 10), Ino: strconv.FormatUint(st.Ino, 10)}
}

// TestOwnerBootstrapRecordPublication proves the record is published before
// the identity token becomes observable and before readiness.
func TestOwnerBootstrapRecordPublication(t *testing.T) {
	t.Setenv("AVENOR_TEST_HOOKS", "1")
	t.Setenv("AVENOR_TEST_OWNER_PUBLISH_DELAY", "300ms")

	path := testSocketPath(t)
	s, cap := NewTestServer(NewState("run_1", "", 0))
	s.identityTokenOverride = "attempt-token"
	started := make(chan error, 1)
	go func() { started <- s.Start(path) }()
	defer func() {
		if err := s.Stop(); err != nil {
			t.Fatalf("stop: %v", err)
		}
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Lstat(path); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("socket never appeared during publication delay")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := os.Lstat(ownerRecordPath(path)); err == nil {
		t.Fatal("record was observable before publication completed")
	}

	// The socket accepts at the kernel backlog, but nothing answers yet: the
	// identity token is not observable before this child's record exists.
	conn := mustDial(t, path)
	_ = conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	if err := writeReq(t, conn, Request{JSONRPC: "2.0", ID: 1, Method: "identity"}); err != nil {
		t.Fatalf("write identity: %v", err)
	}
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("identity answered before the record was published")
	}
	_ = conn.Close()

	if err := <-started; err != nil {
		t.Fatalf("start: %v", err)
	}
	record, err := LoadOwnerRecord(path)
	if err != nil {
		t.Fatalf("load record: %v", err)
	}
	if record.CapabilityID != cap.ID {
		t.Fatalf("record capability = %q, want %q", record.CapabilityID, cap.ID)
	}
	if record.SocketIdentity != socketIdentityOf(t, path) {
		t.Fatalf("record identity = %+v, want the live socket's", record.SocketIdentity)
	}
}

// TestOwnerBootstrapStartupFailureCleanup proves a publication failure leaves no
// half-started server behind.
func TestOwnerBootstrapStartupFailureCleanup(t *testing.T) {
	path := testSocketPath(t)
	// A directory in the record's place cannot be replaced by the atomic rename.
	if err := os.MkdirAll(ownerRecordPath(path), 0o700); err != nil {
		t.Fatalf("mkdir record path: %v", err)
	}
	s, _ := NewTestServer(NewState("run_1", "", 0))
	err := s.Start(path)
	if err == nil {
		_ = s.Stop()
		t.Fatal("start succeeded with an unusable record path")
	}
	if _, statErr := os.Lstat(path); !os.IsNotExist(statErr) {
		t.Fatalf("failed start left its socket behind: %v", statErr)
	}
	if info, statErr := os.Lstat(ownerRecordPath(path)); statErr != nil || !info.IsDir() {
		t.Fatalf("failed start removed the foreign record path: %v", statErr)
	}
	if conn, dialErr := dialControlSocket("unix", path, 100*time.Millisecond); dialErr == nil {
		_ = conn.Close()
		t.Fatal("failed start left a listening socket")
	}
}

// TestOwnerPublicationRollbackRemovalError reports cleanup failures instead of
// claiming success.
func TestOwnerPublicationRollbackRemovalError(t *testing.T) {
	removeErr := errors.New("socket remove failed")
	publishErr := errors.New("publication failed")
	origRemove, origPublish := testOwnerSocketRemove, beforeOwnerRecordPublish
	testOwnerSocketRemove = func(string) error { return removeErr }
	beforeOwnerRecordPublish = func(string) error { return publishErr }
	t.Cleanup(func() {
		testOwnerSocketRemove = origRemove
		beforeOwnerRecordPublish = origPublish
	})

	path := testSocketPath(t)
	s, _ := NewTestServer(NewState("run_1", "", 0))
	err := s.Start(path)
	if err == nil {
		_ = s.Stop()
		t.Fatal("start succeeded although publication failed")
	}
	if !errors.Is(err, publishErr) {
		t.Fatalf("start error = %v, want the publication failure", err)
	}
	if !errors.Is(err, removeErr) {
		t.Fatalf("start error = %v, want the cleanup failure joined", err)
	}
	// The artifact that could not be removed is reported, not hidden.
	if _, statErr := os.Lstat(path); statErr != nil {
		t.Fatalf("expected the unremoved socket to remain for the caller to see: %v", statErr)
	}
	_ = os.Remove(path)
}

// TestOwnerPublicationRollbackKeepsReplacement proves an old owner's cleanup
// never touches a replacement's artifacts.
func TestOwnerPublicationRollbackKeepsReplacement(t *testing.T) {
	path := testSocketPath(t)
	old, _ := NewTestServer(NewState("run_1", "", 0))
	if err := old.Start(path); err != nil {
		t.Fatalf("start old: %v", err)
	}
	// Lose the listener without running cleanup, so the path is a dead socket.
	dropListener(t, old)

	replacement, replacementCap := publishTestServer(t, path)
	recordBefore, err := os.ReadFile(ownerRecordPath(path))
	if err != nil {
		t.Fatalf("read replacement record: %v", err)
	}

	if err := old.Stop(); err != nil {
		t.Fatalf("old stop: %v", err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("old cleanup removed the replacement socket: %v", err)
	}
	recordAfter, err := os.ReadFile(ownerRecordPath(path))
	if err != nil {
		t.Fatalf("read replacement record after cleanup: %v", err)
	}
	if string(recordBefore) != string(recordAfter) {
		t.Fatal("old cleanup rewrote the replacement record")
	}
	loaded, err := LoadOwnerRecord(path)
	if err != nil {
		t.Fatalf("load replacement record: %v", err)
	}
	if loaded.CapabilityID != replacementCap.ID {
		t.Fatalf("record capability = %q, want the replacement's", loaded.CapabilityID)
	}
	_ = replacement
}

// TestOwnerLockSurvivesRestartAndConcurrentCreators proves the persistent lock
// is create-or-open, converges concurrent creators on one inode and refuses a
// symlinked lock without touching its target.
func TestOwnerLockSurvivesRestartAndConcurrentCreators(t *testing.T) {
	path := testSocketPath(t)
	lockPath := path + ".control.lock"
	if err := os.WriteFile(lockPath, nil, 0o644); err != nil {
		t.Fatalf("write leftover lock: %v", err)
	}
	leftover, err := os.Stat(lockPath)
	if err != nil {
		t.Fatalf("stat leftover lock: %v", err)
	}

	dir := t.TempDir()
	target := filepath.Join(dir, "lock-target")
	if err := os.WriteFile(target, []byte("untouched"), 0o600); err != nil {
		t.Fatalf("write symlink target: %v", err)
	}
	symlinkPath := filepath.Join(t.TempDir(), "s.sock")
	if err := os.Symlink(target, symlinkPath+".control.lock"); err != nil {
		t.Fatalf("symlink lock: %v", err)
	}
	symlinked, _ := NewTestServer(NewState("run_1", "", 0))
	if err := symlinked.Start(symlinkPath); err == nil {
		_ = symlinked.Stop()
		t.Fatal("start accepted a symlinked control lock")
	}
	if got, err := os.ReadFile(target); err != nil || string(got) != "untouched" {
		t.Fatalf("symlinked lock target = %q, %v; want it untouched", got, err)
	}

	first, _ := publishTestServer(t, path)
	if err := first.Stop(); err != nil {
		t.Fatalf("first stop: %v", err)
	}
	afterStop, err := os.Stat(lockPath)
	if err != nil {
		t.Fatalf("stop removed the persistent lock: %v", err)
	}
	if !os.SameFile(leftover, afterStop) {
		t.Fatal("the lock inode was replaced instead of reopened")
	}
	if afterStop.Mode().Perm() != 0o600 {
		t.Fatalf("lock mode = %04o, want 0600", afterStop.Mode().Perm())
	}

	// Two concurrent creators converge on the one lock inode and one bind.
	a, _ := NewTestServer(NewState("run_a", "", 0))
	b, _ := NewTestServer(NewState("run_b", "", 0))
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, srv := range []*ControlServer{a, b} {
		wg.Add(1)
		go func(i int, srv *ControlServer) {
			defer wg.Done()
			errs[i] = srv.Start(path)
		}(i, srv)
	}
	wg.Wait()
	defer func() {
		_ = a.Stop()
		_ = b.Stop()
	}()
	winners := 0
	for _, err := range errs {
		if err == nil {
			winners++
			continue
		}
		if !errors.Is(err, ErrSocketInUse) {
			t.Fatalf("concurrent creator error = %v, want ErrSocketInUse", err)
		}
	}
	if winners != 1 {
		t.Fatalf("concurrent creators: %d winners, want 1", winners)
	}
	converged, err := os.Stat(lockPath)
	if err != nil {
		t.Fatalf("stat lock: %v", err)
	}
	if !os.SameFile(afterStop, converged) {
		t.Fatal("concurrent creators did not converge on the leftover lock inode")
	}
}

// TestConcurrentOwnedStartsNeverOwnEachOthersWinner proves the record beside a
// socket always belongs to the process that won the bind.
func TestConcurrentOwnedStartsNeverOwnEachOthersWinner(t *testing.T) {
	for _, tokens := range [][2]string{{"token-a", "token-b"}, {"token-b", "token-a"}} {
		path := testSocketPath(t)
		servers := make([]*ControlServer, 2)
		caps := make([]OwnerCapability, 2)
		for i := range servers {
			servers[i], caps[i] = NewTestServer(NewState("run_"+tokens[i], "", 0))
			servers[i].identityTokenOverride = tokens[i]
		}
		var wg sync.WaitGroup
		errs := make([]error, 2)
		for i := range servers {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				errs[i] = servers[i].Start(path)
			}(i)
		}
		wg.Wait()
		defer func() {
			for _, srv := range servers {
				_ = srv.Stop()
			}
		}()

		winner := -1
		for i, err := range errs {
			if err == nil {
				if winner >= 0 {
					t.Fatalf("both starts won the bind for tokens %v", tokens)
				}
				winner = i
				continue
			}
			if !errors.Is(err, ErrSocketInUse) {
				t.Fatalf("loser error = %v, want ErrSocketInUse", err)
			}
		}
		if winner < 0 {
			t.Fatalf("no start won the bind for tokens %v", tokens)
		}

		record, err := LoadOwnerRecord(path)
		if err != nil {
			t.Fatalf("load record: %v", err)
		}
		if record.CapabilityID != caps[winner].ID {
			t.Fatalf("record capability = %q, want the winner's %q", record.CapabilityID, caps[winner].ID)
		}
		if record.SocketIdentity != socketIdentityOf(t, path) {
			t.Fatalf("record identity = %+v, want the live socket's", record.SocketIdentity)
		}

		// The winner answers identity with its own attempt token; the loser's
		// token is never the answer.
		conn := mustDial(t, path)
		if err := writeReq(t, conn, Request{JSONRPC: "2.0", ID: 1, Method: "identity"}); err != nil {
			t.Fatalf("write identity: %v", err)
		}
		resp := readResp(t, conn)
		_ = conn.Close()
		if resp.Error != nil {
			t.Fatalf("identity: %+v", resp.Error)
		}
		result, ok := resp.Result.(map[string]any)
		if !ok || result["token"] != tokens[winner] {
			t.Fatalf("identity = %#v, want the winner's token %q", resp.Result, tokens[winner])
		}
	}
}

// TestOwnerRecordSymlinkRejected proves neither publication nor loading follows
// a symlinked path.
func TestOwnerRecordSymlinkRejected(t *testing.T) {
	dir := t.TempDir()
	real := filepath.Join(dir, "real")
	if err := os.MkdirAll(real, 0o700); err != nil {
		t.Fatalf("mkdir real: %v", err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatalf("symlink dir: %v", err)
	}
	s, _ := NewTestServer(NewState("run_1", "", 0))
	if err := s.Start(filepath.Join(link, "c.sock")); err == nil {
		_ = s.Stop()
		t.Fatal("publication accepted a symlinked directory component")
	}

	// A symlinked record is rejected at load even when the target is valid.
	path := testSocketPath(t)
	published, _ := publishTestServer(t, path)
	recordPath := ownerRecordPath(path)
	body, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatalf("read record: %v", err)
	}
	elsewhere := filepath.Join(t.TempDir(), "moved.json")
	if err := os.WriteFile(elsewhere, body, 0o600); err != nil {
		t.Fatalf("write moved record: %v", err)
	}
	if err := os.Remove(recordPath); err != nil {
		t.Fatalf("remove record: %v", err)
	}
	if err := os.Symlink(elsewhere, recordPath); err != nil {
		t.Fatalf("symlink record: %v", err)
	}
	if _, err := LoadOwnerRecord(path); !errors.Is(err, ErrStaleOwnerRecord) {
		t.Fatalf("load symlinked record = %v, want stale-owner-record", err)
	}
	_ = published
}

// TestOwnerRecordModeRejected proves a record with the wrong mode, owner or
// file type is rejected and never replaced.
func TestOwnerRecordModeRejected(t *testing.T) {
	path := testSocketPath(t)
	published, _ := publishTestServer(t, path)
	recordPath := ownerRecordPath(path)

	if err := os.Chmod(recordPath, 0o644); err != nil {
		t.Fatalf("chmod record: %v", err)
	}
	if _, err := LoadOwnerRecord(path); !errors.Is(err, ErrStaleOwnerRecord) {
		t.Fatalf("load 0644 record = %v, want stale-owner-record", err)
	}
	if err := os.Chmod(recordPath, 0o600); err != nil {
		t.Fatalf("restore record mode: %v", err)
	}

	if err := os.Remove(recordPath); err != nil {
		t.Fatalf("remove record: %v", err)
	}
	if err := os.Mkdir(recordPath, 0o700); err != nil {
		t.Fatalf("mkdir record path: %v", err)
	}
	if _, err := LoadOwnerRecord(path); !errors.Is(err, ErrStaleOwnerRecord) {
		t.Fatalf("load directory record = %v, want stale-owner-record", err)
	}
	if _, err := os.Lstat(recordPath); err != nil {
		t.Fatalf("rejected record was removed: %v", err)
	}

	if os.Geteuid() == 0 {
		if err := os.Remove(recordPath); err != nil {
			t.Fatalf("remove directory record: %v", err)
		}
		if err := os.WriteFile(recordPath, []byte("{}"), 0o600); err != nil {
			t.Fatalf("write foreign record: %v", err)
		}
		if err := os.Chown(recordPath, 1, 1); err != nil {
			t.Fatalf("chown record: %v", err)
		}
		if _, err := LoadOwnerRecord(path); !errors.Is(err, ErrStaleOwnerRecord) {
			t.Fatalf("load foreign-owned record = %v, want stale-owner-record", err)
		}
	}
	_ = published
}

// TestOwnerRecordSwapUnderReaderRejected proves the opened descriptor is
// re-checked against the inspected path.
func TestOwnerRecordSwapUnderReaderRejected(t *testing.T) {
	path := testSocketPath(t)
	published, _ := publishTestServer(t, path)
	recordPath := ownerRecordPath(path)

	orig := beforeOwnerRecordOpen
	beforeOwnerRecordOpen = func() {
		// Replace the record between the path inspection and the open.
		_ = os.Remove(recordPath)
		body, err := os.ReadFile(recordPath + ".stash")
		if err != nil {
			_ = os.WriteFile(recordPath, []byte("{}"), 0o600)
			return
		}
		_ = os.WriteFile(recordPath, body, 0o600)
	}
	t.Cleanup(func() { beforeOwnerRecordOpen = orig })
	if err := os.WriteFile(recordPath+".stash", []byte("{\"version\":1}"), 0o600); err != nil {
		t.Fatalf("write stash: %v", err)
	}

	if _, err := LoadOwnerRecord(path); !errors.Is(err, ErrStaleOwnerRecord) {
		t.Fatalf("load swapped record = %v, want stale-owner-record", err)
	}
	_ = published
}

// TestStaleOwnerSocketTakenOverUnderLock proves Start is the only unlinker and
// that it replaces a dead owner's record with its own.
func TestStaleOwnerSocketTakenOverUnderLock(t *testing.T) {
	path := testSocketPath(t)
	old, oldCap := NewTestServer(NewState("run_1", "", 0))
	if err := old.Start(path); err != nil {
		t.Fatalf("start old: %v", err)
	}
	dropListener(t, old)

	replacement, replacementCap := publishTestServer(t, path)
	record, err := LoadOwnerRecord(path)
	if err != nil {
		t.Fatalf("load record: %v", err)
	}
	if record.CapabilityID != replacementCap.ID {
		t.Fatalf("record capability = %q, want the replacement's", record.CapabilityID)
	}
	if record.CapabilityID == oldCap.ID {
		t.Fatal("record still names the dead owner")
	}
	_ = replacement
}

// TestStartOnLiveOwnerChangesNothing proves a competing starter mutates
// nothing.
func TestStartOnLiveOwnerChangesNothing(t *testing.T) {
	path := testSocketPath(t)
	live, _ := publishTestServer(t, path)
	recordBefore, err := os.ReadFile(ownerRecordPath(path))
	if err != nil {
		t.Fatalf("read record: %v", err)
	}
	socketBefore, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}
	lockBefore, err := os.Stat(path + ".control.lock")
	if err != nil {
		t.Fatalf("stat lock: %v", err)
	}

	competitor, _ := NewTestServer(NewState("run_2", "", 0))
	err = competitor.Start(path)
	if !errors.Is(err, ErrSocketInUse) {
		t.Fatalf("start on live owner = %v, want ErrSocketInUse", err)
	}

	recordAfter, err := os.ReadFile(ownerRecordPath(path))
	if err != nil {
		t.Fatalf("read record after: %v", err)
	}
	if string(recordBefore) != string(recordAfter) {
		t.Fatal("competing starter rewrote the record")
	}
	socketAfter, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat socket after: %v", err)
	}
	if !os.SameFile(socketBefore, socketAfter) {
		t.Fatal("competing starter replaced the socket")
	}
	lockAfter, err := os.Stat(path + ".control.lock")
	if err != nil {
		t.Fatalf("stat lock after: %v", err)
	}
	if !os.SameFile(lockBefore, lockAfter) {
		t.Fatal("competing starter replaced the lock")
	}
	_ = live
}

// TestStartRefusesUncertainProbe proves a non-refusal probe error authorizes
// nothing.
func TestStartRefusesUncertainProbe(t *testing.T) {
	path := testSocketPath(t)
	live, _ := publishTestServer(t, path)
	recordBefore, err := os.ReadFile(ownerRecordPath(path))
	if err != nil {
		t.Fatalf("read record: %v", err)
	}

	orig := dialControlSocket
	dialControlSocket = func(network, address string, timeout time.Duration) (net.Conn, error) {
		return nil, os.ErrDeadlineExceeded
	}
	t.Cleanup(func() { dialControlSocket = orig })

	competitor, _ := NewTestServer(NewState("run_2", "", 0))
	if err := competitor.Start(path); err == nil || errors.Is(err, ErrSocketInUse) {
		t.Fatalf("start with an uncertain probe = %v, want a probe failure", err)
	}
	recordAfter, err := os.ReadFile(ownerRecordPath(path))
	if err != nil {
		t.Fatalf("read record after: %v", err)
	}
	if string(recordBefore) != string(recordAfter) {
		t.Fatal("uncertain probe mutated the record")
	}
	_ = live
}

// TestOwnerSecretNeverEscapesDiagnosticsOrChildEnv proves the capability never
// reaches a child environment, a diagnostics payload or an error text, while
// launch identity still propagates.
func TestOwnerSecretNeverEscapesDiagnosticsOrChildEnv(t *testing.T) {
	t.Setenv("AVENOR_TEST_HOOKS", "1")
	t.Setenv("AVENOR_TEST_OWNER_PUBLISH_DELAY", "1ms")

	path := testSocketPath(t)
	s, cap := publishTestServer(t, path)
	conn := mustDial(t, path)
	defer conn.Close()
	for i, method := range []string{"identity", "status", "result", "list"} {
		if err := writeReq(t, conn, Request{JSONRPC: "2.0", ID: i + 1, Method: method}); err != nil {
			t.Fatalf("%s: write: %v", method, err)
		}
		resp := readResp(t, conn)
		raw, err := json.Marshal(resp)
		if err != nil {
			t.Fatalf("%s: marshal response: %v", method, err)
		}
		if contains(raw, cap.Secret) || contains(raw, cap.ID) {
			t.Fatalf("%s response leaked owner credential material", method)
		}
	}

	// A failed publication's error text never carries the secret.
	failing, failingCap := NewTestServer(NewState("run_2", "", 0))
	failingPath := testSocketPath(t)
	if err := os.MkdirAll(ownerRecordPath(failingPath), 0o700); err != nil {
		t.Fatalf("mkdir record path: %v", err)
	}
	err := failing.Start(failingPath)
	if err == nil {
		_ = failing.Stop()
		t.Fatal("start succeeded with an unusable record path")
	}
	if contains([]byte(err.Error()), failingCap.Secret) || contains([]byte(err.Error()), failingCap.ID) {
		t.Fatal("startup error leaked owner credential material")
	}

	// A provider child inherits no test hook and no capability, and keeps its
	// launch identity.
	provenance := runtime.DirectLaunchProvenance("run_child", "label", 1)
	env := runtime.LaunchEnv(os.Environ(), provenance)
	for _, entry := range env {
		key, _, _ := strings.Cut(entry, "=")
		switch key {
		case "AVENOR_TEST_HOOKS", "AVENOR_TEST_OWNER_PUBLISH_DELAY":
			t.Fatalf("test hook %s reached a child environment", key)
		}
		if contains([]byte(entry), cap.Secret) || contains([]byte(entry), failingCap.Secret) {
			t.Fatalf("child environment leaked owner credential material via %s", key)
		}
	}
	identityPropagated := false
	for _, entry := range env {
		if entry == "AVENOR_RUNTIME_ID=run_child" {
			identityPropagated = true
		}
	}
	if !identityPropagated {
		t.Fatal("launch identity stopped propagating to children")
	}

	// The same environment, observed by a real child process.
	command := exec.Command("sh", "-c", "env")
	command.Env = env
	observed, err := command.Output()
	if err != nil {
		t.Fatalf("run child: %v", err)
	}
	if contains(observed, "AVENOR_TEST_HOOKS=") || contains(observed, "AVENOR_TEST_OWNER_PUBLISH_DELAY=") {
		t.Fatal("a test hook reached a real child process")
	}
	for _, candidate := range []OwnerCapability{cap, failingCap} {
		if contains(observed, candidate.Secret) || contains(observed, candidate.ID) {
			t.Fatal("a real child process inherited owner credential material")
		}
	}
	if !contains(observed, "AVENOR_RUNTIME_ID=run_child") {
		t.Fatal("a real child process lost its launch identity")
	}
	_ = s
}
