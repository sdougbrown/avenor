package stable

import (
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

// reserveStableLoopbackAddr reserves a loopback port and releases it so a test
// can bind the same fixed address.
func reserveStableLoopbackAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve loopback port: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("release loopback port: %v", err)
	}
	return addr
}

// awaitBrokerURLRecord waits for the broker URL record Run publishes once the
// control socket is owned, and returns its contents.
func awaitBrokerURLRecord(t *testing.T, socketPath string) string {
	t.Helper()
	path := BrokerURLPath(socketPath)
	deadline := time.Now().Add(5 * time.Second)
	for {
		data, err := os.ReadFile(path)
		if err == nil {
			return string(data)
		}
		if time.Now().After(deadline) {
			t.Fatalf("broker URL record %s never appeared: %v", path, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestMissingBrokerListenerPublishesNoURL pins that a broker that never bound a
// listener reports no URL and publishes no record. Broker messaging is optional,
// so a supervisor whose default bind failed keeps running without one, but it
// must not advertise a listener-less "http://".
func TestMissingBrokerListenerPublishesNoURL(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	socketPath := newStableSocketPath(t, "brokerurl-nolistener")
	sup := &Supervisor{config: Config{ControlSocket: socketPath}, broker: broker.New("")}
	t.Cleanup(func() { _ = sup.broker.Stop() })

	if got := sup.brokerURL(); got != "" {
		t.Fatalf("brokerURL() = %q with no listener bound, want empty", got)
	}
	if err := sup.publishBrokerURL(); err != nil {
		t.Fatalf("publishBrokerURL: %v", err)
	}
	if _, err := os.Stat(BrokerURLPath(socketPath)); !os.IsNotExist(err) {
		t.Fatalf("broker URL record published with no listener bound: %v", err)
	}
}

func TestBrokerURLPublishedBesideControlSocket(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	socketPath := newStableSocketPath(t, "brokerurl-publish")
	sup := newTestSupervisor(t, Config{
		ControlSocket:   socketPath,
		MaxRuntimes:     1,
		Stderr:          io.Discard,
		ShutdownTimeout: time.Second,
	})
	done := startStableRun(t, sup, socketPath)

	record := strings.TrimSpace(awaitBrokerURLRecord(t, socketPath))
	want := "http://" + sup.broker.Addr()
	if record != want {
		t.Fatalf("published broker URL = %q, want the bound address %q", record, want)
	}
	if host, port, err := net.SplitHostPort(sup.broker.Addr()); err != nil || host != "127.0.0.1" || port == "" || port == "0" {
		t.Fatalf("bound broker address = %q (host %q, port %q), want concrete loopback with an ephemeral port", sup.broker.Addr(), host, port)
	}
	info, err := os.Stat(BrokerURLPath(socketPath))
	if err != nil {
		t.Fatalf("stat broker URL record: %v", err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Fatalf("broker URL record mode = %o, want 600", mode)
	}

	if err := sup.Shutdown("graceful"); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("Run() = %d, want 0", code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run() did not return after a graceful shutdown")
	}
	if _, err := os.Stat(BrokerURLPath(socketPath)); !os.IsNotExist(err) {
		t.Fatalf("broker URL record survived shutdown: %v", err)
	}
}

// TestBrokerURLShutdownPreservesChangedRecord pins that shutdown removes only
// the record this supervisor published: a record another writer replaced is
// left exactly as that writer left it.
func TestBrokerURLShutdownPreservesChangedRecord(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	socketPath := newStableSocketPath(t, "brokerurl-changed")
	sup := newTestSupervisor(t, Config{
		ControlSocket:   socketPath,
		MaxRuntimes:     1,
		Stderr:          io.Discard,
		ShutdownTimeout: time.Second,
	})
	done := startStableRun(t, sup, socketPath)
	awaitBrokerURLRecord(t, socketPath)

	foreign := "http://foreign.invalid:1/replaced\n"
	if err := os.WriteFile(BrokerURLPath(socketPath), []byte(foreign), 0o600); err != nil {
		t.Fatalf("overwrite broker URL record: %v", err)
	}
	if err := sup.Shutdown("graceful"); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("Run() = %d, want 0", code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run() did not return after a graceful shutdown")
	}
	data, err := os.ReadFile(BrokerURLPath(socketPath))
	if err != nil {
		t.Fatalf("changed broker URL record removed by shutdown: %v", err)
	}
	if got := string(data); got != foreign {
		t.Fatalf("changed broker URL record = %q, want it untouched as %q", got, foreign)
	}
}

// TestCompetingSupervisorLeavesLiveBrokerURLRecord pins that a supervisor that
// loses the control socket neither overwrites nor removes the live owner's
// record.
func TestCompetingSupervisorLeavesLiveBrokerURLRecord(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	socketPath := newStableSocketPath(t, "brokerurl-competition")
	owner := newTestSupervisor(t, Config{
		ControlSocket:   socketPath,
		MaxRuntimes:     1,
		Stderr:          io.Discard,
		ShutdownTimeout: time.Second,
	})
	done := startStableRun(t, owner, socketPath)
	ownerRecord := strings.TrimSpace(awaitBrokerURLRecord(t, socketPath))

	competitor := newTestSupervisor(t, Config{
		ControlSocket: socketPath,
		MaxRuntimes:   1,
		Stderr:        io.Discard,
	})
	if code := competitor.Run(); code != 1 {
		t.Fatalf("competing Run() = %d, want 1", code)
	}
	after, err := os.ReadFile(BrokerURLPath(socketPath))
	if err != nil {
		t.Fatalf("competing supervisor removed the live record: %v", err)
	}
	if got := strings.TrimSpace(string(after)); got != ownerRecord {
		t.Fatalf("record after the competing start = %q, want the live owner's %q", got, ownerRecord)
	}

	if err := owner.Shutdown("graceful"); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("owner Run() did not return after a graceful shutdown")
	}
	if _, err := os.Stat(BrokerURLPath(socketPath)); !os.IsNotExist(err) {
		t.Fatalf("broker URL record survived the owner's shutdown: %v", err)
	}
}

// TestStartupFailureLeavesRecordItNeverPublished pins that a supervisor whose
// control socket acquisition fails leaves an existing record at that path alone:
// it was never this process's to remove.
func TestStartupFailureLeavesRecordItNeverPublished(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root keeps an unwritable directory writable, so the control lock cannot be denied")
	}
	t.Setenv("HOME", t.TempDir())
	dir, err := os.MkdirTemp("/tmp", "ast-brokerurl-foreign-")
	if err != nil {
		t.Fatalf("create socket dir: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(dir, 0o700)
		_ = os.RemoveAll(dir)
	})
	socketPath := dir + "/foreign.sock"
	foreign := "http://foreign.invalid:1\n"
	if err := os.WriteFile(BrokerURLPath(socketPath), []byte(foreign), 0o600); err != nil {
		t.Fatalf("write foreign broker URL record: %v", err)
	}
	// The socket directory cannot host the control lock, so acquisition fails
	// before this supervisor publishes anything.
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatalf("make socket dir unwritable: %v", err)
	}

	sup := newTestSupervisor(t, Config{
		ControlSocket: socketPath,
		MaxRuntimes:   1,
		Stderr:        io.Discard,
	})
	if code := sup.Run(); code != 1 {
		t.Fatalf("Run() = %d, want 1 when the control socket cannot be acquired", code)
	}
	data, err := os.ReadFile(BrokerURLPath(socketPath))
	if err != nil {
		t.Fatalf("foreign broker URL record removed by a startup that never published: %v", err)
	}
	if got := string(data); got != foreign {
		t.Fatalf("foreign broker URL record = %q, want it untouched", got)
	}
}

func TestBrokerURLRecordRemovedWhenStartupFailsAfterPublication(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	socketPath := newStableSocketPath(t, "brokerurl-late-failure")
	debugListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("occupy http debug port: %v", err)
	}
	defer debugListener.Close()

	sup := newTestSupervisor(t, Config{
		ControlSocket: socketPath,
		HTTPDebug:     debugListener.Addr().String(),
		MaxRuntimes:   1,
		Stderr:        io.Discard,
	})
	if code := sup.Run(); code != 1 {
		t.Fatalf("Run() = %d, want 1 when the http debug listener cannot bind", code)
	}
	if _, err := os.Stat(BrokerURLPath(socketPath)); !os.IsNotExist(err) {
		t.Fatalf("broker URL record survived a startup failure: %v", err)
	}
	if _, err := os.Stat(socketPath); !os.IsNotExist(err) {
		t.Fatalf("control socket survived a startup failure: %v", err)
	}
	resp, err := http.Get("http://" + sup.broker.Addr() + "/health")
	if err == nil {
		resp.Body.Close()
		t.Fatalf("broker still served %s after Run returned", sup.broker.Addr())
	}
}

// TestSupervisorBindsConfiguredBrokerAddr pins that Config.BrokerAddr reaches
// the broker listener, so a fixed hub port is what gets published.
func TestSupervisorBindsConfiguredBrokerAddr(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	addr := reserveStableLoopbackAddr(t)
	sup := newTestSupervisor(t, Config{
		ControlSocket: newStableSocketPath(t, "brokeraddr"),
		BrokerAddr:    addr,
		MaxRuntimes:   1,
		Stderr:        io.Discard,
	})
	t.Cleanup(func() { _ = sup.broker.Stop() })

	if got := sup.broker.Addr(); got != addr {
		t.Fatalf("broker address = %q, want the configured %q", got, addr)
	}
	resp, err := http.Get("http://" + addr + "/health")
	if err != nil {
		t.Fatalf("health on the configured broker address: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health = %d, want 200", resp.StatusCode)
	}
}

func TestConfiguredBindFailureFailsStartup(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("occupy port: %v", err)
	}
	defer occupied.Close()
	addr := occupied.Addr().String()
	socketPath := newStableSocketPath(t, "brokeraddr-occupied")

	stderr := captureStderr(t, func(w io.Writer) {
		sup := newTestSupervisor(t, Config{
			ControlSocket: socketPath,
			BrokerAddr:    addr,
			MaxRuntimes:   1,
			Stderr:        w,
		})
		if code := sup.Run(); code != 1 {
			t.Fatalf("Run() = %d, want 1 when the configured broker address is occupied", code)
		}
	})
	if !strings.Contains(stderr, addr) {
		t.Fatalf("stderr = %q, want it to name the occupied broker address %q", stderr, addr)
	}
	if _, err := os.Stat(socketPath); !os.IsNotExist(err) {
		t.Fatalf("control socket bound despite the broker bind failure: %v", err)
	}
}

func TestWildcardBrokerAddrIsRejected(t *testing.T) {
	for _, addr := range []string{"0.0.0.0:8787", "[::]:8787", "[::%lo]:8787", "[::ffff:0.0.0.0]:8787", ":8787"} {
		t.Run(addr, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			socketPath := newStableSocketPath(t, "brokeraddr-wildcard")
			stderr := captureStderr(t, func(w io.Writer) {
				sup := newTestSupervisor(t, Config{
					ControlSocket: socketPath,
					BrokerAddr:    addr,
					MaxRuntimes:   1,
					Stderr:        w,
				})
				if got := sup.broker.Addr(); got != "" {
					t.Fatalf("broker bound %q for a wildcard address", got)
				}
				if code := sup.Run(); code != 1 {
					t.Fatalf("Run() = %d, want 1 for a wildcard broker address", code)
				}
			})
			if !strings.Contains(stderr, "wildcard") {
				t.Fatalf("stderr = %q, want the wildcard-bind refusal", stderr)
			}
			if !strings.Contains(stderr, "concrete interface address") {
				t.Fatalf("stderr = %q, want it to point at a concrete interface address", stderr)
			}
			if _, err := os.Stat(socketPath); !os.IsNotExist(err) {
				t.Fatalf("control socket bound despite the wildcard broker address: %v", err)
			}
		})
	}
}
