package broker

import (
	"net"
	"net/http"
	"strings"
	"testing"
)

// reserveLoopbackAddr reserves a loopback port and releases it, so a caller can
// bind the same address and prove it honours a fixed port.
func reserveLoopbackAddr(t *testing.T) string {
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

// TestBrokerStartOnBindsConfiguredAddr pins the durable-listener contract: the
// broker binds the address it was given, reports it unchanged, and serves on
// it.
func TestBrokerStartOnBindsConfiguredAddr(t *testing.T) {
	addr := reserveLoopbackAddr(t)
	b := New("")
	if err := b.StartOn(addr); err != nil {
		t.Fatalf("StartOn(%q): %v", addr, err)
	}
	defer b.Stop()

	if got := b.Addr(); got != addr {
		t.Fatalf("Addr() = %q, want the configured %q", got, addr)
	}
	resp, err := http.Get("http://" + addr + "/health")
	if err != nil {
		t.Fatalf("health on configured addr: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health on configured addr = %d, want 200", resp.StatusCode)
	}
}

// TestBrokerStartKeepsLoopbackEphemeralDefault pins the unchanged default: no
// configured address still means an ephemeral loopback port.
func TestBrokerStartKeepsLoopbackEphemeralDefault(t *testing.T) {
	b := New("")
	if err := b.StartOn(""); err != nil {
		t.Fatalf("StartOn(\"\"): %v", err)
	}
	defer b.Stop()

	host, port, err := net.SplitHostPort(b.Addr())
	if err != nil {
		t.Fatalf("split broker addr %q: %v", b.Addr(), err)
	}
	if host != "127.0.0.1" {
		t.Fatalf("default bind host = %q, want loopback", host)
	}
	if port == "" || port == "0" {
		t.Fatalf("default bind port = %q, want an ephemeral port", port)
	}
}

// TestBrokerStartOnReportsOccupiedAddr pins that an occupied configured address
// is a reported failure that names the address, not a silent fallback to
// another port.
func TestBrokerStartOnReportsOccupiedAddr(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("occupy loopback port: %v", err)
	}
	defer l.Close()
	addr := l.Addr().String()

	b := New("")
	defer b.Stop()
	err = b.StartOn(addr)
	if err == nil {
		t.Fatalf("StartOn(%q) succeeded on an occupied address", addr)
	}
	if !strings.Contains(err.Error(), addr) {
		t.Fatalf("StartOn error = %q, want it to name %q", err, addr)
	}
	if got := b.Addr(); got != "" {
		t.Fatalf("Addr() = %q after a failed bind, want empty", got)
	}
}

// TestBrokerStartOnRejectsInvalidAddr pins that a malformed address is reported
// rather than replaced by the default bind.
func TestBrokerStartOnRejectsInvalidAddr(t *testing.T) {
	b := New("")
	defer b.Stop()
	if err := b.StartOn("not-an-address"); err == nil {
		t.Fatal("StartOn with an invalid address succeeded")
	}
	if got := b.Addr(); got != "" {
		t.Fatalf("Addr() = %q after an invalid bind, want empty", got)
	}
}
