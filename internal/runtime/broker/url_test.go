package broker

import (
	"net/url"
	"testing"
)

// TestURLPreservesScopedIPv6Zone pins the formatter's escape of a zoned
// link-local host, so the URL a child receives parses back to the bound host.
func TestURLPreservesScopedIPv6Zone(t *testing.T) {
	got := URL("[fe80::1%eth0]:8787")
	parsed, err := url.Parse(got)
	if err != nil {
		t.Fatalf("parse %q: %v", got, err)
	}
	if parsed.Scheme != "http" {
		t.Errorf("scheme = %q, want http", parsed.Scheme)
	}
	if host := parsed.Hostname(); host != "fe80::1%eth0" {
		t.Errorf("hostname = %q, want the scoped host %q", host, "fe80::1%eth0")
	}
	if port := parsed.Port(); port != "8787" {
		t.Errorf("port = %q, want 8787", port)
	}
}

func TestURLEmptyAddrIsEmpty(t *testing.T) {
	if got := URL(""); got != "" {
		t.Fatalf("URL(\"\") = %q, want empty", got)
	}
}
