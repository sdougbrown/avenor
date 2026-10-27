package messaging

import (
	"net/url"
	"strings"
	"testing"
)

// TestBrokerURLPreservesScopedIPv6Zone pins the formatter's escape of a zoned
// link-local host, so the URL a child receives parses back to the bound host.
func TestBrokerURLPreservesScopedIPv6Zone(t *testing.T) {
	got := BrokerURL("[fe80::1%eth0]:8787")
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

func TestBrokerURLEmptyAddrIsEmpty(t *testing.T) {
	if got := BrokerURL(""); got != "" {
		t.Fatalf("BrokerURL(\"\") = %q, want empty", got)
	}
}

// TestParseBrokerURLAcceptsAReachableBaseURL pins what an operator may
// advertise: an absolute http or https base URL, canonical either as typed or
// with one cosmetic trailing slash, and blank meaning no override.
func TestParseBrokerURLAcceptsAReachableBaseURL(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want string
	}{
		{raw: "", want: ""},
		{raw: " \t\n", want: ""},
		{raw: "https://hub.example.com", want: "https://hub.example.com"},
		{raw: "https://hub.example.com/", want: "https://hub.example.com"},
		{raw: " http://192.0.2.10:8787 ", want: "http://192.0.2.10:8787"},
		{raw: "http://[2001:db8::1]:8787", want: "http://[2001:db8::1]:8787"},
		{raw: "http://[::1]", want: "http://[::1]"},
		{raw: "HTTPS://hub.example.com", want: "https://hub.example.com"},
	} {
		got, err := ParseBrokerURL(tc.raw)
		if err != nil {
			t.Fatalf("ParseBrokerURL(%q) = %v, want the canonical %q", tc.raw, err, tc.want)
		}
		if got != tc.want {
			t.Errorf("ParseBrokerURL(%q) = %q, want %q", tc.raw, got, tc.want)
		}
	}
}

// TestParseBrokerURLRefusesAnUnreachableBaseURL pins the other side: a value a
// peer could not post its broker routes to is refused instead of being carried
// into the published record or the parent link.
func TestParseBrokerURLRefusesAnUnreachableBaseURL(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
	}{
		{name: "no scheme", raw: "hub.example.com:8787"},
		{name: "a scheme a peer cannot dial", raw: "unix:///run/avenor/hub.sock"},
		{name: "a bind address without a scheme", raw: "127.0.0.1:8787"},
		{name: "no host", raw: "http://"},
		{name: "empty hostname", raw: "http://:8080"},
		{name: "empty port", raw: "http://hub.example.com:"},
		{name: "a host with two ports", raw: "http://1.2.3.4:8080:9090"},
		{name: "an unbracketed IPv6 host", raw: "http://::1"},
		{name: "zero port", raw: "http://hub.example.com:0"},
		{name: "port out of range", raw: "https://hub.example.com:65536"},
		{name: "IPv4 wildcard", raw: "http://0.0.0.0:8787"},
		{name: "IPv6 wildcard", raw: "http://[::]"},
		{name: "a path a route would never reach", raw: "https://hub.example.com/avenor"},
		{name: "credentials", raw: "https://user:secret@hub.example.com"},
		{name: "a query", raw: "https://hub.example.com?generation=x"},
		{name: "a fragment", raw: "https://hub.example.com#broker"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseBrokerURL(tc.raw)
			if err == nil {
				t.Fatalf("ParseBrokerURL(%q) = %q, want an error", tc.raw, got)
			}
			if got != "" {
				t.Fatalf("ParseBrokerURL(%q) = %q beside the error, want empty", tc.raw, got)
			}
			if !strings.Contains(err.Error(), tc.raw) {
				t.Fatalf("error %q does not name the rejected value %q", err, tc.raw)
			}
		})
	}
}
