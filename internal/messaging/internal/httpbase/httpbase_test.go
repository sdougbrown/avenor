package httpbase

import (
	"errors"
	"net/http"
	"testing"
	"time"
)

// defaultTimeout is what New was called with, so a case can pin that an
// adoption left it alone.
const defaultTimeout = time.Second

// TestUseHTTPClientKeepsDedicatedTransport pins that a caller client without a
// transport keeps the dedicated transport New installs, while a caller that
// supplies one gets it.
func TestUseHTTPClientKeepsDedicatedTransport(t *testing.T) {
	malformed := errors.New("malformed response")
	c := New(malformed, defaultTimeout)
	dedicated := c.http.Transport
	if dedicated == nil || dedicated == http.DefaultTransport {
		t.Fatalf("New transport = %v, want a dedicated transport", dedicated)
	}

	c.UseHTTPClient(&http.Client{Timeout: 2 * time.Second})
	if c.http.Transport != dedicated {
		t.Fatalf("transport = %v, want the dedicated transport kept", c.http.Transport)
	}
	if c.timeout != 2*time.Second {
		t.Fatalf("timeout = %v, want the caller's 2s", c.timeout)
	}

	own := &http.Transport{}
	c.UseHTTPClient(&http.Client{Transport: own})
	if c.http.Transport != own {
		t.Fatalf("transport = %v, want the caller's transport", c.http.Transport)
	}
}

// TestUseHTTPClientNilKeepsDefaults pins that a surface leaving
// Options.HTTPClient unset keeps the dedicated transport and the default
// timeout New installs instead of dereferencing a nil client.
func TestUseHTTPClientNilKeepsDefaults(t *testing.T) {
	c := New(errors.New("malformed response"), defaultTimeout)
	dedicated := c.http.Transport

	c.UseHTTPClient(nil)

	if c.http.Transport != dedicated {
		t.Fatalf("transport = %v, want the dedicated transport kept", c.http.Transport)
	}
	if c.timeout != defaultTimeout {
		t.Fatalf("timeout = %v, want the default %v", c.timeout, defaultTimeout)
	}
}
