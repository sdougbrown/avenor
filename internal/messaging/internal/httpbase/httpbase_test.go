package httpbase

import (
	"errors"
	"net/http"
	"testing"
	"time"
)

// TestUseHTTPClientKeepsDedicatedTransport pins that a caller client without a
// transport keeps the dedicated transport New installs, while a caller that
// supplies one gets it.
func TestUseHTTPClientKeepsDedicatedTransport(t *testing.T) {
	malformed := errors.New("malformed response")
	c := New(malformed, time.Second)
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
