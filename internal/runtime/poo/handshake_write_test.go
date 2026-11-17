package poo

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
)

// TestHandshakeWriteToClosedStdinReportsRefusal pins how a handshake write
// failure is classified. exec's Wait closes the parent's stdin pipe once the
// child is reaped, so a write racing that close fails with ErrClosed where an
// earlier write got EPIPE; both mean the child is gone, and the stderr it
// printed before exiting still names the manifest refusal.
func TestHandshakeWriteToClosedStdinReportsRefusal(t *testing.T) {
	opts, _ := fallbackFixture(t, "")
	script := "#!/bin/sh\necho 'feeds.paths: invalid' >&2\nsleep 0.3\nexit 1\n"
	if err := os.WriteFile(opts.Launch.Command, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	p := NewWithOptions(opts)
	t.Cleanup(func() { _ = p.Close() })
	ctx := context.Background()
	c, err := p.ensureClient(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	// Force the close the exec Wait race produces, while the child is still
	// alive so stdout has not reported EOF yet.
	if err := c.stdin.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = p.openProcess(ctx, opts)
	var failure *messagingFailure
	if !errors.As(err, &failure) || !strings.Contains(failure.reason, "feeds.paths") {
		t.Fatalf("openProcess error = %v, want a messaging failure naming feeds.paths", err)
	}
}
