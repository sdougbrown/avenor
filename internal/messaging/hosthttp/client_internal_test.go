package hosthttp

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// peerFaultRoundTripper holds one request until released, then fails it the way
// a peer that reset the connection does.
type peerFaultRoundTripper struct {
	started chan struct{}
	release chan struct{}
}

func (rt *peerFaultRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	close(rt.started)
	<-rt.release
	return nil, errors.New("read tcp: connection reset by peer")
}

// TestWaitSurfacesPeerErrorRacingClose pins that Close suppresses only its own
// cancellation: a peer fault that arrives while the transport is closed still
// reaches the caller instead of being reported as possibly-pending.
func TestWaitSurfacesPeerErrorRacingClose(t *testing.T) {
	built, err := New(messaging.EndpointDescriptor{
		BaseURL:          "http://127.0.0.1:1",
		CapabilityID:     "cap-id",
		CapabilitySecret: "cap-secret",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	tr := built.(*Transport)
	fault := &peerFaultRoundTripper{started: make(chan struct{}), release: make(chan struct{})}
	tr.client.Transport = fault

	type waitOutcome struct {
		res messaging.WaitResult
		err error
	}
	waited := make(chan waitOutcome, 1)
	go func() {
		res, err := tr.Wait(context.Background(), messaging.WaitRequest{
			Sender: messaging.Identity{RunID: "peer-fault"}, MessageID: "msg", Generation: "gen",
			Timeout: time.Second,
		})
		waited <- waitOutcome{res: res, err: err}
	}()
	<-fault.started
	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	close(fault.release)

	select {
	case out := <-waited:
		if out.err == nil || !strings.Contains(out.err.Error(), "connection reset") {
			t.Fatalf("Wait error = %v, want the peer fault surfaced", out.err)
		}
		if out.res.Outcome != messaging.OutcomePossiblyPending {
			t.Fatalf("Wait outcome = %q, want possibly-pending", out.res.Outcome)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return")
	}
}
