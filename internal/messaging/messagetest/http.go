package messagetest

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// roundTripperSetter is the capability an HTTP-backed transport exposes so the
// suites can inject a connection-level fault.
type roundTripperSetter interface {
	SetTransport(http.RoundTripper)
}

// refusedDial fails every request the way a refused connection does: no byte
// is ever written.
type refusedDial struct{}

func (refusedDial) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, &net.OpError{Op: "dial", Net: "tcp: connect", Err: errors.New("connection refused")}
}

// peerFault holds one request until released, then fails it the way a peer that
// reset the connection does.
type peerFault struct {
	started chan struct{}
	release chan struct{}
}

func (rt *peerFault) RoundTrip(*http.Request) (*http.Response, error) {
	close(rt.started)
	<-rt.release
	return nil, errors.New("read tcp: connection reset by peer")
}

// RunPeerFaultCloseSuite pins that Close suppresses only its own cancellation:
// a peer fault that arrives while the transport is closed still reaches the
// caller, as the peer's error with an uncertain outcome.
func RunPeerFaultCloseSuite(t *testing.T, newTransport TransportFactory) {
	t.Helper()

	tr := newTransport(t, messaging.Identity{RunID: "fault-alice", Sender: "alice"})
	setter, ok := tr.(roundTripperSetter)
	if !ok {
		t.Skip("transport has no pluggable round tripper")
	}
	fault := &peerFault{started: make(chan struct{}), release: make(chan struct{})}
	setter.SetTransport(fault)

	type waitOutcome struct {
		res messaging.WaitResult
		err error
	}
	waited := make(chan waitOutcome, 1)
	go func() {
		res, err := tr.Wait(context.Background(), messaging.WaitRequest{
			Sender: messaging.Identity{RunID: "fault-alice"}, MessageID: "msg", Generation: "gen",
			Timeout: time.Second,
		})
		waited <- waitOutcome{res: res, err: err}
	}()
	select {
	case <-fault.started:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait never reached the peer")
	}
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
