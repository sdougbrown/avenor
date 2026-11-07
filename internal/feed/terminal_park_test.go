package feed

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestTerminalFeedParksWithStableDiagnostic pins the contract a host-side
// supervisor relies on: after an authoritative terminal refusal the feed
// neither polls again nor exits — it parks until shutdown or parent loss, and
// its diagnostic line begins with the stable "feed broker: terminal" prefix a
// host can match without parsing arbitrary text. Parking is the feed's
// no-restart-storm contract; a host that wants the slot back stops the parked
// process itself and rebinds.
func TestTerminalFeedParksWithStableDiagnostic(t *testing.T) {
	var polls atomic.Int64
	var s *httptest.Server
	s = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sessions" {
			fmt.Fprint(w, `{"messaging_protocol":1,"generation":"g"}`)
			return
		}
		polls.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer s.Close()
	logs := &lockedBuffer{wrote: make(chan struct{}, 8)}
	cancel, done := startFeed(t, Broker{RunID: "r", Token: "t", BrokerURL: s.URL, LockDir: t.TempDir(), Log: logs, RequestTimeout: time.Second})
	defer cancel()

	deadline := time.Now().Add(4 * time.Second)
	for !strings.Contains(logs.String(), "terminal") && time.Now().Before(deadline) {
		select {
		case <-logs.wrote:
		case <-time.After(200 * time.Millisecond):
		}
	}
	if !strings.Contains(logs.String(), "feed broker: terminal") {
		t.Fatalf("diagnostic lacks the stable terminal prefix: %q", logs.String())
	}

	// The feed parks: it neither exits nor polls again while ctx stays open.
	select {
	case code := <-done:
		t.Fatalf("terminal feed exited with %d instead of parking", code)
	case <-time.After(600 * time.Millisecond):
	}
	if polls.Load() != 1 {
		t.Fatalf("polls=%d, want exactly the one refused poll", polls.Load())
	}
	cancel()
	awaitExit(t, done)
}
