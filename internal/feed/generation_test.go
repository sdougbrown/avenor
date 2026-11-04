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

// TestGenerationMismatchRebuildsProvider checks delivery after a generation failure.
func TestGenerationMismatchRebuildsProvider(t *testing.T) {
	var preflights atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sessions" {
			if preflights.Add(1) == 1 {
				fmt.Fprint(w, `{"messaging_protocol":1,"generation":""}`)
			} else {
				fmt.Fprint(w, `{"messaging_protocol":1,"generation":"replacement"}`)
			}
			return
		}
		fmt.Fprint(w, `[{"id":"later","payload":{"message":"delivered"}}]`)
	}))
	defer s.Close()
	output := &lockedBuffer{wrote: make(chan struct{}, 1)}
	logs := &lockedBuffer{}
	cancel, done := startFeed(t, Broker{RunID: "r", Token: "t", BrokerURL: s.URL, LockDir: t.TempDir(), Out: output, Log: logs, RequestTimeout: time.Second})
	defer cancel()
	select {
	case <-output.wrote:
	case <-time.After(4 * time.Second):
		cancel()
		awaitExit(t, done)
		t.Fatalf("no delivery after generation mismatch; preflights=%d log=%q", preflights.Load(), logs.String())
	}
	cancel()
	awaitExit(t, done)
	if preflights.Load() < 2 || !strings.Contains(output.String(), "delivered") || !strings.Contains(logs.String(), "temporary broker error") {
		t.Fatalf("preflights=%d output=%q log=%q", preflights.Load(), output.String(), logs.String())
	}
}
