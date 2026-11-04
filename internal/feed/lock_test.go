package feed

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// TestCompetingFeedDoesNotContactBroker verifies the lock covers preflight.
func TestCompetingFeedDoesNotContactBroker(t *testing.T) {
	var requests atomic.Int32
	entered := make(chan struct{})
	release := make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path == "/sessions" {
			fmt.Fprint(w, `{"messaging_protocol":1,"generation":"g"}`)
			return
		}
		close(entered)
		<-release
		fmt.Fprint(w, `[]`)
	}))
	defer s.Close()
	b := Broker{RunID: "r", Token: "t", BrokerURL: s.URL, LockDir: t.TempDir(), RequestTimeout: 2 * time.Second}
	cancel, done := startFeed(t, b)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("poll not started")
	}
	b.BrokerURL += "/"
	if code := b.Run(context.Background()); code != 0 {
		t.Fatalf("second feed exit %d", code)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("competing feed contacted broker: %d requests", got)
	}
	cancel()
	close(release)
	awaitExit(t, done)
}
