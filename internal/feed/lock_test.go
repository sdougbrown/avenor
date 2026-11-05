package feed

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSeparateRunLocksAllowBothFeeds checks independent run IDs on one broker.
func TestSeparateRunLocksAllowBothFeeds(t *testing.T) {
	polled := make(chan string, 20)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sessions" {
			fmt.Fprint(w, `{"messaging_protocol":1,"generation":"g"}`)
			return
		}
		var body struct {
			RunID string `json:"run_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		select {
		case polled <- body.RunID:
		default:
		}
		fmt.Fprint(w, `[]`)
	}))
	defer s.Close()
	b := Broker{RunID: "first", Token: "t", BrokerURL: s.URL, LockDir: t.TempDir(), RequestTimeout: time.Second}
	cancel1, done1 := startFeed(t, b)
	b.RunID = "second"
	cancel2, done2 := startFeed(t, b)
	seen := map[string]bool{}
	deadline := time.After(3 * time.Second)
	for !seen["first"] || !seen["second"] {
		select {
		case id := <-polled:
			seen[id] = true
		case <-deadline:
			cancel1()
			cancel2()
			awaitExit(t, done1)
			awaitExit(t, done2)
			t.Fatalf("polled: %v", seen)
		}
	}
	cancel1()
	cancel2()
	awaitExit(t, done1)
	awaitExit(t, done2)
}

// TestCompetingFeedDoesNotContactBroker verifies the lock covers preflight.
func TestCompetingFeedDoesNotContactBroker(t *testing.T) {
	var requests atomic.Int32
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path == "/sessions" {
			fmt.Fprint(w, `{"messaging_protocol":1,"generation":"g"}`)
			return
		}
		once.Do(func() { close(entered) })
		<-release
		fmt.Fprint(w, `[]`)
	}))
	defer s.Close()
	defer close(release)
	logs := &lockedBuffer{}
	b := Broker{RunID: "r", Token: "t", BrokerURL: s.URL, LockDir: t.TempDir(), RequestTimeout: 2 * time.Second, Log: logs}
	cancel, done := startFeed(t, b)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("poll not started")
	}
	b.BrokerURL += "/"
	ctx, stop := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer stop()
	if code := b.Run(ctx); code != 0 || ctx.Err() != nil {
		t.Fatalf("second feed exit %d, context %v", code, ctx.Err())
	}
	if logs.String() == "" {
		t.Fatal("held lock was not logged")
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("competing feed contacted broker: %d requests", got)
	}
	cancel()
	awaitExit(t, done)
}
