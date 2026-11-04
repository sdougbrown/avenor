package feed

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// flushWriter records the order in which records become visible.
type flushWriter struct {
	mu     sync.Mutex
	buffer bytes.Buffer
	events []string
}

// Write buffers a record until Flush.
func (w *flushWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.events = append(w.events, "write")
	return w.buffer.Write(p)
}

// Flush makes a record visible to the reader.
func (w *flushWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.events = append(w.events, "flush")
	w.buffer.Reset()
	return nil
}

// record observes a poll alongside write and flush events.
func (w *flushWriter) record(event string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.events = append(w.events, event)
}

// snapshot copies the observed event sequence.
func (w *flushWriter) snapshot() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.events...)
}

// TestFlushBeforeNextRowAndPoll verifies feed delivery ordering.
func TestFlushBeforeNextRowAndPoll(t *testing.T) {
	out := &flushWriter{}
	next := make(chan struct{}, 1)
	var polls int
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sessions" {
			fmt.Fprint(w, `{"messaging_protocol":1,"generation":"g"}`)
			return
		}
		out.record("poll")
		polls++
		if polls == 1 {
			fmt.Fprint(w, `[{"payload":{"message":"one"}},{"payload":{"message":"two"}}]`)
		} else {
			select {
			case next <- struct{}{}:
			default:
			}
			fmt.Fprint(w, `[]`)
		}
	}))
	defer s.Close()
	cancel, done := startFeed(t, Broker{RunID: "r", Token: "t", BrokerURL: s.URL, LockDir: t.TempDir(), Out: out, RequestTimeout: time.Second})
	select {
	case <-next:
	case <-time.After(3 * time.Second):
		t.Fatal("second poll missing")
	}
	cancel()
	awaitExit(t, done)
	events := out.snapshot()
	want := []string{"poll", "write", "flush", "write", "flush", "poll"}
	if len(events) < len(want) {
		t.Fatalf("events %v", events)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("events %v", events)
		}
	}
}

// TestParentAlreadyGoneDoesNotContactBroker verifies the first parent check.
func TestParentAlreadyGoneDoesNotContactBroker(t *testing.T) {
	called := make(chan struct{}, 1)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called <- struct{}{} }))
	defer s.Close()
	b := Broker{RunID: "r", Token: "t", BrokerURL: s.URL, LockDir: t.TempDir()}
	if code := b.run(context.Background(), 1, func() int { return 2 }); code != 0 {
		t.Fatalf("exit %d", code)
	}
	select {
	case <-called:
		t.Fatal("broker contacted after parent loss")
	default:
	}
}
