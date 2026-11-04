package feed

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

// TestOversizedPollReducesTheNextLimit checks the bounded recovery poll.
func TestOversizedPollReducesTheNextLimit(t *testing.T) {
	limits := make(chan int, 4)
	var polls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sessions" {
			fmt.Fprint(w, `{"messaging_protocol":1,"generation":"g"}`)
			return
		}
		var body struct {
			Limit int `json:"limit"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		select {
		case limits <- body.Limit:
		default:
		}
		if polls.Add(1) == 1 {
			fmt.Fprint(w, strings.Repeat("x", (1<<20)+1))
			return
		}
		fmt.Fprint(w, `[]`)
	}))
	defer s.Close()
	cancel, done := startFeed(t, Broker{RunID: "r", Token: "t", BrokerURL: s.URL, LockDir: t.TempDir(), RequestTimeout: time.Second})
	for _, want := range []int{8, 1, 8} {
		select {
		case got := <-limits:
			if got != want {
				cancel()
				awaitExit(t, done)
				t.Fatalf("limit=%d want %d", got, want)
			}
		case <-time.After(4 * time.Second):
			cancel()
			awaitExit(t, done)
			t.Fatal("poll missing")
		}
	}
	cancel()
	awaitExit(t, done)
}

// TestOversizedPollReportsLossAndContinues exercises the real draining broker.
func TestOversizedPollReportsLossAndContinues(t *testing.T) {
	b := broker.New("")
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()
	token, _, err := b.EnsureRun("receiver", "receiver")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = b.EnsureRun("sender", "sender"); err != nil {
		t.Fatal(err)
	}
	for _, msg := range []struct{ id, text string }{{"large1", strings.Repeat("x", 600<<10)}, {"large2", strings.Repeat("y", 600<<10)}, {"small", "survived"}} {
		body, err := json.Marshal(map[string]string{"id": msg.id, "message": msg.text})
		if err != nil {
			t.Fatal(err)
		}
		if err = b.SendTo("sender", "receiver", "agent_message", body, msg.id); err != nil {
			t.Fatal(err)
		}
	}
	out := &lockedBuffer{wrote: make(chan struct{}, 1)}
	logs := &lockedBuffer{}
	cancel, done := startFeed(t, Broker{RunID: "receiver", Token: token, BrokerURL: b.URL(), LockDir: t.TempDir(), Out: out, Log: logs, Limit: 2, RequestTimeout: time.Second})
	select {
	case <-out.wrote:
	case <-time.After(5 * time.Second):
		cancel()
		awaitExit(t, done)
		t.Fatalf("no delivery: log=%q", logs.String())
	}
	cancel()
	awaitExit(t, done)
	if !strings.Contains(out.String(), "survived") || !strings.Contains(logs.String(), "drained batch") || !strings.Contains(logs.String(), "lost") {
		t.Fatalf("output=%q log=%q", out.String(), logs.String())
	}
}
