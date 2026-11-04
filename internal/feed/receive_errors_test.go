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

// TestReceiveErrorRecovery checks poll failures after a successful preflight.
func TestReceiveErrorRecovery(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   int
		terminal bool
		body     string
		refused  bool
	}{
		{"unauthorized", 401, true, "", false}, {"forbidden", 403, true, "", false}, {"not found", 404, true, "", false}, {"gone", 410, true, "", false},
		{"conflict", 409, false, "", false}, {"rate limit", 429, false, "", false}, {"internal error", 500, false, "", false}, {"unavailable", 503, false, "", false},
		{"connection refused", 0, false, "", true}, {"bad poll body", 200, true, "{bad", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var polls atomic.Int32
			hit := make(chan struct{}, 8)
			var s *httptest.Server
			s = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/sessions" {
					if tc.refused {
						w.Header().Set("Connection", "close")
						_ = s.Listener.Close()
					}
					fmt.Fprint(w, `{"messaging_protocol":1,"generation":"g"}`)
					return
				}
				polls.Add(1)
				select {
				case hit <- struct{}{}:
				default:
				}
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			}))
			defer s.Close()
			logs := &lockedBuffer{wrote: make(chan struct{}, 8)}
			cancel, done := startFeed(t, Broker{RunID: "r", Token: "t", BrokerURL: s.URL, LockDir: t.TempDir(), Log: logs, RequestTimeout: time.Second})
			defer cancel()
			if tc.refused {
				for i := 0; i < 2; i++ {
					select {
					case <-logs.wrote:
					case <-time.After(4 * time.Second):
						cancel()
						awaitExit(t, done)
						t.Fatalf("no retry: %q", logs.String())
					}
				}
			} else {
				select {
				case <-hit:
				case <-time.After(3 * time.Second):
					cancel()
					awaitExit(t, done)
					t.Fatal("no poll")
				}
				if tc.terminal {
					select {
					case <-logs.wrote:
					case <-time.After(time.Second):
						cancel()
						awaitExit(t, done)
						t.Fatal("no terminal log")
					}
					select {
					case <-hit:
						t.Fatal("terminal poll retried")
					case <-time.After(250 * time.Millisecond):
					}
				} else {
					select {
					case <-hit:
					case <-time.After(4 * time.Second):
						cancel()
						awaitExit(t, done)
						t.Fatalf("no retry: %q", logs.String())
					}
				}
			}
			cancel()
			awaitExit(t, done)
			if tc.terminal && (!strings.Contains(logs.String(), "terminal") || polls.Load() != 1) {
				t.Fatalf("polls=%d log=%q", polls.Load(), logs.String())
			}
			if !tc.terminal && !strings.Contains(logs.String(), "temporary") {
				t.Fatalf("log=%q", logs.String())
			}
		})
	}
}
