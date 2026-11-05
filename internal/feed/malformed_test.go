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
)

// TestMalformedPayloadDoesNotStopDelivery checks both invalid payload forms.
func TestMalformedPayloadDoesNotStopDelivery(t *testing.T) {
	for _, tc := range []struct{ name, payload string }{{"wrong type", `"bad payload"`}, {"empty message", `{"message":""}`}} {
		t.Run(tc.name, func(t *testing.T) {
			var polls atomic.Int32
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/sessions" {
					fmt.Fprint(w, `{"messaging_protocol":1,"generation":"g"}`)
					return
				}
				switch polls.Add(1) {
				case 1:
					fmt.Fprintf(w, `[{"id":"first","payload":{"message":"first"}},{"id":"bad","payload":%s},{"id":"second","payload":{"message":"second"}}]`, tc.payload)
				case 2:
					fmt.Fprint(w, `[{"id":"later","payload":{"message":"later"}}]`)
				default:
					fmt.Fprint(w, `[]`)
				}
			}))
			defer s.Close()
			output := &lockedBuffer{wrote: make(chan struct{}, 3)}
			logs := &lockedBuffer{}
			cancel, done := startFeed(t, Broker{RunID: "r", Token: "t", BrokerURL: s.URL, LockDir: t.TempDir(), Out: output, Log: logs, RequestTimeout: time.Second})
			defer cancel()
			for i := 0; i < 3; i++ {
				select {
				case <-output.wrote:
				case <-time.After(time.Second):
					cancel()
					awaitExit(t, done)
					t.Fatalf("missing output: %q; log: %q", output.String(), logs.String())
				}
			}
			cancel()
			awaitExit(t, done)
			lines := strings.Split(strings.TrimSpace(output.String()), "\n")
			if len(lines) != 3 {
				t.Fatalf("output: %q", output.String())
			}
			for i, want := range []string{"first", "second", "later"} {
				var line struct{ Text, ID string }
				if err := json.Unmarshal([]byte(lines[i]), &line); err != nil || line.Text != want || line.ID != want {
					t.Fatalf("line %d: %q, error %v", i, lines[i], err)
				}
			}
			if got := logs.String(); !strings.Contains(got, `"bad"`) || strings.Contains(got, "bad payload") || strings.Contains(got, "first") || strings.Contains(got, "second") || strings.Contains(got, "later") {
				t.Fatalf("log: %q", got)
			}
		})
	}
}
