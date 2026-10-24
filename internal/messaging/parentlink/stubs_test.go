package parentlink

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// stubRequest is one request a stub broker received.
type stubRequest struct {
	Path string
	Body map[string]any
}

// stubBroker is one stub broker: it answers every request from a scripted
// responder and records what it received, so a test can assert the shape of
// the requests the link and the relay put on the wire.
type stubBroker struct {
	t      *testing.T
	server *httptest.Server

	mu       sync.Mutex
	requests []stubRequest
	respond  func(stubRequest) (int, string)
}

// newStubBroker starts one stub broker whose responder answers every request.
func newStubBroker(t *testing.T, respond func(stubRequest) (int, string)) *stubBroker {
	t.Helper()
	b := &stubBroker{t: t, respond: respond}
	b.server = httptest.NewServer(http.HandlerFunc(b.serve))
	t.Cleanup(b.server.Close)
	return b
}

// URL reports the address the stub broker answers on.
func (b *stubBroker) URL() string { return b.server.URL }

// setResponder replaces the responder the stub broker answers with.
func (b *stubBroker) setResponder(respond func(stubRequest) (int, string)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.respond = respond
}

// recorded reports every request the stub broker received, in order.
func (b *stubBroker) recorded() []stubRequest {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]stubRequest(nil), b.requests...)
}

// countPath reports how many requests the stub broker received on a path.
func (b *stubBroker) countPath(path string) int {
	count := 0
	for _, req := range b.recorded() {
		if req.Path == path {
			count++
		}
	}
	return count
}

// lastRequest reports the most recent request the stub broker received on a
// path.
func (b *stubBroker) lastRequest(t *testing.T, path string) stubRequest {
	t.Helper()
	requests := b.recorded()
	for i := len(requests) - 1; i >= 0; i-- {
		if requests[i].Path == path {
			return requests[i]
		}
	}
	t.Fatalf("no request was received on %s", path)
	return stubRequest{}
}

func (b *stubBroker) serve(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	body := map[string]any{}
	_ = json.Unmarshal(raw, &body)
	request := stubRequest{Path: r.URL.Path, Body: body}

	b.mu.Lock()
	b.requests = append(b.requests, request)
	respond := b.respond
	b.mu.Unlock()

	status, payload := respond(request)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(payload))
}

// testRuns reports the run set a test link hands its parent.
func testRuns() []messaging.RunClaim {
	return []messaging.RunClaim{{RunID: "run-a", Label: "alpha", Token: "tok-a"}}
}

// newTestLink builds one link pointed at a stub parent, with the run set and
// the generation the stub parent answers with.
func newTestLink(t *testing.T, stub *stubBroker, generation string) *Link {
	t.Helper()
	link, err := NewLink(Options{
		ParentURL:  stub.URL(),
		Endpoint:   "http://child-one",
		ChildID:    "child-one",
		Runs:       testRuns,
		Generation: func() string { return generation },
		RunToken:   func(string) (string, bool) { return "tok-a", true },
	})
	if err != nil {
		t.Fatalf("NewLink: %v", err)
	}
	t.Cleanup(func() { _ = link.Close() })
	return link
}

// askRequest reports one ask the test link forwards to its parent.
func askRequest(messageID string) messaging.SendRequest {
	return messaging.SendRequest{
		From:      messaging.Identity{RunID: "run-a", Sender: "alice"},
		To:        messaging.Identity{RunID: "run-b", Sender: "bob"},
		MessageID: messageID,
		Body:      `{"message":"ping"}`,
	}
}

// waitRequest reports one wait the test link forwards to its parent.
func waitRequest(messageID string) messaging.WaitRequest {
	return messaging.WaitRequest{
		Sender:     messaging.Identity{RunID: "run-a", Sender: "alice"},
		MessageID:  messageID,
		Timeout:    messaging.MaxWait,
	}
}

// withdrawRequest reports one withdrawal the test link forwards to its parent.
func withdrawRequest(messageID string) messaging.WithdrawRequest {
	return messaging.WithdrawRequest{
		Sender:    messaging.Identity{RunID: "run-a", Sender: "alice"},
		MessageID: messageID,
	}
}

// testContext reports the context every test request runs under.
func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return ctx
}
