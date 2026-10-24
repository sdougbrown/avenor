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
	if status < 0 {
		// A negative status hangs up on the peer without an answer, so the
		// caller sees a transmission that failed after it left.
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
		return
	}
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

// sendBody builds the /send request body an ask posts over a broker's HTTP
// surface: the run credential the broker authenticates with, the ask's route,
// and the pin the broker translates into the generation of the hop it admits.
func sendBody(fromRunID, token, toRunID, msgID, generation string) map[string]any {
	return map[string]any{
		"run_id":        fromRunID,
		"token":         token,
		"from_run_id":   fromRunID,
		"to_run_id":     toRunID,
		"type":          "agent_message",
		"payload":       map[string]any{"message": "ping"},
		"message_id":    msgID,
		"generation":    generation,
		"expects_reply": true,
	}
}

// waitRequest reports one wait the test link forwards to its parent.
func waitRequest(messageID string) messaging.WaitRequest {
	return messaging.WaitRequest{
		Sender:    messaging.Identity{RunID: "run-a", Sender: "alice"},
		MessageID: messageID,
		Timeout:   messaging.MaxWait,
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

// stubMessenger is one local messaging seam: it records the calls the
// parent-aware messenger makes and answers them from scripted verdicts.
type stubMessenger struct {
	mu       sync.Mutex
	sends    []messaging.SendRequest
	waits    []messaging.WaitRequest
	withdraw []messaging.WithdrawRequest
	replies  []messaging.ReplyRequest
	delivers []messaging.DeliverRequest

	reachable map[string]bool
	peers     []messaging.Peer
	peersErr  error

	sendResult     messaging.SendResult
	sendErr        error
	withdrawResult messaging.WithdrawResult
	withdrawErr    error
}

func newStubMessenger() *stubMessenger {
	return &stubMessenger{reachable: map[string]bool{}}
}

func (s *stubMessenger) Send(_ context.Context, req messaging.SendRequest) (messaging.SendResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sends = append(s.sends, req)
	return s.sendResult, s.sendErr
}

func (s *stubMessenger) Wait(_ context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.waits = append(s.waits, req)
	return messaging.WaitResult{Outcome: messaging.OutcomeEdgeAbsent}, nil
}

func (s *stubMessenger) Withdraw(_ context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.withdraw = append(s.withdraw, req)
	return s.withdrawResult, s.withdrawErr
}

func (s *stubMessenger) Reply(_ context.Context, req messaging.ReplyRequest) (messaging.Outcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replies = append(s.replies, req)
	return messaging.OutcomeEdgeAbsent, nil
}

func (s *stubMessenger) Deliver(_ context.Context, req messaging.DeliverRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delivers = append(s.delivers, req)
	return nil
}

func (s *stubMessenger) Reachable(runID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reachable[runID]
}

func (s *stubMessenger) Peers(_ context.Context) ([]messaging.Peer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peers, s.peersErr
}

// sentRuns reports the runs the local seam was asked to send to.
func (s *stubMessenger) sentRuns() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	targets := make([]string, 0, len(s.sends))
	for _, send := range s.sends {
		targets = append(targets, send.To.RunID)
	}
	return targets
}

// withdrawnMessages reports the message ids the local seam was asked to
// withdraw.
func (s *stubMessenger) withdrawnMessages() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.withdraw))
	for _, withdrawal := range s.withdraw {
		ids = append(ids, withdrawal.MessageID)
	}
	return ids
}
