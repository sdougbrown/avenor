package hosthttp_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/hostfixture"
	"github.com/sdougbrown/avenor/internal/messaging/hosthttp"
	"github.com/sdougbrown/avenor/internal/messaging/messagetest"
)

var alice = messaging.Identity{RunID: "host-alice", Sender: "alice"}
var bob = messaging.Identity{RunID: "host-bob", Sender: "bob"}

func newTransport(t *testing.T, fx *hostfixture.Fixture, id messaging.Identity) messaging.Transport {
	t.Helper()
	tr, err := hosthttp.New(fx.Descriptor(id))
	if err != nil {
		t.Fatalf("hosthttp.New: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	return tr
}

func fixtureFactory(fx *hostfixture.Fixture) messagetest.TransportFactory {
	return func(t *testing.T, id messaging.Identity) messaging.Transport {
		return newTransport(t, fx, id)
	}
}

func mintID(t *testing.T, ttl time.Duration) string {
	t.Helper()
	now := time.Now().UTC()
	id, err := messaging.NewMsgID(now, now.Add(ttl))
	if err != nil {
		t.Fatalf("NewMsgID: %v", err)
	}
	return id
}

func TestHostClientContract(t *testing.T) {
	messagetest.RunTransportSuite(t, fixtureFactory(hostfixture.New(t)))
}

func TestHostClientFailureMatrix(t *testing.T) {
	messagetest.RunFailureMatrix(t, fixtureFactory(hostfixture.New(t)))
}

// TestHostFixturePausesSendAdmission pauses the fixture's send admission for
// one key, withdraws that key, then resumes the send. The resumed send must not
// deliver, must leave no edge, and the fence must stay terminal.
func TestHostFixturePausesSendAdmission(t *testing.T) {
	fx := hostfixture.New(t)
	sender := newTransport(t, fx, alice)
	gen := fx.Store().Generation()
	msgID := mintID(t, time.Minute)

	fx.PauseSend(alice, msgID, gen)

	type sendOutcome struct {
		res messaging.SendResult
		err error
	}
	sent := make(chan sendOutcome, 1)
	go func() {
		res, err := sender.Send(context.Background(), messaging.SendRequest{
			From: alice, To: bob, MessageID: msgID, Generation: gen, Body: `{"message":"paused ask"}`,
		})
		sent <- sendOutcome{res: res, err: err}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for !fx.Reached(alice, msgID, gen) {
		if time.Now().After(deadline) {
			t.Fatal("send never reached the fixture's admission seam")
		}
		time.Sleep(time.Millisecond)
	}

	withdrawn, err := sender.Withdraw(context.Background(), messaging.WithdrawRequest{
		Sender: alice, MessageID: msgID, Generation: gen,
	})
	if err != nil {
		t.Fatalf("Withdraw: %v", err)
	}
	if !withdrawn.Fence.Terminal {
		t.Fatalf("withdrawal did not install a terminal fence: %+v", withdrawn)
	}

	fx.ResumeSend(alice, msgID, gen)

	select {
	case out := <-sent:
		if out.err != nil {
			t.Fatalf("resumed send error: %v", out.err)
		}
		if out.res.Outcome != messaging.OutcomeWithdrawn {
			t.Fatalf("resumed send outcome = %q, want withdrawn", out.res.Outcome)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("resumed send did not return")
	}

	if queued := fx.Queued(bob.RunID); queued != 0 {
		t.Fatalf("withdrawn ask was delivered %d time(s)", queued)
	}
	store := fx.Store()
	if store.Pending(alice.RunID, msgID) {
		t.Fatal("withdrawn ask left a pending edge")
	}
	ack, ok := store.Fence(messaging.FenceKey{Sender: alice.RunID, MessageID: msgID, Generation: gen})
	if !ok || !ack.Terminal {
		t.Fatalf("fence = (%+v, %v), want terminal", ack, ok)
	}
}

// TestHostClientAdmissionRejections pins the shared admission-rejection table on
// the host wire: a rejected credential, a replaced epoch, a duplicate ID and a
// withdrawal with no edge.
func TestHostClientAdmissionRejections(t *testing.T) {
	t.Run("credential rejected", func(t *testing.T) {
		fx := hostfixture.New(t)
		desc := fx.Descriptor(alice)
		desc.CapabilitySecret = "wrong-secret"
		tr, err := hosthttp.New(desc)
		if err != nil {
			t.Fatalf("hosthttp.New: %v", err)
		}
		t.Cleanup(func() { _ = tr.Close() })

		res, err := tr.Send(t.Context(), messaging.SendRequest{
			From: alice, To: bob, MessageID: mintID(t, time.Minute), Generation: desc.ExpectedEpoch, Body: `{}`,
		})
		if !errors.Is(err, messaging.ErrAuthRejected) || res.Outcome != messaging.OutcomeAuthRejected {
			t.Fatalf("Send = (%+v, %v)", res, err)
		}
		if fx.Store().ActiveCount() != 0 {
			t.Fatal("rejected send left an edge")
		}
	})

	t.Run("replaced epoch", func(t *testing.T) {
		fx := hostfixture.New(t)
		tr := newTransport(t, fx, alice)
		gen := fx.Store().Generation()
		fx.Store().Reset()

		res, err := tr.Send(t.Context(), messaging.SendRequest{
			From: alice, To: bob, MessageID: mintID(t, time.Minute), Generation: gen, Body: `{}`,
		})
		if !errors.Is(err, messaging.ErrGenerationMismatch) || res.Outcome != messaging.OutcomeNotSent {
			t.Fatalf("Send = (%+v, %v)", res, err)
		}
	})

	t.Run("duplicate id", func(t *testing.T) {
		fx := hostfixture.New(t)
		tr := newTransport(t, fx, alice)
		gen := fx.Store().Generation()
		msgID := mintID(t, time.Minute)
		if _, err := tr.Send(t.Context(), messaging.SendRequest{
			From: alice, To: bob, MessageID: msgID, Generation: gen, Body: `{}`,
		}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		res, err := tr.Send(t.Context(), messaging.SendRequest{
			From: alice, To: bob, MessageID: msgID, Generation: gen, Body: `{}`,
		})
		if !errors.Is(err, messaging.ErrDuplicateMessageID) || res.Outcome != messaging.OutcomeNotSent {
			t.Fatalf("duplicate Send = (%+v, %v)", res, err)
		}
	})

	t.Run("withdrawal without an edge", func(t *testing.T) {
		fx := hostfixture.New(t)
		tr := newTransport(t, fx, alice)
		withdrawn, err := tr.Withdraw(t.Context(), messaging.WithdrawRequest{
			Sender: alice, MessageID: mintID(t, time.Minute), Generation: fx.Store().Generation(),
		})
		if err != nil {
			t.Fatalf("Withdraw: %v", err)
		}
		if withdrawn.Outcome != messaging.OutcomeEdgeAbsent || !withdrawn.Fence.Terminal {
			t.Fatalf("Withdraw = %+v", withdrawn)
		}
	})
}

func stubServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv
}

func plainDescriptor(baseURL string) messaging.EndpointDescriptor {
	return messaging.EndpointDescriptor{
		BaseURL:          baseURL,
		CapabilityID:     "cap-id",
		CapabilitySecret: "cap-secret",
		Timeout:          2 * time.Second,
	}
}

// TestHostClientMalformedResponses pins that an untrustworthy send response is
// never classified as never-sent, and that a wait without an outcome retains
// the message ID.
func TestHostClientMalformedResponses(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
	}{
		{"empty body", http.StatusInternalServerError, ""},
		{"not json", http.StatusOK, "not-json"},
		{"no outcome field", http.StatusOK, `{"message_id":"av1.0.0"}`},
		{"server error without outcome", http.StatusInternalServerError, `{"error":"internal"}`},
		{"unknown endpoint", http.StatusNotFound, `{"error":"not-found"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			})
			tr, err := hosthttp.New(plainDescriptor(srv.URL))
			if err != nil {
				t.Fatalf("hosthttp.New: %v", err)
			}
			t.Cleanup(func() { _ = tr.Close() })

			msgID := mintID(t, time.Minute)
			res, err := tr.Send(t.Context(), messaging.SendRequest{
				From: alice, To: bob, MessageID: msgID, Generation: "gen", Body: `{}`,
			})
			if res.Outcome != messaging.OutcomePossiblyPending {
				t.Fatalf("Send outcome = %q, want possibly-pending", res.Outcome)
			}
			if res.MessageID != msgID {
				t.Fatalf("Send dropped the message id: %q", res.MessageID)
			}
			if err == nil {
				t.Fatal("Send reported no error for an untrustworthy response")
			}

			waited, err := tr.Wait(t.Context(), messaging.WaitRequest{
				Sender: alice, MessageID: msgID, Generation: "gen", Timeout: 20 * time.Millisecond,
			})
			if waited.Outcome != messaging.OutcomePossiblyPending || err == nil {
				t.Fatalf("Wait = (%+v, %v), want possibly-pending and an error", waited, err)
			}
		})
	}

	t.Run("unparseable body reports the malformed class", func(t *testing.T) {
		srv := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("not-json"))
		})
		tr, err := hosthttp.New(plainDescriptor(srv.URL))
		if err != nil {
			t.Fatalf("hosthttp.New: %v", err)
		}
		t.Cleanup(func() { _ = tr.Close() })
		if _, err := tr.Send(t.Context(), messaging.SendRequest{
			From: alice, To: bob, MessageID: mintID(t, time.Minute), Generation: "gen",
		}); !errors.Is(err, hosthttp.ErrMalformedResponse) {
			t.Fatalf("Send error = %v, want ErrMalformedResponse", err)
		}
	})
}

func TestHostClientTimeoutIsUncertain(t *testing.T) {
	srv := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		_, _ = w.Write([]byte(`{"outcome":"sent"}`))
	})
	desc := plainDescriptor(srv.URL)
	desc.Timeout = 50 * time.Millisecond
	tr, err := hosthttp.New(desc)
	if err != nil {
		t.Fatalf("hosthttp.New: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close() })

	msgID := mintID(t, time.Minute)
	res, err := tr.Send(t.Context(), messaging.SendRequest{
		From: alice, To: bob, MessageID: msgID, Generation: "gen", Body: `{}`,
	})
	if res.Outcome != messaging.OutcomePossiblyPending || res.MessageID != msgID {
		t.Fatalf("Send = %+v, want possibly-pending with the id retained", res)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Send error = %v, want a deadline", err)
	}
}

func TestHostPreflightRejectsOldProtocol(t *testing.T) {
	srv := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"protocol":0,"epoch":"old"}`))
	})
	tr, err := hosthttp.New(plainDescriptor(srv.URL))
	if err != nil {
		t.Fatalf("hosthttp.New: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	if _, err := tr.Preflight(t.Context()); !errors.Is(err, messaging.ErrUnsupportedProtocol) {
		t.Fatalf("Preflight error = %v, want ErrUnsupportedProtocol", err)
	}
}

// TestHostClientRejectsUnboundEndpoints pins that the fixture contract never
// sends capability material to a cleartext non-loopback endpoint.
func TestHostClientRejectsUnboundEndpoints(t *testing.T) {
	if _, err := hosthttp.New(messaging.EndpointDescriptor{
		BaseURL: "http://example.com", CapabilityID: "cap-id", CapabilitySecret: "cap-secret",
	}); !errors.Is(err, messaging.ErrInvalidRequest) {
		t.Fatalf("cleartext non-loopback error = %v, want ErrInvalidRequest", err)
	}
	if _, err := hosthttp.New(messaging.EndpointDescriptor{
		BaseURL: "https://example.com", CapabilityID: "cap-id", CapabilitySecret: "cap-secret",
	}); err != nil {
		t.Fatalf("https endpoint rejected: %v", err)
	}
	if _, err := hosthttp.New(messaging.EndpointDescriptor{
		BaseURL: "https://example.com",
	}); !errors.Is(err, messaging.ErrAuthRejected) {
		t.Fatalf("missing capability error = %v, want ErrAuthRejected", err)
	}
}

func TestHostClientCloseUnblocksWait(t *testing.T) {
	fx := hostfixture.New(t)
	tr := newTransport(t, fx, alice)
	gen := fx.Store().Generation()
	msgID := mintID(t, time.Minute)
	if _, err := tr.Send(t.Context(), messaging.SendRequest{
		From: alice, To: bob, MessageID: msgID, Generation: gen, Body: `{}`,
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	type waitOutcome struct {
		res messaging.WaitResult
		err error
	}
	waited := make(chan waitOutcome, 1)
	go func() {
		res, err := tr.Wait(context.Background(), messaging.WaitRequest{
			Sender: alice, MessageID: msgID, Generation: gen, Timeout: time.Minute,
		})
		waited <- waitOutcome{res: res, err: err}
	}()
	time.Sleep(20 * time.Millisecond)

	closed := make(chan error, 1)
	go func() { closed <- tr.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(messaging.CleanupDeadline + time.Second):
		t.Fatal("Close exceeded the cleanup deadline")
	}
	select {
	case out := <-waited:
		if out.err != nil {
			t.Fatalf("Wait: %v", out.err)
		}
		if out.res.Outcome != messaging.OutcomePossiblyPending {
			t.Fatalf("Wait outcome = %q, want possibly-pending", out.res.Outcome)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not unblock on Close")
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	// Closing a client never stops the service it was talking to.
	other := newTransport(t, fx, bob)
	if _, err := other.Preflight(t.Context()); err != nil {
		t.Fatalf("fixture stopped serving after Close: %v", err)
	}
}

// TestHostWireFixture pins the declared v1 wire document against the endpoints
// the fixture actually serves and the shared outcome vocabulary.
func TestHostWireFixture(t *testing.T) {
	raw, err := os.ReadFile("../testdata/host_wire.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var doc struct {
		Protocol  int `json:"protocol"`
		Endpoints []struct {
			Path     string         `json:"path"`
			Request  map[string]any `json:"request"`
			Response map[string]any `json:"response"`
		} `json:"endpoints"`
		Errors []struct {
			Status  int               `json:"status"`
			Error   string            `json:"error"`
			Outcome messaging.Outcome `json:"outcome"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	if doc.Protocol != messaging.ProtocolVersion {
		t.Fatalf("fixture protocol = %d, want %d", doc.Protocol, messaging.ProtocolVersion)
	}

	declared := make([]string, 0, len(doc.Endpoints))
	for _, ep := range doc.Endpoints {
		declared = append(declared, ep.Path)
		if ep.Request == nil || ep.Response == nil {
			t.Fatalf("endpoint %q has no declared request/response", ep.Path)
		}
		if raw, ok := ep.Response["outcome"]; ok {
			if !validOutcome(t, raw) {
				t.Fatalf("endpoint %q declares an unknown outcome %v", ep.Path, raw)
			}
		}
	}
	served := hostfixture.New(t).Paths()
	slices.Sort(declared)
	slices.Sort(served)
	if !slices.Equal(declared, served) {
		t.Fatalf("declared endpoints %v, served %v", declared, served)
	}

	for _, entry := range doc.Errors {
		if entry.Error == "" || !validOutcome(t, string(entry.Outcome)) {
			t.Fatalf("error entry = %+v", entry)
		}
	}
}

func validOutcome(t *testing.T, raw any) bool {
	t.Helper()
	value, ok := raw.(string)
	if !ok {
		return false
	}
	switch messaging.Outcome(value) {
	case messaging.OutcomeSent, messaging.OutcomeReplied, messaging.OutcomeTimedOut,
		messaging.OutcomeWithdrawn, messaging.OutcomeEdgeAbsent, messaging.OutcomeAuthRejected,
		messaging.OutcomeNotSent, messaging.OutcomePossiblyPending, messaging.OutcomeLost:
		return true
	}
	return false
}
