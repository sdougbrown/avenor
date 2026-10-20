package hosthttp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/askstore"
	"github.com/sdougbrown/avenor/internal/messaging/hosthttp"
	"github.com/sdougbrown/avenor/internal/messaging/internal/hostfixture"
	"github.com/sdougbrown/avenor/internal/messaging/internal/httpbase"
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

func TestHostClientPeerFaultRacingClose(t *testing.T) {
	messagetest.RunPeerFaultCloseSuite(t, fixtureFactory(hostfixture.New(t)))
}

func TestHostReplyRouting(t *testing.T) {
	fx := hostfixture.New(t)
	messagetest.RunReplyRoutingSuite(t, fixtureFactory(fx), messagetest.ReplyRoutingHooks{
		AdmitLegacy: func(t *testing.T, sender messaging.Identity, messageID, target string) {
			if _, err := fx.Store().Admit(t.Context(), askstore.AdmitRequest{
				Sender: sender, Target: target, MessageID: messageID, Legacy: true,
			}); err != nil {
				t.Fatalf("legacy Admit: %v", err)
			}
		},
		Rotate: func(t *testing.T) { fx.Store().Reset() },
	})
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

}

// TestHostSendDerivesOutcomeFromCode pins that a peer-supplied outcome never
// overrides the outcome the wire contract assigns to the error code.
func TestHostSendDerivesOutcomeFromCode(t *testing.T) {
	srv := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"duplicate-message-id","outcome":"sent"}`))
	})
	tr, err := hosthttp.New(plainDescriptor(srv.URL))
	if err != nil {
		t.Fatalf("hosthttp.New: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close() })

	res, err := tr.Send(t.Context(), messaging.SendRequest{
		From: alice, To: bob, MessageID: mintID(t, time.Minute), Generation: "gen", Body: `{}`,
	})
	if !errors.Is(err, messaging.ErrDuplicateMessageID) || res.Outcome != messaging.OutcomeNotSent {
		t.Fatalf("Send = (%+v, %v), want not-sent", res, err)
	}
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
			if !errors.Is(err, hosthttp.ErrMalformedResponse) {
				t.Fatalf("Send error = %v, want ErrMalformedResponse", err)
			}

			waited, err := tr.Wait(t.Context(), messaging.WaitRequest{
				Sender: alice, MessageID: msgID, Generation: "gen", Timeout: 20 * time.Millisecond,
			})
			if waited.Outcome != messaging.OutcomePossiblyPending || err == nil {
				t.Fatalf("Wait = (%+v, %v), want possibly-pending and an error", waited, err)
			}
		})
	}

	t.Run("oversized body", func(t *testing.T) {
		srv := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
			// A valid envelope inside the read limit followed by padding: a
			// silent truncation would report this send as admitted.
			_, _ = w.Write([]byte(`{"outcome":"sent"}`))
			_, _ = w.Write(bytes.Repeat([]byte(" "), 2<<20))
		})
		tr, err := hosthttp.New(plainDescriptor(srv.URL))
		if err != nil {
			t.Fatalf("hosthttp.New: %v", err)
		}
		t.Cleanup(func() { _ = tr.Close() })
		res, err := tr.Send(t.Context(), messaging.SendRequest{
			From: alice, To: bob, MessageID: mintID(t, time.Minute), Generation: "gen",
		})
		if !errors.Is(err, hosthttp.ErrMalformedResponse) || res.Outcome != messaging.OutcomePossiblyPending {
			t.Fatalf("Send = (%+v, %v), want possibly-pending and ErrMalformedResponse", res, err)
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

// TestHostClientRefusesRedirects pins that a redirect is never followed, so the
// capability headers cannot be replayed to another host.
func TestHostClientRefusesRedirects(t *testing.T) {
	var mu sync.Mutex
	var reached int
	target := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		reached++
		mu.Unlock()
		_, _ = w.Write([]byte(`{"outcome":"sent"}`))
	})
	srv := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+r.URL.Path, http.StatusFound)
	})
	tr, err := hosthttp.New(plainDescriptor(srv.URL))
	if err != nil {
		t.Fatalf("hosthttp.New: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close() })

	msgID := mintID(t, time.Minute)
	calls := map[string]func() error{
		"send": func() error {
			_, err := tr.Send(t.Context(), messaging.SendRequest{
				From: alice, To: bob, MessageID: msgID, Generation: "gen", Body: `{}`,
			})
			return err
		},
		"wait": func() error {
			_, err := tr.Wait(t.Context(), messaging.WaitRequest{
				Sender: alice, MessageID: msgID, Generation: "gen", Timeout: 50 * time.Millisecond,
			})
			return err
		},
		"reply": func() error {
			_, err := tr.Reply(t.Context(), messaging.ReplyRequest{
				From: bob, ToRunID: alice.RunID, MessageID: msgID, Body: `{}`,
			})
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			if err := call(); !errors.Is(err, httpbase.ErrRedirect) {
				t.Fatalf("%s error = %v, want ErrRedirect", name, err)
			}
			mu.Lock()
			served := reached
			mu.Unlock()
			if served != 0 {
				t.Fatalf("redirect target served %d request(s)", served)
			}
		})
	}
}

// TestHostFixtureRejectsForgedReply pins that a reply is only accepted from the
// capability that names its sender.
func TestHostFixtureRejectsForgedReply(t *testing.T) {
	fx := hostfixture.New(t)
	sender := newTransport(t, fx, alice)
	target := newTransport(t, fx, bob)
	eve := messaging.Identity{RunID: "host-eve", Sender: "eve"}
	intruder := newTransport(t, fx, eve)
	gen := fx.Store().Generation()
	msgID := mintID(t, time.Minute)
	if _, err := sender.Send(t.Context(), messaging.SendRequest{
		From: alice, To: bob, MessageID: msgID, Generation: gen, Body: `{"message":"ping"}`,
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	forged, err := intruder.Reply(t.Context(), messaging.ReplyRequest{
		From: bob, ToRunID: alice.RunID, MessageID: msgID, Body: `{"message":"forged"}`,
	})
	if !errors.Is(err, messaging.ErrAuthRejected) || forged.Outcome != messaging.OutcomeAuthRejected {
		t.Fatalf("forged Reply = (%+v, %v)", forged, err)
	}
	if !fx.Store().Pending(alice.RunID, msgID) {
		t.Fatal("forged reply completed the ask")
	}

	replied, err := target.Reply(t.Context(), messaging.ReplyRequest{
		From: bob, ToRunID: alice.RunID, MessageID: msgID, Body: `{"message":"pong"}`,
	})
	if err != nil || replied.Outcome != messaging.OutcomeReplied {
		t.Fatalf("target Reply = (%+v, %v)", replied, err)
	}
}

// TestHostFixtureWithdrawalRetractsQueuedDelivery pins that a withdrawal
// removes the delivery an already-admitted ask queued for its target.
func TestHostFixtureWithdrawalRetractsQueuedDelivery(t *testing.T) {
	fx := hostfixture.New(t)
	sender := newTransport(t, fx, alice)
	target := newTransport(t, fx, bob)
	gen := fx.Store().Generation()
	msgID := mintID(t, time.Minute)
	if _, err := sender.Send(t.Context(), messaging.SendRequest{
		From: alice, To: bob, MessageID: msgID, Generation: gen, Body: `{"message":"ping"}`,
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if queued := fx.Queued(bob.RunID); queued != 1 {
		t.Fatalf("queued = %d, want 1", queued)
	}

	withdrawn, err := sender.Withdraw(t.Context(), messaging.WithdrawRequest{
		Sender: alice, MessageID: msgID, Generation: gen,
	})
	if err != nil || withdrawn.Outcome != messaging.OutcomeWithdrawn {
		t.Fatalf("Withdraw = (%+v, %v)", withdrawn, err)
	}
	if queued := fx.Queued(bob.RunID); queued != 0 {
		t.Fatalf("withdrawal left %d queued delivery(ies)", queued)
	}
	received, err := target.Receive(t.Context(), messaging.ReceiveRequest{
		Sender: bob, Generation: gen, Limit: 8,
	})
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if len(received.Messages) != 0 {
		t.Fatalf("withdrawn ask was delivered: %+v", received.Messages)
	}
}

// TestHostClientPreflightGenerationMismatch pins the expected-epoch check.
func TestHostClientPreflightGenerationMismatch(t *testing.T) {
	fx := hostfixture.New(t)
	desc := fx.Descriptor(alice)
	desc.ExpectedEpoch = "stale-generation"
	tr, err := hosthttp.New(desc)
	if err != nil {
		t.Fatalf("hosthttp.New: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close() })

	pre, err := tr.Preflight(t.Context())
	if !errors.Is(err, messaging.ErrGenerationMismatch) {
		t.Fatalf("Preflight error = %v, want ErrGenerationMismatch", err)
	}
	if pre.Generation != fx.Store().Generation() {
		t.Fatalf("Preflight generation = %q, want the served one", pre.Generation)
	}
}

// TestHostClientClosedOperations pins that a call made after Close is provably
// never dispatched and is reported as not-sent, not as pending.
func TestHostClientClosedOperations(t *testing.T) {
	fx := hostfixture.New(t)
	tr := newTransport(t, fx, alice)
	gen := fx.Store().Generation()
	msgID := mintID(t, time.Minute)
	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	res, err := tr.Send(t.Context(), messaging.SendRequest{
		From: alice, To: bob, MessageID: msgID, Generation: gen, Body: `{}`,
	})
	if !errors.Is(err, messaging.ErrTransportClosed) || res.Outcome != messaging.OutcomeNotSent {
		t.Fatalf("Send = (%+v, %v)", res, err)
	}
	waited, err := tr.Wait(t.Context(), messaging.WaitRequest{
		Sender: alice, MessageID: msgID, Generation: gen, Timeout: time.Second,
	})
	if !errors.Is(err, messaging.ErrTransportClosed) || waited.Outcome != messaging.OutcomeNotSent {
		t.Fatalf("Wait = (%+v, %v)", waited, err)
	}
	replied, err := tr.Reply(t.Context(), messaging.ReplyRequest{
		From: bob, ToRunID: alice.RunID, MessageID: msgID, Body: `{}`,
	})
	if !errors.Is(err, messaging.ErrTransportClosed) || replied.Outcome != messaging.OutcomeNotSent {
		t.Fatalf("Reply = (%+v, %v)", replied, err)
	}
	if active := fx.Store().ActiveCount(); active != 0 {
		t.Fatalf("a closed transport admitted %d ask(s)", active)
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

// wireDoc is the declared v1 wire document.
type wireDoc struct {
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

func readWireDoc(t *testing.T) wireDoc {
	t.Helper()
	raw, err := os.ReadFile("../testdata/host_wire.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var doc wireDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	return doc
}

// TestHostWireFixture pins the declared v1 wire document against the endpoints
// the fixture serves, the shared outcome vocabulary, and the error table
// internal/messaging owns.
func TestHostWireFixture(t *testing.T) {
	doc := readWireDoc(t)
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

	classes := messaging.WireErrors()
	if len(doc.Errors) != len(classes) {
		t.Fatalf("wire document declares %d error row(s), the contract has %d", len(doc.Errors), len(classes))
	}
	for i, class := range classes {
		entry := doc.Errors[i]
		if entry.Error != class.Code || entry.Status != class.Status || entry.Outcome != class.Outcome {
			t.Fatalf("error row %d = %+v, want %s/%d/%s", i, entry, class.Code, class.Status, class.Outcome)
		}
	}
}

// TestHostWireErrorReplay drives the whole error table. A code the fixture
// produces is replayed through it, and the served status and outcome must match
// the contract; a code it cannot produce is replayed through the client against
// a server that reports that row.
func TestHostWireErrorReplay(t *testing.T) {
	aliceDesc := func(fx *hostfixture.Fixture) messaging.EndpointDescriptor { return fx.Descriptor(alice) }
	triggers := map[string]func(t *testing.T, fx *hostfixture.Fixture) (int, messaging.Outcome){
		"auth-rejected": func(t *testing.T, fx *hostfixture.Fixture) (int, messaging.Outcome) {
			t.Helper()
			desc := aliceDesc(fx)
			desc.CapabilitySecret = "wrong-secret"
			return fixturePost(t, fx, "/v1/send", desc, sendBody(t, fx.Store().Generation(), mintID(t, time.Minute), alice, bob.RunID))
		},
		"invalid-request": func(t *testing.T, fx *hostfixture.Fixture) (int, messaging.Outcome) {
			t.Helper()
			return fixturePost(t, fx, "/v1/send", aliceDesc(fx), "not-json")
		},
		"generation-mismatch": func(t *testing.T, fx *hostfixture.Fixture) (int, messaging.Outcome) {
			t.Helper()
			return fixturePost(t, fx, "/v1/send", aliceDesc(fx), sendBody(t, "stale-generation", mintID(t, time.Minute), alice, bob.RunID))
		},
		"malformed-message-id": func(t *testing.T, fx *hostfixture.Fixture) (int, messaging.Outcome) {
			t.Helper()
			return fixturePost(t, fx, "/v1/send", aliceDesc(fx), sendBody(t, fx.Store().Generation(), "not-a-versioned-id", alice, bob.RunID))
		},
		"duplicate-message-id": func(t *testing.T, fx *hostfixture.Fixture) (int, messaging.Outcome) {
			t.Helper()
			desc := aliceDesc(fx)
			gen := fx.Store().Generation()
			msgID := mintID(t, time.Minute)
			body := sendBody(t, gen, msgID, alice, bob.RunID)
			if status, _ := fixturePost(t, fx, "/v1/send", desc, body); status != http.StatusOK {
				t.Fatalf("first send status = %d, want %d", status, http.StatusOK)
			}
			// Let the wait expire, so the duplicate meets a terminal fence whose
			// own outcome is not a send outcome.
			if status, outcome := fixturePost(t, fx, "/v1/wait", desc, waitBody(t, gen, msgID, alice)); status != http.StatusOK || outcome != messaging.OutcomeTimedOut {
				t.Fatalf("wait = (%d, %q), want (%d, %q)", status, outcome, http.StatusOK, messaging.OutcomeTimedOut)
			}
			return fixturePost(t, fx, "/v1/send", desc, body)
		},
		"mutual-ask": func(t *testing.T, fx *hostfixture.Fixture) (int, messaging.Outcome) {
			t.Helper()
			gen := fx.Store().Generation()
			if status, _ := fixturePost(t, fx, "/v1/send", aliceDesc(fx), sendBody(t, gen, mintID(t, time.Minute), alice, bob.RunID)); status != http.StatusOK {
				t.Fatalf("first send status = %d, want %d", status, http.StatusOK)
			}
			return fixturePost(t, fx, "/v1/send", fx.Descriptor(bob), sendBody(t, gen, mintID(t, time.Minute), bob, alice.RunID))
		},
		"not-ask-target": func(t *testing.T, fx *hostfixture.Fixture) (int, messaging.Outcome) {
			t.Helper()
			eve := messaging.Identity{RunID: "wire-eve", Sender: "eve"}
			msgID := mintID(t, time.Minute)
			if status, _ := fixturePost(t, fx, "/v1/send", aliceDesc(fx), sendBody(t, fx.Store().Generation(), msgID, alice, bob.RunID)); status != http.StatusOK {
				t.Fatalf("send status = %d, want %d", status, http.StatusOK)
			}
			return fixturePost(t, fx, "/v1/reply", fx.Descriptor(eve), replyBody(t, msgID, eve, alice.RunID))
		},
	}

	for _, class := range messaging.WireErrors() {
		t.Run(class.Code, func(t *testing.T) {
			if trigger, ok := triggers[class.Code]; ok {
				status, outcome := trigger(t, hostfixture.New(t))
				if status != class.Status {
					t.Fatalf("served status = %d, want %d", status, class.Status)
				}
				if outcome != class.Outcome {
					t.Fatalf("served outcome = %q, want %q", outcome, class.Outcome)
				}
				return
			}

			srv := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(class.Status)
				_, _ = w.Write([]byte(`{"error":"` + class.Code + `","outcome":"` + string(class.Outcome) + `"}`))
			})
			tr, err := hosthttp.New(plainDescriptor(srv.URL))
			if err != nil {
				t.Fatalf("hosthttp.New: %v", err)
			}
			t.Cleanup(func() { _ = tr.Close() })
			res, err := tr.Send(t.Context(), messaging.SendRequest{
				From: alice, To: bob, MessageID: mintID(t, time.Minute), Generation: "gen", Body: `{}`,
			})
			if !errors.Is(err, class.Err) {
				t.Fatalf("Send error = %v, want %v", err, class.Err)
			}
			if !messaging.IsSendOutcome(res.Outcome) {
				t.Fatalf("Send outcome = %q, outside the send vocabulary", res.Outcome)
			}
			if messaging.IsSendOutcome(class.Outcome) && res.Outcome != class.Outcome {
				t.Fatalf("Send outcome = %q, want %q", res.Outcome, class.Outcome)
			}
		})
	}
}

func sendBody(t *testing.T, epoch, msgID string, from messaging.Identity, toRunID string) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"epoch":       epoch,
		"message_id":  msgID,
		"from_run_id": from.RunID,
		"from_sender": from.Sender,
		"to_run_id":   toRunID,
		"body":        `{"message":"ping"}`,
	})
	if err != nil {
		t.Fatalf("marshal send body: %v", err)
	}
	return string(raw)
}

func waitBody(t *testing.T, epoch, msgID string, sender messaging.Identity) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"epoch":         epoch,
		"sender_run_id": sender.RunID,
		"message_id":    msgID,
		"timeout_ms":    20,
	})
	if err != nil {
		t.Fatalf("marshal wait body: %v", err)
	}
	return string(raw)
}

func replyBody(t *testing.T, msgID string, from messaging.Identity, toRunID string) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"from_run_id": from.RunID,
		"to_run_id":   toRunID,
		"message_id":  msgID,
		"body":        `{"message":"pong"}`,
	})
	if err != nil {
		t.Fatalf("marshal reply body: %v", err)
	}
	return string(raw)
}

func fixturePost(t *testing.T, fx *hostfixture.Fixture, path string, desc messaging.EndpointDescriptor, body string) (int, messaging.Outcome) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, fx.URL()+path, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(messaging.HeaderCapability, desc.CapabilityID)
	req.Header.Set(messaging.HeaderSecret, desc.CapabilitySecret)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	var env struct {
		Outcome messaging.Outcome `json:"outcome"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode %s response: %v", path, err)
	}
	return resp.StatusCode, env.Outcome
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
