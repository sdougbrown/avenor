package localhttp_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/askstore"
	"github.com/sdougbrown/avenor/internal/messaging/localhttp"
	"github.com/sdougbrown/avenor/internal/messaging/messagetest"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

var alice = messaging.Identity{RunID: "local-alice", Sender: "alice"}
var bob = messaging.Identity{RunID: "local-bob", Sender: "bob"}

func newBroker(t *testing.T) *broker.Broker {
	t.Helper()
	b := broker.New("")
	if err := b.Start(); err != nil {
		t.Fatalf("broker.Start: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	return b
}

func credentials(t *testing.T, b *broker.Broker, id messaging.Identity) messaging.LocalCredentials {
	t.Helper()
	token, _ := b.EnsureRun(id.RunID, id.Sender)
	return messaging.LocalCredentials{RunID: id.RunID, Token: token}
}

func newAdapter(t *testing.T, b *broker.Broker, id messaging.Identity) messaging.Transport {
	t.Helper()
	tr, err := localhttp.New(b.Addr(), credentials(t, b, id))
	if err != nil {
		t.Fatalf("localhttp.New: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	return tr
}

func brokerFactory(t *testing.T, b *broker.Broker) messagetest.TransportFactory {
	return func(t *testing.T, id messaging.Identity) messaging.Transport {
		registerPeers(t, b, id)
		return newAdapter(t, b, id)
	}
}

// registerPeers registers the run identities the shared suites address but
// never build a transport for. The local broker refuses delivery to a run it
// does not know, unlike the in-process and fixture backends, so the peer has to
// exist here.
func registerPeers(t *testing.T, b *broker.Broker, id messaging.Identity) {
	t.Helper()
	prefix, _, ok := strings.Cut(id.RunID, "-")
	if !ok {
		return
	}
	for _, peer := range []string{"alice", "bob", "eve"} {
		b.EnsureRun(prefix+"-"+peer, peer)
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

func TestLocalAdapterPeerFaultRacingClose(t *testing.T) {
	messagetest.RunPeerFaultCloseSuite(t, brokerFactory(t, newBroker(t)))
}

func TestLocalReplyRouting(t *testing.T) {
	b := newBroker(t)
	messagetest.RunReplyRoutingSuite(t, brokerFactory(t, b), messagetest.ReplyRoutingHooks{
		AdmitLegacy: func(t *testing.T, sender messaging.Identity, messageID, target string) {
			if _, err := b.Store().Admit(t.Context(), askstore.AdmitRequest{
				Sender: sender, Target: target, MessageID: messageID, Legacy: true,
			}); err != nil {
				t.Fatalf("legacy Admit: %v", err)
			}
		},
		Rotate: func(t *testing.T) { b.Store().Reset() },
	})
}

// TestLocalAdapterGenerationRotation pins the rotation rule: a rejected
// generation is never re-sent, a changed generation proves the broker was
// replaced, and the next ask carries the refreshed generation.
func TestLocalAdapterGenerationRotation(t *testing.T) {
	b := newBroker(t)
	sender := newAdapter(t, b, alice)
	newAdapter(t, b, bob)

	before, err := sender.Preflight(t.Context())
	if err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	msgID := mintID(t, time.Minute)
	b.Store().Reset()
	res, err := sender.Send(t.Context(), messaging.SendRequest{
		From: alice, To: bob, MessageID: msgID, Generation: before.Generation, Body: `{"message":"ping"}`,
	})
	if !errors.Is(err, messaging.ErrGenerationMismatch) {
		t.Fatalf("stale-generation send error = %v", err)
	}
	if res.Outcome != messaging.OutcomeLost {
		t.Fatalf("stale-generation send outcome = %q, want lost", res.Outcome)
	}
	if b.Store().Pending(alice.RunID, msgID) {
		t.Fatal("rejected send left a live edge")
	}

	after, err := sender.Preflight(t.Context())
	if err != nil {
		t.Fatalf("refresh Preflight: %v", err)
	}
	if after.Generation == before.Generation {
		t.Fatal("generation did not rotate")
	}
	next := mintID(t, time.Minute)
	sent, err := sender.Send(t.Context(), messaging.SendRequest{
		From: alice, To: bob, MessageID: next, Generation: after.Generation, Body: `{"message":"ping"}`,
	})
	if err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("send with the refreshed generation = (%+v, %v)", sent, err)
	}
}

// TestLocalAdapterUnchangedGenerationIsUncertain pins the other half of the
// rotation rule: a rejection that leaves the generation unchanged keeps the ask
// uncertain and is not retried.
func TestLocalAdapterUnchangedGenerationIsUncertain(t *testing.T) {
	var sends atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sessions":
			_, _ = io.WriteString(w, `{"sessions":[],"generation":"gen-1","messaging_protocol":1}`)
		case "/send":
			sends.Add(1)
			w.WriteHeader(http.StatusConflict)
			_, _ = io.WriteString(w, `{"error":"generation mismatch","code":"generation-mismatch","outcome":"not-sent"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	tr := newStubAdapter(t, srv.URL)
	res, err := tr.Send(t.Context(), messaging.SendRequest{
		From: alice, To: bob, MessageID: mintID(t, time.Minute), Generation: "gen-1", Body: `{"message":"ping"}`,
	})
	if !errors.Is(err, messaging.ErrGenerationMismatch) {
		t.Fatalf("send error = %v", err)
	}
	if res.Outcome != messaging.OutcomePossiblyPending {
		t.Fatalf("send outcome = %q, want possibly-pending", res.Outcome)
	}
	if got := sends.Load(); got != 1 {
		t.Fatalf("send attempts = %d, want 1 (never replayed)", got)
	}
}

// TestLocalAdapterOldRouterProtocol pins the old-supervisor case: a broker that
// does not advertise the protocol marker never receives the ask.
func TestLocalAdapterOldRouterProtocol(t *testing.T) {
	var sends atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sessions":
			_, _ = io.WriteString(w, `{"sessions":[],"generation":"old-generation"}`)
		case "/send":
			sends.Add(1)
			_, _ = io.WriteString(w, `{"queued":true}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	tr := newStubAdapter(t, srv.URL)
	pre, err := tr.Preflight(t.Context())
	if !errors.Is(err, messaging.ErrUnsupportedProtocol) {
		t.Fatalf("Preflight error = %v, want unsupported protocol", err)
	}
	if pre.Protocol != 0 {
		t.Fatalf("Preflight protocol = %d, want 0 for an old broker", pre.Protocol)
	}
	if pre.Generation != "old-generation" {
		t.Fatalf("Preflight generation = %q", pre.Generation)
	}
	res, err := tr.Send(t.Context(), messaging.SendRequest{
		From: alice, To: bob, MessageID: mintID(t, time.Minute), Generation: pre.Generation, Body: `{"message":"ping"}`,
	})
	if !errors.Is(err, messaging.ErrUnsupportedProtocol) {
		t.Fatalf("send error = %v, want unsupported protocol", err)
	}
	if res.Outcome != messaging.OutcomeNotSent {
		t.Fatalf("send outcome = %q, want not-sent", res.Outcome)
	}
	if got := sends.Load(); got != 0 {
		t.Fatalf("old broker received %d sends, want none", got)
	}
	if hint := err.Error(); !strings.Contains(hint, "restart the supervisor") {
		t.Fatalf("error %q does not name the recovery step", hint)
	}
}

// TestLocalAdapterMissingOutcomeAfterSend pins the post-send race: a response
// without an outcome cannot prove the ask was not admitted, so it stays
// possibly-pending and keeps its message ID.
func TestLocalAdapterMissingOutcomeAfterSend(t *testing.T) {
	var sends atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sessions":
			_, _ = io.WriteString(w, `{"sessions":[],"generation":"gen-1","messaging_protocol":1}`)
		case "/send":
			sends.Add(1)
			_, _ = io.WriteString(w, `{"queued":true}`)
		case "/cancel_message":
			_, _ = io.WriteString(w, `{"cancelled":true,"message_id":"m","outcome":"withdrawn","fence":{"terminal":true}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	tr := newStubAdapter(t, srv.URL)
	res, err := tr.Send(t.Context(), messaging.SendRequest{
		From: alice, To: bob, MessageID: mintID(t, time.Minute), Generation: "gen-1", Body: `{"message":"ping"}`,
	})
	if err == nil {
		t.Fatal("send reported success for a response without an outcome")
	}
	if !errors.Is(err, messaging.ErrUnsupportedProtocol) {
		t.Fatalf("send error = %v", err)
	}
	if res.Outcome != messaging.OutcomePossiblyPending {
		t.Fatalf("send outcome = %q, want possibly-pending", res.Outcome)
	}
	if res.MessageID == "" {
		t.Fatal("possibly-pending send dropped its message id")
	}
	if got := sends.Load(); got != 1 {
		t.Fatalf("send attempts = %d, want 1 (never replayed)", got)
	}
}

// TestLocalAdapterPausesSendAdmission drives the terminal-fence regression
// through the real router: a send paused before admission, withdrawn for the
// same key, then resumed must not deliver.
func TestLocalAdapterPausesSendAdmission(t *testing.T) {
	b := newBroker(t)
	sender := newAdapter(t, b, alice)
	newAdapter(t, b, bob)

	messagetest.RunWithdrawFencesPausedSend(t, sender, b.Store(), alice, bob)

	target := b.GetRun(bob.RunID)
	target.Mu.Lock()
	delivered := len(target.ControlQueue)
	target.Mu.Unlock()
	if delivered != 0 {
		t.Fatalf("withdrawn ask was delivered %d time(s)", delivered)
	}
	if live := b.Store().ActiveCount(); live != 0 {
		t.Fatalf("withdrawn ask left %d live edge(s)", live)
	}
}

// TestLocalAdapterCloseLeavesBrokerRunning pins that closing a client is a
// client-side operation only.
func TestLocalAdapterCloseLeavesBrokerRunning(t *testing.T) {
	b := newBroker(t)
	tr := newAdapter(t, b, alice)
	newAdapter(t, b, bob)
	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	_, err := tr.Send(t.Context(), messaging.SendRequest{
		From: alice, To: bob, MessageID: mintID(t, time.Minute), Generation: "gen", Body: `{}`,
	})
	if !errors.Is(err, messaging.ErrTransportClosed) {
		t.Fatalf("post-close Send error = %v", err)
	}
	other := newAdapter(t, b, bob)
	pre, err := other.Preflight(t.Context())
	if err != nil || pre.Protocol != messaging.ProtocolVersion {
		t.Fatalf("broker stopped serving after a client closed: (%+v, %v)", pre, err)
	}
}

func TestNewAcceptsZonedIPv6BrokerAddr(t *testing.T) {
	tr, err := localhttp.New("[fe80::1%eth0]:8787", messaging.LocalCredentials{RunID: "run_1", Token: "tok_1"})
	if err != nil {
		t.Fatalf("localhttp.New with a zoned IPv6 address: %v", err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func newStubAdapter(t *testing.T, url string) messaging.Transport {
	t.Helper()
	tr, err := localhttp.New("", messaging.LocalCredentials{RunID: alice.RunID, Token: "stub-token", BaseURL: url})
	if err != nil {
		t.Fatalf("localhttp.New: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	return tr
}

// TestLocalAdapterGetSendsCredentialsAsHeaders pins that a GET keeps the run
// credentials out of its URL.
func TestLocalAdapterGetSendsCredentialsAsHeaders(t *testing.T) {
	var gotURL, gotRunID, gotToken string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sessions" {
			http.NotFound(w, r)
			return
		}
		gotURL = r.URL.String()
		gotRunID = r.Header.Get(messaging.HeaderRunID)
		gotToken = r.Header.Get(messaging.HeaderRunToken)
		_, _ = io.WriteString(w, `{"sessions":[],"generation":"gen-1","messaging_protocol":1}`)
	}))
	defer srv.Close()

	tr := newStubAdapter(t, srv.URL)
	if _, err := tr.Preflight(t.Context()); err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	if strings.Contains(gotURL, "run_id=") || strings.Contains(gotURL, "token=") {
		t.Fatalf("GET URL carries credentials: %s", gotURL)
	}
	if gotRunID != alice.RunID || gotToken != "stub-token" {
		t.Fatalf("GET credential headers = %q/%q", gotRunID, gotToken)
	}
}

// wireDoc is the local messaging wire contract recorded in
// testdata/local_wire.json. The adapter and the router are checked against it
// so a path, field or code cannot drift silently.
type wireDoc struct {
	Protocol       int                 `json:"protocol"`
	MarkerField    string              `json:"marker_field"`
	Paths          map[string]string   `json:"paths"`
	RequestFields  map[string][]string `json:"request_fields"`
	ResponseFields map[string][]string `json:"response_fields"`
	FenceFields    []string            `json:"fence_fields"`
	Codes          []string            `json:"codes"`
}

func loadWireDoc(t *testing.T) wireDoc {
	t.Helper()
	raw, err := os.ReadFile("../testdata/local_wire.json")
	if err != nil {
		t.Fatalf("read local wire document: %v", err)
	}
	var doc wireDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode local wire document: %v", err)
	}
	return doc
}

func assertFields(t *testing.T, label string, want []string, body map[string]any) {
	t.Helper()
	if len(want) == 0 {
		t.Fatalf("%s declares no fields", label)
	}
	for _, field := range want {
		if _, ok := body[field]; !ok {
			t.Fatalf("%s response is missing %q: %v", label, field, body)
		}
	}
}

func rawGet(t *testing.T, addr, path string, creds messaging.LocalCredentials) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://"+addr+path, nil)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	req.Header.Set(messaging.HeaderRunID, creds.RunID)
	req.Header.Set(messaging.HeaderRunToken, creds.Token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode GET %s: %v", path, err)
	}
	return resp.StatusCode, body
}

func rawPostList(t *testing.T, addr, path string, payload map[string]any) (int, []any) {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal %s body: %v", path, err)
	}
	resp, err := http.Post("http://"+addr+path, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	var body []any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode POST %s: %v", path, err)
	}
	return resp.StatusCode, body
}

func rawPost(t *testing.T, addr, path string, payload map[string]any) (int, map[string]any) {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal %s body: %v", path, err)
	}
	resp, err := http.Post("http://"+addr+path, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode POST %s: %v", path, err)
	}
	return resp.StatusCode, body
}

// TestLocalWireFixture pins the recorded local wire contract against the live
// router: every declared path serves, every declared request field is accepted,
// every declared response field is present, and every declared code is part of
// the shared error table.
func TestLocalWireFixture(t *testing.T) {
	doc := loadWireDoc(t)
	if doc.Protocol != messaging.ProtocolVersion {
		t.Fatalf("documented protocol = %d, want %d", doc.Protocol, messaging.ProtocolVersion)
	}
	known := make(map[string]bool)
	for _, class := range messaging.WireErrors() {
		known[class.Code] = true
	}
	for _, code := range doc.Codes {
		if !known[code] {
			t.Fatalf("documented code %q is not in the shared error table", code)
		}
	}

	b := newBroker(t)
	credsA := credentials(t, b, alice)
	credsB := credentials(t, b, bob)
	addr := b.Addr()

	// build renders every field the document declares for op, so the fixture
	// pins that the router accepts the declared request shape and returns every
	// declared response field. The router ignores unknown JSON fields, so a
	// field it silently stops reading is not caught here.
	values := map[string]any{
		"run_id": credsA.RunID, "token": credsA.Token,
		"from_run_id": alice.RunID, "to_run_id": bob.RunID, "type": "agent_message",
		"sender": alice.Sender, "payload": map[string]any{"message": "ping"},
		"timeout_ms": 2000, "agent_only": true, "limit": 0,
	}
	build := func(op string, overrides map[string]any) map[string]any {
		t.Helper()
		if len(doc.RequestFields[op]) == 0 {
			t.Fatalf("%s request declares no fields", op)
		}
		request := make(map[string]any, len(doc.RequestFields[op]))
		for _, field := range doc.RequestFields[op] {
			value, ok := overrides[field]
			if !ok {
				value, ok = values[field]
			}
			if !ok {
				t.Fatalf("%s request declares %q, which the fixture has no value for", op, field)
			}
			request[field] = value
		}
		return request
	}

	status, sessions := rawGet(t, addr, doc.Paths["sessions"], credsA)
	if status != http.StatusOK {
		t.Fatalf("sessions status = %d", status)
	}
	assertFields(t, "sessions", doc.ResponseFields["sessions"], sessions)
	marker, ok := sessions[doc.MarkerField].(float64)
	if !ok {
		t.Fatalf("marker %q = %v, want a number", doc.MarkerField, sessions[doc.MarkerField])
	}
	if int(marker) != doc.Protocol {
		t.Fatalf("marker %q = %v, want %d", doc.MarkerField, marker, doc.Protocol)
	}
	generation, _ := sessions["generation"].(string)
	values["generation"] = generation

	msgID := mintID(t, time.Minute)
	values["message_id"], values["expects_reply"], values["waiting_for"] = msgID, true, msgID
	status, sent := rawPost(t, addr, doc.Paths["send"], build("send", nil))
	if status != http.StatusOK || sent["outcome"] != string(messaging.OutcomeSent) {
		t.Fatalf("send = (%d, %v)", status, sent)
	}
	assertFields(t, "send", doc.ResponseFields["send"], sent)

	status, replied := rawPost(t, addr, doc.Paths["send"], build("reply", map[string]any{
		"run_id": credsB.RunID, "token": credsB.Token,
		"from_run_id": bob.RunID, "to_run_id": alice.RunID,
		"sender": bob.Sender, "reply_to": msgID,
		"payload": map[string]any{"message": "pong"},
	}))
	if status != http.StatusOK || replied["outcome"] != string(messaging.OutcomeReplied) {
		t.Fatalf("reply = (%d, %v)", status, replied)
	}
	assertFields(t, "reply", doc.ResponseFields["reply"], replied)

	status, waited := rawPost(t, addr, doc.Paths["wait"], build("wait", nil))
	if status != http.StatusOK || waited["outcome"] != string(messaging.OutcomeReplied) {
		t.Fatalf("wait = (%d, %v)", status, waited)
	}
	assertFields(t, "wait_replied", doc.ResponseFields["wait_replied"], waited)

	timeoutID := mintID(t, time.Minute)
	if status, body := rawPost(t, addr, doc.Paths["send"], build("send", map[string]any{
		"message_id": timeoutID, "payload": map[string]any{"message": "silence"},
	})); status != http.StatusOK {
		t.Fatalf("send = (%d, %v)", status, body)
	}
	status, timedOut := rawPost(t, addr, doc.Paths["wait"], build("wait", map[string]any{
		"waiting_for": timeoutID, "timeout_ms": 20,
	}))
	if status != http.StatusGatewayTimeout || timedOut["outcome"] != string(messaging.OutcomeTimedOut) {
		t.Fatalf("timed-out wait = (%d, %v)", status, timedOut)
	}
	assertFields(t, "wait_timeout", doc.ResponseFields["wait_timeout"], timedOut)

	status, withdrawn := rawPost(t, addr, doc.Paths["withdraw"], build("withdraw", map[string]any{
		"cancel_message_id": timeoutID,
	}))
	if status != http.StatusOK {
		t.Fatalf("withdraw = (%d, %v)", status, withdrawn)
	}
	assertFields(t, "withdraw", doc.ResponseFields["withdraw"], withdrawn)
	fence, ok := withdrawn["fence"].(map[string]any)
	if !ok {
		t.Fatalf("withdraw response carries no fence object: %v", withdrawn)
	}
	assertFields(t, "fence", doc.FenceFields, fence)

	status, absent := rawPost(t, addr, doc.Paths["withdraw"], build("withdraw", map[string]any{
		"cancel_message_id": mintID(t, time.Minute),
	}))
	if status != http.StatusNotFound {
		t.Fatalf("absent withdraw = (%d, %v)", status, absent)
	}
	assertFields(t, "withdraw_absent", doc.ResponseFields["withdraw_absent"], absent)

	status, duplicate := rawPost(t, addr, doc.Paths["send"], build("send", map[string]any{
		"message_id": timeoutID, "payload": map[string]any{"message": "again"},
	}))
	if status != http.StatusConflict {
		t.Fatalf("duplicate send = (%d, %v)", status, duplicate)
	}
	assertFields(t, "error", doc.ResponseFields["error"], duplicate)
	if duplicate["code"] != "duplicate-message-id" {
		t.Fatalf("duplicate send code = %v", duplicate["code"])
	}

	status, polled := rawPostList(t, addr, doc.Paths["receive"], build("receive", map[string]any{
		"run_id": credsB.RunID, "token": credsB.Token,
	}))
	if status != http.StatusOK {
		t.Fatalf("poll-control = (%d, %v)", status, polled)
	}
}

// TestLocalAdapterRejectsBadCredentials pins that a rejected run token is a
// credential fact, not transport uncertainty, and that nothing is sent.
func TestLocalAdapterRejectsBadCredentials(t *testing.T) {
	b := newBroker(t)
	newAdapter(t, b, bob)
	tr, err := localhttp.New(b.Addr(), messaging.LocalCredentials{RunID: alice.RunID, Token: "wrong-token"})
	if err != nil {
		t.Fatalf("localhttp.New: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close() })

	if _, err := tr.Preflight(t.Context()); !errors.Is(err, messaging.ErrAuthRejected) {
		t.Fatalf("Preflight error = %v, want auth rejected", err)
	}
	res, err := tr.Send(t.Context(), messaging.SendRequest{
		From: alice, To: bob, MessageID: mintID(t, time.Minute), Generation: "gen", Body: `{"message":"ping"}`,
	})
	if !errors.Is(err, messaging.ErrAuthRejected) {
		t.Fatalf("Send error = %v, want auth rejected", err)
	}
	if res.Outcome != messaging.OutcomeAuthRejected {
		t.Fatalf("Send outcome = %q, want auth-rejected", res.Outcome)
	}
	target := b.GetRun(bob.RunID)
	target.Mu.Lock()
	delivered := len(target.ControlQueue)
	target.Mu.Unlock()
	if delivered != 0 {
		t.Fatalf("rejected credential delivered %d message(s)", delivered)
	}
}

// TestLocalAdapterLegacyForbidden pins the code-less 403 an older router sends
// for a reply from outside the ask: it is the not-ask-target refusal, not a
// rejected credential.
func TestLocalAdapterLegacyForbidden(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sessions":
			_, _ = io.WriteString(w, `{"sessions":[],"generation":"gen-1","messaging_protocol":1}`)
		case "/send":
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `{"error":"not the target of this ask"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	tr := newStubAdapter(t, srv.URL)
	res, err := tr.Reply(t.Context(), messaging.ReplyRequest{
		From: bob, ToRunID: alice.RunID, MessageID: mintID(t, time.Minute), Body: `{"message":"pong"}`,
	})
	if !errors.Is(err, messaging.ErrNotAskTarget) {
		t.Fatalf("Reply error = %v, want not-ask-target", err)
	}
	if res.Outcome != messaging.OutcomeEdgeAbsent {
		t.Fatalf("Reply outcome = %q, want edge-absent", res.Outcome)
	}
}

// TestLocalAdapterSenderMismatch pins the broker's sender binding through the
// adapter: a run that has claimed a display name cannot claim another, and the
// refusal is a credential fact rather than transport uncertainty.
func TestLocalAdapterSenderMismatch(t *testing.T) {
	b := newBroker(t)
	tr := newAdapter(t, b, alice)
	newAdapter(t, b, bob)

	pre, err := tr.Preflight(t.Context())
	if err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	if _, err := tr.Send(t.Context(), messaging.SendRequest{
		From: alice, To: bob, MessageID: mintID(t, time.Minute), Generation: pre.Generation, Body: `{"message":"ping"}`,
	}); err != nil {
		t.Fatalf("first Send: %v", err)
	}

	res, err := tr.Send(t.Context(), messaging.SendRequest{
		From: messaging.Identity{RunID: alice.RunID, Sender: "mallory"}, To: bob,
		MessageID: mintID(t, time.Minute), Generation: pre.Generation, Body: `{"message":"ping"}`,
	})
	if !errors.Is(err, messaging.ErrSenderMismatch) {
		t.Fatalf("mismatched Send error = %v, want sender-mismatch", err)
	}
	if res.Outcome != messaging.OutcomeAuthRejected {
		t.Fatalf("mismatched Send outcome = %q, want auth-rejected", res.Outcome)
	}
}
