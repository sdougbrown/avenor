package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sdougbrown/avenor/internal/askclaim"
	"github.com/sdougbrown/avenor/internal/messaging"
)

// The identity these tests bind: one host conversation on its own run, plus a
// target it can address and an asker whose ask it answers.
const (
	boundTestRunID    = "run_bound"
	boundTestToken    = "tok_bound"
	boundTestTargetID = "run_target"
	boundTestAskID    = "msg_ask"
	boundTestAskerID  = "run_asker"
)

// brokerCall is one request the shared provider made, decoded so a test can
// assert which identity the tool spoke as and which message it sent.
type brokerCall struct {
	Method string
	Path   string
	Query  string
	RunID  string
	Token  string
	Body   map[string]any
}

// fakeBroker serves the local broker routes the shared provider drives. It
// records every request and answers with the envelope the transport expects, so
// a test can prove the identity and the route a bound tool call reached.
type fakeBroker struct {
	mu       sync.Mutex
	calls    []brokerCall
	sessions []string
	replied  map[string]bool
	// failPath scripts one failing route, so a test can fail the transport
	// without failing the identity negotiation that precedes it.
	failPath   string
	failStatus int
	failBody   string
}

func newFakeBroker(t *testing.T, sessions ...string) (*fakeBroker, string) {
	t.Helper()
	broker := &fakeBroker{sessions: sessions, replied: map[string]bool{}}
	srv := httptest.NewServer(broker)
	t.Cleanup(srv.Close)
	return broker, srv.URL
}

// fail makes one route answer with status and body.
func (b *fakeBroker) fail(path string, status int, body string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failPath, b.failStatus, b.failBody = path, status, body
}

// heal stops failing the scripted route.
func (b *fakeBroker) heal() { b.fail("", 0, "") }

func (b *fakeBroker) recorded() []brokerCall {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]brokerCall(nil), b.calls...)
}

// lastCall returns the most recent request the provider made on path.
func (b *fakeBroker) lastCall(t *testing.T, path string) brokerCall {
	t.Helper()
	calls := b.recorded()
	for i := len(calls) - 1; i >= 0; i-- {
		if calls[i].Path == path {
			return calls[i]
		}
	}
	t.Fatalf("the provider made no request on %s; calls = %+v", path, calls)
	return brokerCall{}
}

func (b *fakeBroker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	call := brokerCall{Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery}
	if r.Method == http.MethodGet {
		call.RunID = r.Header.Get(messaging.HeaderRunID)
		call.Token = r.Header.Get(messaging.HeaderRunToken)
	} else {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			call.Body = body
		}
		call.RunID, _ = call.Body["run_id"].(string)
		call.Token, _ = call.Body["token"].(string)
	}
	b.mu.Lock()
	b.calls = append(b.calls, call)
	status, payload := b.answer(call)
	b.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(payload)
}

// answer is the broker's one route table. It runs under the lock, so the reply
// ledger a second answer reads is the one the first wrote.
func (b *fakeBroker) answer(call brokerCall) (int, []byte) {
	if b.failPath == call.Path {
		return b.failStatus, []byte(b.failBody)
	}
	switch call.Path {
	case "/sessions":
		sessions := make([]map[string]string, 0, len(b.sessions))
		for _, runID := range b.sessions {
			sessions = append(sessions, map[string]string{"run_id": runID, "label": runID, "status": "running"})
		}
		body, _ := json.Marshal(map[string]any{
			"messaging_protocol": messaging.ProtocolVersion,
			"generation":         "gen-broker",
			"sessions":           sessions,
		})
		return http.StatusOK, body
	case "/send":
		if replyTo, _ := call.Body["reply_to"].(string); replyTo != "" {
			if b.replied[replyTo] {
				return http.StatusOK, []byte(`{"outcome":"edge-absent"}`)
			}
			b.replied[replyTo] = true
			return http.StatusOK, []byte(`{"outcome":"replied"}`)
		}
		messageID, _ := call.Body["message_id"].(string)
		body, _ := json.Marshal(map[string]any{"outcome": "sent", "message_id": messageID})
		return http.StatusOK, body
	case "/wait_reply":
		return http.StatusOK, []byte(`{"outcome":"timed-out"}`)
	case "/cancel_message":
		return http.StatusOK, []byte(`{"outcome":"withdrawn"}`)
	case "/rename":
		name, _ := call.Body["name"].(string)
		body, _ := json.Marshal(map[string]any{"run_id": boundTestRunID, "label": name, "previous_label": "before"})
		return http.StatusOK, body
	case "/last_message":
		query, _ := url.ParseQuery(call.Query)
		target := query.Get("target")
		body, _ := json.Marshal(map[string]any{
			"run_id": target, "label": target, "has_record": true,
			"final_output": "bye", "stop_reason": "end_turn",
		})
		return http.StatusOK, body
	}
	return http.StatusNotFound, []byte(`{"error":"unexpected route"}`)
}

// declaredBoundServer builds a server whose launch declared one host
// conversation and ran its one bind against brokerURL, so every message and
// peers call speaks as the bound run. The bound supervisor path is the server's
// default, which is what the launch's bind resolves.
func declaredBoundServer(t *testing.T, brokerURL, supervisorPath string) (*Server, *bytes.Buffer, *bindingClient) {
	t.Helper()
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	cl := &bindingClient{}
	cl.setResult(messaging.MessageBindResult{
		BrokerURL:  brokerURL,
		RunID:      boundTestRunID,
		RunToken:   boundTestToken,
		Generation: "gen-bind",
	})
	s, diag := declaredServer(t, cl, "poo", "ses_bound", nil)
	t.Cleanup(func() { _ = s.Close() })
	s.defaultSupervisorPath = supervisorPath
	bindDeclared(t, s)
	return s, diag, cl
}

// toolText returns the rendered sentence a tool result carries.
func toolText(t *testing.T, res *mcpsdk.CallToolResult) string {
	t.Helper()
	var parts []string
	for _, content := range res.Content {
		if text, ok := content.(*mcpsdk.TextContent); ok {
			parts = append(parts, text.Text)
		}
	}
	return strings.Join(parts, "\n")
}

// resultObject returns one tool result's structured object.
func resultObject(t *testing.T, res *mcpsdk.CallToolResult) map[string]any {
	t.Helper()
	object, _ := res.StructuredContent.(map[string]any)
	if object == nil {
		t.Fatalf("tool result carries no structured object: %#v", res)
	}
	return object
}

// TestBoundMessageReplyClaimsTheFeedAsk proves a bound conversation answers an
// ask the shared feed recorded: the reply claims the ask under the conversation's
// own inbox directory and posts it to the broker as the conversation's run, so a
// reply works whether the MCP inbox or `avenor feed broker` delivered the ask.
func TestBoundMessageReplyClaimsTheFeedAsk(t *testing.T) {
	broker, brokerURL := newFakeBroker(t, boundTestRunID, boundTestTargetID)
	s, _, _ := declaredBoundServer(t, brokerURL, "")
	bound, ok := s.hostBind.boundState()
	if !ok {
		t.Fatal("the launch's bind did not record an identity")
	}
	if err := askclaim.Record(bound.asksDir, boundTestAskID, boundTestAskerID); err != nil {
		t.Fatalf("record the ask: %v", err)
	}

	res, err := s.handleAvenorMessage(context.Background(), toolRequest(t, map[string]any{
		"action": "reply", "reply_to": boundTestAskID, "message": "answer",
	}))
	if err != nil {
		t.Fatalf("reply: %v", err)
	}
	if res.IsError {
		t.Fatalf("reply reported an error: %#v", res.StructuredContent)
	}
	sent := broker.lastCall(t, "/send")
	if sent.RunID != boundTestRunID || sent.Token != boundTestToken {
		t.Fatalf("reply authenticated as (%q, %q), want the bound run's own credentials", sent.RunID, sent.Token)
	}
	if got, _ := sent.Body["reply_to"].(string); got != boundTestAskID {
		t.Fatalf("reply_to = %q, want the ask it answered", got)
	}
	if got, _ := sent.Body["to_run_id"].(string); got != boundTestAskerID {
		t.Fatalf("to_run_id = %q, want the asker the claim stamped", got)
	}
	if object := resultObject(t, res); object["outcome"] != string(messaging.OutcomeReplied) {
		t.Fatalf("reply result = %#v, want replied", object)
	}
}

// TestBoundMessageReplyRestoresAFailedClaim proves a reply whose delivery is
// unproven keeps the ask answerable, and that a delivered ask answers
// edge-absent afterwards.
func TestBoundMessageReplyRestoresAFailedClaim(t *testing.T) {
	broker, brokerURL := newFakeBroker(t, boundTestRunID, boundTestTargetID)
	s, _, _ := declaredBoundServer(t, brokerURL, "")
	bound, _ := s.hostBind.boundState()
	if err := askclaim.Record(bound.asksDir, boundTestAskID, boundTestAskerID); err != nil {
		t.Fatalf("record the ask: %v", err)
	}
	broker.fail("/send", http.StatusInternalServerError, `{"error":"the broker is restarting"}`)

	res, err := s.handleAvenorMessage(context.Background(), toolRequest(t, map[string]any{
		"action": "reply", "reply_to": boundTestAskID, "message": "answer",
	}))
	if err != nil {
		t.Fatalf("failed reply: %v", err)
	}
	if object := resultObject(t, res); object["outcome"] != string(messaging.OutcomePossiblyPending) {
		t.Fatalf("failed reply result = %#v, want possibly-pending", object)
	}
	if got, _ := broker.lastCall(t, "/send").Body["to_run_id"].(string); got != boundTestAskerID {
		t.Fatalf("failed reply to_run_id = %q, want the claimed asker", got)
	}

	broker.heal()
	res, err = s.handleAvenorMessage(context.Background(), toolRequest(t, map[string]any{
		"action": "reply", "reply_to": boundTestAskID, "message": "answer",
	}))
	if err != nil {
		t.Fatalf("retried reply: %v", err)
	}
	if object := resultObject(t, res); object["outcome"] != string(messaging.OutcomeReplied) {
		t.Fatalf("retried reply result = %#v, want replied", object)
	}
	if got, _ := broker.lastCall(t, "/send").Body["to_run_id"].(string); got != boundTestAskerID {
		t.Fatalf("retried reply to_run_id = %q, want the restored claim's asker", got)
	}

	res, err = s.handleAvenorMessage(context.Background(), toolRequest(t, map[string]any{
		"action": "reply", "reply_to": boundTestAskID, "message": "answer again",
	}))
	if err != nil {
		t.Fatalf("second reply: %v", err)
	}
	if object := resultObject(t, res); object["outcome"] != string(messaging.OutcomeEdgeAbsent) {
		t.Fatalf("second reply result = %#v, want edge-absent", object)
	}
	if got, _ := broker.lastCall(t, "/send").Body["to_run_id"].(string); got != "" {
		t.Fatalf("second reply to_run_id = %q, want no claimed asker", got)
	}
}

// TestBoundMessageTellAskAndWithdrawSpeakAsTheBoundRun proves the remaining
// message actions reach the conversation's own broker as its own run, withdraw
// included: a withdraw names no run, so the bound identity is the only thing that
// can route it back.
func TestBoundMessageTellAskAndWithdrawSpeakAsTheBoundRun(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    map[string]any
		path    string
		outcome messaging.Outcome
		check   func(t *testing.T, call brokerCall)
	}{
		{
			name:    "tell",
			args:    map[string]any{"action": "tell", "target": boundTestTargetID, "message": "hi"},
			path:    "/send",
			outcome: messaging.OutcomeSent,
			check: func(t *testing.T, call brokerCall) {
				if got, _ := call.Body["to_run_id"].(string); got != boundTestTargetID {
					t.Fatalf("tell target = %q, want %q", got, boundTestTargetID)
				}
			},
		},
		{
			name:    "ask",
			args:    map[string]any{"action": "ask", "target": boundTestTargetID, "message": "q", "timeout_ms": 1000},
			path:    "/send",
			outcome: messaging.OutcomeTimedOut,
			check: func(t *testing.T, call brokerCall) {
				if got, _ := call.Body["to_run_id"].(string); got != boundTestTargetID {
					t.Fatalf("ask target = %q, want %q", got, boundTestTargetID)
				}
			},
		},
		{
			name:    "withdraw",
			args:    map[string]any{"action": "withdraw", "message_id": "msg_old"},
			path:    "/cancel_message",
			outcome: messaging.OutcomeWithdrawn,
			check: func(t *testing.T, call brokerCall) {
				if got, _ := call.Body["cancel_message_id"].(string); got != "msg_old" {
					t.Fatalf("withdraw id = %q, want the named message", got)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			broker, brokerURL := newFakeBroker(t, boundTestRunID, boundTestTargetID)
			s, _, _ := declaredBoundServer(t, brokerURL, "")

			res, err := s.handleAvenorMessage(context.Background(), toolRequest(t, tc.args))
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if res.IsError {
				t.Fatalf("%s reported an error: %#v", tc.name, res.StructuredContent)
			}
			call := broker.lastCall(t, tc.path)
			if call.RunID != boundTestRunID || call.Token != boundTestToken {
				t.Fatalf("%s authenticated as (%q, %q), want the bound run's own credentials", tc.name, call.RunID, call.Token)
			}
			tc.check(t, call)
			if object := resultObject(t, res); object["outcome"] != string(tc.outcome) {
				t.Fatalf("%s result = %#v, want %s", tc.name, object, tc.outcome)
			}
		})
	}
}

// TestBoundMessageResumeStaysOnTheOwnerPath proves the routed actions stop where
// they must: a resume is an owner verb, so a bound server still hands it to the
// control connection and never to the conversation's broker.
func TestBoundMessageResumeStaysOnTheOwnerPath(t *testing.T) {
	broker, brokerURL := newFakeBroker(t)
	s, _, cl := declaredBoundServer(t, brokerURL, "")
	if err := s.registry.Store(&RunInfo{RunID: "run_mine", RuntimeID: "rt_mine"}); err != nil {
		t.Fatalf("register the run: %v", err)
	}
	var resumed messaging.ResumeRequest
	cl.resumeFunc = func(req messaging.ResumeRequest) (messaging.ResumeResult, error) {
		resumed = req
		return messaging.ResumeResult{RunID: "run_new", RuntimeID: "rt_new", Label: "again"}, nil
	}

	res, err := s.handleAvenorMessage(context.Background(), toolRequest(t, map[string]any{
		"action": "resume", "target": "run_mine", "message": "again", "idempotency_key": "k1",
	}))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if res.IsError {
		t.Fatalf("resume reported an error: %#v", res.StructuredContent)
	}
	if resumed.Target != "rt_mine" {
		t.Fatalf("resume target = %q, want the resolved runtime", resumed.Target)
	}
	if calls := broker.recorded(); len(calls) != 0 {
		t.Fatalf("resume reached the bound broker: %+v", calls)
	}
	if object := resultObject(t, res); object["outcome"] != "resumed" {
		t.Fatalf("resume result = %#v, want resumed", object)
	}
}

// TestBoundPeersSpeakAsTheBoundRun proves the peers tool reads and writes the
// broker as the conversation's run: a listing, a rename and a last-message read
// all carry its credentials.
func TestBoundPeersSpeakAsTheBoundRun(t *testing.T) {
	broker, brokerURL := newFakeBroker(t, boundTestRunID, boundTestTargetID)
	s, _, _ := declaredBoundServer(t, brokerURL, "")
	ctx := context.Background()

	res, err := s.handleAvenorPeers(ctx, toolRequest(t, map[string]any{"action": "list"}))
	if err != nil {
		t.Fatalf("peers list: %v", err)
	}
	if res.IsError {
		t.Fatalf("peers list reported an error: %#v", res.StructuredContent)
	}
	listing := broker.lastCall(t, "/sessions")
	if listing.RunID != boundTestRunID || listing.Token != boundTestToken {
		t.Fatalf("peers list authenticated as (%q, %q), want the bound run's own credentials", listing.RunID, listing.Token)
	}
	details := resultObject(t, res)
	peers, _ := details["peers"].([]messaging.PeerRecord)
	var self []string
	for _, peer := range peers {
		if peer.Self {
			self = append(self, peer.RunID)
		}
	}
	if len(self) != 1 || self[0] != boundTestRunID {
		t.Fatalf("peers self marks = %v, want the bound run", self)
	}

	res, err = s.handleAvenorPeers(ctx, toolRequest(t, map[string]any{"action": "rename", "name": "renamed"}))
	if err != nil {
		t.Fatalf("peers rename: %v", err)
	}
	if res.IsError {
		t.Fatalf("peers rename reported an error: %#v", res.StructuredContent)
	}
	renamed := broker.lastCall(t, "/rename")
	if renamed.RunID != boundTestRunID || renamed.Token != boundTestToken {
		t.Fatalf("peers rename authenticated as (%q, %q), want the bound run's own credentials", renamed.RunID, renamed.Token)
	}
	if got, _ := renamed.Body["name"].(string); got != "renamed" {
		t.Fatalf("rename name = %q", got)
	}

	res, err = s.handleAvenorPeers(ctx, toolRequest(t, map[string]any{"action": "last", "target": boundTestTargetID}))
	if err != nil {
		t.Fatalf("peers last: %v", err)
	}
	if res.IsError {
		t.Fatalf("peers last reported an error: %#v", res.StructuredContent)
	}
	last := broker.lastCall(t, "/last_message")
	if last.RunID != boundTestRunID || last.Token != boundTestToken {
		t.Fatalf("peers last authenticated as (%q, %q), want the bound run's own credentials", last.RunID, last.Token)
	}
	if query, _ := url.ParseQuery(last.Query); query.Get("target") != boundTestTargetID {
		t.Fatalf("last target = %q, want %q", query.Get("target"), boundTestTargetID)
	}
	if text := toolText(t, res); !strings.Contains(text, "bye") {
		t.Fatalf("last text = %q, want the record's final output", text)
	}
}

// TestBoundMessagePrefersAnExplicitPeerFile proves the peer file a hook puts on
// the call still wins over the launch declaration: a host that names a session
// record speaks as that record, not as the conversation it was launched for.
func TestBoundMessagePrefersAnExplicitPeerFile(t *testing.T) {
	broker, brokerURL := newFakeBroker(t, boundTestRunID, boundTestTargetID, "run_codex")
	s, _, _ := declaredBoundServer(t, brokerURL, "")
	file := writeCodexPeerRecord(t, brokerURL, "run_codex", "tok_codex")

	res, err := s.handleAvenorMessage(context.Background(), toolRequest(t, map[string]any{
		"action": "tell", "target": boundTestTargetID, "message": "hi", "codex_peer_file": file,
	}))
	if err != nil {
		t.Fatalf("tell: %v", err)
	}
	if res.IsError {
		t.Fatalf("tell reported an error: %#v", res.StructuredContent)
	}
	sent := broker.lastCall(t, "/send")
	if sent.RunID != "run_codex" || sent.Token != "tok_codex" {
		t.Fatalf("tell authenticated as (%q, %q), want the explicit peer record", sent.RunID, sent.Token)
	}
}

// TestBoundMessageRoutesAnAlternateSupervisorToItsOwner proves the binding is not
// absolute: a call naming a run another supervisor registered leaves the
// conversation's broker for that supervisor's owner path, so the call reaches the
// store that holds the run.
func TestBoundMessageRoutesAnAlternateSupervisorToItsOwner(t *testing.T) {
	broker, brokerURL := newFakeBroker(t, boundTestRunID)
	s, _, _ := declaredBoundServer(t, brokerURL, "/run/own.sock")
	if err := s.registry.Store(&RunInfo{RunID: "run_alt", RuntimeID: "rt_alt", SupervisorID: "/run/other.sock"}); err != nil {
		t.Fatalf("register the alternate run: %v", err)
	}

	_, err := s.handleAvenorMessage(context.Background(), toolRequest(t, map[string]any{
		"action": "tell", "target": "run_alt", "message": "hi",
	}))
	if err == nil || !strings.Contains(err.Error(), "/run/other.sock") {
		t.Fatalf("tell aimed at another supervisor = %v, want a failure naming that supervisor", err)
	}
	if calls := broker.recorded(); len(calls) != 0 {
		t.Fatalf("a call aimed at another supervisor used the bound broker: %+v", calls)
	}
}

// TestBoundMessageRefusesAConversationTheLaunchNeverBound proves the removed
// fallback: a launch that declared a conversation but never bound it has no
// identity to speak as, so the call is refused instead of being answered as the
// supervisor.
func TestBoundMessageRefusesAConversationTheLaunchNeverBound(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	cl := &bindingClient{}
	cl.setResult(messaging.MessageBindResult{
		BrokerURL: "http://127.0.0.1:1", RunID: boundTestRunID, RunToken: boundTestToken,
	})
	s, _ := declaredServer(t, cl, "poo", "ses_bound", nil)
	t.Cleanup(func() { _ = s.Close() })

	res, err := s.handleAvenorMessage(context.Background(), toolRequest(t, map[string]any{
		"action": "tell", "target": "run_x", "message": "hi",
	}))
	if err != nil {
		t.Fatalf("an unbound conversation returned a transport error instead of a typed refusal: %v", err)
	}
	if !res.IsError {
		t.Fatalf("an unbound conversation answered the call: %#v", res.StructuredContent)
	}
	object := resultObject(t, res)
	want := "the bound host conversation has no active broker identity"
	if object["code"] != "refused" || object["text"] != want {
		t.Fatalf("unbound refusal = %#v, want code refused and %q", object, want)
	}
	if text := toolText(t, res); text != want {
		t.Fatalf("unbound refusal content = %q, want %q", text, want)
	}
}

// TestBoundMessageBrokerFailureKeepsTheCredentialOutOfTheResult proves the
// refusal a broker failure produces is safe to hand a model: a broker that echoes
// the run token back is answered with the sentence, not with the credential, on
// the tool result and in the server's diagnostics.
func TestBoundMessageBrokerFailureKeepsTheCredentialOutOfTheResult(t *testing.T) {
	broker, brokerURL := newFakeBroker(t, boundTestRunID)
	broker.fail("/sessions", http.StatusBadRequest, `{"error":"rejected run token tok_bound"}`)
	s, diag, _ := declaredBoundServer(t, brokerURL, "")

	res, err := s.handleAvenorMessage(context.Background(), toolRequest(t, map[string]any{
		"action": "tell", "target": boundTestTargetID, "message": "hi",
	}))
	if err != nil {
		t.Fatalf("a broker failure returned a transport error instead of a typed refusal: %v", err)
	}
	if !res.IsError {
		t.Fatalf("a broker failure answered the call: %#v", res.StructuredContent)
	}
	object := resultObject(t, res)
	if object["code"] != "refused" {
		t.Fatalf("broker failure code = %v, want refused", object["code"])
	}
	text, _ := object["text"].(string)
	if !strings.Contains(text, "cannot reach its broker") {
		t.Fatalf("broker failure text = %q, want the bound-broker sentence", text)
	}
	encoded, err := json.Marshal(res.StructuredContent)
	if err != nil {
		t.Fatalf("encode the structured result: %v", err)
	}
	for _, surface := range []string{text, toolText(t, res), string(encoded), diag.String()} {
		if strings.Contains(surface, boundTestToken) {
			t.Fatalf("the run token reached a surface: %q", surface)
		}
	}
}
