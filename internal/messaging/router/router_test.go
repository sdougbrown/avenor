package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"reflect"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/runstate"
)

const conformancePath = "../../../schemas/message_route.conformance.json"

// fakeProvider is the provider every conformance case drives. It answers from
// the case's conditions and provider object and records what the router sent
// it, so a case asserts request construction as well as the result.
type fakeProvider struct {
	caller  messaging.Caller
	surface messaging.Surface

	resolution messaging.Resolution
	runFacts   messaging.RunFacts

	outcome         messaging.Outcome
	reply           string
	replyID         string
	fenceFull       bool
	peers           []messaging.Peer
	peerErr         error
	last            messaging.PeerLast
	lastErr         error
	resumed         messaging.ResumeResult
	resumeErr       error
	renamed         messaging.RenameResult
	renameErr       error
	control         json.RawMessage
	controlErr      error
	withdrawErr     error
	replyErr        error
	generation      string
	emptyGeneration bool
	preflightCall   int
	runStateErr     error
	runStateCalls   int

	resolveRef string
	sent       messaging.SendRequest
	waited     messaging.WaitRequest
	replied    messaging.ReplyRequest
	withdrew   messaging.WithdrawRequest
	peered     messaging.PeersRequest
	resumeReq  messaging.ResumeRequest
	renameReq  messaging.RenameRequest
	lastRunID  string
	controlTo  string
}

func (p *fakeProvider) Send(_ context.Context, req messaging.SendRequest) (messaging.SendResult, error) {
	p.sent = req
	if p.fenceFull {
		return messaging.SendResult{Outcome: messaging.OutcomeNotSent, MessageID: req.MessageID}, nil
	}
	if req.NoReply {
		return messaging.SendResult{Outcome: p.outcome, MessageID: req.MessageID}, nil
	}
	if p.outcome == messaging.OutcomeNotSent || p.outcome == messaging.OutcomeAuthRejected {
		return messaging.SendResult{Outcome: p.outcome, MessageID: req.MessageID}, nil
	}
	return messaging.SendResult{Outcome: messaging.OutcomeSent, MessageID: req.MessageID}, nil
}

func (p *fakeProvider) Wait(_ context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	p.waited = req
	result := messaging.WaitResult{Outcome: p.outcome}
	if p.outcome == messaging.OutcomeReplied {
		fields := map[string]string{"message": p.reply}
		if p.replyID != "" {
			fields["id"] = p.replyID
		}
		payload, err := json.Marshal(fields)
		if err != nil {
			return result, err
		}
		result.Reply = &messaging.InboundMessage{MessageID: req.MessageID, FromRunID: "run-asker", Payload: payload}
	}
	return result, nil
}

func (p *fakeProvider) Reply(_ context.Context, req messaging.ReplyRequest) (messaging.SendResult, error) {
	p.replied = req
	if p.replyErr != nil {
		return messaging.SendResult{MessageID: req.MessageID}, p.replyErr
	}
	return messaging.SendResult{Outcome: p.outcome, MessageID: req.MessageID}, nil
}

func (p *fakeProvider) Withdraw(_ context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	p.withdrew = req
	return messaging.WithdrawResult{Outcome: p.outcome}, p.withdrawErr
}

func (p *fakeProvider) Receive(context.Context, messaging.ReceiveRequest) (messaging.ReceiveResult, error) {
	return messaging.ReceiveResult{}, nil
}

func (p *fakeProvider) Peers(_ context.Context, req messaging.PeersRequest) (messaging.PeersResult, error) {
	p.peered = req
	return messaging.PeersResult{Peers: p.peers}, p.peerErr
}

func (p *fakeProvider) Preflight(context.Context) (messaging.PreflightResult, error) {
	p.preflightCall++
	return messaging.PreflightResult{Protocol: messaging.ProtocolVersion, Generation: "preflight-2"}, nil
}

func (p *fakeProvider) Generation() string {
	if p.emptyGeneration {
		return ""
	}
	if p.generation == "" {
		return "gen-7"
	}
	return p.generation
}

func (p *fakeProvider) Close() error { return nil }

func (p *fakeProvider) Resolve(_ context.Context, ref string) (messaging.Resolution, error) {
	p.resolveRef = ref
	return p.resolution, nil
}

func (p *fakeProvider) Caller() messaging.Caller { return p.caller }

func (p *fakeProvider) Surface() messaging.Surface { return p.surface }

func (p *fakeProvider) Role() string { return "agent" }

func (p *fakeProvider) RunState(_ context.Context, runID string) (messaging.RunFacts, error) {
	p.runStateCalls++
	if p.runStateErr != nil {
		return messaging.RunFacts{}, p.runStateErr
	}
	facts := p.runFacts
	facts.RunID = runID
	return facts, nil
}

func (p *fakeProvider) Resume(_ context.Context, req messaging.ResumeRequest) (messaging.ResumeResult, error) {
	p.resumeReq = req
	return p.resumed, p.resumeErr
}

func (p *fakeProvider) Rename(_ context.Context, req messaging.RenameRequest) (messaging.RenameResult, error) {
	p.renameReq = req
	return p.renamed, p.renameErr
}

func (p *fakeProvider) Last(_ context.Context, runID string) (messaging.PeerLast, error) {
	p.lastRunID = runID
	return p.last, p.lastErr
}

// Optional capabilities are separate wrappers, so a surface that has no
// inbound delivery is exactly a provider without AskClaimer.
type claimProvider struct {
	*fakeProvider
	claimed map[string]string
}

func (p claimProvider) ClaimAsk(messageID string) (string, bool) {
	asker, ok := p.claimed[messageID]
	if ok {
		delete(p.claimed, messageID)
	}
	return asker, ok
}

func (p claimProvider) RestoreAsk(messageID, askerRunID string) {
	p.claimed[messageID] = askerRunID
}

type controlProvider struct{ *fakeProvider }

func (p controlProvider) ControlReply(_ context.Context, target string, payload json.RawMessage) error {
	p.controlTo = target
	p.control = payload
	return p.controlErr
}

type claimControlProvider struct{ claimProvider }

func (p claimControlProvider) ControlReply(_ context.Context, target string, payload json.RawMessage) error {
	p.controlTo = target
	p.control = payload
	return p.controlErr
}

func stringOf(value any) string {
	text, _ := value.(string)
	return text
}

func boolOf(value any) bool {
	flag, _ := value.(bool)
	return flag
}

func stringsOf(value any) []string {
	items, _ := value.([]any)
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, stringOf(item))
	}
	return out
}

func objectOf(value any) map[string]any {
	object, _ := value.(map[string]any)
	return object
}

func newFake(t *testing.T, c messaging.ConformanceCase) *fakeProvider {
	t.Helper()
	kind := stringOf(c.Conditions["callerKind"])
	runID := "run-caller"
	if kind == "owner" {
		runID = "supervisor"
	}
	provider := &fakeProvider{
		caller:  messaging.Caller{Kind: messaging.CallerKind(kind), RunID: runID},
		surface: messaging.Surface(stringOf(c.Conditions["surface"])),
		runFacts: messaging.RunFacts{
			Live:      boolOf(c.Conditions["targetLive"]),
			Resumable: boolOf(c.Conditions["targetResumable"]),
		},
		outcome:   messaging.Outcome(stringOf(c.Provider["outcome"])),
		reply:     stringOf(c.Provider["reply"]),
		fenceFull: boolOf(c.Provider["fence_full"]),
	}
	if resolved := objectOf(c.Provider["resolved"]); resolved != nil {
		provider.resolution = messaging.Resolution{
			RunID:       stringOf(resolved["run_id"]),
			PublicRunID: stringOf(resolved["public_run_id"]),
			Label:       stringOf(resolved["label"]),
			Candidates:  stringsOf(resolved["candidates"]),
			Match:       messaging.ResolutionMatch(stringOf(c.Conditions["targetMatch"])),
		}
		if provider.resolution.Match == "" {
			provider.resolution.Match = messaging.MatchUnique
		}
	}
	if state := objectOf(c.Provider["state"]); state != nil {
		provider.runFacts.Status = runstate.RuntimeStatus(stringOf(state["status"]))
	}
	if resumed := objectOf(c.Provider["resume"]); resumed != nil {
		provider.resumed = messaging.ResumeResult{
			RunID:        stringOf(resumed["run_id"]),
			Label:        stringOf(resumed["label"]),
			RuntimeID:    stringOf(resumed["runtime_id"]),
			SessionID:    stringOf(resumed["session_id"]),
			SentinelFile: stringOf(resumed["sentinel_file"]),
			OnEvent:      stringOf(resumed["on_event"]),
		}
	}
	if renamed := objectOf(c.Provider["rename"]); renamed != nil {
		provider.renamed = messaging.RenameResult{
			RunID:         stringOf(renamed["run_id"]),
			Label:         stringOf(renamed["label"]),
			PreviousLabel: stringOf(renamed["previous_label"]),
		}
	}
	if renameErr := objectOf(c.Provider["rename_error"]); renameErr != nil {
		provider.renameErr = &ToolError{Code: stringOf(renameErr["code"]), Message: stringOf(renameErr["message"])}
	}
	if last := objectOf(c.Provider["last"]); last != nil {
		provider.last = messaging.PeerLast{
			RunID:      stringOf(last["run_id"]),
			Label:      stringOf(last["label"]),
			HasRecord:  boolOf(last["has_record"]),
			Text:       stringOf(last["final_output"]),
			StopReason: stringOf(last["stop_reason"]),
			Truncated:  boolOf(last["truncated"]),
		}
	}
	if message := stringOf(c.Provider["last_error"]); message != "" {
		provider.lastErr = errors.New(message)
	}
	if message := stringOf(c.Provider["peers_error"]); message != "" {
		provider.peerErr = errors.New(message)
	}
	for _, peer := range objectsOf(c.Provider["peers"]) {
		session, err := json.Marshal(peer)
		if err != nil {
			t.Fatalf("encode peer: %v", err)
		}
		envelopeRunID := firstNonEmpty(stringOf(peer["peer_run_id"]), stringOf(peer["run_id"]))
		provider.peers = append(provider.peers, messaging.Peer{RunID: envelopeRunID, Session: session})
	}
	return provider
}

func objectsOf(value any) []map[string]any {
	items, _ := value.([]any)
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if object := objectOf(item); object != nil {
			out = append(out, object)
		}
	}
	return out
}

// providerFor applies the case's optional capabilities.
func providerFor(provider *fakeProvider, c messaging.ConformanceCase) Provider {
	claim, hasClaim := c.Provider["claim"].(map[string]any)
	_, hasControl := c.Provider["control_reply"].(map[string]any)
	claimed := map[string]string{}
	if hasClaim {
		claimed[stringOf(claim["message_id"])] = stringOf(claim["asker_run_id"])
	}
	claiming := claimProvider{fakeProvider: provider, claimed: claimed}
	switch {
	case hasClaim && hasControl:
		return claimControlProvider{claiming}
	case hasClaim:
		return claiming
	case hasControl:
		return controlProvider{fakeProvider: provider}
	default:
		return provider
	}
}

func substituteMessageID(text, messageID string) string {
	return strings.ReplaceAll(text, "{message_id}", messageID)
}

func newTestRouter(t *testing.T, provider Provider) *Router {
	t.Helper()
	router, err := NewRouter(provider)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	return router
}

// TestRouterRequiresBoundGeneration makes the generation a construction
// invariant: a provider that cannot name its bound generation is rejected, so
// no request can reach askclient with an empty generation and trigger its
// per-message preflight fallback.
func TestRouterRequiresBoundGeneration(t *testing.T) {
	for _, tc := range []struct {
		name    string
		empty   bool
		wantErr error
	}{
		{"a bound generation constructs", false, nil},
		{"an empty generation fails construction", true, ErrNoGeneration},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := &fakeProvider{emptyGeneration: tc.empty}
			router, err := NewRouter(provider)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			if (router == nil) != (tc.wantErr != nil) {
				t.Fatalf("router = %v with error %v", router, err)
			}
		})
	}
}

func TestConformanceCases(t *testing.T) {
	cases, err := messaging.LoadConformanceCases(conformancePath)
	if err != nil {
		t.Fatalf("load conformance fixture: %v", err)
	}
	if len(cases) == 0 {
		t.Fatal("conformance fixture has no cases")
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			var input struct {
				Action string `json:"action"`
			}
			if err := json.Unmarshal(c.Input, &input); err != nil {
				t.Fatalf("decode case input: %v", err)
			}
			provider := newFake(t, c)
			router := newTestRouter(t, providerFor(provider, c))

			result, err := router.Execute(t.Context(), c.Input)
			messageID := firstNonEmpty(provider.sent.MessageID, provider.replied.MessageID, provider.withdrew.MessageID)

			if c.Expect.Code != "" {
				var toolErr *ToolError
				if !errors.As(err, &toolErr) {
					t.Fatalf("error = %v, want tool error %q", err, c.Expect.Code)
				}
				if toolErr.Code != c.Expect.Code {
					t.Fatalf("code = %q, want %q", toolErr.Code, c.Expect.Code)
				}
				if got, want := toolErr.Message, c.Expect.Message; got != want {
					t.Fatalf("message = %q, want %q", got, want)
				}
				if len(c.Expect.Facts) == 0 {
					if len(toolErr.Facts) != 0 {
						t.Fatalf("facts = %v, want none", toolErr.Facts)
					}
					assertNoTransport(t, provider)
					return
				}
				if !reflect.DeepEqual(toolErr.Facts, c.Expect.Facts) {
					t.Fatalf("facts = %v, want %v", toolErr.Facts, c.Expect.Facts)
				}
				assertNoTransport(t, provider)
				return
			}
			if err != nil {
				t.Fatalf("execute: %v", err)
			}
			if result.Text == "" {
				t.Fatal("a successful call produced no text")
			}
			if got, want := result.Outcome, messaging.Outcome(c.Expect.Outcome); got != want {
				t.Fatalf("outcome = %q, want %q", got, want)
			}
			if got, want := result.Text, substituteMessageID(c.Expect.Message, messageID); got != want {
				t.Fatalf("text = %q, want %q", got, want)
			}
			if got, want := result.Action, input.Action; got != want {
				t.Fatalf("action = %q, want %q", got, want)
			}
			if messageID != "" && result.MessageID != messageID {
				t.Fatalf("message id = %q, want %q", result.MessageID, messageID)
			}
			if want := c.Expect.Facts["message_id"]; want != "" {
				if got := quote(result.MessageID); got != substituteMessageID(want, messageID) {
					t.Fatalf("message_id fact = %q, want %q", got, substituteMessageID(want, messageID))
				}
			}
			if want := c.Expect.Facts["target"]; want != "" && quote(result.Label) != want {
				t.Fatalf("target fact = %q, want %q", quote(result.Label), want)
			}
			if provider.resumed.Label != "" && input.Action == "resume" && result.Label != provider.resumed.Label {
				t.Fatalf("label = %q, want %q", result.Label, provider.resumed.Label)
			}
			if input.Action == "ask" && result.Outcome == messaging.OutcomeReplied {
				if result.Reply != provider.reply {
					t.Fatalf("reply = %q, want %q", result.Reply, provider.reply)
				}
			} else if result.Reply != "" {
				t.Fatalf("reply = %q, want empty", result.Reply)
			}
			if input.Action == "resume" {
				if result.SentinelFile != provider.resumed.SentinelFile || result.OnEvent != provider.resumed.OnEvent {
					t.Fatalf("resume paths = %q/%q, want %q/%q", result.SentinelFile, result.OnEvent, provider.resumed.SentinelFile, provider.resumed.OnEvent)
				}
			}
			assertRequest(t, c, provider, messageID)
		})
	}
}

// assertNoTransport proves a rejected call never reached the provider's
// transport or runtime write methods.
func assertNoTransport(t *testing.T, provider *fakeProvider) {
	t.Helper()
	if provider.sent.MessageID != "" || provider.replied.MessageID != "" || provider.withdrew.MessageID != "" ||
		provider.peered.Generation != "" || provider.resumeReq != (messaging.ResumeRequest{}) ||
		provider.renameReq != (messaging.RenameRequest{}) || provider.lastRunID != "" ||
		provider.controlTo != "" || len(provider.control) != 0 || provider.preflightCall != 0 {
		t.Fatal("a rejected request reached the provider")
	}
}

// assertRequest asserts what the case says the router must have sent the
// provider, and the request-construction facts a case does not express as text.
func assertRequest(t *testing.T, c messaging.ConformanceCase, provider *fakeProvider, messageID string) {
	t.Helper()
	if ref := stringOf(c.Provider["resolve_ref"]); ref != "" && provider.resolveRef != ref {
		t.Fatalf("resolved %q, want %q", provider.resolveRef, ref)
	}
	request := objectOf(c.Provider["request"])
	var input struct {
		Action string `json:"action"`
	}
	if err := json.Unmarshal(c.Input, &input); err != nil {
		t.Fatalf("decode case input: %v", err)
	}
	substitute := func(value string) string { return substituteMessageID(value, messageID) }

	switch input.Action {
	case "tell", "ask":
		if got, want := provider.sent.To.RunID, provider.resolution.RunID; got != want {
			t.Fatalf("send target = %q, want %q", got, want)
		}
		if got := provider.sent.Generation; got != "gen-7" {
			t.Fatalf("send generation = %q, want gen-7", got)
		}
		var body struct {
			Message string `json:"message"`
		}
		if err := json.Unmarshal([]byte(provider.sent.Body), &body); err != nil {
			t.Fatalf("decode send body: %v", err)
		}
		if input.Action == "tell" && !provider.sent.NoReply {
			t.Fatal("tell must send with NoReply set")
		}
		if input.Action == "ask" {
			if provider.sent.NoReply {
				t.Fatal("ask must send with NoReply unset")
			}
			sendFailed := c.Expect.Code == "" && (provider.outcome == messaging.OutcomeNotSent || provider.outcome == messaging.OutcomeAuthRejected)
			if !sendFailed && provider.waited.MessageID == "" {
				t.Fatal("ask reached no wait")
			}
			if got := provider.waited.Generation; got != "" && got != "gen-7" {
				t.Fatalf("wait generation = %q, want gen-7", got)
			}
		}
		if want := stringOf(request["message"]); want != "" && body.Message != want {
			t.Fatalf("message = %q, want %q", body.Message, want)
		}
		if want, ok := request["no_reply"]; ok && provider.sent.NoReply != boolOf(want) {
			t.Fatalf("no_reply = %v, want %v", provider.sent.NoReply, boolOf(want))
		}
		if want, ok := request["timeout_ms"]; ok && provider.waited.Timeout.Milliseconds() != int64(want.(float64)) {
			t.Fatalf("timeout_ms = %d, want %v", provider.waited.Timeout.Milliseconds(), want)
		}
	case "reply":
		if want, ok := request["to_run_id"]; ok && provider.replied.ToRunID != substitute(stringOf(want)) {
			t.Fatalf("reply target = %q, want %q", provider.replied.ToRunID, stringOf(want))
		}
		if want := stringOf(request["message_id"]); want != "" && provider.replied.MessageID != want {
			t.Fatalf("reply message_id = %q, want %q", provider.replied.MessageID, want)
		}
		if want := stringOf(request["correlation_id"]); want != "" && provider.replied.CorrelationID != want {
			t.Fatalf("reply correlation_id = %q, want %q", provider.replied.CorrelationID, want)
		}
		if provider.replied.MessageID == "" {
			return
		}
		var body struct {
			Message string `json:"message"`
		}
		if err := json.Unmarshal([]byte(provider.replied.Body), &body); err != nil {
			t.Fatalf("decode reply body: %v", err)
		}
		if got := body.Message; got != stringOf(request["message"]) && stringOf(request["message"]) != "" {
			t.Fatalf("reply message = %q, want %q", got, stringOf(request["message"]))
		}
	case "withdraw":
		if want := stringOf(request["message_id"]); want != "" && provider.withdrew.MessageID != want {
			t.Fatalf("withdraw message_id = %q, want %q", provider.withdrew.MessageID, want)
		}
		if provider.withdrew.Generation != "gen-7" {
			t.Fatalf("withdraw generation = %q, want gen-7", provider.withdrew.Generation)
		}
	case "control_reply":
		expected := objectOf(c.Provider["control_reply"])
		controlRequest := objectOf(expected["request"])
		if want := stringOf(controlRequest["target"]); want != "" && provider.controlTo != want {
			t.Fatalf("control_reply target = %q, want %q", provider.controlTo, want)
		}
		if expected != nil {
			wantRaw, err := json.Marshal(expected["payload"])
			if err != nil {
				t.Fatalf("encode expected payload: %v", err)
			}
			var got, want any
			if err := json.Unmarshal(provider.control, &got); err != nil {
				t.Fatalf("decode control payload %s: %v", provider.control, err)
			}
			if err := json.Unmarshal(wantRaw, &want); err != nil {
				t.Fatalf("decode expected payload %s: %v", wantRaw, err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("control_reply payload = %s, want %s", provider.control, wantRaw)
			}
		}
	case "resume":
		if want := stringOf(request["target"]); want != "" && provider.resumeReq.Target != want {
			t.Fatalf("resume target = %q, want %q", provider.resumeReq.Target, want)
		}
		if want := stringOf(request["message"]); want != "" && provider.resumeReq.Message != want {
			t.Fatalf("resume message = %q, want %q", provider.resumeReq.Message, want)
		}
		if want := stringOf(request["label"]); want != "" && provider.resumeReq.Label != want {
			t.Fatalf("resume label = %q, want %q", provider.resumeReq.Label, want)
		}
		if want := stringOf(request["idempotency_key"]); want != "" && provider.resumeReq.IdempotencyKey != want {
			t.Fatalf("resume idempotency_key = %q, want %q", provider.resumeReq.IdempotencyKey, want)
		}
		if provider.resumeReq.RunID != "" || provider.resumeReq.SentinelFile != "" || provider.resumeReq.EventLog != "" {
			t.Fatalf("resume minted provider paths: %+v", provider.resumeReq)
		}
	}
}

// TestPermissionReasonsMatchFixture pins the two permission reasons to the
// fixture's text. They are read from the generated route table through one
// extraction point, so a generator change that drops a rule fails here.
func TestPermissionReasonsMatchFixture(t *testing.T) {
	cases, err := messaging.LoadConformanceCases(conformancePath)
	if err != nil {
		t.Fatalf("load conformance fixture: %v", err)
	}
	want := map[string]string{}
	for _, c := range cases {
		switch c.Expect.Code {
		case "unsupported_action", "not_permitted":
			want[c.Expect.Code] = c.Expect.Message
		}
	}
	controlReply := actionControlReply
	availability := messaging.Check(
		messaging.MessageRouteFields{Action: &controlReply},
		messaging.MessageRouteConditions{CallerKind: string(messaging.CallerBroker), ControlReplySupported: false},
		messaging.MessageRouteFields{},
	)
	if got := firstReason(availability.Target); got != want["unsupported_action"] {
		t.Fatalf("unsupported_action reason = %q, want %q", got, want["unsupported_action"])
	}
	resume := actionResume
	availability = messaging.Check(
		messaging.MessageRouteFields{Action: &resume},
		messaging.MessageRouteConditions{CallerKind: string(messaging.CallerBroker), ControlReplySupported: true},
		messaging.MessageRouteFields{},
	)
	if got := firstReason(availability.Target); got != want["not_permitted"] {
		t.Fatalf("not_permitted reason = %q, want %q", got, want["not_permitted"])
	}
}

// TestOutcomeOutsideVocabularyFallsBackToPossiblyPending guards the "text on
// every result" rule for an outcome that is not in the vocabulary at all: it is
// reported as possibly-pending with text rather than as an outcome with no
// sentence.
func TestOutcomeOutsideVocabularyFallsBackToPossiblyPending(t *testing.T) {
	provider := &fakeProvider{
		caller:     messaging.Caller{Kind: messaging.CallerBroker, RunID: "run-caller"},
		surface:    messaging.SurfaceChanneltools,
		resolution: messaging.Resolution{RunID: "rt_7", Label: "mesh-a", Match: messaging.MatchUnique},
		runFacts:   messaging.RunFacts{Status: runstate.StatusRunning, Live: true},
		outcome:    messaging.Outcome("mystery"),
	}
	result, err := newTestRouter(t, provider).Execute(t.Context(), json.RawMessage(`{"action": "tell", "target": "rt_7", "message": "hi"}`))
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.Outcome != messaging.OutcomePossiblyPending {
		t.Fatalf("outcome = %q, want possibly-pending", result.Outcome)
	}
	if !strings.Contains(result.Text, "may still be pending") || result.MessageID == "" {
		t.Fatalf("text = %q, message id = %q", result.Text, result.MessageID)
	}
}

// TestInVocabularyUnlistedOutcomeIsNotRewritten makes a missing matrix row a
// visible gap: an outcome the vocabulary defines but the action does not list is
// returned as-is with no text, never silently rewritten to possibly-pending.
func TestInVocabularyUnlistedOutcomeIsNotRewritten(t *testing.T) {
	outcome, text := outcomeText(actionTell, messaging.OutcomeLost, map[string]string{
		"message_id": quote("av1.t.1"),
		"target":     quote("mesh-a"),
	})
	if outcome != messaging.OutcomeLost {
		t.Fatalf("outcome = %q, want lost", outcome)
	}
	if text != "" {
		t.Fatalf("text = %q, want empty for an unlisted pair", text)
	}
}

// TestReachableOutcomesAreRenderable is the matrix totality check the router
// itself relies on: every outcome a transport can return for an action has a
// render template, so the fallback in outcomeText never fires for a
// vocabulary outcome. The sets follow the transports: tell is limited to the
// send outcomes IsSendOutcome declares, while ask, reply and withdraw pass
// their provider's outcome through unfiltered and so may see the whole
// vocabulary.
func TestReachableOutcomesAreRenderable(t *testing.T) {
	vocabulary := []messaging.Outcome{
		messaging.OutcomeSent, messaging.OutcomeReplied, messaging.OutcomeTimedOut,
		messaging.OutcomeWithdrawn, messaging.OutcomeEdgeAbsent, messaging.OutcomeAuthRejected,
		messaging.OutcomeNotSent, messaging.OutcomePossiblyPending, messaging.OutcomeLost,
	}
	send := make([]messaging.Outcome, 0, len(vocabulary))
	for _, outcome := range vocabulary {
		if messaging.IsSendOutcome(outcome) {
			send = append(send, outcome)
		}
	}
	wait := []messaging.Outcome{
		messaging.OutcomeReplied, messaging.OutcomeTimedOut, messaging.OutcomeWithdrawn,
		messaging.OutcomeEdgeAbsent, messaging.OutcomePossiblyPending, messaging.OutcomeLost,
	}
	ask := append([]messaging.Outcome(nil), wait...)
	for _, outcome := range send {
		if outcome != messaging.OutcomeSent {
			ask = append(ask, outcome)
		}
	}
	reachable := map[string][]messaging.Outcome{
		actionTell:         send,
		actionAsk:          ask,
		actionReply:        vocabulary,
		actionWithdraw:     vocabulary,
		actionResume:       {outcomeResumed},
		actionControlReply: {messaging.OutcomeSent},
	}
	for action, outcomes := range reachable {
		for _, outcome := range outcomes {
			if !rendersOutcome(action, outcome) {
				t.Fatalf("%s:%s has no render template", action, outcome)
			}
		}
	}
}

// TestReplyClaimRestoredUnlessDelivered pins the claim rule: only a replied
// outcome proves the asker received the answer, so every other outcome leaves
// the ask claimable.
func TestReplyClaimRestoredUnlessDelivered(t *testing.T) {
	for _, tc := range []struct {
		name     string
		outcome  messaging.Outcome
		restored bool
	}{
		{"replied consumes the claim", messaging.OutcomeReplied, false},
		{"not-sent restores the claim", messaging.OutcomeNotSent, true},
		{"possibly-pending restores the claim", messaging.OutcomePossiblyPending, true},
		{"edge-absent restores the claim", messaging.OutcomeEdgeAbsent, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := &fakeProvider{
				caller:  messaging.Caller{Kind: messaging.CallerBroker, RunID: "run-caller"},
				outcome: tc.outcome,
			}
			claimed := map[string]string{"av1.reply.1": "run-asker"}
			router := newTestRouter(t, claimProvider{fakeProvider: provider, claimed: claimed})
			if _, err := router.Execute(t.Context(), json.RawMessage(`{"action": "reply", "reply_to": "av1.reply.1", "message": "OK"}`)); err != nil {
				t.Fatalf("execute: %v", err)
			}
			_, restored := claimed["av1.reply.1"]
			if restored != tc.restored {
				t.Fatalf("claim restored = %v, want %v", restored, tc.restored)
			}
		})
	}
}

// TestReplyTransportFailureIsRefusedOnBothPaths pins the unified failure
// rule: a Reply call whose provider returns an error with no outcome is a
// refusal on the claim path and the tell path alike, and a claimed ask stays
// answerable after the refusal.
func TestReplyTransportFailureIsRefusedOnBothPaths(t *testing.T) {
	for _, tc := range []struct {
		name    string
		claimed bool
	}{
		{"a claimed ask", true},
		{"a tell with no claim", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := &fakeProvider{
				caller:   messaging.Caller{Kind: messaging.CallerBroker, RunID: "run-caller"},
				outcome:  messaging.OutcomePossiblyPending,
				replyErr: errors.New("broker down"),
			}
			claimed := map[string]string{}
			if tc.claimed {
				claimed["av1.reply.1"] = "run-asker"
			}
			router := newTestRouter(t, claimProvider{fakeProvider: provider, claimed: claimed})
			_, err := router.Execute(t.Context(), json.RawMessage(`{"action": "reply", "reply_to": "av1.reply.1", "message": "OK"}`))
			var toolErr *ToolError
			if !errors.As(err, &toolErr) {
				t.Fatalf("error = %v, want a tool error", err)
			}
			if toolErr.Code != "refused" {
				t.Fatalf("code = %q, want refused", toolErr.Code)
			}
			if tc.claimed {
				if _, ok := claimed["av1.reply.1"]; !ok {
					t.Fatal("the claimed ask was consumed by a refused reply")
				}
			}
		})
	}
}

// TestResumePermissionPrecedesStateRead pins the phase order: a broker resume
// is denied at the permission phase, before the router reads the target's run
// state, so an unreadable sentinel cannot turn not_permitted into a refusal for
// the caller the rule exists to stop.
func TestResumePermissionPrecedesStateRead(t *testing.T) {
	provider := &fakeProvider{
		caller:     messaging.Caller{Kind: messaging.CallerBroker, RunID: "run-caller"},
		resolution: messaging.Resolution{RunID: "rt_9", Label: "mesh-b", Match: messaging.MatchUnique},
		runStateErr: &fs.PathError{
			Op:   "open",
			Path: "/home/u/.avenor/runs/run-9/sentinel.done",
			Err:  fs.ErrNotExist,
		},
	}
	_, err := newTestRouter(t, provider).Execute(t.Context(), json.RawMessage(`{"action": "resume", "target": "rt_9", "message": "again"}`))
	var toolErr *ToolError
	if !errors.As(err, &toolErr) {
		t.Fatalf("error = %v, want a tool error", err)
	}
	if toolErr.Code != "not_permitted" {
		t.Fatalf("code = %q, want not_permitted", toolErr.Code)
	}
	if provider.runStateCalls != 0 {
		t.Fatalf("RunState was called %d times before permission", provider.runStateCalls)
	}
}

// TestKeyedResumeReachesTheAuthorityWithoutTheStateGate pins the resume
// short-circuit: a keyed resume skips the RunState + stateDenial gate and
// reaches the resume authority even when the state read fails, because the
// authority checks idempotency before mutable run state and a replay must get
// the stored result instead of a not_resumable refusal for a turn the first
// resume already ended. An unkeyed resume still takes the state gate.
func TestKeyedResumeReachesTheAuthorityWithoutTheStateGate(t *testing.T) {
	for _, tc := range []struct {
		name          string
		input         string
		runStateErr   error
		wantResume    bool
		wantRunStates int
		wantCode      string
	}{
		{
			name:          "a keyed resume skips the state gate and reaches resume",
			input:         `{"action": "resume", "target": "rt_9", "message": "again", "idempotency_key": "key-1"}`,
			runStateErr:   errors.New("state unavailable"),
			wantResume:    true,
			wantRunStates: 0,
		},
		{
			name:        "an unkeyed resume still takes the state gate",
			input:       `{"action": "resume", "target": "rt_9", "message": "again"}`,
			runStateErr: errors.New("state unavailable"),
			wantCode:    "refused",
		},
		{
			name:     "an unkeyed resume on a live target is refused by the state gate",
			input:    `{"action": "resume", "target": "rt_9", "message": "again"}`,
			wantCode: "target_not_terminal",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := &fakeProvider{
				caller:      messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"},
				resolution:  messaging.Resolution{RunID: "rt_9", Label: "mesh-b", Match: messaging.MatchUnique},
				runStateErr: tc.runStateErr,
				runFacts:    messaging.RunFacts{Live: true},
				resumed:     messaging.ResumeResult{RunID: "run-2", Label: "follow"},
			}
			result, err := newTestRouter(t, provider).Execute(t.Context(), json.RawMessage(tc.input))
			if tc.wantCode != "" {
				var toolErr *ToolError
				if !errors.As(err, &toolErr) || toolErr.Code != tc.wantCode {
					t.Fatalf("error = %v, want the %s tool error", err, tc.wantCode)
				}
				if provider.resumeReq != (messaging.ResumeRequest{}) {
					t.Fatal("a gated resume reached the authority")
				}
				return
			}
			if err != nil {
				t.Fatalf("execute: %v", err)
			}
			if provider.resumeReq != (messaging.ResumeRequest{}) != tc.wantResume {
				t.Fatalf("resume reached authority = %v, want %v", provider.resumeReq != messaging.ResumeRequest{}, tc.wantResume)
			}
			if got := provider.resumeReq.IdempotencyKey; got != "key-1" {
				t.Fatalf("resume idempotency_key = %q, want key-1", got)
			}
			if provider.runStateCalls != tc.wantRunStates {
				t.Fatalf("RunState calls = %d, want %d", provider.runStateCalls, tc.wantRunStates)
			}
			if result.Outcome != outcomeResumed {
				t.Fatalf("outcome = %q, want resumed", result.Outcome)
			}
		})
	}
}

// TestWithdrawFenceCapacityReportsPossiblyPending keeps the router from claiming
// "nothing is pending" when the absence fence could not be installed and a
// delayed send can still be admitted.
func TestWithdrawFenceCapacityReportsPossiblyPending(t *testing.T) {
	provider := &fakeProvider{
		caller:      messaging.Caller{Kind: messaging.CallerBroker, RunID: "run-caller"},
		outcome:     messaging.OutcomeEdgeAbsent,
		withdrawErr: messaging.ErrFenceCapacity,
	}
	result, err := newTestRouter(t, provider).Execute(t.Context(), json.RawMessage(`{"action": "withdraw", "message_id": "av1.w.1"}`))
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.Outcome != messaging.OutcomePossiblyPending {
		t.Fatalf("outcome = %q, want possibly-pending", result.Outcome)
	}
	if !strings.Contains(result.Text, "may still be pending") {
		t.Fatalf("text = %q", result.Text)
	}
}

// TestRefusedClassifiesPathErrors keeps an absolute provider path out of tool
// text.
func TestRefusedClassifiesPathErrors(t *testing.T) {
	toolErr := refused(&fs.PathError{Op: "open", Path: "/home/u/.avenor/runs/run-1/sentinel.done", Err: fs.ErrNotExist})
	if want := messaging.Repair("refused_io", nil); toolErr.Message != want {
		t.Fatalf("message = %q, want %q", toolErr.Message, want)
	}
	if strings.Contains(toolErr.Message, "/home/u") {
		t.Fatalf("message leaks a path: %q", toolErr.Message)
	}
}

// TestRouterUsesBoundGeneration proves the router never re-reads the generation
// per call: every request carries the provider's construction-time binding.
func TestRouterUsesBoundGeneration(t *testing.T) {
	live := messaging.RunFacts{Status: runstate.StatusRunning, Live: true}
	for _, tc := range []struct {
		name       string
		input      string
		generation func(*fakeProvider) string
	}{
		{"tell", `{"action": "tell", "target": "rt_7", "message": "hi"}`, func(p *fakeProvider) string { return p.sent.Generation }},
		{"ask", `{"action": "ask", "target": "rt_7", "message": "hi"}`, func(p *fakeProvider) string { return p.sent.Generation }},
		{"withdraw", `{"action": "withdraw", "message_id": "av1.w.1"}`, func(p *fakeProvider) string { return p.withdrew.Generation }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := &fakeProvider{
				caller:     messaging.Caller{Kind: messaging.CallerBroker, RunID: "run-caller"},
				resolution: messaging.Resolution{RunID: "rt_7", Label: "mesh-a", Match: messaging.MatchUnique},
				runFacts:   live,
				outcome:    messaging.OutcomeReplied,
				reply:      "OK",
				generation: "bound-9",
			}
			if _, err := newTestRouter(t, provider).Execute(t.Context(), json.RawMessage(tc.input)); err != nil {
				t.Fatalf("execute: %v", err)
			}
			if provider.preflightCall != 0 {
				t.Fatalf("router called Preflight %d times", provider.preflightCall)
			}
			if got := tc.generation(provider); got != "bound-9" {
				t.Fatalf("generation = %q, want bound-9", got)
			}
		})
	}

	provider := &fakeProvider{
		caller:     messaging.Caller{Kind: messaging.CallerBroker, RunID: "run-caller"},
		generation: "bound-9",
	}
	if _, err := ExecutePeers(t.Context(), provider, json.RawMessage(`{"action": "list"}`)); err != nil {
		t.Fatalf("execute peers: %v", err)
	}
	if provider.preflightCall != 0 || provider.peered.Generation != "bound-9" {
		t.Fatalf("peers preflight=%d generation=%q", provider.preflightCall, provider.peered.Generation)
	}
}

// TestQuoteEscapesNonPrintable pins the label-safety rule: a producer-controlled
// label must not be able to smuggle a bidi override or a zero-width character
// into tool text.
func TestQuoteEscapesNonPrintable(t *testing.T) {
	const raw = "a\u200bb\u202ec\u0085d\"e\\f\ng"
	const want = "\"a\\u200bb\\u202ec\\u0085d\\\"e\\\\f\\ng\""
	if got := quote(raw); got != want {
		t.Fatalf("quote = %q, want %q", got, want)
	}
	if got := sanitize(raw); got != want[1:len(want)-1] {
		t.Fatalf("sanitize = %q, want %q", got, want[1:len(want)-1])
	}
	for _, forbidden := range []string{"\u200b", "\u202e", "\u0085"} {
		if strings.Contains(quote(raw), forbidden) || strings.Contains(sanitize(raw), forbidden) {
			t.Fatalf("escaped value still carries %q", forbidden)
		}
	}
}

// TestIntegerWireTypeAcceptsMathematicalIntegers pins the integer verdict the
// schema and the table now share: 1.0 and 1e3 are integers, 1.5 is not.
func TestIntegerWireTypeAcceptsMathematicalIntegers(t *testing.T) {
	for _, raw := range []string{"1", "1.0", "1e3", "-2.0"} {
		if !hasWireType(json.RawMessage(raw), "integer") {
			t.Fatalf("%s should be an integer", raw)
		}
	}
	for _, raw := range []string{"1.5", "1e-1", `"1"`, "999999999999999999999"} {
		if hasWireType(json.RawMessage(raw), "integer") {
			t.Fatalf("%s should not be an integer", raw)
		}
	}
}

// TestValidateIntegerConstraintEnforcesAZeroMaximum pins the declared bound's
// presence, not its truthiness: a schema may legally cap an integer at zero,
// and the table must still refuse everything above it.
func TestValidateIntegerConstraintEnforcesAZeroMaximum(t *testing.T) {
	constraint := messaging.PropertyConstraints{Maximum: 0, HasMaximum: true}
	if detail := validateIntegerConstraint("timeout_ms", json.RawMessage(`1`), constraint); detail != `property "timeout_ms" must be at most 0` {
		t.Fatalf("detail = %q, want the zero maximum to refuse 1", detail)
	}
	if detail := validateIntegerConstraint("timeout_ms", json.RawMessage(`0`), constraint); detail != "" {
		t.Fatalf("detail = %q, want the zero maximum to accept 0", detail)
	}
}

// TestToolFailure pins the one mapping a surface uses to render a router
// refusal, including the unwrapping the child surfaces rely on.
func TestToolFailure(t *testing.T) {
	toolErr := &ToolError{Code: "invalid_name", Message: "rename refused: bad name"}

	t.Run("tool error", func(t *testing.T) {
		object, text, ok := ToolFailure(toolErr)
		if !ok {
			t.Fatal("ToolFailure(*ToolError) ok = false, want true")
		}
		if !reflect.DeepEqual(object, toolErr.Output()) {
			t.Fatalf("object = %#v, want %#v", object, toolErr.Output())
		}
		if text != toolErr.Message {
			t.Fatalf("text = %q, want %q", text, toolErr.Message)
		}
	})

	t.Run("wrapped tool error", func(t *testing.T) {
		object, text, ok := ToolFailure(fmt.Errorf("context: %w", toolErr))
		if !ok {
			t.Fatal("ToolFailure(wrapped *ToolError) ok = false, want true")
		}
		if !reflect.DeepEqual(object, toolErr.Output()) {
			t.Fatalf("object = %#v, want %#v", object, toolErr.Output())
		}
		if text != toolErr.Message {
			t.Fatalf("text = %q, want %q", text, toolErr.Message)
		}
	})

	t.Run("other error", func(t *testing.T) {
		if object, text, ok := ToolFailure(errors.New("boom")); ok {
			t.Fatalf("ToolFailure(plain error) = %#v, %q, true; want false", object, text)
		}
	})
}

// TestAskResultCarriesTheReplyRouteKey pins that an ask reply exposes the
// reply's own route key, so the asker can answer the answer instead of reading
// the raw wait payload.
func TestAskResultCarriesTheReplyRouteKey(t *testing.T) {
	provider := &fakeProvider{
		caller:     messaging.Caller{Kind: messaging.CallerBroker, RunID: "run-caller"},
		resolution: messaging.Resolution{RunID: "rt_7", Label: "mesh-a", Match: messaging.MatchUnique},
		runFacts:   messaging.RunFacts{Status: runstate.StatusRunning, Live: true},
		outcome:    messaging.OutcomeReplied,
		reply:      "pong",
		replyID:    "ask-1.reply",
	}
	got, err := newTestRouter(t, provider).Execute(t.Context(), json.RawMessage(`{"action": "ask", "target": "rt_7", "message": "ping"}`))
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if got.ReplyID != "ask-1.reply" {
		t.Fatalf("reply_id = %q, want the reply's route key", got.ReplyID)
	}
	if out := got.Output(); out["reply_id"] != "ask-1.reply" {
		t.Fatalf("output reply_id = %v, want the reply's route key", out["reply_id"])
	}
}

// TestAskReplyIDIsEmptyWithoutAnID pins the absence rule: a reply payload with
// no id yields no reply_id, so the result never exposes an empty handle.
func TestAskReplyIDIsEmptyWithoutAnID(t *testing.T) {
	provider := &fakeProvider{
		caller:     messaging.Caller{Kind: messaging.CallerBroker, RunID: "run-caller"},
		resolution: messaging.Resolution{RunID: "rt_7", Label: "mesh-a", Match: messaging.MatchUnique},
		runFacts:   messaging.RunFacts{Status: runstate.StatusRunning, Live: true},
		outcome:    messaging.OutcomeReplied,
		reply:      "pong",
	}
	got, err := newTestRouter(t, provider).Execute(t.Context(), json.RawMessage(`{"action": "ask", "target": "rt_7", "message": "ping"}`))
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if _, ok := got.Output()["reply_id"]; ok {
		t.Fatal("output carried a reply_id for a reply without an id")
	}
}
