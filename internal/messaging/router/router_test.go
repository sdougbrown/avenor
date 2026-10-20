package router

import (
	"context"
	"encoding/json"
	"errors"
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

	outcome    messaging.Outcome
	reply      string
	peers      []messaging.Peer
	peerErr    error
	last       messaging.PeerLast
	lastErr    error
	resumed    messaging.ResumeResult
	resumeErr  error
	renamed    messaging.RenameResult
	renameErr  error
	control    json.RawMessage
	controlErr error

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
		payload, err := json.Marshal(map[string]string{"message": p.reply})
		if err != nil {
			return result, err
		}
		result.Reply = &messaging.InboundMessage{MessageID: req.MessageID, FromRunID: "run-asker", Payload: payload}
	}
	return result, nil
}

func (p *fakeProvider) Reply(_ context.Context, req messaging.ReplyRequest) (messaging.SendResult, error) {
	p.replied = req
	return messaging.SendResult{Outcome: p.outcome, MessageID: req.MessageID}, nil
}

func (p *fakeProvider) Withdraw(_ context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	p.withdrew = req
	return messaging.WithdrawResult{Outcome: p.outcome}, nil
}

func (p *fakeProvider) Receive(context.Context, messaging.ReceiveRequest) (messaging.ReceiveResult, error) {
	return messaging.ReceiveResult{}, nil
}

func (p *fakeProvider) Peers(_ context.Context, req messaging.PeersRequest) (messaging.PeersResult, error) {
	p.peered = req
	return messaging.PeersResult{Peers: p.peers}, p.peerErr
}

func (p *fakeProvider) Preflight(context.Context) (messaging.PreflightResult, error) {
	return messaging.PreflightResult{Protocol: messaging.ProtocolVersion, Generation: "gen-7"}, nil
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
		outcome: messaging.Outcome(stringOf(c.Provider["outcome"])),
		reply:   stringOf(c.Provider["reply"]),
	}
	if resolved := objectOf(c.Provider["resolved"]); resolved != nil {
		provider.resolution = messaging.Resolution{
			RunID:       stringOf(resolved["run_id"]),
			PublicRunID: stringOf(resolved["public_run_id"]),
			Label:       stringOf(resolved["label"]),
			Kind:        stringOf(resolved["kind"]),
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
	for _, peer := range objectsOf(c.Provider["peers"]) {
		session, err := json.Marshal(peer)
		if err != nil {
			t.Fatalf("encode peer: %v", err)
		}
		provider.peers = append(provider.peers, messaging.Peer{RunID: stringOf(peer["run_id"]), Session: session})
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
			provider := newFake(t, c)
			router := NewRouter(providerFor(provider, c))

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
					return
				}
				if !reflect.DeepEqual(toolErr.Facts, c.Expect.Facts) {
					t.Fatalf("facts = %v, want %v", toolErr.Facts, c.Expect.Facts)
				}
				return
			}
			if err != nil {
				t.Fatalf("execute: %v", err)
			}
			if got, want := result.Outcome, messaging.Outcome(c.Expect.Outcome); got != want {
				t.Fatalf("outcome = %q, want %q", got, want)
			}
			if got, want := result.Text, substituteMessageID(c.Expect.Message, messageID); got != want {
				t.Fatalf("text = %q, want %q", got, want)
			}
			if c.Expect.Outcome != "" && result.Action == "" {
				t.Fatal("result carries no action")
			}
			assertRequest(t, c, provider, messageID)
		})
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
		if got, want := provider.replied.ToRunID, substitute(stringOf(request["to_run_id"])); want != "" && got != want {
			t.Fatalf("reply target = %q, want %q", got, want)
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

// TestInvalidRequestMatrix is the Go twin of the TypeScript router's
// assertMessageInput test: for each action, an unknown property, a missing
// required property and a wrong-typed property each produce invalid_request
// with the exact fixed detail sentence and no provider call.
func TestInvalidRequestMatrix(t *testing.T) {
	required := map[string][]string{
		"tell":          {"target", "message"},
		"ask":           {"target", "message"},
		"reply":         {"reply_to", "message"},
		"resume":        {"target", "message"},
		"withdraw":      {"message_id"},
		"control_reply": {"target"},
	}
	wrongType := map[string]struct {
		property string
		value    any
		detail   string
	}{
		"tell":          {"target", 5, `property "target" must be a string`},
		"ask":           {"timeout_ms", 1.5, `property "timeout_ms" must be an integer`},
		"reply":         {"reply_to", true, `property "reply_to" must be a string`},
		"resume":        {"label", 5, `property "label" must be a string`},
		"withdraw":      {"message_id", []any{"x"}, `property "message_id" must be a string`},
		"control_reply": {"payload", "x", `property "payload" must be an object`},
	}
	for action, keys := range required {
		valid := map[string]any{"action": action}
		if action == "reply" {
			valid["reply_to"] = "av1.r.1"
		} else if action == "withdraw" {
			valid["message_id"] = "av1.w.1"
		} else {
			valid["target"] = "rt_7"
		}
		if action != "withdraw" && action != "control_reply" {
			valid["message"] = "hi"
		}

		unknown := map[string]any{}
		for key, value := range valid {
			unknown[key] = value
		}
		unknown["bogus"] = 1

		missing := map[string]any{}
		for key, value := range valid {
			missing[key] = value
		}
		delete(missing, keys[0])

		wrong := map[string]any{}
		for key, value := range valid {
			wrong[key] = value
		}
		wrong[wrongType[action].property] = wrongType[action].value

		for detail, input := range map[string]map[string]any{
			`unknown property "bogus"`:                    unknown,
			`missing required property "` + keys[0] + `"`: missing,
			wrongType[action].detail:                      wrong,
		} {
			t.Run(action+"/"+detail, func(t *testing.T) {
				raw, err := json.Marshal(input)
				if err != nil {
					t.Fatalf("encode input: %v", err)
				}
				provider := &fakeProvider{caller: messaging.Caller{Kind: messaging.CallerBroker, RunID: "run-caller"}}
				_, execErr := NewRouter(provider).Execute(t.Context(), raw)
				var toolErr *ToolError
				if !errors.As(execErr, &toolErr) || toolErr.Code != "invalid_request" {
					t.Fatalf("error = %v, want invalid_request", execErr)
				}
				if got := toolErr.Facts["detail"]; got != detail {
					t.Fatalf("detail = %q, want %q", got, detail)
				}
				if want := `invalid input for action "` + action + `": ` + detail; toolErr.Message != want {
					t.Fatalf("message = %q, want %q", toolErr.Message, want)
				}
				if provider.sent.MessageID != "" || provider.replied.MessageID != "" || provider.withdrew.MessageID != "" {
					t.Fatal("a rejected request reached the transport")
				}
			})
		}
	}
}

// TestInvalidActionMatchesFixture pins the invalid_action reason the router
// reads out of the generated route table to the conformance fixture's text.
func TestInvalidActionMatchesFixture(t *testing.T) {
	cases, err := messaging.LoadConformanceCases(conformancePath)
	if err != nil {
		t.Fatalf("load conformance fixture: %v", err)
	}
	want := ""
	for _, c := range cases {
		if c.Expect.Code == "invalid_action" {
			want = c.Expect.Message
		}
	}
	if want == "" {
		t.Fatal("fixture carries no invalid_action case")
	}
	for _, input := range []string{`{"target": "rt_7", "message": "hi"}`, `{"action": "forward"}`} {
		_, err := NewRouter(&fakeProvider{}).Execute(t.Context(), json.RawMessage(input))
		var toolErr *ToolError
		if !errors.As(err, &toolErr) || toolErr.Code != "invalid_action" {
			t.Fatalf("%s: error = %v, want invalid_action", input, err)
		}
		if toolErr.Message != want {
			t.Fatalf("%s: message = %q, want %q", input, toolErr.Message, want)
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

// TestUnlistedOutcomeFallsBackToPossiblyPending guards the "text on every
// result" rule: a transport that reports an outcome the action's matrix does not
// list is a transport bug, and is reported as possibly-pending with text rather
// than as an outcome with no sentence.
func TestUnlistedOutcomeFallsBackToPossiblyPending(t *testing.T) {
	provider := &fakeProvider{
		caller:     messaging.Caller{Kind: messaging.CallerBroker, RunID: "run-caller"},
		surface:    messaging.SurfaceChanneltools,
		resolution: messaging.Resolution{RunID: "rt_7", Label: "mesh-a", Match: messaging.MatchUnique},
		runFacts:   messaging.RunFacts{Status: runstate.StatusRunning, Live: true},
		outcome:    messaging.OutcomeWithdrawn,
	}
	result, err := NewRouter(provider).Execute(t.Context(), json.RawMessage(`{"action": "tell", "target": "rt_7", "message": "hi"}`))
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
