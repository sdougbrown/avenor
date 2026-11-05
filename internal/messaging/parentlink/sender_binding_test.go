package parentlink

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// numberedPayload adds metadata whose exact JSON rendering the parent hop must
// carry through unchanged. Both integers are beyond float64's exact range, so a
// float64 round trip would visibly change them.
func numberedPayload(t *testing.T, body []byte) []byte {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	fields := map[string]any{}
	if err := decoder.Decode(&fields); err != nil {
		t.Fatalf("decode message body %s: %v", body, err)
	}
	fields["seq"] = json.Number("9007199254740993")
	fields["snowflake"] = json.Number("12345678901234567890")
	rendered, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("encode message body: %v", err)
	}
	return rendered
}

// assertNumberedPayload pins that the queued payload kept each number's exact
// rendering instead of a float64 round trip.
func assertNumberedPayload(t *testing.T, payload json.RawMessage) {
	t.Helper()
	for _, literal := range []string{`"seq":9007199254740993`, `"snowflake":12345678901234567890`} {
		if !bytes.Contains(payload, []byte(literal)) {
			t.Fatalf("queued payload %s does not carry the exact numeric rendering %s", payload, literal)
		}
	}
}

// payloadObject reports one queued control row's payload as a JSON object.
func payloadObject(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode queued payload %s: %v", raw, err)
	}
	return payload
}

// Payload routing must use the parent's qualified identity without losing the
// return route to the asking branch.
func TestCrossBranchAskBindsThePayloadSenderToTheParentIdentity(t *testing.T) {
	_, first, second := crossBranch(t)
	asker := first.key("asker")

	msgID := newMsgID(t)
	rendered, err := messaging.AskBody(msgID, "asker", second.key("target"), "ping", "agent")
	if err != nil {
		t.Fatalf("render ask body: %v", err)
	}
	status, body := postJSON(t, first.broker.Addr(), "/send", "asker", first.token(t, "asker"), map[string]any{
		"from_run_id":   "asker",
		"to_run_id":     second.key("target"),
		"type":          "agent_message",
		"payload":       json.RawMessage(numberedPayload(t, rendered)),
		"message_id":    msgID,
		"generation":    first.broker.Store().Generation(),
		"expects_reply": true,
	})
	if status != http.StatusOK || body["outcome"] != string(messaging.OutcomeSent) {
		t.Fatalf("cross-branch ask = (%d, %v), want 200 sent", status, body)
	}

	queued := queuedAgentMessages(t, second.broker, "target")
	if len(queued) != 1 {
		t.Fatalf("target queue = %d messages, want exactly one relayed ask", len(queued))
	}
	if queued[0].FromRunID != asker {
		t.Fatalf("relayed ask from_run_id = %q, want the parent's name for the asker %q", queued[0].FromRunID, asker)
	}
	if got, _ := payloadObject(t, queued[0].Payload)["from_run_id"].(string); got != asker {
		t.Fatalf("relayed ask payload from_run_id = %q, want the authenticated sender %q", got, asker)
	}
	assertNumberedPayload(t, queued[0].Payload)

	// The reply carries its own routing in the payload, the way the Claude hook's
	// agent message body does, and still completes the asking branch's edge.
	replyBody := map[string]any{
		"id":          messaging.ReplyID(msgID),
		"from":        "target",
		"from_run_id": "target",
		"to_run_id":   asker,
		"message":     "pong",
		"role":        "agent",
		"reply_to":    msgID,
	}
	status, body = postJSON(t, second.broker.Addr(), "/send", "target", second.token(t, "target"), map[string]any{
		"from_run_id": "target",
		"to_run_id":   asker,
		"type":        "agent_message",
		"payload":     replyBody,
		"reply_to":    msgID,
	})
	if status != http.StatusOK || body["outcome"] != string(messaging.OutcomeReplied) {
		t.Fatalf("cross-branch reply = (%d, %v), want 200 replied", status, body)
	}
	status, body = waitReply(t, first, "asker", msgID, 5000)
	if status != http.StatusOK || body["outcome"] != string(messaging.OutcomeReplied) {
		t.Fatalf("cross-branch wait = (%d, %v), want 200 replied", status, body)
	}
	if replied, ok := body["payload"].(map[string]any); !ok || replied["message"] != "pong" {
		t.Fatalf("wait payload = %v, want the reply body", body["payload"])
	}
}

// Legacy tells without message IDs use a separate forwarding path.
func TestBranchTellBindsThePayloadSenderToTheParentIdentity(t *testing.T) {
	parent := startBranch(t, "", "hub")
	first := startBranch(t, parent.url(), "asker")
	awaitRegistered(t, parent, first.key("asker"))
	asker := first.key("asker")

	rendered, err := messaging.SendBody("asker", "hub", "hello", "agent")
	if err != nil {
		t.Fatalf("render tell body: %v", err)
	}
	if err := first.link.up.Tell(testContext(t), messaging.TellRequest{
		FromRunID: "asker",
		ToRunID:   "hub",
		Kind:      "agent_message",
		Payload:   numberedPayload(t, rendered),
	}); err != nil {
		t.Fatalf("tell to the parent's run = %v, want it accepted", err)
	}

	queued := queuedAgentMessages(t, parent.broker, "hub")
	if len(queued) != 1 {
		t.Fatalf("parent queue = %d messages, want exactly one tell", len(queued))
	}
	if queued[0].FromRunID != asker {
		t.Fatalf("tell from_run_id = %q, want the parent's name for the asker %q", queued[0].FromRunID, asker)
	}
	if got, _ := payloadObject(t, queued[0].Payload)["from_run_id"].(string); got != asker {
		t.Fatalf("tell payload from_run_id = %q, want the authenticated sender %q", got, asker)
	}
	assertNumberedPayload(t, queued[0].Payload)
}

// Qualifying the envelope must not launder a forged payload sender.
func TestLinkForwardsAPayloadNamingAnotherRunUnchanged(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			// The parent addresses the claimed run by a namespaced key, so the
			// link posts a sender that differs from the branch-local run ID.
			return http.StatusOK, `{"generation":"gen-a","run_keys":{"run-a":"run-a@child-one"}}`
		}
		return http.StatusOK, `{"outcome":"sent"}`
	})
	link := newTestLink(t, stub, "gen-a")

	request := askRequest("msg-1")
	request.Body = `{"id":"msg-1","message":"ping","from_run_id":"run-victim"}`
	if _, err := link.Send(testContext(t), request); err != nil {
		t.Fatalf("Send = %v, want the body forwarded for the parent to judge", err)
	}
	forwarded := stub.lastRequest(t, pathSend)
	if forwarded.Body["from_run_id"] != "run-a@child-one" {
		t.Fatalf("forwarded envelope from_run_id = %v, want the qualified sender %q",
			forwarded.Body["from_run_id"], "run-a@child-one")
	}
	forwardedPayload, ok := forwarded.Body["payload"].(map[string]any)
	if !ok {
		t.Fatalf("forwarded payload = %v, want a JSON object", forwarded.Body["payload"])
	}
	if forwardedPayload["from_run_id"] != "run-victim" {
		t.Fatalf("forwarded payload from_run_id = %v, want the forged identity kept so the parent refuses it",
			forwardedPayload["from_run_id"])
	}
}
