package messaging

import "encoding/json"
import "fmt"

// AskBody builds the agent_message payload for an ask: the reply correlation
// id, both run identities, the question, and the reply expectation the receiver
// reads. The sender's envelope repeats the id and the identities, so every ask
// surface builds both from the same values.
func AskBody(messageID, fromRunID, toRunID, message, role string) ([]byte, error) {
	return json.Marshal(map[string]any{
		"id":            messageID,
		"from":          fromRunID,
		"from_run_id":   fromRunID,
		"to_run_id":     toRunID,
		"message":       message,
		"role":          role,
		"expects_reply": true,
	})
}

// SendBody builds the agent_message payload for a fire-and-forget message: both
// run identities, the text the receiver renders and the role it displays. It
// carries no reply expectation, so the broker allocates no edge.
func SendBody(fromRunID, toRunID, message, role string) ([]byte, error) {
	return json.Marshal(map[string]any{
		"from":        fromRunID,
		"from_run_id": fromRunID,
		"to_run_id":   toRunID,
		"message":     message,
		"role":        role,
	})
}

// ReplyID reports the message id a reply carries: the id it answers with
// .reply, so the reply's own route is keyed by the id a recipient's surface
// renders as its reply_to target.
func ReplyID(messageID string) string {
	return messageID + ".reply"
}

// ReplyBody stamps the correlation fields a reply needs onto the caller's
// payload: the reply's own message id (what a recipient's surface renders as
// the reply_to target) and the id the reply answers, so the sender can
// correlate the answer. The replier's identity travels in the envelope the
// transport stamps and validates, never in the payload: a relay rewrites the
// envelope's from_run_id to the id the receiving branch knows, and a payload
// that named a different run would be refused as a sender mismatch. The
// caller's own fields are preserved; these two win, because they name the
// conversation the reply belongs to. This stamping is the correlation
// authority on the tell path: the route is keyed by the request's message id,
// and the rendered reply_to names that same id. The router's own reply_to
// body field serves the ask path, whose transport (Store.Reply) stores the
// payload without re-stamping — the two never disagree because both derive
// from the router's single correlation id.
// The payload must be a JSON object or
// absent; a JSON null is an empty body.
func ReplyBody(payload json.RawMessage, replyTo string) (json.RawMessage, error) {
	fields := map[string]any{}
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &fields); err != nil {
			return nil, fmt.Errorf("%w: reply body is not a JSON object", ErrInvalidRequest)
		}
		// A JSON null unmarshals into a nil map without error.
		if fields == nil {
			fields = map[string]any{}
		}
	}
	fields["id"] = ReplyID(replyTo)
	fields["reply_to"] = replyTo
	return json.Marshal(fields)
}
