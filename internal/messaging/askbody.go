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

// ReplyBody stamps the correlation fields a reply needs onto the caller's
// payload: the message id it answers, so the sender can correlate the answer.
// The replier's identity travels in the envelope the transport stamps and
// validates, never in the payload: a relay rewrites the envelope's from_run_id
// to the id the receiving branch knows, and a payload that named a different
// run would be refused as a sender mismatch. The caller's own fields are
// preserved; the id and reply_to win, because they name the conversation the
// reply belongs to. The payload must be a JSON object or absent; a JSON null is
// an empty body.
func ReplyBody(payload json.RawMessage, messageID string) (json.RawMessage, error) {
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
	fields["id"] = messageID
	fields["reply_to"] = messageID
	return json.Marshal(fields)
}
