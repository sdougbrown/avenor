package messaging

import "encoding/json"

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
