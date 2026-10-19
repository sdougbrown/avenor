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
