package messaging

import (
	"encoding/json"
	"errors"
	"testing"
)

// TestReplyBodyStampsTheConversationFields pins the reply payload's contract:
// the reply's own id and the id it answer win over the caller's fields, a JSON
// null is an empty body, and a non-object payload is refused.
func TestReplyBodyStampsTheConversationFields(t *testing.T) {
	stamped, err := ReplyBody(json.RawMessage(`{"message":"ack","id":"forged","reply_to":"other"}`), "m1.reply", "m1")
	if err != nil {
		t.Fatalf("ReplyBody: %v", err)
	}
	var body map[string]any
	if err := json.Unmarshal(stamped, &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["id"] != "m1.reply" || body["reply_to"] != "m1" || body["message"] != "ack" {
		t.Fatalf("body = %v, want the stamped fields and the caller's message", body)
	}
	if _, hasFrom := body["from_run_id"]; hasFrom {
		t.Fatalf("body = %v, want no from_run_id: the identity travels in the envelope", body)
	}

	empty, err := ReplyBody(json.RawMessage(`null`), "m2.reply", "m2")
	if err != nil {
		t.Fatalf("ReplyBody(null): %v", err)
	}
	var emptyBody map[string]any
	if err := json.Unmarshal(empty, &emptyBody); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if emptyBody["id"] != "m2.reply" || emptyBody["reply_to"] != "m2" {
		t.Fatalf("empty body = %v, want the stamped fields", emptyBody)
	}

	if _, err := ReplyBody(json.RawMessage(`[1,2]`), "m3.reply", "m3"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("ReplyBody(array) = %v, want invalid-request", err)
	}
}
