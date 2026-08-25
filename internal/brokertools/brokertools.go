// Package brokertools holds small helpers shared by the agent-facing MCP
// servers (claude-channel sidecar and channeltools) for interacting with the
// in-process broker.
package brokertools

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
)

// MakeMsgID returns a 128-bit (16-byte) random hex string used as a broker
// ask message id. 128 bits keeps birthday-collision risk negligible across a
// multi-agent system, matching broker.MakeToken's length.
func MakeMsgID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// ExtractReplyMessage returns a human-readable reply from a broker wait_reply
// result map (the ask/reply tool result). It extracts the "message" field from
// the payload, or a timeout/cancelled notice, falling back to a JSON rendering
// of the full result when the shape is unexpected.
func ExtractReplyMessage(result map[string]any) string {
	if p, ok := result["payload"]; ok {
		if payloadBytes, err := json.Marshal(p); err == nil {
			var payload struct {
				Message string `json:"message"`
			}
			if json.Unmarshal(payloadBytes, &payload) == nil && payload.Message != "" {
				return payload.Message
			}
		}
	}
	if timeout, ok := result["timeout"]; ok && timeout == true {
		return "(timeout: no reply received)"
	}
	if result["cancelled"] == true {
		return "(cancelled)"
	}
	if b, err := json.Marshal(result); err == nil {
		return string(b)
	}
	return "(empty reply)"
}
