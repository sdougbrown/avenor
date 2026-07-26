package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"unicode/utf8"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

// Provider is the interface that all ACP runtime backends must implement.
type Provider interface {
	Start(ctx context.Context, opts StartOptions) (Session, error)
	Resume(ctx context.Context, sessionID string) (Session, error)
	Prompt(ctx context.Context, sessionID string, prompt string) error
	Cancel(ctx context.Context, sessionID string) error
	Events(ctx context.Context, sessionID string) (<-chan events.Event, error)
	AnswerPermission(ctx context.Context, sessionID string, requestID string, response PermissionResponse) error
	Capabilities(ctx context.Context) (Capabilities, error)
}

// StartOptions holds options for starting a new session.
type StartOptions struct {
	Agent        string
	Label        string
	Dir          string
	ServerURL    string
	Model        string
	AgentProfile string
	RuntimeID    string         // supervisor-assigned runtime ID (rt_N), for parent-child routing
	Broker       *broker.Broker // optional shared broker instance; backends may create their own if nil
}

// Session represents an active ACP session.
type Session struct {
	SessionID string
	Backend   string
	Dir       string
	PID       int // Consumed by longe halt (SIGTERM); set by opencode-acp backend. 0 otherwise.
}

// MaxPermissionMessageBytes is the maximum allowed size for a write-in
// permission message. Messages are validated as UTF-8 and must not exceed
// this bound.
const MaxPermissionMessageBytes = 64 << 10 // 64 KiB

// ValidatePermissionMessage checks that a message is valid UTF-8 and within
// the size bound. Returns nil for an empty message (ordinary options).
func ValidatePermissionMessage(msg string) error {
	if len(msg) == 0 {
		return nil
	}
	if len(msg) > MaxPermissionMessageBytes {
		return fmt.Errorf("permission message exceeds %d bytes", MaxPermissionMessageBytes)
	}
	if !utf8.ValidString(msg) {
		return fmt.Errorf("permission message is not valid UTF-8")
	}
	return nil
}

// DecodePermissionMessageJSON decodes a JSON string without accepting the
// replacement behavior encoding/json applies to malformed UTF-8 or unpaired
// UTF-16 surrogate escapes.
func DecodePermissionMessageJSON(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil
	}
	if !utf8.Valid(raw) || !validJSONSurrogatePairs(raw) {
		return "", fmt.Errorf("permission message is not valid UTF-8")
	}
	var message string
	if err := json.Unmarshal(raw, &message); err != nil {
		return "", fmt.Errorf("permission message is not a JSON string")
	}
	if err := ValidatePermissionMessage(message); err != nil {
		return "", err
	}
	return message, nil
}

func validJSONSurrogatePairs(raw []byte) bool {
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		i++
		if i >= len(raw) {
			return false
		}
		if raw[i] != 'u' {
			continue
		}
		value, ok := jsonHexWord(raw, i+1)
		if !ok {
			return false
		}
		i += 4
		switch {
		case value >= 0xD800 && value <= 0xDBFF:
			if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
				return false
			}
			low, ok := jsonHexWord(raw, i+3)
			if !ok || low < 0xDC00 || low > 0xDFFF {
				return false
			}
			i += 6
		case value >= 0xDC00 && value <= 0xDFFF:
			return false
		}
	}
	return true
}

func jsonHexWord(raw []byte, offset int) (uint16, bool) {
	if offset < 0 || offset+4 > len(raw) {
		return 0, false
	}
	var value uint16
	for _, char := range raw[offset : offset+4] {
		value <<= 4
		switch {
		case char >= '0' && char <= '9':
			value |= uint16(char - '0')
		case char >= 'a' && char <= 'f':
			value |= uint16(char-'a') + 10
		case char >= 'A' && char <= 'F':
			value |= uint16(char-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}

// PermissionResponse is the response to a permission request.
type PermissionResponse struct {
	Allow    bool
	OptionID string
	Message  string
}

// AgentResult holds the result of a completed agent session.
type AgentResult struct {
	SessionID   string   `json:"session_id"`
	StopReason  string   `json:"stop_reason"`
	ExitCode    int      `json:"exit_code"`
	OutputFiles []string `json:"output_files,omitempty"`
}

// Capabilities describes what a runtime backend supports.
type Capabilities struct {
	Backend             string
	Permissions         bool
	Resume              bool
	ExternalServerURL   bool
	SubprocessDiscovery bool
	ModelSelection      bool
}

// MergeStartOptions returns a new StartOptions with non-zero fields from
// override applied over base. Use this to combine provider-scoped defaults
// with per-start overrides.
func MergeStartOptions(base, override StartOptions) StartOptions {
	merged := base
	if override.Agent != "" {
		merged.Agent = override.Agent
	}
	if override.Label != "" {
		merged.Label = override.Label
	}
	if override.Dir != "" {
		merged.Dir = override.Dir
	}
	if override.ServerURL != "" {
		merged.ServerURL = override.ServerURL
	}
	if override.Model != "" {
		merged.Model = override.Model
	}
	if override.AgentProfile != "" {
		merged.AgentProfile = override.AgentProfile
	}
	if override.RuntimeID != "" {
		merged.RuntimeID = override.RuntimeID
	}
	if override.Broker != nil {
		merged.Broker = override.Broker
	}
	return merged
}
