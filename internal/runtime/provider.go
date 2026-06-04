package runtime

import (
	"context"

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
	Agent     string
	Label     string
	Dir       string
	ServerURL string
	Model     string
	RuntimeID string // supervisor-assigned runtime ID (rt_N), for parent-child routing
	Broker    *broker.Broker // optional shared broker instance; backends may create their own if nil
}

// Session represents an active ACP session.
type Session struct {
	SessionID string
	Backend   string
	Dir       string
	PID       int // Consumed by longe halt (SIGTERM); set by opencode-acp backend. 0 otherwise.
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
	if override.RuntimeID != "" {
		merged.RuntimeID = override.RuntimeID
	}
	if override.Broker != nil {
		merged.Broker = override.Broker
	}
	return merged
}
