package runtime

import (
	"context"

	"github.com/sdougbrown/avenor/internal/events"
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

// Capabilities describes what a runtime backend supports.
type Capabilities struct {
	Backend             string
	Permissions         bool
	Resume              bool
	ExternalServerURL   bool
	SubprocessDiscovery bool
	ModelSelection      bool
}
