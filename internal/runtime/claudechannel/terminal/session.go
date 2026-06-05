// Package terminal provides an abstraction for interactive terminal sessions.
package terminal

import "context"

// Key represents a terminal key for send-keys operations.
type Key string

const (
	KeyEnter Key = "Enter"
	KeyEsc   Key = "Esc"
)

// Session represents an interactive terminal session.
type Session interface {
	// Kind identifies the terminal backend (e.g. "tmux", "pty"). Used by
	// callers to tag events with the actual backend rather than hardcoding.
	Kind() string
	Name() string
	PID() int
	Capture(ctx context.Context) (string, error)
	PasteAndEnter(ctx context.Context, text string) error
	SendKeys(ctx context.Context, keys ...Key) error
	Alive(ctx context.Context) bool
	Kill(ctx context.Context) error
}

// StartOptions holds parameters for launching a new terminal session.
type StartOptions struct {
	Name    string
	Dir     string
	Cols    int
	Rows    int
	Command string
}

// Launcher creates and manages terminal sessions.
type Launcher interface {
	Start(ctx context.Context, opts StartOptions) (Session, error)
	// SupportsResume reports whether sessions from this launcher survive
	// the parent process and can be reattached later. Tmux sessions live
	// in the tmux server and outlive the launching process; PTY sessions
	// die with their parent.
	SupportsResume() bool
}
