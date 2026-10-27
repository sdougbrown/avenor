// Package terminal provides an abstraction for interactive terminal sessions.
package terminal

import (
	"context"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
)

// Key represents a terminal key for send-keys operations.
type Key string

const (
	KeyEnter Key = "Enter"
	KeyEsc   Key = "Esc"
	KeyDown  Key = "Down"
)

// Session represents an interactive terminal session.
// InterruptSession is the optional exact-process interruption seam. Callers
// that need graceful shutdown must use this capability rather than deriving a
// PID and signaling an ambient process.
type InterruptSession interface {
	Interrupt(ctx context.Context) error
}

// ProgStatusSession is the optional OSC 7501 (Program Status Protocol) seam: a
// session that observes the hosted program's status reports. Reports come from
// the program's output and are untrusted.
type ProgStatusSession interface {
	// ProgStatusAnswering reports whether the session answers the program's
	// detection query. A program only reports after its query is answered.
	ProgStatusAnswering() bool
	// ProgStatusAnswered reports whether a detection query has been answered.
	ProgStatusAnswered() bool
	// ProgStatusRoot returns the current root record. Records survive until
	// the program replaces or clears them, so reports sent before anyone
	// asked are not lost.
	ProgStatusRoot() (ProgStatus, bool)
	// ProgStatusSince returns the retained reports with Seq greater than
	// after, oldest first. The log is bounded; lost reports whether reports
	// after `after` were dropped before this call.
	ProgStatusSince(after uint64) (reports []ProgReport, lost bool)
	// ProgStatusChanged returns a channel that receives a value after new
	// reports are logged or the records change. The sender never blocks, so
	// one pending value may stand for several changes.
	ProgStatusChanged() <-chan struct{}
}

// ProgReport is one accepted report in arrival order. A terminal reset (RIS)
// appears as a root ProgClear.
type ProgReport struct {
	Seq    uint64
	At     time.Time
	Status ProgStatus
}

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
	// Wait blocks until the hosted process/session has exited. It is the
	// synchronous lifecycle seam callers use after Kill to ensure resources are
	// reaped before they release ownership.
	Wait(ctx context.Context) error
}

// StartOptions holds parameters for launching a new terminal session.
type StartOptions struct {
	Name    string
	Dir     string
	Cols    int
	Rows    int
	Command string
	// Provenance is the informational launch identity exported to the hosted
	// process. Inherited identity is removed before it is added.
	Provenance *runtime.LaunchProvenance
	// Env holds KEY=VALUE entries exported to the hosted process after the
	// launch identity: the caller's own vocabulary rather than the harness's.
	// Inherited entries of the same names are stripped first. A tmux pane can
	// only be given values this way, so the field is not a PTY-only seam.
	Env []string
}

// Launcher creates and manages terminal sessions.
type Launcher interface {
	Start(ctx context.Context, opts StartOptions) (Session, error)
}
