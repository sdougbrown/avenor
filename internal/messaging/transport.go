package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

const (
	// ProtocolVersion is the shared messaging protocol the client negotiates.
	ProtocolVersion = 1

	// MaxWait caps a single wait and is also the acceptance-deadline horizon
	// a receiver accepts from a message ID.
	MaxWait = 10 * time.Minute

	// MaxAcceptanceHorizon bounds an ID-encoded acceptance deadline relative to
	// the receiver's own clock.
	MaxAcceptanceHorizon = 10 * time.Minute

	// CleanupDeadline is the independent budget for transport cleanup.
	CleanupDeadline = 5 * time.Second
)

// Transport is the shared messaging surface every wire transport and the
// broker's own outbound seam answers. Every method takes the caller's
// authenticated identities and expected generation; no method reads ambient
// credentials or classifies a result from an HTTP status alone.
type Transport interface {
	// Send admits an ask and publishes it to its target.
	Send(ctx context.Context, req SendRequest) (SendResult, error)
	// Wait blocks for the reply to an ask this transport routed.
	Wait(ctx context.Context, req WaitRequest) (WaitResult, error)
	// Reply completes the ask edge MessageID names for ToRunID. An edge this
	// transport did not route reports OutcomeEdgeAbsent and the caller falls
	// back to Tell.
	Reply(ctx context.Context, req ReplyRequest) (SendResult, error)
	// Withdraw withdraws an ask this transport routed.
	Withdraw(ctx context.Context, req WithdrawRequest) (WithdrawResult, error)
	// Receive drains the messages queued for the caller.
	Receive(ctx context.Context, req ReceiveRequest) (ReceiveResult, error)
	// Peers lists the participants reachable from here. A listing the transport
	// could not finish returns the peers it did read beside its error, so a
	// partial listing never reads as a complete one.
	Peers(ctx context.Context, req PeersRequest) (PeersResult, error)
	Preflight(ctx context.Context) (PreflightResult, error)
	Close() error
}

// TellRequest is one fire-and-forget message a seam publishes to a target. It
// allocates no reply edge: a tell has no ask to answer.
type TellRequest struct {
	FromRunID     string
	ToRunID       string
	Kind          string
	Payload       json.RawMessage
	CorrelationID string
}

// Seam is the broker's outbound messaging surface: the shared Transport plus the
// two operations only a broker can answer. It is one interface with one shape
// per operation, so the broker's local implementation, a parent-aware host that
// replaces it, and a client-side transport cannot disagree about what a send, a
// reply or a peer listing looks like.
//
// Tell and Reachable stay here rather than on Transport because a client-side
// transport has no counterpart for either: a tell is a bare publication into a
// broker's queue, and reachability is a broker-local ownership test, so a wire
// client could only answer them with a stub. Everything else, including the
// store-backed reads and Preflight, comes from Transport.
type Seam interface {
	Transport
	// Tell publishes a fire-and-forget message. It allocates no reply edge, so
	// it admits nothing and fences nothing.
	Tell(ctx context.Context, req TellRequest) error
	// Reachable reports whether runID can be reached from here.
	Reachable(runID string) bool
}

// Typed error classes. Transports report facts through these; callers map them
// to an Outcome and never parse response text.
var (
	ErrAuthRejected         = errors.New("messaging: credentials rejected")
	ErrInvalidRequest       = errors.New("messaging: invalid request")
	ErrGenerationMismatch   = errors.New("messaging: generation mismatch")
	ErrUnsupportedProtocol  = errors.New("messaging: unsupported protocol")
	ErrMalformedMessageID   = errors.New("messaging: malformed message id")
	ErrMessageIDExpired     = errors.New("messaging: message id expired")
	ErrMessageIDHorizon     = errors.New("messaging: message id deadline beyond horizon")
	ErrDuplicateMessageID   = errors.New("messaging: message id already in use")
	ErrMutualAsk            = errors.New("messaging: mutual ask refused")
	ErrFenceCapacity        = errors.New("messaging: fence capacity exhausted")
	ErrSenderMismatch       = errors.New("messaging: sender does not match this run")
	ErrNotAskSender         = errors.New("messaging: not the sender of this ask")
	ErrNotAskTarget         = errors.New("messaging: not the target of this ask")
	ErrTargetUnavailable    = errors.New("messaging: target unavailable")
	ErrTransportClosed      = errors.New("messaging: transport closed")
	ErrUnsupportedOperation = errors.New("messaging: operation unsupported by this transport")
)
