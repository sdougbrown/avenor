package messaging

import (
	"context"
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

// Transport is the shared messaging surface. Every method takes the caller's
// authenticated identities and expected generation; no method reads ambient
// credentials or classifies a result from an HTTP status alone.
type Transport interface {
	Send(ctx context.Context, req SendRequest) (SendResult, error)
	Wait(ctx context.Context, req WaitRequest) (WaitResult, error)
	Reply(ctx context.Context, req ReplyRequest) (SendResult, error)
	Withdraw(ctx context.Context, req WithdrawRequest) (WithdrawResult, error)
	Receive(ctx context.Context, req ReceiveRequest) (ReceiveResult, error)
	Peers(ctx context.Context, req PeersRequest) (PeersResult, error)
	Preflight(ctx context.Context) (PreflightResult, error)
	Close() error
}

// AskRoute is the delegate seam the broker exposes for ask operations. The
// broker installs a local implementation by default and a host implementation
// replaces it only when a host coordinator exists.
type AskRoute interface {
	Send(ctx context.Context, req SendRequest) (SendResult, error)
	Wait(ctx context.Context, req WaitRequest) (WaitResult, error)
	Withdraw(ctx context.Context, req WithdrawRequest) (WithdrawResult, error)
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
	ErrNotAskSender         = errors.New("messaging: not the sender of this ask")
	ErrNotAskTarget         = errors.New("messaging: not the target of this ask")
	ErrTargetUnavailable    = errors.New("messaging: target unavailable")
	ErrTransportClosed      = errors.New("messaging: transport closed")
	ErrUnsupportedOperation = errors.New("messaging: operation unsupported by this transport")
)
