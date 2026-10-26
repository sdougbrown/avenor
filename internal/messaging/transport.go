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

// DeliverRequest is one fire-and-forget message a messenger publishes to a
// target. It allocates no reply edge: a tell has no ask to answer.
type DeliverRequest struct {
	FromRunID     string
	ToRunID       string
	Kind          string
	Payload       json.RawMessage
	CorrelationID string
}

// Messenger is the broker's outbound seam for local messaging. The broker
// installs a local implementation that publishes into its own queues and ask
// store; a host that owns a parent link replaces it so asks, tells, replies and
// peer lookups all have one place to reach the parent. An implementation
// composes the local seam for targets that are still local instead of owning a
// queue or an ask store of its own.
type Messenger interface {
	// Send admits an ask and publishes it to its target.
	Send(ctx context.Context, req SendRequest) (SendResult, error)
	// Wait blocks for the reply to an ask this messenger routed.
	Wait(ctx context.Context, req WaitRequest) (WaitResult, error)
	// Withdraw withdraws an ask this messenger routed.
	Withdraw(ctx context.Context, req WithdrawRequest) (WithdrawResult, error)
	// Reply completes the ask edge MessageID names for ToRunID. An edge this
	// messenger did not route reports OutcomeEdgeAbsent and the caller falls
	// back to Deliver.
	Reply(ctx context.Context, req ReplyRequest) (Outcome, error)
	// Deliver publishes a fire-and-forget message.
	Deliver(ctx context.Context, req DeliverRequest) error
	// Reachable reports whether runID can be reached from here.
	Reachable(runID string) bool
	// Peers lists the participants reachable from here.
	Peers(ctx context.Context) ([]Peer, error)
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
