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
//
// Peers reports the visible peer set. A transport that could not read every
// peer returns the peers it did read together with ErrIncompleteListing, so a
// caller keeps the peers it can see and reports the listing as incomplete
// rather than reading a partial set as a complete one. The host transport
// reports a listing it could not read as an error, because the host's own
// envelope has no field for a partial listing.
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
	FromRunID string
	ToRunID   string
	Kind      string
	Payload   json.RawMessage
	// CorrelationID is the id the publication carries, so a receiver can
	// relate it to the tell that produced it.
	CorrelationID string
}

// Messenger is the broker's outbound seam for local messaging. The broker
// installs a local implementation that publishes into its own queues and ask
// store; a host that owns a parent link replaces it so asks, tells, replies and
// peer lookups all have one place to reach the parent. An implementation
// composes the local seam for targets that are still local instead of owning a
// queue or an ask store of its own.
//
// An implementation honours the cancellation of the context it is handed, so a
// transport that joins a delegated call can join it when the transport closes.
type Messenger interface {
	// Send admits an ask and publishes it to its target. legacy admits an
	// unversioned message id without a generation, which only the broker's
	// legacy local HTTP router does.
	Send(ctx context.Context, req SendRequest, legacy bool) (SendResult, error)
	// Wait blocks for the reply to an ask this messenger routed.
	Wait(ctx context.Context, req WaitRequest) (WaitResult, error)
	// Withdraw withdraws an ask this messenger routed.
	Withdraw(ctx context.Context, req WithdrawRequest) (WithdrawResult, error)
	// Reply completes the ask edge MessageID names for ToRunID. An edge this
	// messenger did not route reports OutcomeEdgeAbsent and the caller falls
	// back to Deliver. A replier that is not the edge's target reports
	// ErrNotAskTarget, so the broker can refuse it as the local store does.
	Reply(ctx context.Context, req ReplyRequest) (Outcome, error)
	// Deliver publishes a fire-and-forget message. It allocates no reply edge.
	Deliver(ctx context.Context, req DeliverRequest) error
	// Reachable reports whether runID can be reached from here. It is the
	// authority for an ask's target: a target it denies is not askable.
	Reachable(runID string) bool
	// Peers lists the participants reachable from here. A listing that could
	// not be completed returns the peers it did read together with
	// ErrIncompleteListing, so a caller keeps the peers it can see and
	// reports the listing as incomplete. The listing and Reachable answer one
	// question about the same participants, so an implementation keeps them
	// consistent.
	Peers(ctx context.Context) ([]Peer, error)
}

// IncompletePeerListing is the reason a peer listing that could not be
// completed carries on the wire. It names no detail of the failure: a seam's
// error can carry an absolute path or a remote endpoint, and the reason
// reaches every authenticated run and, through a client's tool result, a
// model's context. Every publisher of a peer listing uses this reason.
const IncompletePeerListing = "the broker could not read every peer"

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

// ErrIncompleteListing reports a peer listing that could not be completed. It
// is a client's own classification of the listing's own reason, so it never
// crosses the wire as an error code.
var ErrIncompleteListing = errors.New("messaging: peer listing is incomplete")
