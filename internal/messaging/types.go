package messaging

import (
	"encoding/json"
	"time"
)

// Identity names the authenticated caller of a transport method. RunID is the
// canonical run identity; Sender is the canonical sender name and is always
// supplied by the caller, never inferred by a transport.
type Identity struct {
	RunID  string
	Sender string
}

// SendRequest admits one message for delivery to Target.
type SendRequest struct {
	From                 Identity
	To                   Identity
	MessageID            string
	Generation           string
	Body                 string
	ExpectedSentinelFile string
	// NoReply asks the receiver to deliver the message without creating a reply
	// edge. The zero value is an ask: it admits the message and allocates the
	// edge a Wait can observe.
	NoReply bool
	// Legacy admits an unversioned message id without a generation, which only
	// the broker's legacy local HTTP router does.
	Legacy bool
}

// SendResult reports the admission verdict for an ask.
type SendResult struct {
	Outcome    Outcome
	MessageID  string
	Generation string
}

// WaitRequest blocks until the ask identified by MessageID reaches a terminal
// state or the bounded timeout expires.
type WaitRequest struct {
	Sender     Identity
	MessageID  string
	Generation string
	Timeout    time.Duration
}

// WaitResult reports the terminal state of a wait. Reply is non-nil only for
// OutcomeReplied.
type WaitResult struct {
	Outcome Outcome
	Reply   *InboundMessage
}

// ReplyRequest answers an ask addressed to From, or a tell From received.
// ToRunID is the asker for an ask; it is empty for a tell, whose sender the
// broker resolves from the message id.
type ReplyRequest struct {
	From          Identity
	ToRunID       string
	MessageID     string
	CorrelationID string
	Body          string
}

// WithdrawRequest withdraws the ask identified by MessageID.
type WithdrawRequest struct {
	Sender     Identity
	MessageID  string
	Generation string
}

// WithdrawResult reports a withdrawal plus the terminal fence it installed.
type WithdrawResult struct {
	Outcome Outcome
	Fence   FenceAck
}

// FenceKey identifies one ask/fence record.
type FenceKey struct {
	Sender     string
	MessageID  string
	Generation string
}

// FenceAck is the typed acknowledgement of a terminal fence. Terminal is never
// inferred from a missing edge or an HTTP 404.
type FenceAck struct {
	Sender     string
	MessageID  string
	Generation string
	Deadline   time.Time
	Terminal   bool
}

// ReceiveRequest drains inbound messages queued for Sender.
type ReceiveRequest struct {
	Sender     Identity
	Generation string
	Timeout    time.Duration
	Limit      int
}

// DefaultReceiveLimit bounds a Receive that does not name a limit, so an absent
// or zero limit never drains an unbounded queue.
const DefaultReceiveLimit = 32

// ReceiveLimit resolves an absent, zero or negative Receive limit to
// DefaultReceiveLimit.
func ReceiveLimit(limit int) int {
	if limit <= 0 {
		return DefaultReceiveLimit
	}
	return limit
}

// ReceiveResult carries drained inbound messages.
type ReceiveResult struct {
	Messages   []InboundMessage
	Generation string
}

// PeersRequest lists peers visible to Sender.
type PeersRequest struct {
	Sender     Identity
	Generation string
}

// PeersResult carries peer descriptors.
type PeersResult struct {
	Peers      []Peer
	Generation string
}

// PreflightResult reports the authenticated protocol and generation of a peer.
// Protocol 0 means the peer is too old to speak the shared protocol.
type PreflightResult struct {
	Protocol   int
	Generation string
}

// InboundMessage is a message received over a transport. Payload carries the
// existing wire payload undecoded so no broker DTO is copied here.
type InboundMessage struct {
	MessageID string
	FromRunID string
	Sender    string
	Kind      string
	Payload   json.RawMessage
}

// Peer is one visible peer. Session carries the existing peer record undecoded
// so no SessionInfo copy appears here.
type Peer struct {
	RunID   string
	Session json.RawMessage
}

// EndpointDescriptor names a host endpoint and the credentials to present.
type EndpointDescriptor struct {
	BaseURL          string
	CapabilityID     string
	CapabilitySecret string
	ExpectedEpoch    string
	Timeout          time.Duration
}

// LocalCredentials names the local broker endpoint and the existing run token.
type LocalCredentials struct {
	RunID   string
	Token   string
	BaseURL string
	Timeout time.Duration
}
