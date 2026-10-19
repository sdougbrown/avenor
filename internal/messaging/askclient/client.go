// Package askclient is the single Go ask client. It owns send/wait/withdraw
// ordering and result classification; transports only report facts.
package askclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// Options configures a Client.
type Options struct {
	// Clock mints message IDs; nil means time.Now.
	Clock func() time.Time
	// CleanupDeadline bounds cleanup work after a wait; zero means
	// messaging.CleanupDeadline.
	CleanupDeadline time.Duration
}

// Client drives one transport.
type Client struct {
	transport messaging.Transport
	now       func() time.Time
	cleanup   time.Duration
}

// New builds an ask client over transport.
func New(t messaging.Transport, opts Options) *Client {
	c := &Client{transport: t, now: opts.Clock, cleanup: opts.CleanupDeadline}
	if c.now == nil {
		c.now = time.Now
	}
	if c.cleanup <= 0 {
		c.cleanup = messaging.CleanupDeadline
	}
	return c
}

// AskResult is the caller-facing result of one ask attempt.
type AskResult struct {
	MessageID  string
	Generation string
	Outcome    messaging.Outcome
	Reply      *messaging.InboundMessage
}

// Text renders the ask result as text. It is the single renderer shared by the
// MCP tool surfaces.
func (r AskResult) Text() (string, error) {
	return RenderReply(messaging.WaitResult{Outcome: r.Outcome, Reply: r.Reply})
}

// Error is a failed ask. Pending reports whether the message ID may still be
// outstanding; it is true exactly for OutcomePossiblyPending.
type Error struct {
	MessageID string
	Outcome   messaging.Outcome
	Err       error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}
	return fmt.Sprintf("ask %s: %s", e.MessageID, e.Outcome)
}

func (e *Error) Unwrap() error { return e.Err }

// Pending reports whether the ask may still be outstanding.
func (e *Error) Pending() bool { return e.Outcome == messaging.OutcomePossiblyPending }

// Ask sends one ask and waits for its terminal state. A caller-supplied
// MessageID must be versioned; an empty one is minted with the wait cap as its
// deadline.
//
// Outcome mapping for the callers that adopt this client:
//   - replied: the legacy any-return is {message_id, from_run_id, payload};
//     the tool surfaces render `Reply from <target>:\n` plus Text().
//   - timed-out, withdrawn, edge-absent, lost and possibly-pending return an
//     *Error carrying the outcome; the legacy ask error maps it to
//     control.AskError with Pending set only for possibly-pending.
//   - not-sent and auth-rejected are terminal send failures and never retried.
func (c *Client) Ask(ctx context.Context, send messaging.SendRequest, wait messaging.WaitRequest) (AskResult, error) {
	result := AskResult{MessageID: send.MessageID, Generation: send.Generation}
	if result.MessageID == "" {
		// A body that names its own id keeps it, so the envelope id the
		// transport sends never disagrees with the payload id.
		if id := bodyMessageID(send.Body); id != "" {
			result.MessageID = id
			send.MessageID = id
		} else {
			msgID, err := messaging.NewMsgID(c.now(), time.Time{})
			if err != nil {
				result.Outcome = messaging.OutcomeNotSent
				return result, &Error{Outcome: result.Outcome, Err: err}
			}
			result.MessageID = msgID
			send.MessageID = msgID
		}
	}
	if send.Generation == "" {
		pre, err := c.transport.Preflight(ctx)
		if err != nil {
			result.Outcome = classifySendError(err)
			return result, &Error{MessageID: result.MessageID, Outcome: result.Outcome, Err: err}
		}
		result.Generation = pre.Generation
		send.Generation = pre.Generation
	}
	if wait.MessageID == "" {
		wait.MessageID = send.MessageID
	}
	if wait.Sender.RunID == "" {
		wait.Sender = send.From
	}
	if wait.Generation == "" {
		wait.Generation = send.Generation
	}
	if wait.Timeout <= 0 || wait.Timeout > messaging.MaxWait {
		wait.Timeout = messaging.MaxWait
	}

	sent, err := c.transport.Send(ctx, send)
	if sent.Outcome == "" {
		sent.Outcome = classifySendError(err)
	}
	result.Outcome = sent.Outcome
	if err != nil || sent.Outcome != messaging.OutcomeSent {
		return result, &Error{MessageID: result.MessageID, Outcome: result.Outcome, Err: err}
	}

	waitCtx, cancel := context.WithTimeout(ctx, wait.Timeout+c.cleanup)
	defer cancel()
	waited, err := c.transport.Wait(waitCtx, wait)
	if waited.Outcome == "" {
		waited.Outcome = messaging.OutcomePossiblyPending
	}
	result.Outcome = waited.Outcome
	result.Reply = waited.Reply
	if err != nil || waited.Outcome != messaging.OutcomeReplied {
		return result, &Error{MessageID: result.MessageID, Outcome: result.Outcome, Err: err}
	}
	return result, nil
}

// bodyMessageID reports the id an ask body carries, so a minted envelope id
// never disagrees with the payload id.
func bodyMessageID(body string) string {
	var payload struct {
		ID string `json:"id"`
	}
	if json.Unmarshal([]byte(body), &payload) != nil {
		return ""
	}
	return payload.ID
}

// Withdraw withdraws an ask and returns its terminal fence.
func (c *Client) Withdraw(ctx context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	if req.MessageID == "" {
		return messaging.WithdrawResult{Outcome: messaging.OutcomeNotSent}, messaging.ErrInvalidRequest
	}
	return c.transport.Withdraw(ctx, req)
}

// RenderReply renders a wait result as text. It is the shared result renderer
// for every Go ask caller.
func RenderReply(res messaging.WaitResult) (string, error) {
	switch res.Outcome {
	case messaging.OutcomeReplied:
		if res.Reply == nil {
			return "", errors.New("askclient: replied without a reply")
		}
		if len(res.Reply.Payload) == 0 {
			return "(empty reply)", nil
		}
		var payload struct {
			Message string `json:"message"`
		}
		if json.Unmarshal(res.Reply.Payload, &payload) == nil && payload.Message != "" {
			return payload.Message, nil
		}
		return string(res.Reply.Payload), nil
	case messaging.OutcomeTimedOut:
		return "(timeout: no reply received)", nil
	case messaging.OutcomeWithdrawn:
		return "(cancelled)", nil
	case messaging.OutcomeEdgeAbsent:
		return "(no pending ask)", nil
	case messaging.OutcomePossiblyPending:
		return "(possibly pending: no terminal reply)", nil
	case messaging.OutcomeLost:
		return "(ask lost)", nil
	}
	return "", fmt.Errorf("askclient: cannot render outcome %q", res.Outcome)
}

// classifySendError maps a pre-response send failure to an outcome. A typed
// error proves the ask was not admitted; an untyped one may have been
// transmitted.
func classifySendError(err error) messaging.Outcome {
	switch {
	case err == nil:
		return messaging.OutcomeSent
	case errors.Is(err, messaging.ErrAuthRejected):
		return messaging.OutcomeAuthRejected
	case errors.Is(err, messaging.ErrGenerationMismatch),
		errors.Is(err, messaging.ErrUnsupportedProtocol),
		errors.Is(err, messaging.ErrInvalidRequest),
		errors.Is(err, messaging.ErrMalformedMessageID),
		errors.Is(err, messaging.ErrMessageIDExpired),
		errors.Is(err, messaging.ErrMessageIDHorizon),
		errors.Is(err, messaging.ErrDuplicateMessageID),
		errors.Is(err, messaging.ErrMutualAsk),
		errors.Is(err, messaging.ErrFenceCapacity),
		errors.Is(err, messaging.ErrTargetUnavailable),
		errors.Is(err, messaging.ErrTransportClosed):
		return messaging.OutcomeNotSent
	default:
		return messaging.OutcomePossiblyPending
	}
}
