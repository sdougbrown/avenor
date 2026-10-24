package parentlink

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/internal/httpbase"
)

// Parent broker routes this link drives.
const (
	pathRegister   = "/parent/register"
	pathDeregister = "/parent/deregister"
	pathRelay      = "/relay"
	pathSend       = "/send"
	pathWait       = "/wait_reply"
	pathWithdraw   = "/cancel_message"
	pathSessions   = "/sessions"
)

// ErrUnsupportedParentProtocol reports a parent broker that answered without
// the messaging envelope this link reads.
var ErrUnsupportedParentProtocol = fmt.Errorf(
	"%w: parent broker does not speak the shared messaging protocol",
	messaging.ErrUnsupportedProtocol,
)

// envelope is one parent broker response. Every field is optional so a response
// carries only the operation's result, plus its code class on failure.
type envelope struct {
	Status            int               `json:"-"`
	Outcome           messaging.Outcome `json:"outcome"`
	Code              string            `json:"code"`
	Error             string            `json:"error"`
	MessageID         string            `json:"message_id"`
	FromRunID         string            `json:"from_run_id"`
	Sender            string            `json:"sender"`
	Kind              string            `json:"kind"`
	Payload           json.RawMessage   `json:"payload"`
	Generation        string            `json:"generation"`
	MessagingProtocol int               `json:"messaging_protocol"`
	Sessions          []json.RawMessage `json:"sessions"`
	PeersError        string            `json:"peers_error"`
	// RunKeys reports the run ID the parent addresses each claimed run by, for
	// the runs where that differs from the canonical run ID.
	RunKeys map[string]string `json:"run_keys,omitempty"`
	Fence   *httpbase.Fence   `json:"fence"`
}

// decode reads one parent response. A response that carries a wire code or an
// outcome describes its own result; one that carries neither is classified by
// its status, because the broker's auth layer answers before a messaging
// handler does.
func decode(status int, raw []byte) (envelope, error) {
	body := envelope{Status: status}
	if trimmed := bytes.TrimSpace(raw); len(trimmed) > 0 && trimmed[0] == '{' {
		if err := json.Unmarshal(trimmed, &body); err != nil {
			return envelope{Status: status}, fmt.Errorf("%w: %v", ErrUnsupportedParentProtocol, err)
		}
		body.Status = status
	}
	switch {
	case body.Code != "":
		typed := messaging.WireErrorFor(body.Code)
		if typed == nil {
			return body, fmt.Errorf("%w: parent broker reported unknown code %q", ErrUnsupportedParentProtocol, body.Code)
		}
		if body.Error != "" {
			return body, fmt.Errorf("%w: %s", typed, body.Error)
		}
		return body, typed
	case body.Outcome != "":
		return body, nil
	default:
		return body, statusFailure(status, body)
	}
}

// statusFailure classifies a response the messaging envelope does not describe.
func statusFailure(status int, body envelope) error {
	if status >= 200 && status < 300 {
		return nil
	}
	detail := ""
	if body.Error != "" {
		detail = ": " + body.Error
	}
	switch status {
	case http.StatusUnauthorized:
		return fmt.Errorf("%w: parent broker rejected the link credentials", messaging.ErrAuthRejected)
	case http.StatusForbidden:
		return fmt.Errorf("%w: parent broker refused the request%s", messaging.ErrNotAskTarget, detail)
	case http.StatusNotFound:
		return fmt.Errorf("%w: parent broker cannot reach the target%s", messaging.ErrTargetUnavailable, detail)
	case http.StatusBadRequest:
		return fmt.Errorf("%w: parent broker refused the request%s", messaging.ErrInvalidRequest, detail)
	}
	return fmt.Errorf("%w: parent broker answered %d%s", ErrParentUnavailable, status, detail)
}

// outcomeOf reports the outcome a response carries, for a caller that received
// an error alongside it.
func (e envelope) outcomeOf() messaging.Outcome {
	if e.Outcome != "" {
		return e.Outcome
	}
	if e.Code != "" {
		return messaging.WireOutcomeFor(e.Code)
	}
	return messaging.OutcomePossiblyPending
}

// sendOutcomeOf reports the send verdict a failed send response carries.
func (e envelope) sendOutcomeOf() messaging.Outcome {
	if e.Code != "" {
		if outcome := messaging.SendOutcomeFor(e.Code); outcome != "" {
			return outcome
		}
	}
	if messaging.IsSendOutcome(e.Outcome) {
		return e.Outcome
	}
	return messaging.OutcomePossiblyPending
}

// fenceAck maps the wire fence to the shared acknowledgement.
func (e envelope) fenceAck() messaging.FenceAck {
	if e.Fence == nil {
		return messaging.FenceAck{}
	}
	return e.Fence.Ack()
}
