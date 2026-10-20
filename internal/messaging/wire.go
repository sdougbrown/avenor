package messaging

import (
	"errors"
	"net/http"
	"slices"
)

// Capability wire headers. A client and the server that authenticates it must
// agree on one spelling, so both read them from here.
const (
	HeaderCapability = "X-Avenor-Capability"
	HeaderSecret     = "X-Avenor-Secret"
)

// Local broker credential headers. A GET carries the run identity here so its
// credentials never appear in the request URL.
const (
	HeaderRunID    = "X-Avenor-Run-Id"
	HeaderRunToken = "X-Avenor-Run-Token"
)

// WireErrorClass is one class of protocol failure: the code that names it on
// the wire, the typed error it classifies on this side, and the HTTP status and
// outcome a server reports with it.
type WireErrorClass struct {
	Code    string
	Err     error
	Status  int
	Outcome Outcome
}

// wireErrorClasses is the v1 error contract. Every transport response that
// carries an error carries one of these codes.
var wireErrorClasses = []WireErrorClass{
	{"auth-rejected", ErrAuthRejected, http.StatusUnauthorized, OutcomeAuthRejected},
	{"invalid-request", ErrInvalidRequest, http.StatusBadRequest, OutcomeNotSent},
	{"generation-mismatch", ErrGenerationMismatch, http.StatusConflict, OutcomeNotSent},
	{"unsupported-protocol", ErrUnsupportedProtocol, http.StatusBadRequest, OutcomeNotSent},
	{"malformed-message-id", ErrMalformedMessageID, http.StatusBadRequest, OutcomeNotSent},
	{"message-id-expired", ErrMessageIDExpired, http.StatusBadRequest, OutcomeNotSent},
	{"message-id-horizon", ErrMessageIDHorizon, http.StatusBadRequest, OutcomeNotSent},
	{"duplicate-message-id", ErrDuplicateMessageID, http.StatusConflict, OutcomeNotSent},
	{"mutual-ask", ErrMutualAsk, http.StatusBadRequest, OutcomeNotSent},
	{"fence-capacity", ErrFenceCapacity, http.StatusServiceUnavailable, OutcomeNotSent},
	{"sender-mismatch", ErrSenderMismatch, http.StatusForbidden, OutcomeAuthRejected},
	{"not-ask-sender", ErrNotAskSender, http.StatusForbidden, OutcomeEdgeAbsent},
	{"not-ask-target", ErrNotAskTarget, http.StatusForbidden, OutcomeEdgeAbsent},
	{"target-unavailable", ErrTargetUnavailable, http.StatusServiceUnavailable, OutcomeNotSent},
	{"transport-closed", ErrTransportClosed, http.StatusBadRequest, OutcomeNotSent},
	{"unsupported-operation", ErrUnsupportedOperation, http.StatusBadRequest, OutcomeNotSent},
}

// WireErrors lists the v1 error contract in declaration order.
func WireErrors() []WireErrorClass { return slices.Clone(wireErrorClasses) }

// WireCodeFor reports the code a typed error is reported under. An error that
// names no class is an invalid request.
func WireCodeFor(err error) string {
	for _, class := range wireErrorClasses {
		if errors.Is(err, class.Err) {
			return class.Code
		}
	}
	return "invalid-request"
}

// WireErrorFor returns the typed error code classifies, or nil for a code
// outside the contract.
func WireErrorFor(code string) error {
	for _, class := range wireErrorClasses {
		if class.Code == code {
			return class.Err
		}
	}
	return nil
}

// WireStatusFor returns the HTTP status a server reports with code. An unknown
// code is a bad request.
func WireStatusFor(code string) int {
	for _, class := range wireErrorClasses {
		if class.Code == code {
			return class.Status
		}
	}
	return http.StatusBadRequest
}

// WireOutcomeFor returns the documented outcome for code. An unknown code is a
// request that was never admitted.
func WireOutcomeFor(code string) Outcome {
	for _, class := range wireErrorClasses {
		if class.Code == code {
			return class.Outcome
		}
	}
	return OutcomeNotSent
}

// IsSendOutcome reports whether o is an outcome a send may report: admission,
// a terminal fence, or a refusal that was never admitted.
func IsSendOutcome(o Outcome) bool {
	switch o {
	case OutcomeSent, OutcomeWithdrawn, OutcomeNotSent, OutcomeAuthRejected, OutcomePossiblyPending:
		return true
	}
	return false
}

// SendOutcomeFor returns the outcome a send carries when the peer rejects it
// with code. A code whose documented outcome is not a send outcome is reported
// as not-sent.
func SendOutcomeFor(code string) Outcome {
	if outcome := WireOutcomeFor(code); IsSendOutcome(outcome) {
		return outcome
	}
	return OutcomeNotSent
}
