package parentlink

import (
	"errors"
	"net/http"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestDecodeClassifiesTheParentsAnswer pins the boundary translation every
// parent response passes through: a wire code or an outcome describes its own
// result, and a response with neither is classified by its status, because the
// broker's auth layer answers before a messaging handler does.
func TestDecodeClassifiesTheParentsAnswer(t *testing.T) {
	cases := []struct {
		name        string
		status      int
		raw         string
		wantErr     error
		wantOutcome messaging.Outcome
	}{
		{
			name:        "an admitted ask",
			status:      http.StatusOK,
			raw:         `{"outcome":"sent"}`,
			wantOutcome: messaging.OutcomeSent,
		},
		{
			name:        "a reply",
			status:      http.StatusOK,
			raw:         `{"outcome":"replied","message_id":"msg-1"}`,
			wantOutcome: messaging.OutcomeReplied,
		},
		{
			name:   "a rejected credential",
			status: http.StatusUnauthorized,
			raw:    `{}`,
			// The auth layer answers before a messaging handler does, so the
			// status carries the verdict.
			wantErr:     messaging.ErrAuthRejected,
			wantOutcome: messaging.OutcomePossiblyPending,
		},
		{
			name:        "a refused request",
			status:      http.StatusForbidden,
			raw:         `{}`,
			wantErr:     messaging.ErrNotAskTarget,
			wantOutcome: messaging.OutcomePossiblyPending,
		},
		{
			name:        "an unreachable target",
			status:      http.StatusNotFound,
			raw:         `{}`,
			wantErr:     messaging.ErrTargetUnavailable,
			wantOutcome: messaging.OutcomePossiblyPending,
		},
		{
			name:        "a malformed request",
			status:      http.StatusBadRequest,
			raw:         `{}`,
			wantErr:     messaging.ErrInvalidRequest,
			wantOutcome: messaging.OutcomePossiblyPending,
		},
		{
			name:        "an unreachable parent",
			status:      http.StatusInternalServerError,
			raw:         `{}`,
			wantErr:     ErrParentUnavailable,
			wantOutcome: messaging.OutcomePossiblyPending,
		},
		{
			name:        "a coded refusal",
			status:      http.StatusOK,
			raw:         `{"code":"generation-mismatch","error":"the ask store was replaced"}`,
			wantErr:     messaging.ErrGenerationMismatch,
			wantOutcome: messaging.OutcomeNotSent,
		},
		{
			name:        "a refused credential in a code",
			status:      http.StatusOK,
			raw:         `{"code":"auth-rejected"}`,
			wantErr:     messaging.ErrAuthRejected,
			wantOutcome: messaging.OutcomeAuthRejected,
		},
		{
			name:        "a code outside the contract",
			status:      http.StatusOK,
			raw:         `{"code":"not-a-wire-code"}`,
			wantErr:     ErrUnsupportedParentProtocol,
			wantOutcome: messaging.OutcomeNotSent,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			body, err := decode(testCase.status, []byte(testCase.raw))
			if testCase.wantErr == nil {
				if err != nil {
					t.Fatalf("decode = %v, want no error", err)
				}
			} else if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("decode = %v, want %v", err, testCase.wantErr)
			}
			if got := body.outcomeOf(); got != testCase.wantOutcome {
				t.Fatalf("outcome = %q, want %q", got, testCase.wantOutcome)
			}
		})
	}
}

// TestDecodeReportsASendOutcomeForAFailedAnswer pins the send verdict a failed
// answer carries, so a send that the parent refused reports the refusal's own
// outcome rather than a generic uncertainty.
func TestDecodeReportsASendOutcomeForAFailedAnswer(t *testing.T) {
	body, err := decode(http.StatusOK, []byte(`{"code":"auth-rejected"}`))
	if !errors.Is(err, messaging.ErrAuthRejected) {
		t.Fatalf("decode = %v, want ErrAuthRejected", err)
	}
	if got := body.sendOutcomeOf(); got != messaging.OutcomeAuthRejected {
		t.Fatalf("send outcome = %q, want %q", got, messaging.OutcomeAuthRejected)
	}
}
