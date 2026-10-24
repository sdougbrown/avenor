package parentlink

import (
	"errors"
	"net/http"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestWithdrawReportsTheProtocolErrorWhenTheParentAnswersWithoutAnOutcome
// pins the withdrawal's guard against an answer that describes no verdict: a
// 2xx response with neither a wire code nor an outcome is a protocol
// mismatch, not a withdrawal that succeeded with an empty outcome.
func TestWithdrawReportsTheProtocolErrorWhenTheParentAnswersWithoutAnOutcome(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		case pathSend:
			return http.StatusOK, `{"outcome":"sent"}`
		case pathWithdraw:
			return http.StatusOK, `{"message_id":"msg-1"}`
		}
		return http.StatusOK, `{}`
	})
	ctx := testContext(t)
	link := newTestLink(t, stub, "gen-a")

	sent, err := link.Send(ctx, askRequest("msg-1"))
	if err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("Send = (%+v, %v), want the ask admitted", sent, err)
	}

	withdrawn, err := link.Withdraw(ctx, withdrawRequest("msg-1"))
	if withdrawn.Outcome != messaging.OutcomePossiblyPending {
		t.Fatalf("Withdraw outcome = %q, want %q", withdrawn.Outcome, messaging.OutcomePossiblyPending)
	}
	if !errors.Is(err, ErrUnsupportedParentProtocol) {
		t.Fatalf("Withdraw error = %v, want ErrUnsupportedParentProtocol", err)
	}
}
