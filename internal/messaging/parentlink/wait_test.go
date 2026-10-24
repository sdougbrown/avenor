package parentlink

import (
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestWaitReportsTheRefusalWhenTheParentCannotBeReRegistered pins the wait's
// error contract: a parent that refuses the pinned generation and cannot be
// re-registered leaves the ask's fate unknown, and the wait reports that
// refusal the same way the withdrawal reports it for the same state.
func TestWaitReportsTheRefusalWhenTheParentCannotBeReRegistered(t *testing.T) {
	var registrations atomic.Int32
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			if registrations.Add(1) == 1 {
				return http.StatusOK, `{"generation":"gen-a"}`
			}
			return http.StatusInternalServerError, `{}`
		case pathSend:
			return http.StatusOK, `{"outcome":"sent"}`
		case pathWait, pathWithdraw:
			return http.StatusConflict, `{"code":"generation-mismatch","error":"the ask store was replaced"}`
		}
		return http.StatusOK, `{}`
	})
	ctx := testContext(t)
	link := newTestLink(t, stub, "gen-a")

	sent, err := link.Send(ctx, askRequest("msg-1"))
	if err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("Send = (%+v, %v), want the ask admitted", sent, err)
	}

	waited, err := link.Wait(ctx, waitRequest("msg-1"))
	if waited.Outcome != messaging.OutcomePossiblyPending {
		t.Fatalf("Wait outcome = %q, want %q", waited.Outcome, messaging.OutcomePossiblyPending)
	}
	if !errors.Is(err, messaging.ErrGenerationMismatch) {
		t.Fatalf("Wait error = %v, want the parent's generation refusal", err)
	}

	withdrawn, withdrawErr := link.Withdraw(ctx, withdrawRequest("msg-1"))
	if withdrawn.Outcome != messaging.OutcomePossiblyPending {
		t.Fatalf("Withdraw outcome = %q, want %q", withdrawn.Outcome, messaging.OutcomePossiblyPending)
	}
	if !errors.Is(withdrawErr, messaging.ErrGenerationMismatch) {
		t.Fatalf("Withdraw error = %v, want the parent's generation refusal", withdrawErr)
	}
}
