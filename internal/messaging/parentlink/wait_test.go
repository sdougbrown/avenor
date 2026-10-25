package parentlink

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestWaitReportsAnUnsentWaitWhenTheParentIsUnreachable pins the wait's
// transport classification: a wait whose request was refused before any byte
// was written never carried the wait, so the edge was never asked and the
// wait reports that instead of an unknown fate.
func TestWaitReportsAnUnsentWaitWhenTheParentIsUnreachable(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		case pathSend:
			return http.StatusOK, `{"outcome":"sent"}`
		}
		return http.StatusOK, `{}`
	})
	ctx := testContext(t)
	link := newTestLink(t, stub, "gen-a")
	if sent, err := link.Send(ctx, askRequest("msg-1")); err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("Send = (%+v, %v), want the ask admitted", sent, err)
	}
	stub.server.Close()
	// The wait must be refused before any byte is written. A request carried on a
	// connection the closed stub left pooled can surface the shutdown after the
	// request was written, which reads as an unknown fate instead of a refusal,
	// so the wait is sent on a transport with no connection to reuse.
	link.client.SetTransport(http.DefaultTransport.(*http.Transport).Clone())

	waited, err := link.Wait(ctx, waitRequest("msg-1"))
	if waited.Outcome != messaging.OutcomeNotSent {
		t.Fatalf("Wait outcome = %q, want %q", waited.Outcome, messaging.OutcomeNotSent)
	}
	if !errors.Is(err, ErrParentUnavailable) {
		t.Fatalf("Wait error = %v, want ErrParentUnavailable", err)
	}
}

// TestWaitReportsTheCancellationToTheCaller pins the wait's other transport
// classification: a wait the caller cancelled leaves the edge's fate unknown,
// and the caller hears the cancellation instead of a silent success.
func TestWaitReportsTheCancellationToTheCaller(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		case pathSend:
			return http.StatusOK, `{"outcome":"sent"}`
		}
		return http.StatusOK, `{}`
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	link := newTestLink(t, stub, "gen-a")
	if sent, err := link.Send(ctx, askRequest("msg-1")); err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("Send = (%+v, %v), want the ask admitted", sent, err)
	}
	cancel()

	waited, err := link.Wait(ctx, waitRequest("msg-1"))
	if waited.Outcome != messaging.OutcomePossiblyPending {
		t.Fatalf("Wait outcome = %q, want %q", waited.Outcome, messaging.OutcomePossiblyPending)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait error = %v, want the caller's cancellation", err)
	}
}

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
