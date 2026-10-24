package parentlink

import (
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestWithdrawReportsAnUncertainFateWhenTheCredentialFails pins the
// withdrawal's classification of a credential it cannot establish: the edge
// may already be withdrawn at the parent, so the verdict is unknown and the
// outcome stays inside the withdrawal contract.
func TestWithdrawReportsAnUncertainFateWhenTheCredentialFails(t *testing.T) {
	var registrations atomic.Int32
	var runs atomic.Value
	runs.Store([]messaging.RunClaim{{RunID: "run-a", Label: "alpha", Token: "tok-a"}})
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			if registrations.Add(1) <= 2 {
				return http.StatusOK, `{"generation":"gen-a"}`
			}
			return http.StatusInternalServerError, `{}`
		case pathSend:
			return http.StatusOK, `{"outcome":"sent"}`
		}
		return http.StatusOK, `{}`
	})
	ctx := testContext(t)
	link, err := NewLink(Options{
		ParentURL:  stub.URL(),
		Endpoint:   "http://child-one",
		ChildID:    "child-one",
		Runs:       func() []messaging.RunClaim { return runs.Load().([]messaging.RunClaim) },
		Generation: func() string { return "gen-a" },
	})
	if err != nil {
		t.Fatalf("NewLink: %v", err)
	}
	t.Cleanup(func() { _ = link.Close() })

	if sent, err := link.Send(ctx, askRequest("msg-1")); err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("Send = (%+v, %v), want the ask admitted", sent, err)
	}
	// The branch stops owning the run, so the parent holds none of its
	// credentials, and the parent stops answering registrations so no
	// credential can be established again.
	runs.Store([]messaging.RunClaim{})
	if err := link.Sync(ctx); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	withdrawn, err := link.Withdraw(ctx, withdrawRequest("msg-1"))
	if err == nil {
		t.Fatal("Withdraw error = nil, want the credential failure")
	}
	if withdrawn.Outcome != messaging.OutcomePossiblyPending {
		t.Fatalf("Withdraw outcome = %q, want %q", withdrawn.Outcome, messaging.OutcomePossiblyPending)
	}
	if !messaging.IsWithdrawOutcome(withdrawn.Outcome) {
		t.Fatalf("Withdraw outcome %q is outside the withdrawal contract", withdrawn.Outcome)
	}
}
// pins the withdrawal's guard against an answer that describes no withdrawal
// verdict: a response with neither a wire code nor an outcome, and a response
// with an outcome a withdrawal never reports, are both protocol mismatches
// rather than a withdrawal that succeeded with an off-contract outcome.
func TestWithdrawReportsTheProtocolErrorWhenTheParentAnswersWithoutAVerdict(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{
			name: "no verdict at all",
			body: `{"message_id":"msg-1"}`,
		},
		{
			name: "an outcome a withdrawal never reports",
			body: `{"outcome":"sent","message_id":"msg-1"}`,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			stub := newStubBroker(t, func(req stubRequest) (int, string) {
				switch req.Path {
				case pathRegister:
					return http.StatusOK, `{"generation":"gen-a"}`
				case pathSend:
					return http.StatusOK, `{"outcome":"sent"}`
				case pathWithdraw:
					return http.StatusOK, testCase.body
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
		})
	}
}
