package parentlink

import (
	"errors"
	"net/http"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestRelayRefusalStopsTheBranchAndKeepsAHealthyBranch pins the relay's
// refusal rule: a child that refuses the relay because it is no longer the
// branch it registered as stops being a routing target, a child that retired
// one run loses that run alone, and a child that merely failed keeps its
// claim and is retried.
func TestRelayRefusalStopsTheBranchAndKeepsAHealthyBranch(t *testing.T) {
	cases := []struct {
		name string
		// status and body are the answer the child gives to the relay.
		status int
		body   string
		// wantRunGone reports whether the run that was relayed to stops
		// being routed.
		wantRunGone bool
		// wantSiblingGone reports whether the child's other run stops being
		// routed with it.
		wantSiblingGone bool
		wantError       error
	}{
		{
			name:            "a refused credential",
			status:          http.StatusUnauthorized,
			body:            `{"code":"auth-rejected","error":"the branch refused the credential"}`,
			wantRunGone:     true,
			wantSiblingGone: true,
			wantError:       messaging.ErrTargetUnavailable,
		},
		{
			name:        "a branch that does not know the run",
			status:      http.StatusNotFound,
			body:        `{}`,
			wantRunGone: true,
			wantError:   messaging.ErrTargetUnavailable,
		},
		{
			name:        "a failure that says nothing about the branch",
			status:      http.StatusInternalServerError,
			body:        `{}`,
			wantRunGone: false,
			wantError:   ErrParentUnavailable,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			stub := newStubBroker(t, func(req stubRequest) (int, string) {
				return testCase.status, testCase.body
			})
			registry := NewRegistry()
			relay := newTestRelay(t, registry, "child-one", stub.URL(), "gen-a", "run-b", "run-c")
			ctx := testContext(t)

			err := relay.Ask(ctx, messaging.SendRequest{
				From:      messaging.Identity{RunID: "run-a", Sender: "alice"},
				To:        messaging.Identity{RunID: "run-b", Sender: "bob"},
				MessageID: "msg-1",
			})
			if !errors.Is(err, testCase.wantError) {
				t.Fatalf("Ask = %v, want %v", err, testCase.wantError)
			}
			if _, ok := registry.Hop("run-b"); ok == testCase.wantRunGone {
				t.Fatalf("the relayed run routes = %v, want %v", ok, !testCase.wantRunGone)
			}
			if _, ok := registry.Hop("run-c"); ok == testCase.wantSiblingGone {
				t.Fatalf("the child's other run routes = %v, want %v", ok, !testCase.wantSiblingGone)
			}

			// A branch that is still a routing target is retried, and a branch
			// that is gone is not.
			again := relay.Ask(ctx, messaging.SendRequest{
				From:      messaging.Identity{RunID: "run-a", Sender: "alice"},
				To:        messaging.Identity{RunID: "run-c", Sender: "bob"},
				MessageID: "msg-2",
			})
			if testCase.wantSiblingGone {
				if !errors.Is(again, messaging.ErrTargetUnavailable) {
					t.Fatalf("second Ask = %v, want ErrTargetUnavailable", again)
				}
				if got := stub.countPath(pathRelay); got != 1 {
					t.Fatalf("the refused branch saw %d relays, want one", got)
				}
			} else if got := stub.countPath(pathRelay); got != 2 {
				t.Fatalf("the branch saw %d relays, want the healthy branch to be retried", got)
			}
		})
	}
}
