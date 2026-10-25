package parentlink

import (
	"errors"
	"net/http"
	"slices"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestRelayRefusalRetiresTheRefusedRunOnTheHost pins the host half of a
// refusal: when a child refuses the relay for one run — its credential or the
// run itself — the relay not only drops the claim but also retires the host
// broker's own record for that run, so the run stops being served locally.
// A branch that merely failed retires nothing.
func TestRelayRefusalRetiresTheRefusedRunOnTheHost(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		body       string
		wantRetire []string
	}{
		{
			name:       "a refused credential",
			status:     http.StatusUnauthorized,
			body:       `{"code":"auth-rejected","error":"the branch refused the credential"}`,
			wantRetire: []string{"run-b"},
		},
		{
			name:       "a branch that does not know the run",
			status:     http.StatusNotFound,
			body:       `{}`,
			wantRetire: []string{"run-b"},
		},
		{
			name:   "a failure that says nothing about the branch",
			status: http.StatusInternalServerError,
			body:   `{}`,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			stub := newStubBroker(t, func(req stubRequest) (int, string) {
				return testCase.status, testCase.body
			})
			registry := NewRegistry()
			relay := newTestRelay(t, registry, "child-one", stub.URL(), "gen-a", "run-b", "run-c")
			var retired []string
			relay.retire = func(runID string) { retired = append(retired, runID) }
			ctx := testContext(t)

				err := relay.Ask(ctx, messaging.SendRequest{
				From:      messaging.Identity{RunID: "run-a", Sender: "alice"},
				To:        messaging.Identity{RunID: "run-b", Sender: "bob"},
				MessageID: "msg-1",
			})
			wantError := messaging.ErrTargetUnavailable
			if testCase.wantRetire == nil {
				wantError = ErrParentUnavailable
			}
			if !errors.Is(err, wantError) {
				t.Fatalf("Ask = %v, want %v", err, wantError)
			}
			if !slices.Equal(retired, testCase.wantRetire) {
				t.Fatalf("the relay retired %v, want %v", retired, testCase.wantRetire)
			}
		})
	}
}

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
			name:        "a refused credential",
			status:      http.StatusUnauthorized,
			body:        `{"code":"auth-rejected","error":"the branch refused the credential"}`,
			wantRunGone: true,
			wantError:   messaging.ErrTargetUnavailable,
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

// TestRelayRefusesARunWithoutACredential pins the pre-flight guard: a run
// whose claim handed over no credential has a next hop the relay cannot
// authenticate against, so the relay refuses before any HTTP call, the claim
// stays registered, and a credentialed sibling run still routes.
func TestRelayRefusesARunWithoutACredential(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		return http.StatusOK, `{}`
	})
	registry := NewRegistry()
	if _, err := registry.Register(messaging.DescendantRegistration{
		ChildID:    "child-one",
		Endpoint:   stub.URL(),
		Generation: "gen-a",
		Runs: []messaging.RunClaim{
			{RunID: "run-b", Token: "tok-b"},
			{RunID: "run-c"},
		},
	}); err != nil {
		t.Fatalf("register child: %v", err)
	}
	relay, err := newRelay(Options{Generation: func() string { return "gen-a" }}, registry)
	if err != nil {
		t.Fatalf("newRelay: %v", err)
	}
	t.Cleanup(func() { _ = relay.Close() })
	ctx := testContext(t)

	err = relay.Ask(ctx, messaging.SendRequest{
		From:      messaging.Identity{RunID: "run-a", Sender: "alice"},
		To:        messaging.Identity{RunID: "run-c", Sender: "bob"},
		MessageID: "msg-1",
	})
	if !errors.Is(err, messaging.ErrTargetUnavailable) {
		t.Fatalf("Ask without a credential = %v, want ErrTargetUnavailable", err)
	}
	if got := stub.countPath(pathRelay); got != 0 {
		t.Fatalf("the credential-less run saw %d relays, want the pre-flight guard to fire before any HTTP call", got)
	}
	if _, ok := registry.Hop("run-c"); !ok {
		t.Fatal("the claim without a credential was forgotten, want it kept for a replacement registration")
	}

	if err := relay.Ask(ctx, messaging.SendRequest{
		From:      messaging.Identity{RunID: "run-a", Sender: "alice"},
		To:        messaging.Identity{RunID: "run-b", Sender: "bob"},
		MessageID: "msg-2",
	}); err != nil {
		t.Fatalf("Ask the credentialed sibling = %v, want the relay delivered", err)
	}
	if got := stub.countPath(pathRelay); got != 1 {
		t.Fatalf("the credentialed sibling saw %d relays, want one", got)
	}
}

func TestRelayRefusalRetiresTheAdoptedRunRecords(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		if req.Body["to_run_id"] == "run-c" {
			return http.StatusConflict, `{"code":"generation-mismatch","error":"the ask store was replaced"}`
		}
		return http.StatusUnauthorized, `{"code":"auth-rejected","error":"refused"}`
	})
	registry := NewRegistry()
	var retired []string
	relay, err := newRelay(Options{
		Generation: func() string { return "gen-a" },
		RetireRuns: func(runIDs []string) { retired = append(retired, runIDs...) },
	}, registry)
	if err != nil {
		t.Fatalf("newRelay: %v", err)
	}
	t.Cleanup(func() { _ = relay.Close() })
	if _, err := registry.Register(messaging.DescendantRegistration{
		ChildID:    "child-one",
		Endpoint:   stub.URL(),
		Generation: "gen-a",
		Runs:       []messaging.RunClaim{{RunID: "run-b", Token: "tok-b"}, {RunID: "run-c", Token: "tok-c"}},
	}); err != nil {
		t.Fatalf("register child: %v", err)
	}
	ctx := testContext(t)

	if err := relay.Ask(ctx, messaging.SendRequest{
		From:      messaging.Identity{RunID: "run-a", Sender: "alice"},
		To:        messaging.Identity{RunID: "run-b", Sender: "bob"},
		MessageID: "msg-1",
	}); !errors.Is(err, messaging.ErrTargetUnavailable) {
		t.Fatalf("Ask run-b = %v, want ErrTargetUnavailable", err)
	}
	if err := relay.Ask(ctx, messaging.SendRequest{
		From:      messaging.Identity{RunID: "run-a", Sender: "alice"},
		To:        messaging.Identity{RunID: "run-c", Sender: "bob"},
		MessageID: "msg-2",
	}); !errors.Is(err, messaging.ErrTargetUnavailable) {
		t.Fatalf("Ask run-c = %v, want ErrTargetUnavailable", err)
	}

	if len(retired) != 2 || retired[0] != "run-b" || retired[1] != "run-c" {
		t.Fatalf("retired = %v, want the refused run then the deregistered branch's run", retired)
	}
	if _, ok := registry.Hop("run-b"); ok {
		t.Fatal("run-b still routes after its record was retired")
	}
	if _, ok := registry.Hop("run-c"); ok {
		t.Fatal("run-c still routes after the branch was deregistered")
	}
}
