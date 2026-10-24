package parentlink

import (
	"net/http"
	"sync/atomic"
	"testing"
)

// TestLinkPublishesItsClaimAndCoalescesChanges pins the claim's lifecycle: the
// link publishes its run set when it starts, a burst of changes costs one
// registration, and closing the link retracts the claim.
func TestLinkPublishesItsClaimAndCoalescesChanges(t *testing.T) {
	var registrations atomic.Int32
	registered := make(chan struct{}, 8)
	release := make(chan struct{})
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			count := registrations.Add(1)
			registered <- struct{}{}
			if count == 2 {
				// The second registration waits for the test, so the burst
				// that follows lands while the link is busy publishing.
				<-release
			}
			return http.StatusOK, `{"generation":"gen-a"}`
		case pathDeregister:
			return http.StatusOK, `{}`
		}
		return http.StatusOK, `{}`
	})
	ctx := testContext(t)
	link := newTestLink(t, stub, "gen-a")

	if err := link.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	<-registered
	if got := registrations.Load(); got != 1 {
		t.Fatalf("registrations = %d, want the initial claim alone", got)
	}

	// The first change starts a publication the test holds open.
	link.RunChanged()
	<-registered
	// A burst of further changes lands while that publication is in flight, so
	// it costs one publication rather than one per change.
	for i := 0; i < 3; i++ {
		link.RunChanged()
	}
	close(release)
	<-registered

	if got := registrations.Load(); got != 3 {
		t.Fatalf("registrations = %d, want the initial claim, the held publication and one coalesced publication", got)
	}

	if err := link.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := stub.countPath(pathDeregister); got != 1 {
		t.Fatalf("deregistrations = %d, want the claim retracted once", got)
	}
}

// TestLinkKeepsItsClaimWhenTheParentIsUnreachable pins the claim's rule for
// an unreachable parent: the parent keeps the claim it last accepted, and the
// next publication replaces it.
func TestLinkKeepsItsClaimWhenTheParentIsUnreachable(t *testing.T) {
	var reachable atomic.Bool
	reachable.Store(true)
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			if !reachable.Load() {
				return http.StatusInternalServerError, `{}`
			}
			return http.StatusOK, `{"generation":"gen-a"}`
		case pathSend:
			return http.StatusOK, `{"outcome":"sent"}`
		}
		return http.StatusOK, `{}`
	})
	ctx := testContext(t)
	link := newTestLink(t, stub, "gen-a")

	if err := link.Sync(ctx); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if generation, pinned := link.pinnedGeneration(); !pinned || generation != "gen-a" {
		t.Fatalf("pinned generation = (%q, %v), want the parent's generation", generation, pinned)
	}

	reachable.Store(false)
	if err := link.Sync(ctx); err == nil {
		t.Fatal("Sync against an unreachable parent = nil, want an error")
	}
	if generation, pinned := link.pinnedGeneration(); !pinned || generation != "gen-a" {
		t.Fatalf("pinned generation = (%q, %v), want the claim the parent last accepted", generation, pinned)
	}
	// The claim the parent last accepted is still the one this branch
	// forwards under.
	if _, err := link.Send(ctx, askRequest("msg-1")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	forwarded := stub.lastRequest(t, pathSend)
	if forwarded.Body["generation"] != "gen-a" {
		t.Fatalf("forwarded generation = %v, want the claim the parent last accepted", forwarded.Body["generation"])
	}

	reachable.Store(true)
	if err := link.Sync(ctx); err != nil {
		t.Fatalf("Sync after the parent came back: %v", err)
	}
	if generation, pinned := link.pinnedGeneration(); !pinned || generation != "gen-a" {
		t.Fatalf("pinned generation = (%q, %v), want the republished claim", generation, pinned)
	}
}

// TestLinkClosesWithoutAParent pins the close of a link whose parent is
// already gone: the retraction is best effort, so the close still succeeds.
func TestLinkClosesWithoutAParent(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		return http.StatusInternalServerError, `{}`
	})
	link := newTestLink(t, stub, "gen-a")
	if err := link.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := link.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestLinkRefusesAClaimWithoutItsParts pins the link's construction contract:
// a link needs the parent's address, its own endpoint and the run set and
// generation it publishes.
func TestLinkRefusesAClaimWithoutItsParts(t *testing.T) {
	cases := []struct {
		name string
		opts Options
	}{
		{
			name: "no parent",
			opts: Options{Endpoint: "http://child-one", Runs: testRuns, Generation: func() string { return "gen-a" }},
		},
		{
			name: "no endpoint",
			opts: Options{ParentURL: "http://parent", Runs: testRuns, Generation: func() string { return "gen-a" }},
		},
		{
			name: "no run set",
			opts: Options{ParentURL: "http://parent", Endpoint: "http://child-one", Generation: func() string { return "gen-a" }},
		},
		{
			name: "no generation",
			opts: Options{ParentURL: "http://parent", Endpoint: "http://child-one", Runs: testRuns},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := NewLink(testCase.opts); err == nil {
				t.Fatal("NewLink = nil error, want the missing part refused")
			}
		})
	}
}

// TestLinkRefusesWorkAfterItIsClosed pins the link's closed contract: the
// closed transport refuses to carry a registration, so a late caller cannot
// resurrect the claim the close retracted.
func TestLinkRefusesWorkAfterItIsClosed(t *testing.T) {
	stub := newStubBroker(t, func(req stubRequest) (int, string) {
		return http.StatusOK, `{"generation":"gen-a"}`
	})
	ctx := testContext(t)
	link := newTestLink(t, stub, "gen-a")
	// A wait and a withdrawal with a recorded ask route refuse the same way:
	// the closed transport cannot re-sync the credentials the hop needs.
	link.recordAsk("run-a", "msg-1", askRoute{generation: "gen-a"})
	if err := link.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := link.Sync(ctx); err == nil {
		t.Fatal("Sync after the close = nil, want an error")
	}

	// The routed operations refuse the same way: a closed transport carries no
	// ask, wait, withdrawal, or listing.
	if _, err := link.Send(ctx, askRequest("msg-1")); err == nil {
		t.Fatal("Send after the close = nil, want an error")
	}
	if _, err := link.Wait(ctx, waitRequest("msg-1")); err == nil {
		t.Fatal("Wait after the close = nil, want an error")
	}
	if _, err := link.Withdraw(ctx, withdrawRequest("msg-1")); err == nil {
		t.Fatal("Withdraw after the close = nil, want an error")
	}
	if _, err := link.Peers(ctx); err == nil {
		t.Fatal("Peers after the close = nil, want an error")
	}
}
