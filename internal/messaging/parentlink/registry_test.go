package parentlink

import (
	"errors"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestRegistryClaimsRunsAndRetiresTheOnesAChildStopsOwning pins the descendant
// registry's bookkeeping: a claim replaces the child's previous run set, a run
// another child owns is refused, and the next hop carries the endpoint and
// generation that child registered with.
func TestRegistryClaimsRunsAndRetiresTheOnesAChildStopsOwning(t *testing.T) {
	registry := NewRegistry()
	first, err := registry.Register(messaging.DescendantRegistration{
		ChildID:    "one",
		Endpoint:   "http://one",
		Generation: "gen-one",
		Runs:       []messaging.RunClaim{{RunID: "run-a", Label: "alpha"}, {RunID: "run-b"}},
	})
	if err != nil {
		t.Fatalf("register first child: %v", err)
	}
	if len(first.Adopted) != 2 || len(first.Retired) != 0 {
		t.Fatalf("first claim = %+v, want both runs adopted and none retired", first)
	}
	next, ok := registry.Hop(ParentRunKey("one", "run-a"))
	if !ok || next.Endpoint != "http://one" || next.Generation != "gen-one" || next.Label != "alpha" {
		t.Fatalf("hop for run-a = (%+v, %v), want the child's endpoint, generation and label", next, ok)
	}
	if next.RunID != "run-a" {
		t.Fatalf("hop for run-a targets %q, want the canonical run ID the branch knows", next.RunID)
	}

	// A re-claim by the child that owns the run is the same run: the namespaced
	// key is what identifies it, so the claim is adopted, not refused.
	if _, err := registry.Register(messaging.DescendantRegistration{
		ChildID:    "one",
		Endpoint:   "http://one-again-claim",
		Generation: "gen-one-again",
		Runs:       []messaging.RunClaim{{RunID: "run-a"}},
	}); err != nil {
		t.Fatalf("re-claim by the owning child = %v, want it adopted", err)
	}
	if owner, ok := registry.Owner(ParentRunKey("one", "run-a")); !ok || owner != "one" {
		t.Fatalf("owner of run-a = (%q, %v) after a re-claim, want the first child", owner, ok)
	}
	// A child ID is supplied by the caller, so it is not trusted to keep the
	// namespaced keys of two children apart: a claim key another child already
	// owns is refused whatever run IDs produced it.
	if _, err := registry.Register(messaging.DescendantRegistration{
		ChildID:    "one",
		Endpoint:   "http://one",
		Generation: "gen-one",
		Runs:       []messaging.RunClaim{{RunID: "run-a"}, {RunID: "run-x@two"}},
	}); err != nil {
		t.Fatalf("claim a run under a key a sibling will collide with: %v", err)
	}
	if _, err := registry.Register(messaging.DescendantRegistration{
		ChildID:    "two@one",
		Endpoint:   "http://colliding",
		Generation: "gen-colliding",
		Runs:       []messaging.RunClaim{{RunID: "run-x"}},
	}); !errors.Is(err, ErrRunClaimed) || !errors.Is(err, messaging.ErrInvalidRequest) {
		t.Fatalf("a claim on a key another child owns = %v, want ErrRunClaimed", err)
	}
	if owner, ok := registry.Owner("run-x@two@one"); !ok || owner != "one" {
		t.Fatalf("owner of the colliding key = (%q, %v), want the child that claimed it first", owner, ok)
	}

	// A re-registration replaces the claim, so the runs it stopped naming are
	// retired.
	second, err := registry.Register(messaging.DescendantRegistration{
		ChildID:    "one",
		Endpoint:   "http://one-again",
		Generation: "gen-one-two",
		Runs:       []messaging.RunClaim{{RunID: "run-c"}},
	})
	if err != nil {
		t.Fatalf("re-register first child: %v", err)
	}
	if len(second.Adopted) != 1 || len(second.Retired) != 2 {
		t.Fatalf("re-registration = %+v, want run-c adopted and two retired", second)
	}
	if registry.Owns(ParentRunKey("one", "run-a")) || registry.Owns(ParentRunKey("one", "run-b")) {
		t.Fatal("a retired run is still owned")
	}
	if next, ok := registry.Hop(ParentRunKey("one", "run-c")); !ok || next.Generation != "gen-one-two" {
		t.Fatalf("hop for run-c = (%+v, %v), want the replacement generation", next, ok)
	}

	// Deregistering retires the child and reports everything it held.
	runs, err := registry.Deregister("one")
	if err != nil {
		t.Fatalf("deregister child: %v", err)
	}
	if len(runs) != 1 || runs[0] != ParentRunKey("one", "run-c") {
		t.Fatalf("deregistered runs = %v, want run-c's claim key", runs)
	}
	if _, ok := registry.Hop(ParentRunKey("one", "run-c")); ok {
		t.Fatal("a deregistered child still owns a run")
	}
	if _, err := registry.Deregister("one"); !errors.Is(err, messaging.ErrTargetUnavailable) {
		t.Fatalf("deregistering an unknown child = %v, want ErrTargetUnavailable", err)
	}
}

// TestRegistryNamespacesBranchSupervisorRuns pins the branch-supervisor
// identity: every branch reserves the same supervisor run ID, so a descendant's
// supervisor is addressed under a branch-namespaced run ID while the run the
// relay posts to stays the canonical one that branch knows.
func TestRegistryNamespacesBranchSupervisorRuns(t *testing.T) {
	registry := NewRegistry()
	for _, child := range []string{"one", "two"} {
		if _, err := registry.Register(messaging.DescendantRegistration{
			ChildID: child, Endpoint: "http://" + child, Generation: "g-" + child,
			Runs: []messaging.RunClaim{{RunID: messaging.SupervisorRunID, Label: "supervisor"}},
		}); err != nil {
			t.Fatalf("register %s: %v", child, err)
		}
	}
	for _, child := range []string{"one", "two"} {
		key := messaging.SupervisorRunID + "@" + child
		if owner, ok := registry.Owner(key); !ok || owner != child {
			t.Fatalf("owner of %q = (%q, %v), want %q", key, owner, ok, child)
		}
		hop, ok := registry.Hop(key)
		if !ok {
			t.Fatalf("no hop for %q", key)
		}
		if hop.RunID != messaging.SupervisorRunID {
			t.Fatalf("hop for %q targets %q, want the branch's canonical supervisor run", key, hop.RunID)
		}
		if hop.Generation != "g-"+child {
			t.Fatalf("hop for %q carries generation %q, want the claiming branch's", key, hop.Generation)
		}
	}
	// The reserved ID itself stays unclaimed, so this broker's own supervisor
	// run is never confused with a descendant's.
	if registry.Owns(messaging.SupervisorRunID) {
		t.Fatal("a descendant claim took the parent's own supervisor run ID")
	}
	// Two branches that mint the same run ID own two different runs: a branch
	// mints its own run IDs from its own counter, so the literal ID is not a
	// claim on another branch's run.
	for _, child := range []string{"one", "two"} {
		if _, err := registry.Register(messaging.DescendantRegistration{
			ChildID: child, Endpoint: "http://" + child, Generation: "g-" + child,
			Runs: []messaging.RunClaim{{RunID: "rt_1", Label: "first child"}},
		}); err != nil {
			t.Fatalf("%s claims rt_1: %v, want its own namespaced run", child, err)
		}
	}
	for _, child := range []string{"one", "two"} {
		if owner, ok := registry.Owner(ParentRunKey(child, "rt_1")); !ok || owner != child {
			t.Fatalf("owner of rt_1 in %s = (%q, %v), want that branch", child, owner, ok)
		}
	}
	if registry.Owns("rt_1") {
		t.Fatal("a sibling branch's run ID is claimed as a canonical run")
	}
}

// TestRegistryExportNarrowsToTheCallersBranch pins the export rule at the
// registry: a branch sees its own runs and this broker's own runs, a sibling
// branch's runs stay hidden, and this broker's own caller sees its descendants.
func TestRegistryExportNarrowsToTheCallersBranch(t *testing.T) {
	registry := NewRegistry()
	if _, err := registry.Register(messaging.DescendantRegistration{
		ChildID: "one", Endpoint: "http://one", Generation: "g1",
		Runs: []messaging.RunClaim{{RunID: "asker"}},
	}); err != nil {
		t.Fatalf("register first child: %v", err)
	}
	if _, err := registry.Register(messaging.DescendantRegistration{
		ChildID: "two", Endpoint: "http://two", Generation: "g2",
		Runs: []messaging.RunClaim{{RunID: "target"}},
	}); err != nil {
		t.Fatalf("register second child: %v", err)
	}
	peers := []messaging.Peer{{RunID: "orchestrator"}, {RunID: ParentRunKey("one", "asker")}, {RunID: ParentRunKey("two", "target")}}

	exported := runIDsOf(registry.Export(peers, ParentRunKey("one", "asker")))
	if !exported["orchestrator"] || !exported[ParentRunKey("one", "asker")] || exported[ParentRunKey("two", "target")] {
		t.Fatalf("export to a branch = %v, want the owner's runs and its own branch only", exported)
	}
	exported = runIDsOf(registry.Export(peers, "orchestrator"))
	if !exported["orchestrator"] || !exported[ParentRunKey("one", "asker")] || !exported[ParentRunKey("two", "target")] {
		t.Fatalf("export to this broker's own caller = %v, want every owned descendant", exported)
	}
}

func runIDsOf(peers []messaging.Peer) map[string]bool {
	ids := make(map[string]bool, len(peers))
	for _, peer := range peers {
		ids[peer.RunID] = true
	}
	return ids
}
