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
		Runs:       []messaging.RunClaim{{RunID: "run-a"}, {RunID: "run-b"}},
	})
	if err != nil {
		t.Fatalf("register first child: %v", err)
	}
	if len(first.Retired) != 0 {
		t.Fatalf("first claim = %+v, want both runs adopted and none retired", first)
	}
	next, ok := registry.Hop("run-a")
	if !ok || next.Endpoint != "http://one" || next.Generation != "gen-one" {
		t.Fatalf("hop for run-a = (%+v, %v), want the child's endpoint and generation", next, ok)
	}

	// Another child cannot claim a run this one already owns, and a refused
	// claim applies nothing at all.
	refused, err := registry.Register(messaging.DescendantRegistration{
		ChildID:    "two",
		Endpoint:   "http://two",
		Generation: "gen-two",
		Runs:       []messaging.RunClaim{{RunID: "run-a"}},
	})
	if !errors.Is(err, ErrRunClaimed) || !errors.Is(err, messaging.ErrInvalidRequest) {
		t.Fatalf("conflicting claim = %v, want ErrRunClaimed", err)
	}
	if len(refused.Retired) != 0 {
		t.Fatalf("refused claim = %+v, want nothing adopted and nothing retired", refused)
	}
	if owner, ok := registry.Owner("run-a"); !ok || owner != "one" {
		t.Fatalf("owner of run-a = (%q, %v) after a refused claim, want the first child", owner, ok)
	}
	if next, ok := registry.Hop("run-a"); !ok || next.Endpoint != "http://one" || next.Generation != "gen-one" {
		t.Fatalf("hop for run-a = (%+v, %v) after a refused claim, want the surviving child's hop", next, ok)
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
	if len(second.Retired) != 2 {
		t.Fatalf("re-registration = %+v, want run-c adopted and two retired", second)
	}
	if registry.Owns("run-a") || registry.Owns("run-b") {
		t.Fatal("a retired run is still owned")
	}
	if next, ok := registry.Hop("run-c"); !ok || next.Generation != "gen-one-two" {
		t.Fatalf("hop for run-c = (%+v, %v), want the replacement generation", next, ok)
	}

	// Deregistering retires the child and reports everything it held.
	runs, err := registry.Deregister("one", "gen-one-two")
	if err != nil {
		t.Fatalf("deregister child: %v", err)
	}
	if len(runs) != 1 || runs[0] != "run-c" {
		t.Fatalf("deregistered runs = %v, want run-c", runs)
	}
	if _, ok := registry.Hop("run-c"); ok {
		t.Fatal("a deregistered child still owns a run")
	}
	if _, err := registry.Deregister("one", "g1"); !errors.Is(err, messaging.ErrTargetUnavailable) {
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
	// A canonical run ID that two branches both claim is still a genuine
	// duplicate and is refused.
	if _, err := registry.Register(messaging.DescendantRegistration{
		ChildID: "three", Endpoint: "http://three", Generation: "g-three",
		Runs: []messaging.RunClaim{{RunID: "run-a"}},
	}); err != nil {
		t.Fatalf("register three: %v", err)
	}
	if _, err := registry.Register(messaging.DescendantRegistration{
		ChildID: "four", Endpoint: "http://four", Generation: "g-four",
		Runs: []messaging.RunClaim{{RunID: "run-a"}},
	}); !errors.Is(err, ErrRunClaimed) {
		t.Fatalf("duplicate canonical claim = %v, want ErrRunClaimed", err)
	}
}

// TestRegistryReportsWhetherARegistrationChangedTheRunSet pins the changed
// report: a claim that adopts or retires a run changed the owned run set, a
// refresh that re-registers the same runs did not, and a rotated credential
// alone is not a run-set change.
func TestRegistryReportsWhetherARegistrationChangedTheRunSet(t *testing.T) {
	registry := NewRegistry()
	claim, err := registry.Register(messaging.DescendantRegistration{
		ChildID: "one", Endpoint: "http://one", Generation: "g1",
		Runs: []messaging.RunClaim{{RunID: "run-a", Token: "t1"}},
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if !claim.Changed {
		t.Fatal("the first claim of a run reported no change")
	}
	claim, err = registry.Register(messaging.DescendantRegistration{
		ChildID: "one", Endpoint: "http://one", Generation: "g1",
		Runs: []messaging.RunClaim{{RunID: "run-a", Token: "t1"}},
	})
	if err != nil {
		t.Fatalf("re-register: %v", err)
	}
	if claim.Changed || len(claim.Retired) != 0 {
		t.Fatalf("an unchanged re-registration = %+v, want nothing changed", claim)
	}
	// A rotated credential refreshes the claim without changing the run set.
	claim, err = registry.Register(messaging.DescendantRegistration{
		ChildID: "one", Endpoint: "http://one", Generation: "g1",
		Runs: []messaging.RunClaim{{RunID: "run-a", Token: "t2"}},
	})
	if err != nil {
		t.Fatalf("re-register with a new token: %v", err)
	}
	if claim.Changed {
		t.Fatal("a credential refresh alone reported a run-set change")
	}
	claim, err = registry.Register(messaging.DescendantRegistration{
		ChildID: "one", Endpoint: "http://one", Generation: "g1",
		Runs: []messaging.RunClaim{{RunID: "run-a", Token: "t2"}, {RunID: "run-b"}},
	})
	if err != nil {
		t.Fatalf("register an added run: %v", err)
	}
	if !claim.Changed {
		t.Fatal("adopting a new run reported no change")
	}
	claim, err = registry.Register(messaging.DescendantRegistration{
		ChildID: "one", Endpoint: "http://one", Generation: "g1",
		Runs: []messaging.RunClaim{{RunID: "run-a", Token: "t2"}},
	})
	if err != nil {
		t.Fatalf("register without the retired run: %v", err)
	}
	if !claim.Changed || len(claim.Retired) != 1 {
		t.Fatalf("retiring a run = %+v, want a change and one retired run", claim)
	}
}

// TestRegistryRefusesARunIDCarryingTheNamespacingSeparator pins the run ID a
// descendant may claim: a run ID carrying "@" reads as a branch-namespaced
// key, so a child claiming one could forge another branch's supervisor
// identity. The registration is refused before anything is applied.
func TestRegistryRefusesARunIDCarryingTheNamespacingSeparator(t *testing.T) {
	registry := NewRegistry()
	for _, runID := range []string{messaging.SupervisorRunID + "@victim", "run@a", "@"} {
		claim, err := registry.Register(messaging.DescendantRegistration{
			ChildID: "one", Endpoint: "http://one", Generation: "g1",
			Runs: []messaging.RunClaim{{RunID: runID}},
		})
		if !errors.Is(err, ErrInvalidRunID) || !errors.Is(err, messaging.ErrInvalidRequest) {
			t.Fatalf("claim of %q = %v, want ErrInvalidRunID", runID, err)
		}
		if len(claim.Runs) != 0 || len(claim.Retired) != 0 {
			t.Fatalf("refused claim of %q = %+v, want nothing applied", runID, claim)
		}
		if registry.Owns(runID) {
			t.Fatalf("the refused run %q was adopted", runID)
		}
	}
	// The child itself is not left registered by a refused claim.
	if _, err := registry.Deregister("one", "g1"); !errors.Is(err, messaging.ErrTargetUnavailable) {
		t.Fatalf("deregistering the refused child = %v, want the child never registered", err)
	}
}

// TestRegistryExportNarrowsToTheCallersBranch pins the export rule at the
// registry: a branch sees only this broker's own runs — its own branch's runs
// come from its own listing, so a re-exported alias of its own supervisor would
// only shadow it — a sibling branch's runs stay hidden, and this broker's own
// caller sees its descendants.
func TestRegistryExportNarrowsToTheCallersBranch(t *testing.T) {
	registry := NewRegistry()
	if _, err := registry.Register(messaging.DescendantRegistration{
		ChildID: "one", Endpoint: "http://one", Generation: "g1",
		Runs: []messaging.RunClaim{{RunID: "asker"}, {RunID: "asker-mate"}},
	}); err != nil {
		t.Fatalf("register first child: %v", err)
	}
	if _, err := registry.Register(messaging.DescendantRegistration{
		ChildID: "two", Endpoint: "http://two", Generation: "g2",
		Runs: []messaging.RunClaim{{RunID: "target"}},
	}); err != nil {
		t.Fatalf("register second child: %v", err)
	}
	peers := []messaging.Peer{{RunID: "orchestrator"}, {RunID: "asker"}, {RunID: "asker-mate"}, {RunID: "target"}}

	exported := runIDsOf(registry.Export(peers, "asker"))
	if !exported["orchestrator"] || exported["asker"] || exported["asker-mate"] || exported["target"] {
		t.Fatalf("export to a branch = %v, want this broker's own runs only", exported)
	}
	exported = runIDsOf(registry.Export(peers, "orchestrator"))
	if !exported["orchestrator"] || !exported["asker"] || !exported["target"] {
		t.Fatalf("export to this broker's own caller = %v, want every owned descendant", exported)
	}
}

// TestRegistryExportLeavesTheOwningsBranchItsCanonicalSupervisor pins the
// supervisor alias: a branch's supervisor registers at its parent under the
// branch-namespaced key, so an export that returned the run to the owning
// branch's own caller would hand it a phantom remote peer beside the canonical
// run it already lists locally.
func TestRegistryExportLeavesTheOwningsBranchItsCanonicalSupervisor(t *testing.T) {
	registry := NewRegistry()
	if _, err := registry.Register(messaging.DescendantRegistration{
		ChildID: "one", Endpoint: "http://one", Generation: "g1",
		Runs: []messaging.RunClaim{{RunID: messaging.SupervisorRunID}},
	}); err != nil {
		t.Fatalf("register child: %v", err)
	}
	peers := []messaging.Peer{{RunID: "orchestrator"}, {RunID: ParentRunKey("one", messaging.SupervisorRunID)}}

	exported := runIDsOf(registry.Export(peers, ParentRunKey("one", messaging.SupervisorRunID)))
	if !exported["orchestrator"] || exported["supervisor@one"] {
		t.Fatalf("export to the owning branch = %v, want no alias of its own supervisor", exported)
	}
}

func runIDsOf(peers []messaging.Peer) map[string]bool {
	ids := make(map[string]bool, len(peers))
	for _, peer := range peers {
		ids[peer.RunID] = true
	}
	return ids
}

// TestRegistryHopsWithTheCredentialTheChildRegistered pins where the relay's
// run credential lives: the child hands its parent each run's credential in the
// registration, the hop carries that one, and a re-registration that rotates it
// is what a later relay posts.
func TestRegistryHopsWithTheCredentialTheChildRegistered(t *testing.T) {
	registry := NewRegistry()
	if _, err := registry.Register(messaging.DescendantRegistration{
		ChildID: "one", Endpoint: "http://one", Generation: "g1",
		Runs: []messaging.RunClaim{{RunID: "run-a", Token: "tok-1"}},
	}); err != nil {
		t.Fatalf("register child: %v", err)
	}
	hop, ok := registry.Hop("run-a")
	if !ok || hop.Token != "tok-1" {
		t.Fatalf("Hop = (%+v, %v), want the registered credential", hop, ok)
	}
	if _, err := registry.Register(messaging.DescendantRegistration{
		ChildID: "one", Endpoint: "http://one", Generation: "g1",
		Runs: []messaging.RunClaim{{RunID: "run-a", Token: "tok-2"}},
	}); err != nil {
		t.Fatalf("re-register child: %v", err)
	}
	if hop, ok := registry.Hop("run-a"); !ok || hop.Token != "tok-2" {
		t.Fatalf("Hop after a rotated credential = (%+v, %v), want tok-2", hop, ok)
	}
	if _, err := registry.Deregister("one", "g1"); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	if _, ok := registry.Hop("run-a"); ok {
		t.Fatal("the deregistered child still routes its run")
	}
}

// TestRetirementSkipsAReplacedClaim pins the generation-aware retirement: a
// refusal that observed the child under an older generation retires nothing
// once a fresher registration replaced the claim, so a restart's replacement
// survives a relay refusal that raced it.
func TestRetirementSkipsAReplacedClaim(t *testing.T) {
	registry := NewRegistry()
	if _, err := registry.Register(messaging.DescendantRegistration{
		ChildID: "one", Endpoint: "http://one", Generation: "gen-one",
		Runs: []messaging.RunClaim{{RunID: "run-a"}, {RunID: "run-b"}},
	}); err != nil {
		t.Fatalf("register child: %v", err)
	}

	// A per-run retirement observed under the replaced generation keeps the run
	// a fresher registration re-claimed.
	if _, err := registry.Register(messaging.DescendantRegistration{
		ChildID: "one", Endpoint: "http://one", Generation: "gen-two",
		Runs: []messaging.RunClaim{{RunID: "run-a"}},
	}); err != nil {
		t.Fatalf("re-register child: %v", err)
	}
	registry.Forget("run-a", "gen-one")
	if !registry.Owns("run-a") {
		t.Fatal("the replaced run lost its claim to a stale refusal")
	}
	registry.Forget("run-a", "gen-two")
	if registry.Owns("run-a") {
		t.Fatal("the refusal under the observed generation did not retire the run")
	}

	// A whole-child retirement observed under a replaced generation keeps the
	// replacement claim, and the one under the observed generation retires it.
	if _, err := registry.Register(messaging.DescendantRegistration{
		ChildID: "one", Endpoint: "http://one", Generation: "gen-three",
		Runs: []messaging.RunClaim{{RunID: "run-c"}},
	}); err != nil {
		t.Fatalf("register the replacement claim: %v", err)
	}
	runs, err := registry.Deregister("one", "gen-one")
	if err != nil {
		t.Fatalf("Deregister with a stale generation: %v", err)
	}
	if len(runs) != 0 || !registry.Owns("run-c") {
		t.Fatalf("stale deregistration retired %v, want the replacement claim kept", runs)
	}
	runs, err = registry.Deregister("one", "gen-three")
	if err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	if len(runs) != 1 || runs[0] != "run-c" {
		t.Fatalf("deregistered runs = %v, want run-c", runs)
	}
}
