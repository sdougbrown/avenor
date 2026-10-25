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
	if len(first.Adopted) != 2 || len(first.Retired) != 0 {
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
	if len(refused.Adopted) != 0 || len(refused.Retired) != 0 {
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
	if len(second.Adopted) != 1 || len(second.Retired) != 2 {
		t.Fatalf("re-registration = %+v, want run-c adopted and two retired", second)
	}
	if registry.Owns("run-a") || registry.Owns("run-b") {
		t.Fatal("a retired run is still owned")
	}
	if next, ok := registry.Hop("run-c"); !ok || next.Generation != "gen-one-two" {
		t.Fatalf("hop for run-c = (%+v, %v), want the replacement generation", next, ok)
	}

	// Deregistering retires the child and reports everything it held, with the
	// credential each claim was registered under.
	retired, err := registry.Deregister("one", messaging.DescendantView{
		Endpoint: "http://one-again", Generation: "gen-one-two",
		Runs: []messaging.RunClaim{{RunID: "run-c"}},
	})
	if err != nil {
		t.Fatalf("deregister child: %v", err)
	}
	if len(retired) != 1 || retired["run-c"] != "" {
		t.Fatalf("deregistered runs = %v, want run-c with the credential it claimed", retired)
	}
	if _, ok := registry.Hop("run-c"); ok {
		t.Fatal("a deregistered child still owns a run")
	}
	if _, err := registry.Deregister("one", messaging.DescendantView{}); !errors.Is(err, messaging.ErrTargetUnavailable) {
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
		if len(claim.Runs) != 0 || len(claim.Adopted) != 0 || len(claim.Retired) != 0 {
			t.Fatalf("refused claim of %q = %+v, want nothing applied", runID, claim)
		}
		if registry.Owns(runID) {
			t.Fatalf("the refused run %q was adopted", runID)
		}
	}
	// The child itself is not left registered by a refused claim.
	if _, err := registry.Deregister("one", messaging.DescendantView{}); !errors.Is(err, messaging.ErrTargetUnavailable) {
		t.Fatalf("deregistering the refused child = %v, want the child never registered", err)
	}
}

// TestRegistryExportNarrowsToTheCallersBranch pins the export rule at the
// registry: a branch sees its own runs and this broker's own runs, a sibling
// branch's runs stay hidden, and this broker's own caller sees its descendants.
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
	if !exported["orchestrator"] || !exported["asker"] || !exported["asker-mate"] || exported["target"] {
		t.Fatalf("export to a branch = %v, want the owner's runs and its own branch only", exported)
	}
	exported = runIDsOf(registry.Export(peers, "orchestrator"))
	if !exported["orchestrator"] || !exported["asker"] || !exported["target"] {
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
	if _, err := registry.Deregister("one", messaging.DescendantView{
		Endpoint: "http://one", Generation: "g1",
		Runs: []messaging.RunClaim{{RunID: "run-a", Token: "tok-2"}},
	}); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	if _, ok := registry.Hop("run-a"); ok {
		t.Fatal("the deregistered child still routes its run")
	}
}
