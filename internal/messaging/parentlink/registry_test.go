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
	next, ok := registry.Hop("run-a")
	if !ok || next.Endpoint != "http://one" || next.Generation != "gen-one" || next.Label != "alpha" {
		t.Fatalf("hop for run-a = (%+v, %v), want the child's endpoint, generation and label", next, ok)
	}

	// Another child cannot claim a run this one already owns.
	if _, err := registry.Register(messaging.DescendantRegistration{
		ChildID:    "two",
		Endpoint:   "http://two",
		Generation: "gen-two",
		Runs:       []messaging.RunClaim{{RunID: "run-a"}},
	}); !errors.Is(err, ErrRunClaimed) || !errors.Is(err, messaging.ErrInvalidRequest) {
		t.Fatalf("conflicting claim = %v, want ErrRunClaimed", err)
	}
	if owner, ok := registry.Owner("run-a"); !ok || owner != "one" {
		t.Fatalf("owner of run-a = (%q, %v) after a refused claim, want the first child", owner, ok)
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

	// Deregistering retires the child and reports everything it held.
	runs, err := registry.Deregister("one")
	if err != nil {
		t.Fatalf("deregister child: %v", err)
	}
	if len(runs) != 1 || runs[0] != "run-c" {
		t.Fatalf("deregistered runs = %v, want run-c", runs)
	}
	if _, ok := registry.Hop("run-c"); ok {
		t.Fatal("a deregistered child still owns a run")
	}
	if _, err := registry.Deregister("one"); !errors.Is(err, messaging.ErrTargetUnavailable) {
		t.Fatalf("deregistering an unknown child = %v, want ErrTargetUnavailable", err)
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
	peers := []messaging.Peer{{RunID: "orchestrator"}, {RunID: "asker"}, {RunID: "target"}}

	exported := runIDsOf(registry.Export(peers, "asker"))
	if !exported["orchestrator"] || !exported["asker"] || exported["target"] {
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
