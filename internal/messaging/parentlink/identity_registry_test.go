package parentlink

import (
	"testing"
)

// TestHostQualifiedBranchIDsStayDistinct pins the host half of the branch
// identity: two branches on different hosts that mount the same socket path
// derive distinct child IDs, so each keeps its own claim. Deregistering one
// retires only its own runs and leaves the other branch's claim routed.
func TestHostQualifiedBranchIDsStayDistinct(t *testing.T) {
	parent := startBranch(t, "")

	// The same socket path on two hosts: distinct child IDs, the same stem.
	socket := "/same/avenor.sock"
	idA := BranchID(socket, "host-a")
	idB := BranchID(socket, "host-b")
	if idA == idB {
		t.Fatal("the same socket path on two hosts derived one child ID")
	}

	endpointA := "http://branch-a:8080"
	endpointB := "http://branch-b:8080"
	registerDescendant(t, parent, idA, endpointA, "gen-a", "run-a", "token-a")
	registerDescendant(t, parent, idB, endpointB, "gen-b", "run-b", "token-b")

	// (a) Both runs stay routed to the endpoint their own branch registered.
	hopA, ok := parent.link.Registry().Hop(ParentRunKey(idA, "run-a"))
	if !ok || hopA.Endpoint != endpointA {
		t.Fatalf("hop of run-a = (%+v, %v), want the endpoint branch A registered", hopA, ok)
	}
	hopB, ok := parent.link.Registry().Hop(ParentRunKey(idB, "run-b"))
	if !ok || hopB.Endpoint != endpointB {
		t.Fatalf("hop of run-b = (%+v, %v), want the endpoint branch B registered", hopB, ok)
	}

	// (b) Deregistering branch A with its own view retires only A's claim.
	deregisterDescendant(t, parent, idA, endpointA, "gen-a", "run-a", "token-a")
	if parent.link.Registry().Owns(ParentRunKey(idA, "run-a")) {
		t.Fatal("deregistering branch A left its own run routed")
	}
	if !parent.link.Registry().Owns(ParentRunKey(idB, "run-b")) {
		t.Fatal("deregistering branch A retired branch B's claim")
	}
	if owner, ok := parent.link.Registry().Owner(ParentRunKey(idB, "run-b")); !ok || owner != idB {
		t.Fatalf("owner of run-b = (%q, %v), want branch B's distinct child ID", owner, ok)
	}
	hopB, ok = parent.link.Registry().Hop(ParentRunKey(idB, "run-b"))
	if !ok || hopB.Endpoint != endpointB {
		t.Fatalf("after deregistering branch A, hop of run-b = (%+v, %v), want branch B's endpoint", hopB, ok)
	}
}
