package workflow

// Internal tests for restart recovery (recovery.go). These own the recovery
// contract: restart recovery compares the lease's persisted, heartbeated
// ExpiresAt against the current time — a stale one is expired with reason
// "recovery" and released for replacement, an unexpired one is retained,
// activity (LastActivityAt) is never a liveness signal, and an awaiting_child
// kernel composition claim is exempt. They also pin the startup-resume
// safety gate: a resume over a freshly recovered store never transitions a
// non-awaiting_child lease or an awaiting_child parent whose child is not yet
// terminal.

import (
	"testing"
	"time"
)

// TestRecoveryRetainsHeartbeatedLease proves the recovery liveness contract's
// retain side: a lease whose original expiry has passed but which was
// explicitly heartbeated (renewing ExpiresAt far into the future and stamping
// LastHeartbeatAt) survives a restart Catalog untouched.
func TestRecoveryRetainsHeartbeatedLease(t *testing.T) {
	m, s, wf, node := newManagerFixture(t)
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	nodeID := NodeID(node)
	actID := activationByNode(&snap.Instance, nodeID).ID

	// Claim on an already-expired lease, then heartbeat: the persisted
	// ExpiresAt moves into the future and LastHeartbeatAt is stamped.
	past := time.Now().UTC().Add(-time.Hour)
	claimWithLease(t, s, wf, nodeID, Lease{
		ID:           "lease-hb-retain",
		ActivationID: actID,
		Owner:        "alice",
		TokenDigest:  ownerTokenDigest("tok-hb-retain"),
		AcquiredAt:   past.Add(-time.Minute),
		ExpiresAt:    past,
	}, "alice")
	if _, err := m.WorkflowCommand(string(wf), heartbeatPayload(t, node, string(actID), "lease-hb-retain", "tok-hb-retain")); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	// Simulated restart: a fresh store over the same root runs the recovery.
	s2 := New(s.Root())
	if _, err := s2.Catalog(); err != nil {
		t.Fatalf("Catalog: %v", err)
	}

	snap2, exists, err := s2.loadCurrent(wf)
	if err != nil || !exists {
		t.Fatalf("loadCurrent after recovery: exists=%v err=%v", exists, err)
	}
	act := activationByNode(&snap2.Instance, nodeID)
	if act.Status != ActivationLeased {
		t.Fatalf("status after recovery = %s, want leased (heartbeated lease retained)", act.Status)
	}
	if act.ActiveLease == nil || act.ActiveLease.ID != "lease-hb-retain" {
		t.Fatalf("lease after recovery = %+v, want lease-hb-retain retained", act.ActiveLease)
	}
	if !act.ActiveLease.ExpiresAt.After(time.Now().UTC()) {
		t.Fatalf("expiry after recovery = %v, want the renewed future expiry", act.ActiveLease.ExpiresAt)
	}
	if act.ActiveLease.LastHeartbeatAt == nil {
		t.Fatal("lease lost its heartbeat marker on recovery")
	}

	// The recovery sweep must have emitted no expiry for the retained lease.
	for _, e := range readEvents(t, s, wf) {
		if e.Kind == EventLeaseExpired && e.Identity.NodeID == nodeID {
			t.Fatalf("recovery swept the heartbeated lease: %+v", e)
		}
	}
}

// TestRecoverySweepsUnheartbeatedExpiredLease proves the sweep side: a lease
// whose ExpiresAt has passed and which was never heartbeated is expired with
// reason "recovery" and released, making the activation re-claimable.
func TestRecoverySweepsUnheartbeatedExpiredLease(t *testing.T) {
	_, s, wf, node := newManagerFixture(t)
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	nodeID := NodeID(node)
	actID := activationByNode(&snap.Instance, nodeID).ID

	past := time.Now().UTC().Add(-time.Hour)
	claimWithLease(t, s, wf, nodeID, Lease{
		ID:           "lease-unhb",
		ActivationID: actID,
		Owner:        "bob",
		AcquiredAt:   past.Add(-time.Minute),
		ExpiresAt:    past,
	}, "bob")

	// Simulated restart: a fresh store over the same root runs the recovery.
	s2 := New(s.Root())
	if _, err := s2.Catalog(); err != nil {
		t.Fatalf("Catalog: %v", err)
	}

	snap2, exists, err := s2.loadCurrent(wf)
	if err != nil || !exists {
		t.Fatalf("loadCurrent after recovery: exists=%v err=%v", exists, err)
	}
	act := activationByNode(&snap2.Instance, nodeID)
	if act.Status != ActivationLeaseExpired {
		t.Fatalf("status after recovery = %s, want lease_expired (swept)", act.Status)
	}
	if act.ActiveLease != nil {
		t.Fatalf("lease after recovery = %+v, want released", act.ActiveLease)
	}

	// The expiry is recorded durably with the recovery reason.
	var sawRecovery bool
	for _, e := range readEvents(t, s, wf) {
		if e.Kind == EventLeaseExpired && e.Identity.NodeID == nodeID && e.LeaseID == "lease-unhb" {
			if e.Reason != "recovery" {
				t.Fatalf("lease_expired event reason = %q, want %q", e.Reason, "recovery")
			}
			sawRecovery = true
		}
	}
	if !sawRecovery {
		t.Fatal("event log has no lease_expired event with reason \"recovery\" for the swept lease")
	}
}

// TestRecoveryExemptsAwaitingChildFromSweep proves the exemption side in a
// focused pass: a parent parked awaiting_child on an expired kernel-held
// lease is retained through the restart, and the sweep emits no lease-expired
// event for it (its child-outcome resolution requires awaiting_child, so
// expiring the claim would strand the parent).
func TestRecoveryExemptsAwaitingChildFromSweep(t *testing.T) {
	_, s, wf, childID := compositionPhase2Fixture(t, map[OutcomeName]OutcomeName{"done": "done"})

	// Parent's spawn activation is durably awaiting_child on an expired
	// lease (kernel-held composition claim).
	past := time.Now().UTC().Add(-time.Hour)
	_ = attachWithLeaseExpiry(t, s, wf, "recovery-claim", "att-recovery", past)

	// The child instance exists and is untouched by the recovery pass.
	if _, exists, err := s.loadCurrent(childID); err != nil || !exists {
		t.Fatalf("child loadCurrent: exists=%v err=%v", exists, err)
	}

	// Simulated restart: a fresh store over the same root runs the recovery.
	s2 := New(s.Root())
	if _, err := s2.Catalog(); err != nil {
		t.Fatalf("Catalog: %v", err)
	}

	psnap, exists, err := s2.loadCurrent(wf)
	if err != nil || !exists {
		t.Fatalf("parent loadCurrent: exists=%v err=%v", exists, err)
	}
	pact := activationByNode(&psnap.Instance, "spawn")
	if pact.Status != ActivationAwaitingChild {
		t.Fatalf("spawn status after recovery = %q, want awaiting_child (exempt)", pact.Status)
	}
	if pact.ActiveLease == nil || pact.ActiveLease.ID != "recovery-claim" {
		t.Fatalf("spawn lease after recovery = %+v, want the expired kernel-held claim retained", pact.ActiveLease)
	}

	// No lease-expired event was emitted for the exempted parent.
	for _, e := range readEvents(t, s, wf) {
		if e.Kind == EventLeaseExpired && e.Identity.NodeID == "spawn" {
			t.Fatalf("recovery swept the exempted awaiting_child lease: %+v", e)
		}
	}
}

// TestStartupResumeDoesNotTransitionFreshlyRecoveredLeases proves the startup
// resume safety gate: after a restart (fresh store over the same root,
// Catalog, then ResumeAwaitingChildren), a recovered non-awaiting_child
// activation running on a live (unexpired) lease is untouched — status and
// lease preserved — and an awaiting_child parent whose child is NOT terminal
// stays awaiting_child (no scheduler, no re-attach, no re-await).
func TestStartupResumeDoesNotTransitionFreshlyRecoveredLeases(t *testing.T) {
	m, s, wfRun, node := newManagerFixture(t)
	snap, _, err := s.loadCurrent(wfRun)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	nodeID := NodeID(node)
	actID := activationByNode(&snap.Instance, nodeID).ID

	// A running activation on a live lease (future expiry).
	future := time.Now().UTC().Add(time.Hour)
	claimWithLease(t, s, wfRun, nodeID, Lease{
		ID:           "live-lease",
		ActivationID: actID,
		Owner:        "alice",
		TokenDigest:  ownerTokenDigest("tok-live"),
		AcquiredAt:   time.Now().UTC(),
		ExpiresAt:    future,
	}, "alice")
	startWithToken(t, m, wfRun, node, string(actID), "live-lease", "tok-live")

	// A composition parent parked awaiting_child while its child is still
	// running (non-terminal).
	parent, child := compositionPhase2Templates(t, map[OutcomeName]OutcomeName{"done": "done"})
	if err := s.StoreTemplate(child.TemplateID, child.TemplateVersion, child); err != nil {
		t.Fatalf("StoreTemplate child: %v", err)
	}
	if err := s.StoreTemplate(parent.TemplateID, parent.TemplateVersion, parent); err != nil {
		t.Fatalf("StoreTemplate parent: %v", err)
	}
	wfParent := mustInstantiateTemplate(t, m, "p2-parent", "1")
	childID := DeriveChildWorkflowID(wfParent, "spawn", "c1")
	_ = attachWithLeaseExpiry(t, s, wfParent, "resume-claim", "att-resume", time.Now().UTC().Add(-time.Hour))

	// Simulated restart: fresh store, recovery, then the startup resume.
	s2 := New(s.Root())
	if _, err := s2.Catalog(); err != nil {
		t.Fatalf("Catalog: %v", err)
	}
	m2 := NewManager(s2)
	summary, err := m2.ResumeAwaitingChildren()
	if err != nil {
		t.Fatalf("ResumeAwaitingChildren: %v", err)
	}
	if summary.Resolved != 0 || summary.StillAwaiting != 1 || len(summary.Errors) != 0 {
		t.Fatalf("summary = %+v, want resolved=0 still_awaiting=1 errors=0", summary)
	}

	// The running activation on a live lease is untouched: status and lease
	// preserved.
	snap2, exists, err := s2.loadCurrent(wfRun)
	if err != nil || !exists {
		t.Fatalf("running loadCurrent: exists=%v err=%v", exists, err)
	}
	act := activationByNode(&snap2.Instance, nodeID)
	if act.Status != ActivationRunning {
		t.Fatalf("running status after resume = %s, want running (untouched)", act.Status)
	}
	if act.ActiveLease == nil || act.ActiveLease.ID != "live-lease" {
		t.Fatalf("running lease after resume = %+v, want live-lease preserved", act.ActiveLease)
	}
	if !act.ActiveLease.ExpiresAt.Equal(future) {
		t.Fatalf("running lease expiry = %v, want the original %v (untouched)", act.ActiveLease.ExpiresAt, future)
	}

	// The awaiting_child parent with a non-terminal child stays
	// awaiting_child, and the child was not created, attached to, or resumed.
	psnap, exists, err := s2.loadCurrent(wfParent)
	if err != nil || !exists {
		t.Fatalf("parent loadCurrent: exists=%v err=%v", exists, err)
	}
	pact := activationByNode(&psnap.Instance, "spawn")
	if pact.Status != ActivationAwaitingChild {
		t.Fatalf("parent status after resume = %q, want awaiting_child (child not terminal)", pact.Status)
	}
	csnap, exists, err := s2.loadCurrent(childID)
	if err != nil || !exists {
		t.Fatalf("child loadCurrent: exists=%v err=%v", exists, err)
	}
	if csnap.Instance.Status != WorkflowActive {
		t.Fatalf("child status after resume = %v, want active (untouched)", csnap.Instance.Status)
	}
}
