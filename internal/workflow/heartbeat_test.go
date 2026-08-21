package workflow

// Internal tests for the Stage 13 (Phase 1) lease-liveness surface: the
// manager heartbeat command (explicit liveness renewal), the live
// ExpireStaleLeases detector, replacement claims after expiry, and the
// reducer's durable, replay-stable heartbeat reduction. They use only the
// standard library.

import (
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

// heartbeatPayload builds a "heartbeat" op command payload.
func heartbeatPayload(t *testing.T, nodeID, activationID, leaseID, ownerToken string) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(map[string]any{
		"op":            "heartbeat",
		"node_id":       nodeID,
		"activation_id": activationID,
		"lease_id":      leaseID,
		"owner_token":   ownerToken,
	})
	if err != nil {
		t.Fatalf("marshal heartbeat: %v", err)
	}
	return data
}

// claimWithLease applies a claim directly through the store carrying the
// supplied lease metadata verbatim (so tests can craft past/future expiries
// and known token digests the manager's commandClaim would not).
func claimWithLease(t *testing.T, s *Store, wf WorkflowID, nodeID NodeID, lease Lease, actor string) Snapshot {
	t.Helper()
	snap, exists, err := s.loadCurrent(wf)
	if err != nil || !exists {
		t.Fatalf("loadCurrent: exists=%v err=%v", exists, err)
	}
	act := activationByNode(&snap.Instance, nodeID)
	if act == nil {
		t.Fatalf("activation for node %q not found", nodeID)
	}
	snap, err = s.ApplyCommand(wf, Command{
		Kind:             CommandClaim,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "claim-" + string(lease.ID),
		Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: nodeID, ActivationID: act.ID},
		LeaseID:          lease.ID,
		Actor:            actor,
		Lease:            &lease,
	})
	if err != nil {
		t.Fatalf("claim with lease: %v", err)
	}
	return snap
}

// mustInstantiateTemplate instantiates a stored template and returns the new
// workflow ID.
func mustInstantiateTemplate(t *testing.T, m *Manager, templateID, version string) WorkflowID {
	t.Helper()
	payload, err := json.Marshal(map[string]string{"template_id": templateID, "template_version": version})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out, err := m.WorkflowInstantiate(payload)
	if err != nil {
		t.Fatalf("WorkflowInstantiate %s: %v", templateID, err)
	}
	wf, ok := out.(map[string]any)["workflow_id"].(string)
	if !ok {
		t.Fatalf("instantiate result missing workflow_id: %#v", out)
	}
	return WorkflowID(wf)
}

// readEvents reads every event from an instance's NDJSON log.
func readEvents(t *testing.T, s *Store, wf WorkflowID) []Event {
	t.Helper()
	data, err := os.ReadFile(s.eventsPath(wf))
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	var events []Event
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var e Event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("unmarshal event line %q: %v", line, err)
		}
		events = append(events, e)
	}
	return events
}

// startWithToken issues a start through the manager with an explicit
// lease/token pair (bypassing claimActivation's result plumbing).
func startWithToken(t *testing.T, m *Manager, wf WorkflowID, nodeID, activationID, leaseID, token string) map[string]any {
	t.Helper()
	res := map[string]any{"lease_id": leaseID, "owner_token": token}
	out, err := m.WorkflowCommand(string(wf), startCommandPayload(t, nodeID, activationID, res, nil))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	mm, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("start result = %#v, want map", out)
	}
	return mm
}

// TestHeartbeatExtendsLeaseLiveness proves the core liveness contract: a
// leased+running activation whose ExpiresAt has passed is renewed by an
// explicit heartbeat (ExpiresAt moves into the future, LastHeartbeatAt
// stamped), and a subsequent ExpireStaleLeases sweep does NOT expire it. The
// heartbeat never transitions the activation status and never touches
// LastActivityAt.
func TestHeartbeatExtendsLeaseLiveness(t *testing.T) {
	m, s, wf, node := newManagerFixture(t)
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	nodeID := NodeID(node)
	actID := activationByNode(&snap.Instance, nodeID).ID

	// Claim with an already-expired lease (the holder's clock slipped past
	// the TTL) and a known token.
	past := time.Now().UTC().Add(-time.Hour)
	claimWithLease(t, s, wf, nodeID, Lease{
		ID:           "lease-hb",
		ActivationID: actID,
		Owner:        "alice",
		TokenDigest:  ownerTokenDigest("hb-token"),
		AcquiredAt:   past.Add(-time.Minute),
		ExpiresAt:    past,
	}, "alice")
	// Drive it to running so the renewal happens on a live attempt.
	startWithToken(t, m, wf, node, string(actID), "lease-hb", "hb-token")

	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	act := activationByNode(&snap.Instance, nodeID)
	if act.Status != ActivationRunning {
		t.Fatalf("pre-heartbeat status = %s, want running", act.Status)
	}
	if !act.ActiveLease.ExpiresAt.Before(time.Now().UTC()) {
		t.Fatalf("pre-heartbeat expiry = %v, want in the past", act.ActiveLease.ExpiresAt)
	}

	// The explicit heartbeat renews the lease.
	out, err := m.WorkflowCommand(string(wf), heartbeatPayload(t, node, string(actID), "lease-hb", "hb-token"))
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	mm, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("heartbeat result = %#v, want map", out)
	}
	if mm["lease_id"] != "lease-hb" {
		t.Fatalf("heartbeat lease_id = %#v, want lease-hb", mm["lease_id"])
	}
	hbAt, ok := mm["last_heartbeat_at"].(time.Time)
	if !ok || hbAt.IsZero() {
		t.Fatalf("heartbeat last_heartbeat_at = %#v, want a stamped time", mm["last_heartbeat_at"])
	}
	newExpiry, ok := mm["expires_at"].(time.Time)
	if !ok || !newExpiry.After(time.Now().UTC()) {
		t.Fatalf("heartbeat expires_at = %#v, want in the future", mm["expires_at"])
	}

	// The renewal is durable on disk: ExpiresAt moved into the future, the
	// wall-clock heartbeat marker is set, the status is untouched (still
	// running), and activity was never recorded.
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("reload after heartbeat: %v", err)
	}
	act = activationByNode(&snap.Instance, nodeID)
	if act.Status != ActivationRunning {
		t.Fatalf("post-heartbeat status = %s, want running (heartbeat never transitions status)", act.Status)
	}
	if act.ActiveLease == nil {
		t.Fatal("post-heartbeat: active lease missing")
	}
	if !act.ActiveLease.ExpiresAt.After(time.Now().UTC()) {
		t.Fatalf("post-heartbeat expiry = %v, want renewed into the future", act.ActiveLease.ExpiresAt)
	}
	if !act.ActiveLease.ExpiresAt.Equal(newExpiry) {
		t.Fatalf("post-heartbeat expiry = %v, want %v (from the response)", act.ActiveLease.ExpiresAt, newExpiry)
	}
	if act.ActiveLease.LastHeartbeatAt == nil || !act.ActiveLease.LastHeartbeatAt.Equal(hbAt) {
		t.Fatalf("post-heartbeat LastHeartbeatAt = %v, want the stamped %v", act.ActiveLease.LastHeartbeatAt, hbAt)
	}
	if act.ActiveLease.LastActivityAt != nil {
		t.Fatalf("heartbeat recorded activity: LastActivityAt = %v, want nil", *act.ActiveLease.LastActivityAt)
	}

	// A subsequent live sweep must NOT expire the renewed lease.
	if _, err := m.ExpireStaleLeases(); err != nil {
		t.Fatalf("ExpireStaleLeases: %v", err)
	}
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("reload after sweep: %v", err)
	}
	act = activationByNode(&snap.Instance, nodeID)
	if act.Status != ActivationRunning {
		t.Fatalf("post-sweep status = %s, want running (renewed lease must survive)", act.Status)
	}
	if act.ActiveLease == nil {
		t.Fatal("post-sweep: active lease missing")
	}
}

// TestActivityDoesNotExtendLeaseLiveness proves the other half of the
// contract: observed runtime activity (LastActivityAt) never renews lease
// liveness. A lease whose ExpiresAt has passed is swept by the detector even
// though activity was observed moments before.
func TestActivityDoesNotExtendLeaseLiveness(t *testing.T) {
	m, s, wf, node := newManagerFixture(t)
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	nodeID := NodeID(node)
	actID := activationByNode(&snap.Instance, nodeID).ID

	past := time.Now().UTC().Add(-time.Hour)
	activity := time.Now().UTC().Add(-time.Second) // fresh activity, no heartbeat
	claimWithLease(t, s, wf, nodeID, Lease{
		ID:             "lease-act",
		ActivationID:   actID,
		Owner:          "alice",
		TokenDigest:    ownerTokenDigest("act-token"),
		AcquiredAt:     past.Add(-time.Minute),
		ExpiresAt:      past,
		LastActivityAt: &activity,
	}, "alice")

	summary, err := m.ExpireStaleLeases()
	if err != nil {
		t.Fatalf("ExpireStaleLeases: %v", err)
	}
	if len(summary.Errors) != 0 {
		t.Fatalf("summary.Errors = %v, want none", summary.Errors)
	}

	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	act := activationByNode(&snap.Instance, nodeID)
	if act.Status != ActivationLeaseExpired {
		t.Fatalf("status = %s, want lease_expired (activity must not extend liveness)", act.Status)
	}
	if act.ActiveLease != nil {
		t.Fatalf("active lease = %+v, want released", act.ActiveLease)
	}
}

// TestExpireStaleLeasesSweepsStaleRetainsFreshExemptsAwaitingChild is the
// detector's main sweep test: across multiple instances in one store, a stale
// lease is expired, a fresh lease is retained, and an awaiting_child
// kernel-held claim is exempted even though its lease is stale.
func TestExpireStaleLeasesSweepsStaleRetainsFreshExemptsAwaitingChild(t *testing.T) {
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	if _, err := m.WorkflowCreate([]byte(managerTemplateJSON)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}

	// Instance 1: a stale lease (past expiry, never heartbeated).
	wfStale := mustInstantiateTemplate(t, m, "mgr-test", "1")
	// Instance 2: a fresh lease (future expiry).
	wfFresh := mustInstantiateTemplate(t, m, "mgr-test", "1")
	// Instance 3: a composition parent parked awaiting_child on a stale lease.
	parent, child := compositionPhase2Templates(t, map[OutcomeName]OutcomeName{"done": "done"})
	if err := s.StoreTemplate(child.TemplateID, child.TemplateVersion, child); err != nil {
		t.Fatalf("StoreTemplate child: %v", err)
	}
	if err := s.StoreTemplate(parent.TemplateID, parent.TemplateVersion, parent); err != nil {
		t.Fatalf("StoreTemplate parent: %v", err)
	}
	wfParent := mustInstantiateTemplate(t, m, "p2-parent", "1")

	past := time.Now().UTC().Add(-time.Hour)
	future := time.Now().UTC().Add(30 * time.Second)

	snap, _, err := s.loadCurrent(wfStale)
	if err != nil {
		t.Fatalf("loadCurrent stale: %v", err)
	}
	claimWithLease(t, s, wfStale, "start", Lease{
		ID:           "stale-lease",
		ActivationID: snap.Instance.Activations[0].ID,
		Owner:        "alice",
		AcquiredAt:   past.Add(-time.Minute),
		ExpiresAt:    past,
	}, "alice")

	snap, _, err = s.loadCurrent(wfFresh)
	if err != nil {
		t.Fatalf("loadCurrent fresh: %v", err)
	}
	claimWithLease(t, s, wfFresh, "start", Lease{
		ID:           "fresh-lease",
		ActivationID: snap.Instance.Activations[0].ID,
		Owner:        "bob",
		AcquiredAt:   time.Now().UTC(),
		ExpiresAt:    future,
	}, "bob")

	// The parent's spawn activation is durably awaiting_child on an expired
	// lease (kernel-held composition claim).
	_ = attachWithLeaseExpiry(t, s, wfParent, "child-claim", "att-child", past)

	summary, err := m.ExpireStaleLeases()
	if err != nil {
		t.Fatalf("ExpireStaleLeases: %v", err)
	}
	if len(summary.Errors) != 0 {
		t.Fatalf("summary.Errors = %v, want none", summary.Errors)
	}
	// The fresh lease and the exempted awaiting_child claim are retained.
	if summary.Retained != 2 {
		t.Fatalf("summary.Retained = %d, want exactly 2 (fresh lease + awaiting_child claim)", summary.Retained)
	}

	// Stale lease expired.
	snap, _, err = s.loadCurrent(wfStale)
	if err != nil {
		t.Fatalf("reload stale: %v", err)
	}
	act := activationByNode(&snap.Instance, "start")
	if act.Status != ActivationLeaseExpired {
		t.Fatalf("stale instance status = %s, want lease_expired", act.Status)
	}
	if act.ActiveLease != nil {
		t.Fatalf("stale instance lease = %+v, want released", act.ActiveLease)
	}
	// The expiry was recorded durably as a lease_expired event with the
	// live-detector reason.
	var sawExpired bool
	for _, e := range readEvents(t, s, wfStale) {
		if e.Kind == EventLeaseExpired && e.LeaseID == "stale-lease" {
			if e.Reason != "stale" {
				t.Fatalf("lease_expired event reason = %q, want %q", e.Reason, "stale")
			}
			sawExpired = true
		}
	}
	if !sawExpired {
		t.Fatal("stale instance event log has no lease_expired event for stale-lease")
	}

	// Fresh lease retained.
	snap, _, err = s.loadCurrent(wfFresh)
	if err != nil {
		t.Fatalf("reload fresh: %v", err)
	}
	act = activationByNode(&snap.Instance, "start")
	if act.Status != ActivationLeased {
		t.Fatalf("fresh instance status = %s, want leased (retained)", act.Status)
	}
	if act.ActiveLease == nil || act.ActiveLease.ID != "fresh-lease" {
		t.Fatalf("fresh instance lease = %+v, want fresh-lease retained", act.ActiveLease)
	}

	// Awaiting_child claim exempted despite the stale lease.
	snap, _, err = s.loadCurrent(wfParent)
	if err != nil {
		t.Fatalf("reload parent: %v", err)
	}
	pact := activationByNode(&snap.Instance, "spawn")
	if pact.Status != ActivationAwaitingChild {
		t.Fatalf("parent spawn status = %s, want awaiting_child (exempted)", pact.Status)
	}
	if pact.ActiveLease == nil || pact.ActiveLease.ID != "child-claim" {
		t.Fatalf("parent spawn lease = %+v, want the expired child-claim retained", pact.ActiveLease)
	}
}

// TestSweepStaleLeasesRecordsStaleReason exercises the store-level live sweep
// directly: a stale lease is expired under the lock with the detector's
// "stale" reason, while the recovery path keeps using "recovery".
func TestSweepStaleLeasesRecordsStaleReason(t *testing.T) {
	s := newStore(t)
	wf := WorkflowID("wf-stale")
	mustInstantiate(t, s, wf)

	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	past := time.Now().UTC().Add(-time.Hour)
	snap, err = s.ApplyCommand(wf, Command{
		Kind:             CommandClaim,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "claim-sweep",
		Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: "start", ActivationID: snap.Instance.Activations[0].ID},
		LeaseID:          "sweep-lease",
		Actor:            "alice",
		Lease: &Lease{
			ID:           "sweep-lease",
			ActivationID: snap.Instance.Activations[0].ID,
			Owner:        "alice",
			AcquiredAt:   past.Add(-time.Minute),
			ExpiresAt:    past,
		},
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	expired, retained, err := s.sweepStaleLeases(wf, "stale", time.Now().UTC())
	if err != nil {
		t.Fatalf("sweepStaleLeases: %v", err)
	}
	if expired != 1 || retained != 0 {
		t.Fatalf("sweep = expired:%d retained:%d, want 1/0", expired, retained)
	}

	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if act := activationByNode(&snap.Instance, "start"); act.Status != ActivationLeaseExpired || act.ActiveLease != nil {
		t.Fatalf("activation = %+v, want lease_expired with no lease", act)
	}
	var sawStale bool
	for _, e := range readEvents(t, s, wf) {
		if e.Kind == EventLeaseExpired && e.Reason == "stale" {
			sawStale = true
		}
	}
	if !sawStale {
		t.Fatal("event log has no lease_expired event with reason \"stale\"")
	}

	// Idempotency: a second sweep finds no stale lease.
	expired, retained, err = s.sweepStaleLeases(wf, "stale", time.Now().UTC())
	if err != nil {
		t.Fatalf("second sweepStaleLeases: %v", err)
	}
	if expired != 0 || retained != 0 {
		t.Fatalf("second sweep = expired:%d retained:%d, want 0/0", expired, retained)
	}
}

// TestHeartbeatValidatesLeaseAndToken proves the lease-token validation: a
// wrong owner_token or lease_id is rejected, the correct pair renews, a
// repeated heartbeat is safe, and a duplicate idempotency key is treated as
// already-applied success.
func TestHeartbeatValidatesLeaseAndToken(t *testing.T) {
	m, s, wf, node := newManagerFixture(t)
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	nodeID := NodeID(node)
	actID := activationByNode(&snap.Instance, nodeID).ID
	future := time.Now().UTC().Add(30 * time.Second)
	claimWithLease(t, s, wf, nodeID, Lease{
		ID:           "lease-t",
		ActivationID: actID,
		Owner:        "alice",
		TokenDigest:  ownerTokenDigest("tok-t"),
		AcquiredAt:   time.Now().UTC(),
		ExpiresAt:    future,
	}, "alice")

	// Missing fields.
	if _, err := m.WorkflowCommand(string(wf), heartbeatPayload(t, "", string(actID), "lease-t", "tok-t")); err == nil || !strings.Contains(err.Error(), "heartbeat requires node_id") {
		t.Fatalf("missing node_id err = %v, want node_id required", err)
	}
	if _, err := m.WorkflowCommand(string(wf), heartbeatPayload(t, node, string(actID), "", "tok-t")); err == nil || !strings.Contains(err.Error(), "heartbeat requires lease_id") {
		t.Fatalf("missing lease_id err = %v, want lease_id required", err)
	}
	if _, err := m.WorkflowCommand(string(wf), heartbeatPayload(t, node, string(actID), "lease-t", "")); err == nil || !strings.Contains(err.Error(), "heartbeat requires owner_token") {
		t.Fatalf("missing owner_token err = %v, want owner_token required", err)
	}

	// Wrong owner token.
	if _, err := m.WorkflowCommand(string(wf), heartbeatPayload(t, node, string(actID), "lease-t", "wrong-token")); err == nil || !strings.Contains(err.Error(), "owner token does not match") {
		t.Fatalf("wrong token err = %v, want owner token mismatch", err)
	}
	// Wrong lease id.
	if _, err := m.WorkflowCommand(string(wf), heartbeatPayload(t, node, string(actID), "lease-wrong", "tok-t")); err == nil || !strings.Contains(err.Error(), "does not match the active lease") {
		t.Fatalf("wrong lease err = %v, want lease mismatch", err)
	}

	// A pending activation with no lease is rejected.
	m2, s2, wf2, node2 := newManagerFixture(t)
	snap2, _, err := s2.loadCurrent(wf2)
	if err != nil {
		t.Fatalf("loadCurrent 2: %v", err)
	}
	actID2 := activationByNode(&snap2.Instance, NodeID(node2)).ID
	if _, err := m2.WorkflowCommand(string(wf2), heartbeatPayload(t, node2, string(actID2), "lease-none", "tok-none")); err == nil || !strings.Contains(err.Error(), "no active lease") {
		t.Fatalf("no-lease heartbeat err = %v, want no active lease", err)
	}

	// The correct pair renews.
	if _, err := m.WorkflowCommand(string(wf), heartbeatPayload(t, node, string(actID), "lease-t", "tok-t")); err != nil {
		t.Fatalf("correct heartbeat: %v", err)
	}
	// A repeated heartbeat (a later renewal) is also safe.
	if _, err := m.WorkflowCommand(string(wf), heartbeatPayload(t, node, string(actID), "lease-t", "tok-t")); err != nil {
		t.Fatalf("repeat heartbeat: %v", err)
	}
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	act := activationByNode(&snap.Instance, nodeID)
	if act.ActiveLease == nil || act.ActiveLease.LastHeartbeatAt == nil {
		t.Fatalf("post-heartbeat lease = %+v, want a stamped heartbeat", act.ActiveLease)
	}

	// A duplicate idempotency key for the same renewal is treated as
	// already-applied (idempotent), not an error.
	dupHB := Command{
		Kind:             CommandHeartbeat,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "hb-dup",
		Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: nodeID, ActivationID: actID},
		LeaseID:          "lease-t",
		Lease:            &Lease{ExpiresAt: time.Now().UTC().Add(30 * time.Second), LastHeartbeatAt: ptrTime(time.Now().UTC())},
	}
	if _, err := s.ApplyCommand(wf, dupHB); err != nil {
		t.Fatalf("first duplicate-key heartbeat: %v", err)
	}
	fresh, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("reload for duplicate: %v", err)
	}
	dupHB.ExpectedRevision = fresh.Instance.Revision // retried command re-reads the revision
	if _, err := s.ApplyCommand(wf, dupHB); !errors.Is(err, errDuplicateIdempotency) {
		t.Fatalf("duplicate-key heartbeat err = %v, want errDuplicateIdempotency", err)
	}
}

// TestReplacementClaimAfterLeaseExpiry proves the retry/replacement path:
// after the detector expires a lease, a fresh commandClaim re-claims the same
// node/activation (lease_expired -> leased) and a commandStart records a new
// attempt on the SAME activation with the prior attempt preserved.
func TestReplacementClaimAfterLeaseExpiry(t *testing.T) {
	m, s, wf, node := newManagerFixture(t)
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	nodeID := NodeID(node)
	actID := activationByNode(&snap.Instance, nodeID).ID

	// First worker claims (on an already-expired lease) and starts attempt 1.
	past := time.Now().UTC().Add(-time.Hour)
	claimWithLease(t, s, wf, nodeID, Lease{
		ID:           "lease-1",
		ActivationID: actID,
		Owner:        "alice",
		TokenDigest:  ownerTokenDigest("tok-1"),
		AcquiredAt:   past.Add(-time.Minute),
		ExpiresAt:    past,
	}, "alice")
	first := startWithToken(t, m, wf, node, string(actID), "lease-1", "tok-1")
	attempt1, _ := first["attempt_id"].(string)
	if attempt1 == "" {
		t.Fatal("first start result missing attempt_id")
	}

	// The detector expires the stale lease.
	if _, err := m.ExpireStaleLeases(); err != nil {
		t.Fatalf("ExpireStaleLeases: %v", err)
	}
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("reload after expiry: %v", err)
	}
	act := activationByNode(&snap.Instance, nodeID)
	if act.Status != ActivationLeaseExpired {
		t.Fatalf("status after expiry = %s, want lease_expired", act.Status)
	}
	if act.ActiveLease != nil {
		t.Fatalf("lease after expiry = %+v, want released", act.ActiveLease)
	}

	// A replacement worker re-claims the same activation.
	res := claimActivation(t, m, wf, node, string(actID), "bob")
	lease2, _ := res["lease_id"].(string)
	token2, _ := res["owner_token"].(string)
	if lease2 == "" || token2 == "" || lease2 == "lease-1" {
		t.Fatalf("replacement claim = %#v, want a fresh lease", res)
	}
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("reload after re-claim: %v", err)
	}
	act = activationByNode(&snap.Instance, nodeID)
	if act.Status != ActivationLeased {
		t.Fatalf("status after re-claim = %s, want leased", act.Status)
	}
	if act.ActiveLease == nil || act.ActiveLease.ID != LeaseID(lease2) {
		t.Fatalf("re-claim lease = %+v, want %s", act.ActiveLease, lease2)
	}

	// A start records a new attempt on the SAME activation, preserving the
	// prior attempt ID.
	second := startWithToken(t, m, wf, node, string(actID), lease2, token2)
	attempt2, _ := second["attempt_id"].(string)
	if attempt2 == "" || attempt2 == attempt1 {
		t.Fatalf("second start attempt = %q (first %q), want a distinct attempt", attempt2, attempt1)
	}

	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("reload after second start: %v", err)
	}
	act = activationByNode(&snap.Instance, nodeID)
	if act.Status != ActivationRunning {
		t.Fatalf("status after second start = %s, want running", act.Status)
	}
	if len(act.AttemptIDs) != 2 || act.AttemptIDs[0] != AttemptID(attempt1) || act.AttemptIDs[1] != AttemptID(attempt2) {
		t.Fatalf("attempt ids = %v, want [%s %s] on the same activation", act.AttemptIDs, attempt1, attempt2)
	}
	if len(snap.Instance.Attempts) != 2 {
		t.Fatalf("attempts = %d, want 2 (prior attempt preserved)", len(snap.Instance.Attempts))
	}
}

// TestApplyHeartbeatDurableRenewalIsReplayStable proves the reducer change:
// a heartbeat event carrying lease metadata renews ExpiresAt and
// LastHeartbeatAt from the event (deterministically), replaying the same
// event batch twice yields identical snapshots, and a legacy heartbeat
// without lease metadata keeps the zero-time sentinel (non-nil) without
// touching the expiry.
func TestApplyHeartbeatDurableRenewalIsReplayStable(t *testing.T) {
	state := newInstance(t)
	actID := actStart(state).ID
	acquired := time.Unix(1000, 0).UTC()
	state = mustApply(t, state, Command{
		Kind:             CommandClaim,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "claim-1",
		Identity:         baseIdentity(actID, ""),
		LeaseID:          "lease-1",
		Actor:            "alice",
		Lease: &Lease{
			ID:           "lease-1",
			ActivationID: actID,
			Owner:        "alice",
			TokenDigest:  "sha256:abc",
			AcquiredAt:   acquired,
			ExpiresAt:    time.Unix(2000, 0).UTC(),
		},
	}, "claim")

	hbTime := time.Unix(5000, 0).UTC()
	newExpiry := time.Unix(6000, 0).UTC()
	events, err := Apply(state, Command{
		Kind:             CommandHeartbeat,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "hb-durable",
		Identity:         baseIdentity(actID, ""),
		LeaseID:          "lease-1",
		Lease:            &Lease{ExpiresAt: newExpiry, LastHeartbeatAt: &hbTime},
	})
	if err != nil {
		t.Fatalf("Apply heartbeat: %v", err)
	}
	if len(events) != 1 || events[0].Kind != EventHeartbeat {
		t.Fatalf("events = %+v, want one heartbeat event", events)
	}
	if events[0].Lease == nil {
		t.Fatal("heartbeat event did not carry the lease renewal metadata")
	}
	if !events[0].Lease.ExpiresAt.Equal(newExpiry) || events[0].Lease.LastHeartbeatAt == nil || !events[0].Lease.LastHeartbeatAt.Equal(hbTime) {
		t.Fatalf("heartbeat event lease = %+v, want the command's renewal metadata", events[0].Lease)
	}

	// Reduce the same event batch into two fresh copies of the pre-heartbeat
	// snapshot: the results must be identical (replay determinism). The only
	// wall-clock field Reduce may stamp is Instance.UpdatedAt, so normalize
	// it before comparing.
	replay := func() Snapshot {
		next, err := Reduce(state, events[0])
		if err != nil {
			t.Fatalf("Reduce heartbeat: %v", err)
		}
		next.Instance.UpdatedAt = time.Time{}
		return next
	}
	a, b := replay(), replay()
	if !reflect.DeepEqual(a, b) {
		t.Fatal("replaying the same heartbeat batch twice produced different snapshots")
	}
	act := actStart(a)
	if act.Status != ActivationLeased {
		t.Fatalf("status after heartbeat = %s, want leased (no transition)", act.Status)
	}
	if act.ActiveLease == nil {
		t.Fatal("active lease missing after heartbeat")
	}
	if !act.ActiveLease.ExpiresAt.Equal(newExpiry) {
		t.Fatalf("ExpiresAt = %v, want %v (renewed from event.Lease)", act.ActiveLease.ExpiresAt, newExpiry)
	}
	if act.ActiveLease.LastHeartbeatAt == nil || !act.ActiveLease.LastHeartbeatAt.Equal(hbTime) {
		t.Fatalf("LastHeartbeatAt = %v, want %v (from event.Lease)", act.ActiveLease.LastHeartbeatAt, hbTime)
	}
	if !act.ActiveLease.AcquiredAt.Equal(acquired) {
		t.Fatalf("AcquiredAt = %v, want %v (untouched by the heartbeat)", act.ActiveLease.AcquiredAt, acquired)
	}

	// Legacy heartbeat without lease metadata: zero-time sentinel, non-nil,
	// and the expiry is left as-is.
	legacy, err := Apply(state, Command{
		Kind:             CommandHeartbeat,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "hb-legacy",
		Identity:         baseIdentity(actID, ""),
		LeaseID:          "lease-1",
	})
	if err != nil {
		t.Fatalf("Apply legacy heartbeat: %v", err)
	}
	if legacy[0].Lease != nil {
		t.Fatalf("legacy heartbeat event carries lease metadata: %+v", legacy[0].Lease)
	}
	legacySnap, err := Reduce(state, legacy[0])
	if err != nil {
		t.Fatalf("Reduce legacy heartbeat: %v", err)
	}
	lact := actStart(legacySnap)
	if lact.ActiveLease.LastHeartbeatAt == nil {
		t.Fatal("legacy heartbeat left LastHeartbeatAt nil, want the zero-time sentinel")
	}
	if !lact.ActiveLease.LastHeartbeatAt.IsZero() {
		t.Fatalf("legacy LastHeartbeatAt = %v, want the zero-time sentinel", *lact.ActiveLease.LastHeartbeatAt)
	}
	if !lact.ActiveLease.ExpiresAt.Equal(time.Unix(2000, 0).UTC()) {
		t.Fatalf("legacy heartbeat changed ExpiresAt to %v, want untouched", lact.ActiveLease.ExpiresAt)
	}
}

// TestRecoveryRetainsHeartbeatedLeaseSweepsUnheartbeated is the store-level
// recovery counterpart: across a simulated restart (a fresh store running
// Catalog), a heartbeated lease whose expiry was renewed far into the future
// survives (retained), while an un-heartbeated expired lease is swept with
// reason "recovery".
func TestRecoveryRetainsHeartbeatedLeaseSweepsUnheartbeated(t *testing.T) {
	const twoNodeTemplateJSON = `{
  "schema_version": 1,
  "template_id": "hb-two",
  "template_version": "1",
  "entry_nodes": ["a", "b"],
  "nodes": [{"id": "a", "action": {"type": "manual"}}, {"id": "b", "action": {"type": "manual"}}],
  "terminal_outcomes": ["done"]
}`
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	if _, err := m.WorkflowCreate([]byte(twoNodeTemplateJSON)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	wf := mustInstantiateTemplate(t, m, "hb-two", "1")

	past := time.Now().UTC().Add(-time.Hour)
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	var actA, actB *Activation
	for i := range snap.Instance.Activations {
		switch snap.Instance.Activations[i].NodeID {
		case "a":
			actA = &snap.Instance.Activations[i]
		case "b":
			actB = &snap.Instance.Activations[i]
		}
	}
	if actA == nil || actB == nil {
		t.Fatalf("activations = %+v, want a and b", snap.Instance.Activations)
	}

	// Node "a": claimed on an expired lease, then explicitly heartbeated so
	// its expiry is renewed far into the future.
	claimWithLease(t, s, wf, "a", Lease{
		ID:           "lease-a",
		ActivationID: actA.ID,
		Owner:        "alice",
		TokenDigest:  ownerTokenDigest("tok-a"),
		AcquiredAt:   past.Add(-time.Minute),
		ExpiresAt:    past,
	}, "alice")
	if _, err := m.WorkflowCommand(string(wf), heartbeatPayload(t, "a", string(actA.ID), "lease-a", "tok-a")); err != nil {
		t.Fatalf("heartbeat a: %v", err)
	}
	// Node "b": claimed on an expired lease and never heartbeated.
	claimWithLease(t, s, wf, "b", Lease{
		ID:           "lease-b",
		ActivationID: actB.ID,
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
	a2 := activationByNode(&snap2.Instance, "a")
	if a2.Status != ActivationLeased {
		t.Fatalf("node a status after recovery = %s, want leased (heartbeated lease retained)", a2.Status)
	}
	if a2.ActiveLease == nil || a2.ActiveLease.ID != "lease-a" {
		t.Fatalf("node a lease after recovery = %+v, want lease-a retained", a2.ActiveLease)
	}
	if !a2.ActiveLease.ExpiresAt.After(time.Now().UTC()) {
		t.Fatalf("node a expiry after recovery = %v, want renewed into the future", a2.ActiveLease.ExpiresAt)
	}
	if a2.ActiveLease.LastHeartbeatAt == nil {
		t.Fatal("node a lease lost its heartbeat marker on recovery")
	}
	b2 := activationByNode(&snap2.Instance, "b")
	if b2.Status != ActivationLeaseExpired {
		t.Fatalf("node b status after recovery = %s, want lease_expired (swept)", b2.Status)
	}
	if b2.ActiveLease != nil {
		t.Fatalf("node b lease after recovery = %+v, want released", b2.ActiveLease)
	}

	// The sweep is recorded with the recovery reason.
	var sawRecovery bool
	for _, e := range readEvents(t, s, wf) {
		if e.Kind == EventLeaseExpired && e.Identity.NodeID == "b" && e.Reason == "recovery" {
			sawRecovery = true
		}
	}
	if !sawRecovery {
		t.Fatal("event log has no lease_expired event for node b with reason \"recovery\"")
	}
}

// ptrTime returns a pointer to t (test helper for inline lease metadata).
func ptrTime(t time.Time) *time.Time { return &t }
