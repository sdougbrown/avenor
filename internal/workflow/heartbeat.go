package workflow

// heartbeat.go holds the manager's explicit lease-liveness surface (Stage 13,
// Phase 1): the heartbeat command that renews a lease and the live detector
// that expires leases whose liveness is stale.
//
// Heartbeat and activity are two distinct liveness signals on a lease. Only an
// explicit heartbeat (this command) renews lease liveness: it stamps a real
// wall-clock LastHeartbeatAt and pushes ExpiresAt out by the effective TTL.
// Observed runtime activity never touches the liveness fields, so a lease that
// is not heartbeated still expires once its ExpiresAt passes — that is the
// single staleness oracle the detector relies on.
//
// Wall-clock time is owned by this manager layer, never the reducer: the
// heartbeat command carries the stamp on its Lease metadata so the reducer can
// apply it deterministically during replay without calling nowUTC().

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// commandHeartbeatRequest is the JSON body of the "heartbeat" op. It mirrors
// commandStartRequest's naming and strictness: node_id, lease_id, and
// owner_token are required; activation_id disambiguates when a node has more
// than one activation.
type commandHeartbeatRequest struct {
	NodeID       NodeID       `json:"node_id"`
	ActivationID ActivationID `json:"activation_id"`
	LeaseID      LeaseID      `json:"lease_id"`
	OwnerToken   string       `json:"owner_token"`
}

// commandHeartbeat renews the liveness of the activation's active lease. It
// validates the lease/owner pair, resolves the effective TTL, and applies a
// CommandHeartbeat whose Lease carries the renewed ExpiresAt and the wall-clock
// LastHeartbeatAt. The heartbeat never transitions the activation status and
// never touches LastActivityAt (reducer invariants).
func (m *Manager) commandHeartbeat(wf WorkflowID, payload json.RawMessage) (any, error) {
	var req commandHeartbeatRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("heartbeat payload: %w", err)
	}
	if req.NodeID == "" {
		return nil, errors.New("heartbeat requires node_id")
	}
	if req.LeaseID == "" {
		return nil, errors.New("heartbeat requires lease_id")
	}
	if req.OwnerToken == "" {
		return nil, errors.New("heartbeat requires owner_token")
	}
	snap, exists, err := m.store.loadCurrent(wf)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("workflow not found: %s", wf)
	}
	act, err := findActivation(&snap.Instance, req.NodeID, req.ActivationID)
	if err != nil {
		return nil, err
	}
	if act == nil {
		return nil, fmt.Errorf("activation not found for node %q", req.NodeID)
	}
	if act.ActiveLease == nil {
		return nil, errors.New("activation has no active lease")
	}
	if req.LeaseID != act.ActiveLease.ID {
		return nil, errors.New("lease_id does not match the active lease")
	}
	if ownerTokenDigest(req.OwnerToken) != act.ActiveLease.TokenDigest {
		return nil, errors.New("owner token does not match the lease")
	}
	tmpl, err := m.templateFor(&snap)
	if err != nil {
		return nil, err
	}
	node, err := findNode(tmpl, req.NodeID)
	if err != nil {
		return nil, err
	}
	ttl := leaseTTL(node, tmpl.DefaultLease)
	now := time.Now().UTC()
	newExpiry := now.Add(ttl)
	// One wall-clock read per renewal. The idempotency key is stable for this
	// renewal (derived from now) so a concurrent duplicate of the same renewal
	// is treated as already applied rather than re-recorded.
	idempotencyKey := "hb-" + string(req.LeaseID) + "-" + strconv.FormatInt(now.UnixNano(), 10)
	respond := func() (any, error) {
		return map[string]any{
			"lease_id":          string(req.LeaseID),
			"last_heartbeat_at": now,
			"expires_at":        newExpiry,
		}, nil
	}
	// Bounded-retry apply (mirrors RecordAttemptTerminated): a concurrent
	// command can bump the revision in the window between the read and the
	// apply. A duplicate idempotency key means this exact renewal was already
	// recorded, which is success.
	const maxHeartbeatAttempts = 4
	for attempt := 0; attempt < maxHeartbeatAttempts; attempt++ {
		cur, ok, err := m.store.loadCurrent(wf)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("workflow not found: %s", wf)
		}
		if _, err := m.store.ApplyCommand(wf, Command{
			ID:               NewCommandID(),
			Kind:             CommandHeartbeat,
			ExpectedRevision: cur.Instance.Revision,
			IdempotencyKey:   idempotencyKey,
			Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: req.NodeID, ActivationID: act.ID},
			LeaseID:          req.LeaseID,
			Lease:            &Lease{ExpiresAt: newExpiry, LastHeartbeatAt: &now},
		}); err != nil {
			if errors.Is(err, errDuplicateIdempotency) {
				// This exact renewal is already recorded: idempotent success.
				return respond()
			}
			if !errors.Is(err, errRevisionMismatch) {
				return nil, err
			}
			// Optimistic-concurrency conflict; re-read under a fresh lock and retry.
			continue
		}
		return respond()
	}
	return nil, fmt.Errorf("heartbeat lease %s: revision kept moving under concurrent commands", req.LeaseID)
}

// LeaseExpirySummary reports the outcome of one live lease-expiry sweep.
type LeaseExpirySummary struct {
	// Expired is the number of stale leases this sweep's own "stale" pass
	// expired to ActivationLeaseExpired. (Leases already expired by an earlier
	// recovery pass carry no ActiveLease and are not counted here.)
	Expired int
	// Retained is the number of leases left in place: every non-stale active
	// lease plus the exempted awaiting_child claims (whose kernel-held lease
	// survives regardless of staleness).
	Retained int
	// Errors is one entry per instance whose sweep failed. Errored instances
	// are left as-is and retried on the next sweep.
	Errors []string
}

// ExpireStaleLeases is the manager's live stall detector: it scans every
// instance and expires leases whose liveness is stale (now strictly after the
// lease's ExpiresAt). It performs its own sweep with reason "stale", distinct
// from restart recovery (reason "recovery"), so it is runnable live by the
// manager rather than only on restart — a node whose holder stopped
// heartbeating is reclaimed without a supervisor restart. It enumerates the
// instance directory itself and never invokes the recovery path (Catalog),
// whose recovery sweep would otherwise expire the same leases first with the
// wrong reason. Activity never renews ExpiresAt (only an explicit heartbeat
// does), so the single staleness oracle now.After(ExpiresAt) correctly
// implements "activity alone does not extend liveness". It is idempotent: an
// already-expired lease carries no ActiveLease and is never swept twice.
func (m *Manager) ExpireStaleLeases() (LeaseExpirySummary, error) {
	if err := m.store.CreateRoot(); err != nil {
		return LeaseExpirySummary{}, err
	}
	entries, err := os.ReadDir(filepath.Join(m.store.root, "instances"))
	if err != nil {
		return LeaseExpirySummary{}, err
	}
	now := time.Now().UTC()
	var summary LeaseExpirySummary
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		wf := WorkflowID(entry.Name())
		expired, retained, err := m.store.sweepStaleLeases(wf, "stale", now)
		if err != nil {
			summary.Errors = append(summary.Errors, fmt.Sprintf("workflow %s: %v", wf, err))
			continue
		}
		summary.Expired += expired
		summary.Retained += retained
	}
	return summary, nil
}
