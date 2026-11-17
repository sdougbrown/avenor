package workflow

import (
	"errors"
	"io/fs"
	"log"
	"os"
	"time"

	"github.com/sdougbrown/avenor/internal/durablefile"
)

// This file owns restart recovery for the durable store: replaying each
// instance's events beyond its snapshot and sweeping stale leases on the
// recovery path.
//
// Liveness contract (orchestrator's resolved decision): restart recovery
// compares the lease's persisted, heartbeated ExpiresAt (with LastHeartbeatAt
// available for audit) against the current time. A lease whose heartbeated
// ExpiresAt has passed is expired with reason "recovery" and released for
// replacement; an unexpired one is retained. Activity (LastActivityAt) is
// never a liveness signal — only an explicit workflow.heartbeat renews
// liveness — so a lease that saw activity but never heartbeated still sweeps
// once its expiry passes. The staleness oracle is unchanged: a lease is stale
// iff now is strictly after its ExpiresAt. An awaiting_child kernel
// composition claim is exempt from the sweep: the kernel holds the claim on
// its composed child until the child's terminal outcome lands, and the
// child-outcome resolution requires awaiting_child, so expiring it here would
// strand the parent.
func (s *Store) recoverInstance(workflowID WorkflowID) (Snapshot, bool, error) {
	if _, err := os.Stat(s.workflowPath(workflowID)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// An events log without a snapshot is an orphaned write from a crash
			// between appendEvents and writeSnapshot; the committed events are
			// authoritative, so recovery replays and persists them like any other
			// instance. A log that replays to no instance stays uncataloged.
			return s.recoverOrphanedEvents(workflowID)
		}
		return Snapshot{}, false, err
	}
	unlock, err := durablefile.Lock(s.lockPath(workflowID))
	if err != nil {
		return Snapshot{}, false, err
	}
	defer unlock()

	snap, _, err := s.loadSnapshot(workflowID)
	if err != nil {
		return Snapshot{}, false, err
	}
	sn, n, truncated, err := replayEvents(snap, s.eventsPath(workflowID), s.policies)
	if err != nil {
		return Snapshot{}, false, err
	}
	changed := n > 0 || truncated > 0
	expired, err := s.appendExpiredLeases(workflowID, &sn)
	if err != nil {
		return Snapshot{}, false, err
	}
	if expired > 0 {
		changed = true
	}
	if changed {
		if err := s.writeSnapshot(workflowID, sn); err != nil {
			return Snapshot{}, false, err
		}
	}
	return sn, true, nil
}

// recoverOrphanedEvents recovers an instance whose events were appended but
// whose snapshot was never written: it replays the log from empty, persists
// the recovered snapshot, and runs the recovery lease sweep on it. A log that
// is absent or replays to no instance reports not-exists (uncataloged).
func (s *Store) recoverOrphanedEvents(workflowID WorkflowID) (Snapshot, bool, error) {
	if _, err := os.Stat(s.eventsPath(workflowID)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Snapshot{}, false, nil
		}
		return Snapshot{}, false, err
	}
	unlock, err := durablefile.Lock(s.lockPath(workflowID))
	if err != nil {
		return Snapshot{}, false, err
	}
	defer unlock()
	snap, exists, err := s.persistRecoveredOrphan(workflowID)
	if err != nil {
		// A log that cannot replay stays uncataloged: recovery must not fail
		// the whole catalog on one unreadable instance (matches the pre-recovery
		// skip of instances without a parsable snapshot).
		log.Printf("workflow %s: orphaned event log unreplayable, leaving uncataloged: %v", workflowID, err)
		return Snapshot{}, false, nil
	}
	if !exists {
		return snap, false, nil
	}
	exp, err := s.appendExpiredLeases(workflowID, &snap)
	if err != nil {
		return Snapshot{}, false, err
	}
	if exp > 0 {
		if err := s.writeSnapshot(workflowID, snap); err != nil {
			return Snapshot{}, false, err
		}
	}
	return snap, true, nil
}

// materializeRecoveredSnapshot persists a recovered snapshot for an instance
// whose events were appended but whose snapshot write was lost, so a retry
// that resolved through the read-only replay leaves a fully cataloged instance
// behind instead of a permanent orphan. A no-op when the snapshot already
// exists or the log replays to no instance.
func (s *Store) materializeRecoveredSnapshot(workflowID WorkflowID) error {
	unlock, err := durablefile.Lock(s.lockPath(workflowID))
	if err != nil {
		return err
	}
	defer unlock()
	if _, err := os.Stat(s.workflowPath(workflowID)); err == nil {
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	_, recovered, err := s.persistRecoveredOrphan(workflowID)
	if err != nil || !recovered {
		return err
	}
	return nil
}

// collectExpiredLeaseEvents builds lease-expired events for every stale lease
// in activation slice order. A lease is stale iff now is strictly after its
// ExpiresAt — the single staleness oracle. Activity (LastActivityAt) never
// renews ExpiresAt, so an active-but-un-heartbeated lease still sweeps once
// its expiry passes. awaiting_child activations are exempt: the kernel holds
// the composition claim on its composed child until the child's terminal
// outcome lands, and the child-outcome resolution requires awaiting_child, so
// expiring the lease here would strand the parent. The reason distinguishes
// the recovery sweep ("recovery") from the live manager detector ("stale").
func collectExpiredLeaseEvents(snap *Snapshot, reason string, now time.Time) []Event {
	var events []Event
	for _, a := range snap.Instance.Activations {
		if a.Status == ActivationAwaitingChild {
			continue
		}
		if a.ActiveLease != nil && now.After(a.ActiveLease.ExpiresAt) {
			stamp := now.UTC()
			events = append(events, Event{
				ID:       NewEventID(),
				Kind:     EventLeaseExpired,
				Sequence: snap.Instance.Revision + int64(len(events)+1),
				Reason:   reason,
				Identity: ExecutionIdentity{
					WorkflowID:   snap.Instance.WorkflowID,
					NodeID:       a.NodeID,
					ActivationID: a.ID,
				},
				LeaseID: a.ActiveLease.ID,
				ReadyAt: &stamp,
				At:      stamp,
			})
		}
	}
	return events
}

// appendExpiredLeases emits lease-expired events for stale leases in activation
// slice order and reduces them into the supplied snapshot. It returns the
// number of leases expired.
func (s *Store) appendExpiredLeases(workflowID WorkflowID, snap *Snapshot) (int, error) {
	events := collectExpiredLeaseEvents(snap, "recovery", s.now())
	if len(events) == 0 {
		return 0, nil
	}
	if err := s.appendEvents(workflowID, events); err != nil {
		return 0, err
	}
	for _, e := range events {
		next, err := Reduce(*snap, e, s.policies)
		if err != nil {
			return 0, err
		}
		*snap = next
	}
	return len(events), nil
}

// sweepStaleLeases expires stale leases on one instance under the instance's
// exclusive lock, mirroring appendExpiredLeases (the recovery sweep) but
// runnable live on demand with an explicit now and reason. It replays the
// event log to the end, appends a lease-expired event for each stale
// non-exempt lease, reduces them, and writes the snapshot. It returns the
// number of leases expired and the number of leases left in place (non-stale
// active leases plus exempted awaiting_child claims).
func (s *Store) sweepStaleLeases(workflowID WorkflowID, reason string, now time.Time) (expired int, retained int, err error) {
	if _, err := os.Stat(s.workflowPath(workflowID)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// A snapshot-less log is an orphaned write; recover it like restart
			// recovery does, then sweep the recovered state below.
			if _, err := os.Stat(s.eventsPath(workflowID)); err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					return 0, 0, nil
				}
				return 0, 0, err
			}
		} else {
			return 0, 0, err
		}
	}
	if err := s.ensureInstanceDir(workflowID); err != nil {
		return 0, 0, err
	}
	unlock, err := durablefile.Lock(s.lockPath(workflowID))
	if err != nil {
		return 0, 0, err
	}
	defer unlock()

	snap, exists, err := s.loadSnapshot(workflowID)
	if err != nil {
		return 0, 0, err
	}
	if !exists {
		replayed, recovered, rerr := s.persistRecoveredOrphan(workflowID)
		if rerr != nil {
			return 0, 0, rerr
		}
		if !recovered {
			return 0, 0, nil
		}
		snap = replayed
	}
	replayed, n, truncated, err := replayEvents(snap, s.eventsPath(workflowID), s.policies)
	if err != nil {
		return 0, 0, err
	}
	if n > 0 || truncated > 0 {
		snap = replayed
	}
	// Count the leases that stay in place: every non-stale active lease plus
	// the exempted awaiting_child claims (whose kernel-held lease survives
	// regardless of staleness).
	for _, a := range snap.Instance.Activations {
		if a.Status == ActivationAwaitingChild {
			if a.ActiveLease != nil {
				retained++
			}
			continue
		}
		if a.ActiveLease != nil && !now.After(a.ActiveLease.ExpiresAt) {
			retained++
		}
	}
	events := collectExpiredLeaseEvents(&snap, reason, now)
	if len(events) > 0 {
		if err := s.appendEvents(workflowID, events); err != nil {
			return 0, 0, err
		}
		for _, e := range events {
			next, err := Reduce(snap, e, s.policies)
			if err != nil {
				return 0, 0, err
			}
			snap = next
		}
	}
	if len(events) > 0 || n > 0 || truncated > 0 {
		if err := s.writeSnapshot(workflowID, snap); err != nil {
			return 0, 0, err
		}
		s.regenerateProjections(workflowID, snap)
	}
	return len(events), retained, nil
}
