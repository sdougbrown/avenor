package workflow

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// CatalogedInstance pairs a workflow ID with its recovered snapshot.
type CatalogedInstance struct {
	WorkflowID WorkflowID
	Snapshot   Snapshot
}

// Catalog enumerates every recoverable instance under the root, replaying each
// instance's events beyond its snapshot revision and expiring stale leases.
func (s *Store) Catalog() ([]CatalogedInstance, error) {
	if err := s.CreateRoot(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(filepath.Join(s.root, "instances"))
	if err != nil {
		return nil, err
	}
	var list []CatalogedInstance
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		wf := WorkflowID(entry.Name())
		snap, ok, err := s.recoverInstance(wf)
		if err != nil {
			return nil, err
		}
		if ok {
			list = append(list, CatalogedInstance{WorkflowID: wf, Snapshot: snap})
		}
	}
	return list, nil
}

func (s *Store) recoverInstance(workflowID WorkflowID) (Snapshot, bool, error) {
	if _, err := os.Stat(s.workflowPath(workflowID)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Snapshot{}, false, nil
		}
		return Snapshot{}, false, err
	}
	unlock, err := lockFile(s.lockPath(workflowID))
	if err != nil {
		return Snapshot{}, false, err
	}
	defer unlock()

	snap, _, err := s.loadSnapshot(workflowID)
	if err != nil {
		return Snapshot{}, false, err
	}
	sn, n, truncated, err := replayEvents(snap, s.eventsPath(workflowID))
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
			})
		}
	}
	return events
}

// appendExpiredLeases emits lease-expired events for stale leases in activation
// slice order and reduces them into the supplied snapshot. It returns the
// number of leases expired.
func (s *Store) appendExpiredLeases(workflowID WorkflowID, snap *Snapshot) (int, error) {
	events := collectExpiredLeaseEvents(snap, "recovery", nowUTC())
	if len(events) == 0 {
		return 0, nil
	}
	if err := s.appendEvents(workflowID, events); err != nil {
		return 0, err
	}
	for _, e := range events {
		next, err := Reduce(*snap, e)
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
			return 0, 0, nil
		}
		return 0, 0, err
	}
	if err := s.ensureInstanceDir(workflowID); err != nil {
		return 0, 0, err
	}
	unlock, err := lockFile(s.lockPath(workflowID))
	if err != nil {
		return 0, 0, err
	}
	defer unlock()

	snap, exists, err := s.loadSnapshot(workflowID)
	if err != nil || !exists {
		return 0, 0, err
	}
	replayed, n, truncated, err := replayEvents(snap, s.eventsPath(workflowID))
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
			next, err := Reduce(snap, e)
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
