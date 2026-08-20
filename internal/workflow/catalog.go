package workflow

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
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

// appendExpiredLeases emits lease-expired events for stale leases in activation
// slice order and reduces them into the supplied snapshot. It returns the
// number of leases expired.
func (s *Store) appendExpiredLeases(workflowID WorkflowID, snap *Snapshot) (int, error) {
	now := nowUTC()
	var events []Event
	for _, a := range snap.Instance.Activations {
		if a.ActiveLease != nil && a.ActiveLease.ExpiresAt.Before(now) {
			events = append(events, Event{
				ID:       NewEventID(),
				Kind:     EventLeaseExpired,
				Sequence: snap.Instance.Revision + int64(len(events)+1),
				Reason:   "recovery",
				Identity: ExecutionIdentity{
					WorkflowID:   snap.Instance.WorkflowID,
					NodeID:       a.NodeID,
					ActivationID: a.ID,
				},
				LeaseID: a.ActiveLease.ID,
			})
		}
	}
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
