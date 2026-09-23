package workflowcontroller

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// ControllerStore applies commands to controller records under a single POSIX
// flock per controller and durably persists a locked NDJSON event log plus an
// atomically replaced snapshot, mirroring the durability discipline of the
// workflow store.
type ControllerStore struct {
	root string
	now  func() time.Time
}

// NewStore returns a store rooted at workflowRoot, using the wall clock.
func NewStore(workflowRoot string) *ControllerStore {
	return &ControllerStore{root: workflowRoot, now: time.Now}
}

// NewStoreWithClock returns a store rooted at workflowRoot that reads the
// supplied clock for every time-dependent decision.
func NewStoreWithClock(workflowRoot string, now func() time.Time) *ControllerStore {
	return &ControllerStore{root: workflowRoot, now: now}
}

// Root returns the workflow root the store was constructed with.
func (s *ControllerStore) Root() string { return s.root }

// ControllersRoot returns the directory holding all controller state:
// <root>/controllers.
func (s *ControllerStore) ControllersRoot() string {
	return filepath.Join(s.root, "controllers")
}

func (s *ControllerStore) controllerDir(controllerID string) string {
	return filepath.Join(s.ControllersRoot(), controllerID)
}

func (s *ControllerStore) snapshotPath(controllerID string) string {
	return filepath.Join(s.controllerDir(controllerID), "controller.json")
}

func (s *ControllerStore) eventsPath(controllerID string) string {
	return filepath.Join(s.controllerDir(controllerID), "events.ndjson")
}

func (s *ControllerStore) lockPath(controllerID string) string {
	return filepath.Join(s.controllerDir(controllerID), controllerID+".lock")
}

// lockController takes the controller's exclusive flock. The caller must have
// ensured the controller directory exists.
func (s *ControllerStore) lockController(controllerID string) (func() error, error) {
	return lockFile(s.lockPath(controllerID))
}

// Create registers a controller with the given in-flight limit. Re-creating an
// existing disabled controller with the same limit returns the existing record;
// any other mismatch is a conflict.
func (s *ControllerStore) Create(controllerID string, maxInflight int) (ControllerRecord, error) {
	if err := validateControllerID(controllerID); err != nil {
		return ControllerRecord{}, err
	}
	if maxInflight <= 0 {
		return ControllerRecord{}, fmt.Errorf("%w: max_inflight must be positive, got %d", ErrInvalidController, maxInflight)
	}
	if err := os.MkdirAll(s.controllerDir(controllerID), 0o755); err != nil {
		return ControllerRecord{}, err
	}
	unlock, err := s.lockController(controllerID)
	if err != nil {
		return ControllerRecord{}, err
	}
	defer unlock()

	if _, err := os.Stat(s.snapshotPath(controllerID)); err == nil {
		rec, _, err := s.loadLocked(controllerID)
		if err != nil {
			return ControllerRecord{}, err
		}
		if rec.DesiredState == DesiredDisabled && rec.MaxInflight == maxInflight {
			return rec, nil
		}
		return ControllerRecord{}, fmt.Errorf("controller %s already exists: %w", controllerID, ErrConflict)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return ControllerRecord{}, err
	}

	now := s.now().UTC()
	rec := ControllerRecord{
		SchemaVersion: SchemaVersion,
		ControllerID:  controllerID,
		DesiredState:  DesiredDisabled,
		MaxInflight:   maxInflight,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	event := ControllerEvent{Kind: EventCreated, Seq: 1, Time: now, MaxInflight: maxInflight}
	if err := s.commitLocked(controllerID, &rec, []ControllerEvent{event}); err != nil {
		return ControllerRecord{}, err
	}
	return rec, nil
}

// Get returns the controller's current record, replaying any events beyond its
// snapshot. The second return reports whether the controller exists.
func (s *ControllerStore) Get(controllerID string) (ControllerRecord, bool, error) {
	if err := validateControllerID(controllerID); err != nil {
		return ControllerRecord{}, false, err
	}
	if _, err := os.Stat(s.snapshotPath(controllerID)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return ControllerRecord{}, false, nil
		}
		return ControllerRecord{}, false, err
	}
	unlock, err := s.lockController(controllerID)
	if err != nil {
		return ControllerRecord{}, false, err
	}
	defer unlock()
	rec, _, err := s.loadLocked(controllerID)
	return rec, true, err
}

// List returns every controller record sorted by id. A missing controllers
// directory yields an empty slice and no error.
func (s *ControllerStore) List() ([]ControllerRecord, error) {
	ids, err := s.controllerIDs()
	if err != nil {
		return nil, err
	}
	records := make([]ControllerRecord, 0, len(ids))
	for _, id := range ids {
		rec, ok, err := s.Get(id)
		if err != nil {
			return nil, err
		}
		if ok {
			records = append(records, rec)
		}
	}
	return records, nil
}

// controllerIDs lists the controller directory names in sorted order. A
// missing controllers directory is not an error.
func (s *ControllerStore) controllerIDs() ([]string, error) {
	entries, err := os.ReadDir(s.ControllersRoot())
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var ids []string
	for _, entry := range entries {
		if entry.IsDir() {
			ids = append(ids, entry.Name())
		}
	}
	sort.Strings(ids)
	return ids, nil
}

// SetDesiredState enables or disables a controller. Disabling while a lease is
// live also releases the lease in the same locked command.
func (s *ControllerStore) SetDesiredState(controllerID string, desired DesiredState, reason string) (ControllerRecord, error) {
	if err := validateControllerID(controllerID); err != nil {
		return ControllerRecord{}, err
	}
	if desired != DesiredEnabled && desired != DesiredDisabled {
		return ControllerRecord{}, fmt.Errorf("%w: unknown desired state %q", ErrInvalidController, desired)
	}
	unlock, err := s.lockController(controllerID)
	if err != nil {
		return ControllerRecord{}, err
	}
	defer unlock()
	rec, err := s.loadForCommand(controllerID)
	if err != nil {
		return ControllerRecord{}, err
	}

	now := s.now().UTC()
	seq := rec.Revision
	var events []ControllerEvent
	switch desired {
	case DesiredEnabled:
		if rec.DesiredState == DesiredEnabled {
			return rec, nil
		}
		seq++
		events = append(events, ControllerEvent{Kind: EventEnabled, Seq: seq, Time: now})
	case DesiredDisabled:
		if rec.DesiredState == DesiredDisabled && rec.Leader == nil {
			return rec, nil
		}
		seq++
		events = append(events, ControllerEvent{Kind: EventDisabled, Seq: seq, Time: now, Reason: reason})
		if rec.Leader != nil {
			seq++
			events = append(events, leaderReleasedEvent(seq, now, *rec.Leader))
		}
	}
	if err := s.commitLocked(controllerID, &rec, events); err != nil {
		return ControllerRecord{}, err
	}
	return rec, nil
}

// AcquireLease grants leadership to ownerID when the controller is enabled and
// its current lease (if any) has expired. It reports whether the lease was
// granted; requesting leadership while disabled fails with ErrDisabled.
func (s *ControllerStore) AcquireLease(controllerID, ownerID string) (ControllerRecord, bool, error) {
	if err := validateControllerID(controllerID); err != nil {
		return ControllerRecord{}, false, err
	}
	unlock, err := s.lockController(controllerID)
	if err != nil {
		return ControllerRecord{}, false, err
	}
	defer unlock()
	rec, err := s.loadForCommand(controllerID)
	if err != nil {
		return ControllerRecord{}, false, err
	}

	now := s.now().UTC()
	if rec.DesiredState != DesiredEnabled {
		return rec, false, fmt.Errorf("controller %s: %w", controllerID, ErrDisabled)
	}
	seq := rec.Revision
	var events []ControllerEvent
	if leaseExpired(now, rec.Leader) {
		seq++
		event := ControllerEvent{
			Kind:       EventLeaderExpired,
			Seq:        seq,
			Time:       now,
			Reason:     leaderTimeoutReason,
			LeaseID:    rec.Leader.LeaseID,
			OwnerID:    rec.Leader.OwnerID,
			OwnerEpoch: rec.Leader.OwnerEpoch,
		}
		events = append(events, event)
		if err := applyEvent(&rec, event); err != nil {
			return ControllerRecord{}, false, err
		}
	}
	if rec.Leader != nil {
		return rec, false, nil
	}
	leaseID, err := newLeaseID()
	if err != nil {
		return ControllerRecord{}, false, err
	}
	seq++
	events = append(events, ControllerEvent{
		Kind:       EventLeaderAcquired,
		Seq:        seq,
		Time:       now,
		LeaseID:    leaseID,
		OwnerID:    ownerID,
		OwnerEpoch: rec.OwnerEpoch + 1,
		ExpiresAt:  now.Add(LeaseTTL),
	})
	if err := s.commitLocked(controllerID, &rec, events); err != nil {
		return ControllerRecord{}, false, err
	}
	return rec, true, nil
}

// RenewLease extends the caller's lease by LeaseTTL via a compare-and-swap on
// lease id and owner epoch. The leader_renewed event is appended at most once
// per minute per lease; a renewal inside that window still extends the lease
// but only rewrites the snapshot, leaving Revision untouched.
func (s *ControllerStore) RenewLease(controllerID, leaseID string, ownerEpoch int64) (ControllerRecord, error) {
	if err := validateControllerID(controllerID); err != nil {
		return ControllerRecord{}, err
	}
	unlock, err := s.lockController(controllerID)
	if err != nil {
		return ControllerRecord{}, err
	}
	defer unlock()
	rec, err := s.loadForCommand(controllerID)
	if err != nil {
		return ControllerRecord{}, err
	}

	if err := checkLeaseCAS(rec, leaseID, ownerEpoch); err != nil {
		return ControllerRecord{}, fmt.Errorf("controller %s renew lease: %w", controllerID, err)
	}
	now := s.now().UTC()
	rec.Leader.RenewedAt = now
	rec.Leader.ExpiresAt = now.Add(LeaseTTL)
	rec.UpdatedAt = now
	if rec.LastRenewalEventAt.IsZero() || now.Sub(rec.LastRenewalEventAt) >= time.Minute {
		event := ControllerEvent{
			Kind:       EventLeaderRenewed,
			Seq:        rec.Revision + 1,
			Time:       now,
			LeaseID:    leaseID,
			OwnerEpoch: ownerEpoch,
			ExpiresAt:  rec.Leader.ExpiresAt,
		}
		if err := s.commitLocked(controllerID, &rec, []ControllerEvent{event}); err != nil {
			return ControllerRecord{}, err
		}
		return rec, nil
	}
	// Event suppressed: the renewed snapshot is written in place without
	// bumping Revision, since no event was appended. The snapshot-only renewal
	// intentionally diverges from the event log's ExpiresAt until the next
	// renewal event is appended; a snapshot lost after this point replays the
	// older expiry.
	if err := s.writeSnapshot(controllerID, rec); err != nil {
		return ControllerRecord{}, err
	}
	s.regenerateProjection(controllerID, rec)
	return rec, nil
}

// ReleaseLease voluntarily drops the caller's live lease via a compare-and-swap
// on lease id and owner epoch. Releasing an expired or absent lease is a
// conflict.
func (s *ControllerStore) ReleaseLease(controllerID, leaseID string, ownerEpoch int64) (ControllerRecord, error) {
	if err := validateControllerID(controllerID); err != nil {
		return ControllerRecord{}, err
	}
	unlock, err := s.lockController(controllerID)
	if err != nil {
		return ControllerRecord{}, err
	}
	defer unlock()
	rec, err := s.loadForCommand(controllerID)
	if err != nil {
		return ControllerRecord{}, err
	}

	if err := checkLeaseCAS(rec, leaseID, ownerEpoch); err != nil {
		return ControllerRecord{}, fmt.Errorf("controller %s release lease: %w", controllerID, err)
	}
	now := s.now().UTC()
	if leaseExpired(now, rec.Leader) {
		return ControllerRecord{}, fmt.Errorf("controller %s release lease: lease expired: %w", controllerID, ErrConflict)
	}
	event := leaderReleasedEvent(rec.Revision+1, now, *rec.Leader)
	if err := s.commitLocked(controllerID, &rec, []ControllerEvent{event}); err != nil {
		return ControllerRecord{}, err
	}
	return rec, nil
}

// leaderReleasedEvent builds the leader_released event for a held lease.
func leaderReleasedEvent(seq int64, now time.Time, lease LeaderLease) ControllerEvent {
	return ControllerEvent{
		Kind:       EventLeaderReleased,
		Seq:        seq,
		Time:       now,
		LeaseID:    lease.LeaseID,
		OwnerID:    lease.OwnerID,
		OwnerEpoch: lease.OwnerEpoch,
	}
}

// Recover replays every controller's event log, expires lapsed leases with
// reason "recovery", regenerates each projection, and persists changed
// snapshots. It never creates the controllers directory; a missing directory
// yields an empty slice and no error.
func (s *ControllerStore) Recover() ([]ControllerRecord, error) {
	ids, err := s.controllerIDs()
	if err != nil {
		return nil, err
	}
	records := make([]ControllerRecord, 0, len(ids))
	for _, id := range ids {
		rec, ok, err := s.recoverController(id)
		if err != nil {
			return nil, err
		}
		if ok {
			records = append(records, rec)
		}
	}
	return records, nil
}

// recoverController recovers one controller under its flock, reporting whether
// the controller exists.
func (s *ControllerStore) recoverController(controllerID string) (ControllerRecord, bool, error) {
	if _, err := os.Stat(s.snapshotPath(controllerID)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return ControllerRecord{}, false, nil
		}
		return ControllerRecord{}, false, err
	}
	unlock, err := s.lockController(controllerID)
	if err != nil {
		return ControllerRecord{}, false, err
	}
	defer unlock()

	rec, replayed, err := s.loadLocked(controllerID)
	if err != nil {
		return ControllerRecord{}, false, err
	}
	changed := replayed > 0
	now := s.now().UTC()
	if leaseExpired(now, rec.Leader) {
		event := ControllerEvent{
			Kind:       EventLeaderExpired,
			Seq:        rec.Revision + 1,
			Time:       now,
			Reason:     leaderRecoveryReason,
			LeaseID:    rec.Leader.LeaseID,
			OwnerID:    rec.Leader.OwnerID,
			OwnerEpoch: rec.Leader.OwnerEpoch,
		}
		if err := s.appendEvents(controllerID, []ControllerEvent{event}); err != nil {
			return ControllerRecord{}, false, err
		}
		if err := applyEvent(&rec, event); err != nil {
			return ControllerRecord{}, false, err
		}
		changed = true
	}
	if changed {
		if err := s.writeSnapshot(controllerID, rec); err != nil {
			return ControllerRecord{}, false, err
		}
	}
	s.regenerateProjection(controllerID, rec)
	return rec, true, nil
}

// loadForCommand validates the id, verifies the controller exists, and loads
// its record. It must be called with the controller's flock already held.
func (s *ControllerStore) loadForCommand(controllerID string) (ControllerRecord, error) {
	if err := validateControllerID(controllerID); err != nil {
		return ControllerRecord{}, err
	}
	if _, err := os.Stat(s.snapshotPath(controllerID)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return ControllerRecord{}, fmt.Errorf("controller %s: %w", controllerID, ErrNotFound)
		}
		return ControllerRecord{}, err
	}
	rec, _, err := s.loadLocked(controllerID)
	return rec, err
}

// loadLocked reads the snapshot under a held flock and replays any events
// beyond its revision, truncating a malformed trailing event line. It reports
// whether the snapshot exists and how many events were replayed.
func (s *ControllerStore) loadLocked(controllerID string) (ControllerRecord, int, error) {
	data, err := os.ReadFile(s.snapshotPath(controllerID))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return ControllerRecord{}, 0, nil
		}
		return ControllerRecord{}, 0, err
	}
	var rec ControllerRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return ControllerRecord{}, 0, fmt.Errorf("controller %s snapshot: %w", controllerID, err)
	}
	rec, replayed, err := s.replayEvents(controllerID, rec, s.eventsPath(controllerID))
	return rec, replayed, err
}

// replayEvents applies logged events with a seq beyond the record's revision,
// tolerating a truncated final line (an incomplete append) by cutting the log
// back to the last good line. Corruption on any earlier line is an error.
func (s *ControllerStore) replayEvents(controllerID string, rec ControllerRecord, path string) (ControllerRecord, int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return rec, 0, nil
		}
		return rec, 0, err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return rec, 0, nil
	}
	lines := splitLines(data)
	replayed := 0
	var lastGoodEnd int64
	for i, line := range lines {
		var e ControllerEvent
		if err := json.Unmarshal(data[line.start:line.end], &e); err != nil {
			if i == len(lines)-1 {
				log.Printf("controller %s: truncating incomplete final event at byte %d", controllerID, lastGoodEnd)
				if err := os.Truncate(path, lastGoodEnd); err != nil {
					return rec, replayed, err
				}
				return rec, replayed, nil
			}
			return rec, replayed, fmt.Errorf("controller %s event log corrupted at line %d: %w", controllerID, i+1, err)
		}
		if e.Seq > rec.Revision {
			if err := applyEvent(&rec, e); err != nil {
				return rec, replayed, fmt.Errorf("controller %s event %d: %w", controllerID, e.Seq, err)
			}
			replayed++
		}
		lastGoodEnd = line.end
	}
	return rec, replayed, nil
}

type lineSpan struct {
	start int64
	end   int64
}

// splitLines splits data on '\n', recording each line's byte end offset (just
// after the newline, or len(data) for a final line with no trailing newline).
func splitLines(data []byte) []lineSpan {
	var lines []lineSpan
	start := int64(0)
	for i, b := range data {
		if b == '\n' {
			lines = append(lines, lineSpan{start: start, end: int64(i) + 1})
			start = int64(i) + 1
		}
	}
	if start < int64(len(data)) {
		lines = append(lines, lineSpan{start: start, end: int64(len(data))})
	}
	return lines
}

// commitLocked durably appends events, reduces them into rec, and persists the
// resulting snapshot; events are fsynced before the snapshot is written.
func (s *ControllerStore) commitLocked(controllerID string, rec *ControllerRecord, events []ControllerEvent) error {
	if len(events) > 0 {
		if err := s.appendEvents(controllerID, events); err != nil {
			return err
		}
		for _, e := range events {
			if err := applyEvent(rec, e); err != nil {
				return err
			}
		}
	}
	if err := s.writeSnapshot(controllerID, *rec); err != nil {
		return err
	}
	s.regenerateProjection(controllerID, *rec)
	return nil
}

// appendEvents durably appends complete JSON events to the NDJSON log.
func (s *ControllerStore) appendEvents(controllerID string, events []ControllerEvent) error {
	f, err := os.OpenFile(s.eventsPath(controllerID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	var firstErr error
	for _, e := range events {
		data, err := json.Marshal(e)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if _, err = f.Write(append(data, '\n')); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := f.Sync(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// writeSnapshot atomically replaces the snapshot file and fsyncs the directory.
func (s *ControllerStore) writeSnapshot(controllerID string, rec ControllerRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	dir := s.controllerDir(controllerID)
	tmp, err := os.CreateTemp(dir, "controller.json.tmp*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := os.Chmod(tmpName, 0o644); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, s.snapshotPath(controllerID)); err != nil {
		return err
	}
	return fsyncDir(dir)
}

// fsyncDir fsyncs a directory so renames into it are durable.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// regenerateProjection rewrites the controller's Markdown projection. The
// projection is a derived artifact, never authoritative: a failure to write it
// must not fail an already-committed state transition, so the error is logged
// (non-fatal) and swallowed.
func (s *ControllerStore) regenerateProjection(controllerID string, rec ControllerRecord) {
	if err := WriteProjection(s.controllerDir(controllerID), rec); err != nil {
		log.Printf("controller %s: projection: %v", controllerID, err)
	}
}
