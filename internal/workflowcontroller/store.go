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
	"sync"
	"time"

	"github.com/sdougbrown/avenor/internal/durablefile"
)

// ControllerStore applies commands to controller records under a single POSIX
// flock per controller and durably persists a locked NDJSON event log plus an
// atomically replaced snapshot, mirroring the durability discipline of the
// workflow store.
type ControllerStore struct {
	root string
	now  func() time.Time
	// compactBytes bounds the controller event log: every event in the log is
	// already applied to the snapshot, so once the log grows past this it is
	// rotated after the snapshot superseding it is durable.
	compactBytes int64

	// subMu guards subscribers, a coalescing fan-out of buffered(1) wake
	// channels notified after every committed controller record change.
	subMu       sync.Mutex
	subscribers []chan struct{}
}

// defaultCompactLogBytes is the event-log size past which a controller log is
// rotated.
const defaultCompactLogBytes int64 = 64 << 10

// NewStore returns a store rooted at workflowRoot, using the wall clock.
func NewStore(workflowRoot string) *ControllerStore {
	return &ControllerStore{root: workflowRoot, now: time.Now, compactBytes: defaultCompactLogBytes}
}

// NewStoreWithClock returns a store rooted at workflowRoot that reads the
// supplied clock for every time-dependent decision.
func NewStoreWithClock(workflowRoot string, now func() time.Time) *ControllerStore {
	return &ControllerStore{root: workflowRoot, now: now, compactBytes: defaultCompactLogBytes}
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

// prevEventsPath is the previous log generation, kept so a snapshot restored
// one generation back still replays to the newest lease expiry.
func (s *ControllerStore) prevEventsPath(controllerID string) string {
	return filepath.Join(s.controllerDir(controllerID), "events.ndjson.prev")
}

func (s *ControllerStore) lockPath(controllerID string) string {
	return filepath.Join(s.controllerDir(controllerID), controllerID+".lock")
}

// lockController takes the controller's exclusive flock. If the controller
// directory does not exist, the lock open fails with ENOENT and this returns
// ErrNotFound, so callers that lock before checking existence report not-found.
func (s *ControllerStore) lockController(controllerID string) (func() error, error) {
	unlock, err := durablefile.Lock(s.lockPath(controllerID))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("controller %s: %w", controllerID, ErrNotFound)
		}
		return nil, err
	}
	return unlock, nil
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

// Controllers is the outcome of a fleet-wide read: the records that loaded and
// one failure per controller that was skipped because its state cannot be
// replayed. A skipped controller never hides the records of the others.
type Controllers struct {
	Records  []ControllerRecord
	Failures []ControllerLoadFailure
}

// ControllerLoadFailure names one controller whose persisted state could not
// be loaded.
type ControllerLoadFailure struct {
	ControllerID string
	Err          error
}

// Error returns the wrapped load error's message, which already names the
// controller it belongs to.
func (f ControllerLoadFailure) Error() string {
	return f.Err.Error()
}

// Unwrap exposes the underlying load error to errors.Is.
func (f ControllerLoadFailure) Unwrap() error { return f.Err }

// skip records a per-item load failure and reports whether err is isolated to
// that controller. Only a replay hole is isolated; every other load error stays
// fatal for the whole read.
func (c *Controllers) skip(controllerID string, err error) bool {
	if !errors.Is(err, ErrLogHole) {
		return false
	}
	c.Failures = append(c.Failures, ControllerLoadFailure{ControllerID: controllerID, Err: err})
	return true
}

// loadAll reads every controller in sorted id order, calling load once per
// controller, collecting the records that loaded and reporting a replay hole
// as a per-controller failure. A load error that is not a hole aborts the whole
// read.
func (s *ControllerStore) loadAll(load func(controllerID string) (ControllerRecord, bool, error)) (Controllers, error) {
	ids, err := s.controllerIDs()
	if err != nil {
		return Controllers{}, err
	}
	out := Controllers{Records: make([]ControllerRecord, 0, len(ids))}
	for _, id := range ids {
		rec, ok, err := load(id)
		if err != nil {
			if out.skip(id, err) {
				continue
			}
			return Controllers{}, err
		}
		if ok {
			out.Records = append(out.Records, rec)
		}
	}
	return out, nil
}

// List returns every controller record sorted by id. A controller whose event
// log cannot be replayed is reported per item and skipped, so one unreplayable
// history does not hide the rest. A missing controllers directory yields an
// empty set and no error.
func (s *ControllerStore) List() (Controllers, error) {
	return s.loadAll(s.Get)
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
// lease id and owner epoch. Every renewal is logged, so a snapshot recovered
// from a stale or lost copy never carries an expiry the event log cannot
// reproduce.
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
	event := ControllerEvent{
		Kind:       EventLeaderRenewed,
		Seq:        rec.Revision + 1,
		Time:       now,
		LeaseID:    leaseID,
		OwnerEpoch: ownerEpoch,
		ExpiresAt:  now.Add(LeaseTTL),
	}
	if err := s.commitLocked(controllerID, &rec, []ControllerEvent{event}); err != nil {
		return ControllerRecord{}, err
	}
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
// snapshots. A controller whose history cannot be replayed is reported per
// item and skipped, so one holed log does not disable the others. It never
// creates the controllers directory; a missing directory yields an empty set
// and no error.
func (s *ControllerStore) Recover() (Controllers, error) {
	return s.loadAll(s.recoverController)
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

// WithLeader verifies that (leaseID, ownerEpoch) is the current unexpired
// leader lease of an enabled controller and runs fn while holding the
// controller's flock. ErrNotLeader is returned when the controller does not
// exist, is disabled, or the pair is not the live leader.
func (s *ControllerStore) WithLeader(controllerID, leaseID string, ownerEpoch int64, fn func() error) error {
	if err := validateControllerID(controllerID); err != nil {
		return err
	}
	if _, err := os.Stat(s.snapshotPath(controllerID)); errors.Is(err, fs.ErrNotExist) {
		return ErrNotLeader
	}
	unlock, err := s.lockController(controllerID)
	if err != nil {
		return err
	}
	defer unlock()
	rec, err := s.loadForCommand(controllerID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return ErrNotLeader
		}
		return err
	}
	if rec.DesiredState != DesiredEnabled {
		return ErrNotLeader
	}
	if checkLeaseCAS(rec, leaseID, ownerEpoch) != nil {
		return ErrNotLeader
	}
	if leaseExpired(s.now().UTC(), rec.Leader) {
		return ErrNotLeader
	}
	return fn()
}

// RecordCapacityBlocked appends a capacity_blocked event when the blocked
// reason differs from the record's current reason; an unchanged reason is a
// no-op returning (rec, false, nil). detail is diagnostic text only.
func (s *ControllerStore) RecordCapacityBlocked(controllerID, reason, detail string) (ControllerRecord, bool, error) {
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

	if rec.CapacityBlocked == reason {
		return rec, false, nil
	}
	now := s.now().UTC()
	event := ControllerEvent{
		Kind:          EventCapacityBlocked,
		Seq:           rec.Revision + 1,
		Time:          now,
		BlockedReason: reason,
		BlockedDetail: detail,
	}
	if err := s.commitLocked(controllerID, &rec, []ControllerEvent{event}); err != nil {
		return ControllerRecord{}, false, err
	}
	return rec, true, nil
}

// ClearCapacityBlocked appends a capacity_cleared event when the record is
// currently blocked; an unblocked record is a no-op (rec, false, nil).
func (s *ControllerStore) ClearCapacityBlocked(controllerID string) (ControllerRecord, bool, error) {
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

	if rec.CapacityBlocked == "" {
		return rec, false, nil
	}
	now := s.now().UTC()
	event := ControllerEvent{
		Kind: EventCapacityCleared,
		Seq:  rec.Revision + 1,
		Time: now,
	}
	if err := s.commitLocked(controllerID, &rec, []ControllerEvent{event}); err != nil {
		return ControllerRecord{}, false, err
	}
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
// beyond its revision, previous log generation first, truncating a malformed
// trailing event line. A snapshot older than the oldest retained generation is
// an error rather than a repair. It reports whether the snapshot exists and
// how many events were replayed.
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
	rec, replayed, err := s.replayEvents(controllerID, rec, s.prevEventsPath(controllerID))
	if err != nil {
		return rec, replayed, err
	}
	rec, current, err := s.replayEvents(controllerID, rec, s.eventsPath(controllerID))
	if err != nil {
		return rec, replayed, err
	}
	return rec, replayed + current, nil
}

// replayEvents applies logged events with a seq beyond the record's revision,
// requiring every applied event to follow the record's revision contiguously:
// a skipped seq means the generation holding it is gone, so replay reports the
// missing range instead of jumping the record forward over discarded events.
// A truncated final line (an incomplete append) is tolerated by cutting the
// log back to the last good line; corruption on any earlier line is an error.
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
	lines := durablefile.SplitLines(data)
	replayed := 0
	var lastGoodEnd int64
	for i, line := range lines {
		var e ControllerEvent
		if err := json.Unmarshal(data[line.Start:line.End], &e); err != nil {
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
			if e.Seq != rec.Revision+1 {
				return rec, replayed, fmt.Errorf("controller %s event log: missing events %d-%d before seq %d: %w",
					controllerID, rec.Revision+1, e.Seq-1, e.Seq, ErrLogHole)
			}
			if err := applyEvent(&rec, e); err != nil {
				return rec, replayed, fmt.Errorf("controller %s event %d: %w", controllerID, e.Seq, err)
			}
			replayed++
		}
		lastGoodEnd = line.End
	}
	return rec, replayed, nil
}

// commitLocked durably appends events, reduces them into rec, and persists the
// resulting snapshot; events are fsynced before the snapshot is written.
func (s *ControllerStore) commitLocked(controllerID string, rec *ControllerRecord, events []ControllerEvent) error {
	defer s.notifySubscribers()
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

// SubscribeChanges returns a coalescing wake channel and a cancel func. The
// channel has buffer size 1 and every committed controller record change
// performs a non-blocking send, so a subscriber that falls behind observes one
// coalesced signal instead of one per commit. Notifications are hints only;
// callers must not rely on lossless delivery, and a wake may follow a failed
// commit whose events were already durably appended (the wake reflects the
// event log's committed state, not the in-memory record). Callers must call
// the cancel func: a leaked subscriber adds a send to every future commit.
func (s *ControllerStore) SubscribeChanges() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	s.subMu.Lock()
	s.subscribers = append(s.subscribers, ch)
	s.subMu.Unlock()
	cancel := func() {
		s.subMu.Lock()
		defer s.subMu.Unlock()
		for i, c := range s.subscribers {
			if c == ch {
				s.subscribers = append(s.subscribers[:i], s.subscribers[i+1:]...)
				break
			}
		}
	}
	return ch, cancel
}

// notifySubscribers performs a non-blocking send to every change subscriber
// after a commit. It never fails, blocks, or rolls back the commit.
func (s *ControllerStore) notifySubscribers() {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	for _, ch := range s.subscribers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// appendEvents durably appends complete JSON events to the NDJSON log. A
// failed append is rolled back to the pre-append offset: replay hard-fails on
// a torn line anywhere before the final one, so a partial write must not reach
// the log.
func (s *ControllerStore) appendEvents(controllerID string, events []ControllerEvent) error {
	f, err := os.OpenFile(s.eventsPath(controllerID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	start := fi.Size()
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
	if firstErr == nil {
		return nil
	}
	if err := f.Truncate(start); err != nil {
		log.Printf("controller %s: truncate failed append: %v", controllerID, err)
		return firstErr
	}
	if err := f.Sync(); err != nil {
		log.Printf("controller %s: sync after truncate: %v", controllerID, err)
	}
	return firstErr
}

// writeSnapshot atomically replaces the snapshot file, fsyncs the directory,
// and then compacts the event log the snapshot supersedes.
func (s *ControllerStore) writeSnapshot(controllerID string, rec ControllerRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	dir := s.controllerDir(controllerID)
	path := s.snapshotPath(controllerID)
	if err := durablefile.WriteFileAtomic(path, durablefile.TempPattern(path), data, 0o600); err != nil {
		return err
	}
	if err := durablefile.FsyncDir(dir); err != nil {
		return err
	}
	// Compaction is best-effort: the commit is already durable, so a failed
	// rotation must not report the commit as failed.
	if err := s.compactLog(controllerID); err != nil {
		log.Printf("controller %s: compact log: %v", controllerID, err)
	}
	return nil
}

// compactLog rotates the event log once it outgrows compactBytes, keeping the
// generation it replaces as events.ndjson.prev. It runs only after the
// snapshot superseding the log is durable: replay skips events at or below the
// snapshot's revision, so a rotation lost to a crash leaves skipped events
// behind, while rotating before the snapshot lands would lose events replay
// still needs. The rotation is one rename, and a missing active log replays as
// empty while the next append creates it, so there is no post-rename window to
// repair. Failures are reported to the caller, which only logs them: a failed
// rotation lets the log grow.
func (s *ControllerStore) compactLog(controllerID string) error {
	path := s.eventsPath(controllerID)
	fi, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	if fi.Size() <= s.compactThreshold() {
		return nil
	}
	if err := os.Rename(path, s.prevEventsPath(controllerID)); err != nil {
		return err
	}
	return durablefile.FsyncDir(s.controllerDir(controllerID))
}

// compactThreshold bounds the active log, defaulting for a store built without
// a threshold so a zero value cannot rotate on every write.
func (s *ControllerStore) compactThreshold() int64 {
	if s.compactBytes <= 0 {
		return defaultCompactLogBytes
	}
	return s.compactBytes
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
