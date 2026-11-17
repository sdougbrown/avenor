package workflow

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/sdougbrown/avenor/internal/durablefile"
)

// Store applies commands to workflow instances under a single POSIX flock and
// durably persists a locked NDJSON event log plus an atomically replaced
// snapshot per instance.
type Store struct {
	root string
	// now is the clock for event and readiness timestamps and for the
	// recovery lease sweep. A manager over the store adopts it as its own
	// lease-liveness clock (or replaces it), so injected timestamps and
	// staleness comparisons agree.
	now func() time.Time
	// onCommit, when non-nil, is invoked after every successfully committed
	// command snapshot, outside the flock. Set with SetCommitObserver before
	// the store starts serving commands.
	onCommit func(WorkflowID, Snapshot)

	// policies supplies the template lookups the reducer consults plus the
	// child-snapshot read command-time gate pinning consults. nil reads as
	// no template knowledge. The manager installs itself here.
	policies Policies
	// fsyncDir, when non-nil, replaces durablefile.FsyncDir for directory
	// durability. Tests install it to observe which entries a path records.
	// staging so a test can assert which directories a staging makes durable.
	// Production leaves it nil.
	fsyncDir func(string) error
	// snapshotWriteFault, when non-nil, is consulted for the snapshot replace
	// that follows a durable event append. It exists so a test can reproduce the
	// store's crash window (events durable, snapshot replace failed) for one
	// command. No filesystem-level injection can isolate that write, because
	// every command also repaves the snapshot from the log first. Production
	// leaves it nil. A test installs and clears it while controller goroutines
	// are applying commands, so faultMu guards every access.
	faultMu            sync.Mutex
	snapshotWriteFault func(WorkflowID, Snapshot) error
	// appendFault and discardFault, when non-nil, force one failure that file
	// permissions cannot express portably (a privileged test user ignores them).
	appendFault  func(path string) error
	discardFault func(EvidenceID) error
}

// readinessCommandKinds are the command kinds whose event batch can create a
// claimable activation (instantiate, reroute, and the transition siblings of
// complete/gate/child_outcome) or re-arm one (terminate retry re-arm,
// unblock). The store stamps their ReadyAt when unset.
var readinessCommandKinds = map[CommandKind]bool{
	CommandInstantiate:  true,
	CommandComplete:     true,
	CommandGate:         true,
	CommandChildOutcome: true,
	CommandTerminate:    true,
	CommandUnblock:      true,
	CommandReroute:      true,
}

func New(root string) *Store {
	return &Store{root: root, now: nowUTC}
}

// NewStoreWithClock returns a store that reads the supplied clock for event
// and readiness timestamps and for the recovery lease sweep. Callers must
// supply a UTC time.
func NewStoreWithClock(root string, now func() time.Time) *Store {
	return &Store{root: root, now: now}
}

// SetCommitObserver registers fn as the commit hook: after every successfully
// committed command snapshot (post atomic write, outside the flock), fn is
// invoked with the workflow ID and the committed snapshot. It must be called
// before the store starts serving commands. The observer must never fail or
// block the command: its errors and panics are swallowed.
func (s *Store) SetCommitObserver(fn func(WorkflowID, Snapshot)) {
	s.onCommit = fn
}

// SetSnapshotWriteFault installs a fault consulted for the snapshot replace that
// follows a durable event append; nil removes it. The fault sees the snapshot it
// would write. A test can then reproduce the store's crash window (events durable,
// snapshot replace failed) for a command it identifies by its content. No
// filesystem-level injection can isolate that write, because every command also
// repaves the snapshot from the log first. Tests in other packages use it and may
// install or clear it while controllers are running, so it is safe to call at any
// time; production leaves it unset.
func (s *Store) SetSnapshotWriteFault(fault func(WorkflowID, Snapshot) error) {
	s.faultMu.Lock()
	defer s.faultMu.Unlock()
	s.snapshotWriteFault = fault
}

// snapshotFault returns the installed snapshot-replace fault, or nil. The caller
// invokes it from inside the command's locked section, so a fault must not call
// back into store commands or acquire an instance lock.
func (s *Store) snapshotFault() func(WorkflowID, Snapshot) error {
	s.faultMu.Lock()
	defer s.faultMu.Unlock()
	return s.snapshotWriteFault
}

// SetAppendFault installs a fault consulted before an event batch is appended.
// Tests use it to force a non-durable append failure, which file permissions
// cannot express when the test user is privileged. Like the snapshot fault it may
// be installed or cleared while commands apply, so it is safe to call at any
// time. Production leaves it unset.
func (s *Store) SetAppendFault(fault func(path string) error) {
	s.faultMu.Lock()
	defer s.faultMu.Unlock()
	s.appendFault = fault
}

// appendFaultFunc returns the installed append fault, or nil.
func (s *Store) appendFaultFunc() func(path string) error {
	s.faultMu.Lock()
	defer s.faultMu.Unlock()
	return s.appendFault
}

// SetDiscardFault installs a fault consulted by DiscardEvidence. Tests use it to
// force a removal failure, which file permissions cannot express when the test
// user is privileged. It may be installed or cleared at any time. Production
// leaves it unset.
func (s *Store) SetDiscardFault(fault func(EvidenceID) error) {
	s.faultMu.Lock()
	defer s.faultMu.Unlock()
	s.discardFault = fault
}

// discardFaultFunc returns the installed discard fault, or nil.
func (s *Store) discardFaultFunc() func(EvidenceID) error {
	s.faultMu.Lock()
	defer s.faultMu.Unlock()
	return s.discardFault
}

// notifyCommit invokes the registered commit observer, isolating the command
// path from observer failures.
func (s *Store) notifyCommit(workflowID WorkflowID, snap Snapshot) {
	if s.onCommit == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			log.Printf("workflow %s: commit observer panicked: %v", workflowID, r)
		}
	}()
	s.onCommit(workflowID, snap)
}

func (s *Store) Root() string { return s.root }

func (s *Store) instanceDir(workflowID WorkflowID) string {
	return filepath.Join(s.root, "instances", string(workflowID))
}

func (s *Store) workflowPath(workflowID WorkflowID) string {
	return filepath.Join(s.instanceDir(workflowID), "workflow.json")
}

func (s *Store) eventsPath(workflowID WorkflowID) string {
	return filepath.Join(s.instanceDir(workflowID), "events.ndjson")
}

// Workflow state is private by default: prompts, outputs, and staged evidence
// can contain sensitive data, so the store creates its directories 0700 and its
// files 0600. Exported so the packages that stage data under the same root use
// the one policy instead of restating the numbers.
//
// A root created by an earlier version keeps its directories as they are, and
// everything already staged keeps the mode it was written with. Files this
// version replaces or chmods while rewriting them — snapshots, event logs, and
// projections — end up owner-only.
const (
	StateDirPerm  os.FileMode = 0o700
	StateFilePerm os.FileMode = 0o600
)

func (s *Store) lockPath(workflowID WorkflowID) string {
	return filepath.Join(s.instanceDir(workflowID), string(workflowID)+".lock")
}

// CreateRoot ensures the on-disk directory layout exists: the configured
// workflow root, the templates directory, and the instances directory.
func (s *Store) CreateRoot() error {
	for _, dir := range []string{s.root, filepath.Join(s.root, "templates"), filepath.Join(s.root, "instances")} {
		if err := os.MkdirAll(dir, StateDirPerm); err != nil {
			return err
		}
	}
	return nil
}

// ErrCommandDurable reports that a command's events are, or may be, durable in
// the event log even though the command returned an error. Either the snapshot
// replace failed after the whole batch was appended, or a failed append could not
// be rolled back. A replay recovers whatever landed, so callers must not undo a
// side effect those events already reference. Deleting staged evidence then
// leaves the log pointing at nothing.
var ErrCommandDurable = errors.New("command events are (or may be) durable")

// ApplyCommand applies one command under the instance's exclusive flock. On
// success the commit observer (if any) is invoked after the lock is released.
func (s *Store) ApplyCommand(workflowID WorkflowID, cmd Command) (Snapshot, error) {
	if err := s.ensureInstanceDir(workflowID); err != nil {
		return Snapshot{}, err
	}
	unlock, err := durablefile.Lock(s.lockPath(workflowID))
	if err != nil {
		return Snapshot{}, err
	}
	snap, err := func() (Snapshot, error) {
		defer unlock()
		return s.applyLocked(workflowID, cmd)
	}()
	if err != nil {
		return Snapshot{}, err
	}
	s.notifyCommit(workflowID, snap)
	return snap, nil
}

// ensureInstanceDir creates the instance directory and records its entry in the
// instances directory. Every command path passes through here, so an instance
// whose files are durable can always be found again after a crash, whether this
// call or an earlier one created the directory.
func (s *Store) ensureInstanceDir(workflowID WorkflowID) error {
	if err := os.MkdirAll(s.instanceDir(workflowID), StateDirPerm); err != nil {
		return err
	}
	fsync := s.fsyncDir
	if fsync == nil {
		fsync = durablefile.FsyncDir
	}
	return fsync(filepath.Dir(s.instanceDir(workflowID)))
}

func (s *Store) applyLocked(workflowID WorkflowID, cmd Command) (Snapshot, error) {
	snap, exists, err := s.loadSnapshot(workflowID)
	if err != nil {
		return Snapshot{}, err
	}
	if !exists {
		if cmd.Kind != CommandInstantiate {
			return Snapshot{}, fmt.Errorf("workflow %s does not exist", workflowID)
		}
	} else {
		replayed, n, truncated, err := replayEvents(snap, s.eventsPath(workflowID), s.policies)
		if err != nil {
			return Snapshot{}, err
		}
		if n > 0 || truncated > 0 {
			if err := s.writeSnapshot(workflowID, replayed); err != nil {
				return Snapshot{}, err
			}
			snap = replayed
		}
	}
	// Stamp the command with explicit timestamps: the reducer copies them
	// onto the events' records during replay and never reads a clock.
	now := s.now()
	if cmd.At.IsZero() {
		cmd.At = now
	}
	if readinessCommandKinds[cmd.Kind] && cmd.ReadyAt.IsZero() {
		cmd.ReadyAt = now
	}
	events, err := Apply(snap, cmd, s.policies)
	if err != nil {
		return Snapshot{}, err
	}
	// Reduce before appending: an event the reducer rejects must never reach
	// the log, where every later replay would fail on it.
	next := snap
	for _, e := range events {
		next, err = Reduce(next, e, s.policies)
		if err != nil {
			return Snapshot{}, err
		}
	}
	if err := s.appendEvents(workflowID, events); err != nil {
		return Snapshot{}, err
	}
	if err := s.writeSnapshotAfterAppend(workflowID, next); err != nil {
		return Snapshot{}, err
	}
	s.regenerateProjections(workflowID, next)
	return next, nil
}

// loadSnapshot reads the materialized snapshot, reporting whether it exists.
func (s *Store) loadSnapshot(workflowID WorkflowID) (Snapshot, bool, error) {
	data, err := os.ReadFile(s.workflowPath(workflowID))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Snapshot{}, false, nil
		}
		return Snapshot{}, false, err
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return Snapshot{}, false, err
	}
	return snap, true, nil
}

// appendEvents durably appends complete JSON events to the NDJSON log. A batch
// that fails part-way is rolled back to the log's pre-append length. An append
// error then means the batch is not durable, so its side effects may be undone.
// When even that rollback fails the error wraps ErrCommandDurable. The log may
// then hold a prefix of the batch, and a replay would recover it.
func (s *Store) appendEvents(workflowID WorkflowID, events []Event) error {
	path := s.eventsPath(workflowID)
	if fault := s.appendFaultFunc(); fault != nil {
		if faultErr := fault(path); faultErr != nil {
			return faultErr
		}
	}
	before, statErr := os.Stat(path)
	if statErr != nil && !errors.Is(statErr, fs.ErrNotExist) {
		return statErr
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, StateFilePerm)
	if err != nil {
		return err
	}
	defer f.Close()
	// A log an earlier version created keeps its mode through O_APPEND, while
	// the snapshot beside it is rewritten owner-only. Tighten both, so one
	// instance's files never end up with different access.
	if err := f.Chmod(StateFilePerm); err != nil {
		return err
	}
	var firstErr error
	for _, e := range events {
		data, err := json.Marshal(e)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if _, err = f.Write(append(data, '\n')); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
	}
	if err := f.Sync(); err != nil && firstErr == nil {
		firstErr = err
	}
	if firstErr == nil {
		return nil
	}
	// Roll the batch back so "append failed" means "not durable".
	var offset int64
	if before != nil {
		offset = before.Size()
	}
	if err := rollbackEventAppend(f, offset); err != nil {
		return err
	}
	return firstErr
}

// rollbackEventAppend truncates a failed batch out of the event log and syncs the
// rollback. When it cannot, the log may still hold a prefix of the batch. The
// error then reports ErrCommandDurable: a replay could recover that prefix, and
// its side effects must not be undone.
func rollbackEventAppend(f *os.File, offset int64) error {
	if err := f.Truncate(offset); err != nil {
		return fmt.Errorf("%w: truncate event log back to %d: %v", ErrCommandDurable, offset, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("%w: sync rolled-back event log: %v", ErrCommandDurable, err)
	}
	return nil
}

// writeSnapshotAfterAppend replaces the snapshot once the command's events are
// durable. A failure here leaves the log ahead of the materialized state, which a
// replay repairs. It is therefore reported as ErrCommandDurable: undoing the
// command's side effects now would strand the durable events.
func (s *Store) writeSnapshotAfterAppend(workflowID WorkflowID, snap Snapshot) error {
	if fault := s.snapshotFault(); fault != nil {
		if err := fault(workflowID, snap); err != nil {
			return fmt.Errorf("%w: write snapshot: %v", ErrCommandDurable, err)
		}
	}
	if err := s.writeSnapshot(workflowID, snap); err != nil {
		return fmt.Errorf("%w: write snapshot: %v", ErrCommandDurable, err)
	}
	return nil
}

// writeSnapshot atomically replaces the snapshot file and fsyncs the directory.
func (s *Store) writeSnapshot(workflowID WorkflowID, snap Snapshot) error {
	data, err := snap.MarshalJSON()
	if err != nil {
		return err
	}
	dir := s.instanceDir(workflowID)
	path := s.workflowPath(workflowID)
	if err := durablefile.WriteFileAtomic(path, durablefile.TempPattern(path), data, StateFilePerm); err != nil {
		return err
	}
	return durablefile.FsyncDir(dir)
}

// StoreTemplate atomically persists a versioned template under
// <root>/templates/<templateID>/<version>.json.
func (s *Store) StoreTemplate(templateID TemplateID, templateVersion TemplateVersion, template Template) error {
	// The existence and content checks must not race a concurrent create's
	// rename: without serialization, the rename silently overwrites a
	// different template under the same id+version. Registration is
	// serialized with a root-scoped file lock rather than a mutex so
	// supervisors sharing a workflow root — the default multi-process setup
	// — are mutually excluded too.
	if err := os.MkdirAll(s.root, StateDirPerm); err != nil {
		return err
	}
	unlock, err := durablefile.Lock(filepath.Join(s.root, "templates.lock"))
	if err != nil {
		return err
	}
	defer unlock()
	dir := filepath.Join(s.root, "templates", string(templateID))
	if err := os.MkdirAll(dir, StateDirPerm); err != nil {
		return err
	}
	path := filepath.Join(dir, string(templateVersion)+".json")
	data, err := template.MarshalJSON()
	if err != nil {
		return err
	}
	// Templates are immutable revisions: re-registering identical bytes is a
	// no-op, while different content under the same id+version is rejected so
	// running instances never load a silently replaced template.
	if existing, err := os.ReadFile(path); err == nil {
		identical := bytes.Equal(existing, data)
		if !identical {
			var current Template
			if jerr := json.Unmarshal(existing, &current); jerr != nil {
				return &TemplateCorruptError{TemplateID: templateID, TemplateVersion: templateVersion, Err: jerr}
			}
			if canonical, merr := current.MarshalJSON(); merr == nil {
				identical = bytes.Equal(canonical, data)
			}
		}
		if !identical {
			// Number formatting must not decide the conflict: the stored file
			// may have been produced by a writer that serialized metadata
			// numbers differently (json.Number keeps the literal, a float64
			// drops a trailing ".0"), while the content is semantically the
			// same. Compare both documents decoded generically, where numbers
			// become float64 and formatting differences vanish.
			var existingDoc, incomingDoc any
			if jerr := json.Unmarshal(existing, &existingDoc); jerr == nil {
				if jerr := json.Unmarshal(data, &incomingDoc); jerr == nil {
					identical = reflect.DeepEqual(existingDoc, incomingDoc)
				}
			}
		}
		if identical {
			// Registration is a no-op, so nothing rewrites the file: tighten a
			// template an earlier version stored readable.
			return os.Chmod(path, StateFilePerm)
		}
		return &TemplateConflictError{TemplateID: templateID, TemplateVersion: templateVersion}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := durablefile.WriteFileAtomic(path, durablefile.TempPattern(path), data, StateFilePerm); err != nil {
		return err
	}
	return durablefile.FsyncDir(dir)
}

// LoadTemplate reads a versioned template, returning a not-found error if it
// has not been stored.
func (s *Store) LoadTemplate(templateID TemplateID, templateVersion TemplateVersion) (Template, error) {
	path := filepath.Join(s.root, "templates", string(templateID), string(templateVersion)+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		return Template{}, err
	}
	var template Template
	if err := json.Unmarshal(data, &template); err != nil {
		return Template{}, err
	}
	return template, nil
}

// loadCurrent locks the instance and returns its snapshot advanced in memory
// to the end of the event log (the replay write is ignored; readers never
// mutate). It reports whether the instance exists.
func (s *Store) loadCurrent(workflowID WorkflowID) (Snapshot, bool, error) {
	// Reads must not materialize an empty instance directory for a workflow
	// that does not exist. Writes create the directory via ensureInstanceDir.
	if _, err := os.Stat(s.workflowPath(workflowID)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Snapshot{}, false, nil
		}
		return Snapshot{}, false, err
	}
	if err := os.MkdirAll(s.instanceDir(workflowID), StateDirPerm); err != nil {
		return Snapshot{}, false, err
	}
	unlock, err := durablefile.Lock(s.lockPath(workflowID))
	if err != nil {
		return Snapshot{}, false, err
	}
	defer unlock()
	snap, exists, err := s.loadSnapshot(workflowID)
	if err != nil || !exists {
		return snap, exists, err
	}
	replayed, _, _, err := replayEvents(snap, s.eventsPath(workflowID), s.policies)
	return replayed, true, err
}

// regenerateProjections regenerates derived projections for an instance after a
// snapshot change. Projections are derived artifacts, never authoritative: a
// failure to write them must not fail an already-committed state transition,
// so the error is logged (non-fatal) and swallowed.
func (s *Store) regenerateProjections(workflowID WorkflowID, snap Snapshot) {
	if err := WriteProjections(s.instanceDir(workflowID), snap); err != nil {
		log.Printf("workflow %s: projection: %v", workflowID, err)
	}
}
