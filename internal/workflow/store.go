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

func (s *Store) lockPath(workflowID WorkflowID) string {
	return filepath.Join(s.instanceDir(workflowID), string(workflowID)+".lock")
}

// CreateRoot ensures the on-disk directory layout exists: the configured
// workflow root, the templates directory, and the instances directory.
func (s *Store) CreateRoot() error {
	for _, dir := range []string{s.root, filepath.Join(s.root, "templates"), filepath.Join(s.root, "instances")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	return nil
}

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

func (s *Store) ensureInstanceDir(workflowID WorkflowID) error {
	return os.MkdirAll(s.instanceDir(workflowID), 0o755)
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
	if err := s.writeSnapshot(workflowID, next); err != nil {
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

// appendEvents durably appends complete JSON events to the NDJSON log.
func (s *Store) appendEvents(workflowID WorkflowID, events []Event) error {
	f, err := os.OpenFile(s.eventsPath(workflowID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
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
	return firstErr
}

// writeSnapshot atomically replaces the snapshot file and fsyncs the directory.
func (s *Store) writeSnapshot(workflowID WorkflowID, snap Snapshot) error {
	data, err := snap.MarshalJSON()
	if err != nil {
		return err
	}
	dir := s.instanceDir(workflowID)
	path := s.workflowPath(workflowID)
	if err := durablefile.WriteFileAtomic(path, durablefile.TempPattern(path), data, 0o644); err != nil {
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
	if err := os.MkdirAll(s.root, 0o755); err != nil {
		return err
	}
	unlock, err := durablefile.Lock(filepath.Join(s.root, "templates.lock"))
	if err != nil {
		return err
	}
	defer unlock()
	dir := filepath.Join(s.root, "templates", string(templateID))
	if err := os.MkdirAll(dir, 0o755); err != nil {
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
			return nil
		}
		return &TemplateConflictError{TemplateID: templateID, TemplateVersion: templateVersion}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := durablefile.WriteFileAtomic(path, durablefile.TempPattern(path), data, 0o644); err != nil {
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
	if err := os.MkdirAll(s.instanceDir(workflowID), 0o755); err != nil {
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
