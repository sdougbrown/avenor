package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
)

// Store applies commands to workflow instances under a single POSIX flock and
// durably persists a locked NDJSON event log plus an atomically replaced
// snapshot per instance.
type Store struct {
	root string
}

func New(root string) *Store {
	return &Store{root: root}
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

// ApplyCommand applies one command under the instance's exclusive flock.
func (s *Store) ApplyCommand(workflowID WorkflowID, cmd Command) (Snapshot, error) {
	if err := s.ensureInstanceDir(workflowID); err != nil {
		return Snapshot{}, err
	}
	unlock, err := lockFile(s.lockPath(workflowID))
	if err != nil {
		return Snapshot{}, err
	}
	defer unlock()
	return s.applyLocked(workflowID, cmd)
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
		replayed, n, truncated, err := replayEvents(snap, s.eventsPath(workflowID))
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
	events, err := Apply(snap, cmd)
	if err != nil {
		return Snapshot{}, err
	}
	if err := s.appendEvents(workflowID, events); err != nil {
		return Snapshot{}, err
	}
	next := snap
	for _, e := range events {
		next, err = Reduce(next, e)
		if err != nil {
			return Snapshot{}, err
		}
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
	tmp, err := os.CreateTemp(s.instanceDir(workflowID), "workflow.json.tmp*")
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
	if err := os.Rename(tmpName, s.workflowPath(workflowID)); err != nil {
		return err
	}
	return fsyncDir(s.instanceDir(workflowID))
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

// StoreTemplate atomically persists a versioned template under
// <root>/templates/<templateID>/<version>.json.
func (s *Store) StoreTemplate(templateID TemplateID, templateVersion TemplateVersion, template Template) error {
	dir := filepath.Join(s.root, "templates", string(templateID))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, string(templateVersion)+".json")
	data, err := template.MarshalJSON()
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, string(templateVersion)+".json.tmp*")
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
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	return fsyncDir(dir)
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
	if err := os.MkdirAll(s.instanceDir(workflowID), 0o755); err != nil {
		return Snapshot{}, false, err
	}
	unlock, err := lockFile(s.lockPath(workflowID))
	if err != nil {
		return Snapshot{}, false, err
	}
	defer unlock()
	snap, exists, err := s.loadSnapshot(workflowID)
	if err != nil || !exists {
		return snap, exists, err
	}
	replayed, _, _, err := replayEvents(snap, s.eventsPath(workflowID))
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
