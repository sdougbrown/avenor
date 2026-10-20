package durablefile

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ErrTempNotRemoved reports a commit that published its payload at the target
// path but could not remove the staged temp file it published from. The target
// holds the complete payload, so the write itself succeeded; the leftover temp
// file is a second copy of the same bytes that the caller may want to remove.
var ErrTempNotRemoved = errors.New("temp file not removed after commit")

// TempNotRemovedError is the commit error for a published payload whose temp
// file survived the commit. It matches ErrTempNotRemoved and carries the path
// of the leftover temp file, so a caller reporting the warning can name it.
type TempNotRemovedError struct {
	Path string
	Err  error
}

func (e *TempNotRemovedError) Error() string {
	return fmt.Sprintf("remove temp %s: %v", e.Path, e.Err)
}

// Unwrap matches both the sentinel and the underlying removal error.
func (e *TempNotRemovedError) Unwrap() []error {
	return []error{ErrTempNotRemoved, e.Err}
}

// removeTemp removes a temp file that has served its purpose. Tests replace it
// to exercise a commit whose payload is published but whose temp survives.
var removeTemp = os.Remove

// TempPattern returns the CreateTemp pattern for a staged replacement of path:
// the target's base name plus ".tmp-*", so the temp file sorts beside its target
// and stays recognisable in a directory listing.
func TempPattern(path string) string {
	return filepath.Base(path) + ".tmp-*"
}

// commitFunc moves the synced temp file into place at path. It owns tmpPath once
// called: a move that leaves the temp name behind removes it, and a temp name it
// could not remove after a successful move is reported.
type commitFunc func(tmpPath, path string) error

// WriteFileAtomic replaces path with data through a uniquely named temp file in
// path's directory, so a reader never observes a partial file and concurrent
// writers cannot collide on one temp path. The temp file is created with perm,
// synced before it is renamed into place, and removed when the write fails; the
// caller owns fsyncing the directory when the rename itself must be durable.
func WriteFileAtomic(path, pattern string, data []byte, perm os.FileMode) error {
	return writeAtomic(path, pattern, data, perm, "rename", renameCommit)
}

// WriteFileAtomicExclusive is WriteFileAtomic with an exclusive commit: the temp
// file is hard linked into place instead of renamed, so an existing path is left
// untouched and the write fails with an error matching fs.ErrExist. The temp name
// is removed after the link; a removal that fails returns an error matching
// ErrTempNotRemoved, since the payload is already published and only the temp
// copy survives.
func WriteFileAtomicExclusive(path, pattern string, data []byte, perm os.FileMode) error {
	return writeAtomic(path, pattern, data, perm, "link", linkCommit)
}

// renameCommit replaces path, consuming tmpPath in the process.
func renameCommit(tmpPath, path string) error {
	if err := os.Rename(tmpPath, path); err != nil {
		discardTemp(tmpPath)
		return err
	}
	return nil
}

// linkCommit publishes the temp file at path without replacing an existing
// entry, then removes the temp name it linked from.
func linkCommit(tmpPath, path string) error {
	if err := os.Link(tmpPath, path); err != nil {
		discardTemp(tmpPath)
		return err
	}
	if err := removeTemp(tmpPath); err != nil {
		return &TempNotRemovedError{Path: tmpPath, Err: err}
	}
	return nil
}

// discardTemp removes a staged temp file whose contents were never published.
func discardTemp(tmpPath string) {
	_ = os.Remove(tmpPath)
}

func writeAtomic(path, pattern string, data []byte, perm os.FileMode, commitName string, commit commitFunc) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), pattern)
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmpPath := tmp.Name()
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		discardTemp(tmpPath)
		return fmt.Errorf("chmod temp: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		discardTemp(tmpPath)
		return fmt.Errorf("write temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		discardTemp(tmpPath)
		return fmt.Errorf("sync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		discardTemp(tmpPath)
		return fmt.Errorf("close temp: %w", err)
	}
	if err := commit(tmpPath, path); err != nil {
		return fmt.Errorf("%s: %w", commitName, err)
	}
	return nil
}
