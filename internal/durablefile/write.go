package durablefile

import (
	"fmt"
	"os"
	"path/filepath"
)

// commitFunc moves a synced temp file into place at path.
type commitFunc func(tmpPath, path string) error

// WriteFileAtomic replaces path with data through a uniquely named temp file in
// path's directory, so a reader never observes a partial file and concurrent
// writers cannot collide on one temp path. The temp file is created with perm,
// synced before the rename, and removed on every failure path; the caller owns
// fsyncing the directory when the rename itself must be durable.
func WriteFileAtomic(path, pattern string, data []byte, perm os.FileMode) error {
	return writeAtomic(path, pattern, data, perm, "rename", os.Rename)
}

// WriteFileAtomicExclusive is WriteFileAtomic with an exclusive commit: the temp
// file is hard linked into place instead of renamed, so an existing path is left
// untouched and the write fails with an error matching fs.ErrExist. Removing the
// temp name after a successful link is the helper's job, so callers see the same
// postconditions either way.
func WriteFileAtomicExclusive(path, pattern string, data []byte, perm os.FileMode) error {
	return writeAtomic(path, pattern, data, perm, "link", os.Link)
}

func writeAtomic(path, pattern string, data []byte, perm os.FileMode, commitName string, commit commitFunc) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), pattern)
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmpPath := tmp.Name()
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("chmod temp: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("write temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("sync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close temp: %w", err)
	}
	if err := commit(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("%s: %w", commitName, err)
	}
	// A link leaves the temp name behind; a rename already consumed it.
	_ = os.Remove(tmpPath)
	return nil
}
