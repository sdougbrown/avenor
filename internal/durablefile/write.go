package durablefile

import (
	"fmt"
	"os"
	"path/filepath"
)

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
// is removed after the link; a removal that fails is reported, because it leaves
// a second copy of the payload on disk.
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
	if err := os.Remove(tmpPath); err != nil {
		return fmt.Errorf("remove temp: %w", err)
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
