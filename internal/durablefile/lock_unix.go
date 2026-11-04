//go:build unix

// Package durablefile provides durability primitives shared by the workflow
// and workflow-controller stores.
package durablefile

import (
	"errors"
	"os"
	"syscall"
)

// Lock acquires an exclusive advisory flock on path, creating it if needed.
// Returns an unlock func that releases the lock and closes the file.
func Lock(path string) (func() error, error) {
	unlock, _, err := lock(path, syscall.LOCK_EX)
	return unlock, err
}

// TryLock acquires an exclusive advisory lock without waiting, reporting held when another process holds it.
func TryLock(path string) (unlock func() error, held bool, err error) {
	return lock(path, syscall.LOCK_EX|syscall.LOCK_NB)
}

// lock opens the lock file and applies the requested flock operation.
func lock(path string, operation int) (func() error, bool, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, false, err
	}
	if err := syscall.Flock(int(f.Fd()), operation); err != nil {
		_ = f.Close()
		if operation&syscall.LOCK_NB != 0 && errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, true, nil
		}
		return nil, false, err
	}
	return func() error { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); return f.Close() }, false, nil
}
