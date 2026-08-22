//go:build unix

package workflow

import (
	"os"
	"syscall"
)

// lockFile acquires an exclusive advisory flock on path, creating it if needed.
// Returns an unlock func that releases the lock and closes the file.
func lockFile(path string) (func() error, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, err
	}
	return func() error { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); return f.Close() }, nil
}
