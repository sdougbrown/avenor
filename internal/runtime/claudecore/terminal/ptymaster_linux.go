//go:build linux

package terminal

import (
	"os"

	"golang.org/x/sys/unix"
)

// pollableMaster returns the pty master as a nonblocking file in the runtime
// poller, and always closes the original. creack/pty leaves the master in
// blocking mode, and on Linux a master write blocked on a full input queue
// outlives both the child's exit and a Close from another goroutine. In the
// poller, Close wakes the blocked write with an error.
func pollableMaster(ptmx *os.File) (*os.File, error) {
	defer ptmx.Close()
	fd, err := unix.FcntlInt(ptmx.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	if err := unix.SetNonblock(fd, true); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	// NewFile registers a nonblocking descriptor with the poller.
	return os.NewFile(uintptr(fd), ptmx.Name()), nil
}
