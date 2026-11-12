//go:build linux

package cli

import (
	"os"
	"runtime"
	"syscall"
)

// raiseSignalSynchronously sends sig to the calling thread, so the kernel
// delivers it before the call returns. A process-directed kill may be delivered
// to any thread of the process after it returns.
func raiseSignalSynchronously(sig syscall.Signal) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	return syscall.Tgkill(os.Getpid(), syscall.Gettid(), sig)
}
