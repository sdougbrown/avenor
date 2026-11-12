//go:build !linux

package cli

import (
	"os"
	"syscall"
)

// raiseSignalSynchronously sends sig to this process. Only Linux offers a
// thread-directed send, so delivery here can land after the call returns.
func raiseSignalSynchronously(sig syscall.Signal) error {
	return syscall.Kill(os.Getpid(), sig)
}
