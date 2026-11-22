//go:build !linux

package supervisor

import "syscall"

// shutdownOnParentExit is unavailable on this platform: it has no parent-death
// signal, so a launcher that is killed outright cannot have its supervisor told
// to shut down. The graceful path — the launcher's own shutdown call — is
// unaffected.
func shutdownOnParentExit(_ *syscall.SysProcAttr) {}
