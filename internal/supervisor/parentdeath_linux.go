//go:build linux

package supervisor

import "syscall"

// shutdownOnParentExit asks the kernel to signal the supervisor when the process
// that started it dies, including when that process is killed outright. SIGINT is
// the signal a supervisor shuts down gracefully on, so the supervisor still
// reaps its child runtimes and removes its socket.
func shutdownOnParentExit(attr *syscall.SysProcAttr) {
	attr.Pdeathsig = syscall.SIGINT
}
