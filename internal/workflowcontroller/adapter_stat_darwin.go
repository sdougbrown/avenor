//go:build darwin

package workflowcontroller

import (
	"os"
	"syscall"
)

// statTimestamps returns the file's modification and change times in
// nanoseconds since the Unix epoch. Darwin's Stat_t carries them as
// Mtimespec and Ctimespec.
func statTimestamps(fi os.FileInfo, st *syscall.Stat_t) (mtimeNS, ctimeNS int64) {
	return timespecNS(st.Mtimespec), timespecNS(st.Ctimespec)
}

// timespecNS flattens a syscall.Timespec into nanoseconds since the epoch.
func timespecNS(ts syscall.Timespec) int64 {
	return ts.Sec*1_000_000_000 + int64(ts.Nsec)
}
