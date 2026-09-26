//go:build unix && !linux && !darwin

package workflowcontroller

import (
	"os"
	"syscall"
)

// statTimestamps returns the file's modification and change times in
// nanoseconds since the Unix epoch. This fallback covers unix variants whose
// Stat_t exposes no portable change-time field: the modification time comes
// from the FileInfo and the change time is unknown (zero), so replacement
// detection there relies on device, inode, size, and mtime.
func statTimestamps(fi os.FileInfo, _ *syscall.Stat_t) (mtimeNS, ctimeNS int64) {
	m := fi.ModTime()
	return m.Unix()*1_000_000_000 + int64(m.Nanosecond()), 0
}
