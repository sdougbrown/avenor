//go:build darwin || freebsd

package terminal

import "os"

// pollableMaster returns the master unchanged. On Darwin a blocked master
// write fails once the child's side of the pty closes.
func pollableMaster(ptmx *os.File) (*os.File, error) { return ptmx, nil }
