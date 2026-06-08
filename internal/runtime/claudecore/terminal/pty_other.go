//go:build !(linux || darwin || freebsd)
// +build !linux,!darwin,!freebsd

package terminal

import (
	"context"
	"fmt"
)

// PTYLauncher on unsupported platforms returns an error from Start so the
// package still compiles. The PTY adapter is only implemented for unix-like
// systems where github.com/creack/pty and github.com/chubin/vt10x work.
type PTYLauncher struct {
	Env []string
}

func (PTYLauncher) Start(_ context.Context, _ StartOptions) (Session, error) {
	return nil, fmt.Errorf("PTY launcher is not supported on this platform")
}
