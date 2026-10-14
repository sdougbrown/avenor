// Package launcher builds the exec.Cmd values that start backend processes.
//
// A launch site hands the backend binary and its argv to Command together with
// an optional wrapper argv. Avenor never interprets the wrapper: when present,
// its argv is placed in front of the backend's command line, so the wrapper
// process becomes the direct child and owns the backend process tree. When no
// wrapper is set, the built command is exactly what the site would have built
// itself.
package launcher

import (
	"context"
	"os/exec"
)

// Argv returns the full argv for a backend launch: the wrapper prefix when one
// is configured, then the backend binary and its arguments. An empty wrapper
// yields bin followed by args, unchanged.
func Argv(bin string, args []string, wrapper []string) []string {
	argv := make([]string, 0, len(wrapper)+1+len(args))
	argv = append(argv, wrapper...)
	argv = append(argv, bin)
	return append(argv, args...)
}

// Command builds the exec.Cmd for a backend process. With a wrapper, the
// wrapper's first element is the executable and Args carries the full prefixed
// argv; without one this is exactly exec.CommandContext(ctx, bin, args...).
// Stdio, environment, working directory, and wait/signal handling remain the
// caller's responsibility either way.
func Command(ctx context.Context, wrapper []string, bin string, args ...string) *exec.Cmd {
	argv := Argv(bin, args, wrapper)
	return exec.CommandContext(ctx, argv[0], argv[1:]...)
}
