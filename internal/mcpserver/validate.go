package mcpserver

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// supervisorSocketNameRe matches the socket filename convention shared by the
// TypeScript core and startSupervisor: avenor-mcp-<id>.sock.
var supervisorSocketNameRe = regexp.MustCompile(`^avenor-mcp-[^/\\]+\.sock$`)

// supervisorSocketsRoot returns the canonical sockets root. A var so tests
// can point validation at a scratch directory.
var supervisorSocketsRoot = func() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(filepath.Join(home, ".avenor", "sockets"))
}

// validateSupervisorSocketPath enforces the same containment rule as the
// TypeScript core (packages/core/src/tools/validate.ts): a tool-supplied
// supervisor_id must resolve directly inside the private sockets root with
// the avenor-mcp-*.sock naming. Without this, an MCP client could point
// supervisor_id at an arbitrary local Unix socket (e.g. /var/run/docker.sock)
// and have the server dial it and issue control RPCs. Custom supervisor
// locations stay reachable by configuring them via --supervisor-socket, which
// is exempt as the server's own default. The socket itself is never
// canonicalized (Unix sockets reject realpath on macOS); resolving the
// containing directory is enough to block traversal and symlink escapes.
func validateSupervisorSocketPath(supervisorID string) error {
	if strings.ContainsRune(supervisorID, 0) {
		return fmt.Errorf("invalid supervisorId: %s", supervisorID)
	}
	root, err := supervisorSocketsRoot()
	if err != nil {
		return fmt.Errorf("invalid supervisorId: %s", supervisorID)
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(supervisorID))
	if err != nil {
		return fmt.Errorf("invalid supervisorId: %s", supervisorID)
	}
	if parent != root || !supervisorSocketNameRe.MatchString(filepath.Base(supervisorID)) {
		return fmt.Errorf("invalid supervisorId: %s", supervisorID)
	}
	return nil
}
