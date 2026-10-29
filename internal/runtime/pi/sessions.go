package pi

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/sdougbrown/avenor/internal/appdir"
)

const (
	// SessionsEnv opts a host in to persisted pi sessions. When set to a
	// truthy value, spawned pi children launch without --no-session and
	// store their session file under SessionDirEnv or the avenor state
	// home, which makes the run's transcript survive its process and
	// cross-process resume possible. The default stays ephemeral: session
	// files cost disk on every fan-out, and the session store is a
	// shared-state suspect in the same-model startup race, so persistence
	// is never silently on.
	SessionsEnv = "AVENOR_PI_SESSIONS"
	// SessionDirEnv overrides the directory persisted pi sessions are
	// stored in and looked up from.
	SessionDirEnv = "AVENOR_PI_SESSION_DIR"
)

// SessionsPersistEnabled reports whether the environment opts in to
// persisted pi sessions.
func SessionsPersistEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(SessionsEnv))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// ResolvePersistSessions applies the per-run choice over the environment:
// nil inherits SessionsPersistEnabled, true and false force the choice for
// the run.
func ResolvePersistSessions(opt *bool) bool {
	if opt != nil {
		return *opt
	}
	return SessionsPersistEnabled()
}

// PersistSessionsDir returns the directory persisted pi sessions live in,
// creating it (mode 0700) when missing. An explicit SessionDirEnv value
// wins; the default lives under the avenor state home, outside the artifact
// tree whose retention pruning would delete a transcript someone wants
// later.
func PersistSessionsDir() (string, error) {
	dir := strings.TrimSpace(os.Getenv(SessionDirEnv))
	if dir == "" {
		dir = filepath.Join(appdir.StateHome(), "pi-sessions")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create pi session dir %s: %w", dir, err)
	}
	return dir, nil
}

// SessionFileExists reports whether dir holds a persisted session file for
// sessionID. pi names session files <timestamp>_<session id>.jsonl, so the
// filename check matches the header-id lookup pi itself performs, without
// parsing the file. A directory read failure is an error, not a negative:
// the caller must not tell the user the session is missing when the store
// is unreadable.
func SessionFileExists(dir, sessionID string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, fmt.Errorf("read pi session dir %s: %w", dir, err)
	}
	suffix := "_" + sessionID + ".jsonl"
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), suffix) {
			return true, nil
		}
	}
	return false, nil
}
