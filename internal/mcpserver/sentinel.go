package mcpserver

import (
	"fmt"
	"os"
	"path/filepath"
)

// runArtifactPaths returns the sentinel and event-log paths for a run. It is
// the one place the avenor-run-<runID>.done/.log naming convention lives: the
// sentinel basename is parsed back by runstate.RunIDFromSentinel and matched by
// lookupRun, so spawn, follow-up and resume must mint identical paths.
func runArtifactPaths(runID string) (sentinelFile, eventLog string) {
	dir := os.TempDir()
	return filepath.Join(dir, fmt.Sprintf("avenor-run-%s.done", runID)),
		filepath.Join(dir, fmt.Sprintf("avenor-run-%s.log", runID))
}
