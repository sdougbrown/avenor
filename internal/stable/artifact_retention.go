package stable

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// artifactRetention is how long a supervisor-created artifact directory tree
// survives. A supervisor prunes run directories under $TMPDIR/avenor-stable
// whose newest entry is older than this, which bounds what unattended hosts
// and test runs accumulate without cutting short a read of a finished run's
// sentinel or event log.
const artifactRetention = 24 * time.Hour

// artifactRoot is the directory holding one subdirectory per supervisor run.
// Spawn writes its default on_event and sentinel_file paths under it.
func artifactRoot() string {
	return filepath.Join(os.TempDir(), "avenor-stable")
}

// pruneArtifactDirs removes every run directory under the artifact root whose
// newest entry is older than artifactRetention. A run directory newer than the
// window is kept, so a live run's artifacts stay readable. Pruning is
// best-effort: a failure is reported to diagnostics and never fails startup.
func (s *Supervisor) pruneArtifactDirs() {
	root := artifactRoot()
	entries, err := os.ReadDir(root)
	if err != nil {
		if !os.IsNotExist(err) {
			fmt.Fprintf(s.stderrWriter(), "avenor stable: prune artifact root %s: %v\n", root, err)
		}
		return
	}
	for _, entry := range entries {
		// A symlink reports as a link, not as the directory it names, so a run
		// directory that is itself a symlink is left alone.
		if !entry.IsDir() {
			continue
		}
		dir := filepath.Join(root, entry.Name())
		newest, err := newestArtifactTime(dir)
		if err != nil {
			fmt.Fprintf(s.stderrWriter(), "avenor stable: inspect artifact dir %s: %v\n", dir, err)
			continue
		}
		if time.Since(newest) < artifactRetention {
			continue
		}
		if err := os.RemoveAll(dir); err != nil {
			fmt.Fprintf(s.stderrWriter(), "avenor stable: prune artifact dir %s: %v\n", dir, err)
		}
	}
}

// newestArtifactTime reports the most recent modification time in dir.
// filepath.WalkDir lstats each entry and never descends a symlink, and a
// symlink is not counted: it records when a link was created, not when a run
// wrote an artifact. Pruning therefore stays inside the artifact tree.
func newestArtifactTime(dir string) (time.Time, error) {
	var newest time.Time
	err := filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.ModTime().After(newest) {
			newest = info.ModTime()
		}
		return nil
	})
	return newest, err
}
