package stable

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const (
	// artifactRetention is how long a supervisor-created run directory survives
	// without a write. It bounds what unattended hosts and test runs accumulate
	// without cutting short a read of a finished run's sentinel or event log.
	artifactRetention = 24 * time.Hour
	// artifactPruneInterval is how often a running supervisor refreshes its own
	// run directory and prunes the run directories of supervisors that are gone.
	artifactPruneInterval = time.Hour
	// artifactRunPrefix names the only entries the retention policy owns: one
	// directory per supervisor run. Everything else under the root is left alone.
	artifactRunPrefix = "run_"
)

// artifactRoot is the directory holding one subdirectory per supervisor run.
// Spawn writes its default on_event and sentinel_file paths under it. The euid
// in the name keeps two users of a shared temporary directory in separate
// trees, and the directory itself is created 0700.
func artifactRoot() string {
	return filepath.Join(os.TempDir(), fmt.Sprintf("avenor-stable-%d", os.Geteuid()))
}

// artifactRunDir is the directory holding one supervisor run's artifacts.
func artifactRunDir(runID string) string {
	return filepath.Join(artifactRoot(), artifactRunPrefix+runID)
}

// ensureArtifactRoot creates the artifact root with mode 0700 when it is
// missing and returns it once verifyArtifactRoot accepts it. A caller that
// needs the default artifact paths fails with an error naming the root instead
// of writing into a tree another user controls.
func ensureArtifactRoot() (string, error) {
	root := artifactRoot()
	if err := os.Mkdir(root, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return "", fmt.Errorf("create artifact root %s: %w", root, err)
	}
	if err := verifyArtifactRoot(root); err != nil {
		return "", err
	}
	return root, nil
}

// verifyArtifactRoot refuses a root that is not a real directory owned
// privately by this user. On a shared temporary directory another user can
// otherwise pre-create the root as a symlink, or leave it group-writable and
// add run directories of their own.
func verifyArtifactRoot(root string) error {
	info, err := os.Lstat(root)
	if err != nil {
		return fmt.Errorf("artifact root %s: %w", root, err)
	}
	// Lstat reports a symlink as a link, so a link to a directory is refused
	// like any other non-directory.
	if !info.IsDir() {
		return fmt.Errorf("artifact root %s is not a directory", root)
	}
	if st, ok := info.Sys().(*syscall.Stat_t); !ok || st.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("artifact root %s is not owned by uid %d", root, os.Geteuid())
	}
	if info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("artifact root %s is group or other writable", root)
	}
	return nil
}

// createArtifactRunDir creates one runtime's directory under the verified
// artifact root and returns it.
func createArtifactRunDir(runID, rtID string) (string, error) {
	root, err := ensureArtifactRoot()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(root, artifactRunPrefix+runID, rtID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create artifact dir %s: %w", dir, err)
	}
	return dir, nil
}

// runArtifactRetention keeps this supervisor's run directory fresh and prunes
// stale run directories until stop is closed.
func (s *Supervisor) runArtifactRetention(stop <-chan struct{}) {
	s.touchArtifactRunDir()
	s.pruneArtifactDirs()
	ticker := time.NewTicker(artifactPruneInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			s.touchArtifactRunDir()
			s.pruneArtifactDirs()
		}
	}
}

// touchArtifactRunDir records a write on this supervisor's own run directory so
// pruning sees the run as live. The directory exists only after a spawn that
// needed a default artifact path, and Chtimes does not create it.
func (s *Supervisor) touchArtifactRunDir() {
	root := artifactRoot()
	if err := verifyArtifactRoot(root); err != nil {
		return // the prune that follows reports the refusal once
	}
	dir := filepath.Join(root, artifactRunPrefix+s.runID)
	// Chtimes follows symlinks, so the directory is confirmed to be one first.
	if info, err := os.Lstat(dir); err != nil || !info.IsDir() {
		return
	}
	now := time.Now()
	if err := os.Chtimes(dir, now, now); err != nil {
		s.reportArtifactError(err, "refresh artifact dir %s: %v", dir, err)
	}
}

// pruneArtifactDirs removes every run directory under the artifact root whose
// newest entry is older than artifactRetention. A directory a live supervisor
// keeps refreshing stays, and a root that is not this user's private directory
// is refused before it is read. Pruning is best-effort: a failure is reported
// to diagnostics and never fails the supervisor.
func (s *Supervisor) pruneArtifactDirs() {
	root := artifactRoot()
	if err := verifyArtifactRoot(root); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return
		}
		s.artifactRootWarn.Do(func() {
			s.writeArtifactDiagnostic("artifact retention disabled: %v", err)
		})
		return
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		s.reportArtifactError(err, "read artifact root %s: %v", root, err)
		return
	}
	for _, entry := range entries {
		// A symlink reports as a link, not as the directory it names, so a run
		// directory that is itself a symlink is never a candidate.
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), artifactRunPrefix) {
			continue
		}
		s.pruneArtifactRun(filepath.Join(root, entry.Name()))
	}
}

// pruneArtifactRun removes one run directory once it is stale both when it is
// inspected and again immediately before removal, so a run that wrote in
// between is kept. The second read narrows the window between deciding and
// removing; it does not close it.
func (s *Supervisor) pruneArtifactRun(dir string) {
	if !artifactRunStale(dir) {
		return
	}
	if !artifactRunStale(dir) {
		return
	}
	if err := os.RemoveAll(dir); err != nil {
		s.reportArtifactError(err, "prune artifact dir %s: %v", dir, err)
	}
}

// artifactRunStale reports whether dir's newest write is older than the
// retention window. A directory that cannot be read is not a candidate.
func artifactRunStale(dir string) bool {
	newest, ok := artifactNewestWrite(dir)
	return ok && time.Since(newest) >= artifactRetention
}

// artifactNewestWrite reports the newest modification time under dir, clamped
// to now so a future timestamp cannot pin a directory. A walk that fails falls
// back to dir's own modification time, so an unreadable subtree does not keep
// its run directory forever.
func artifactNewestWrite(dir string) (time.Time, bool) {
	now := time.Now()
	newest, err := newestArtifactTime(dir)
	if err != nil {
		info, statErr := os.Lstat(dir)
		if statErr != nil {
			return time.Time{}, false
		}
		newest = info.ModTime()
	}
	if newest.After(now) {
		newest = now
	}
	return newest, true
}

// newestArtifactTime reports the most recent modification time in dir.
// filepath.WalkDir lstats each entry and never descends a symlink, and a
// symlink is not counted: it records when a link was created, not when a run
// wrote an artifact.
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

// writeArtifactDiagnostic reports one retention diagnostic.
func (s *Supervisor) writeArtifactDiagnostic(format string, args ...any) {
	fmt.Fprintf(s.stderrWriter(), "avenor stable: "+format+"\n", args...)
}

// reportArtifactError reports a retention failure, collapsing repeats of a
// permission denial into its first occurrence: the prune runs hourly, and the
// same denied directory fails identically on every pass.
func (s *Supervisor) reportArtifactError(err error, format string, args ...any) {
	if !errors.Is(err, fs.ErrPermission) {
		s.writeArtifactDiagnostic(format, args...)
		return
	}
	s.artifactPermissionWarn.Do(func() { s.writeArtifactDiagnostic(format, args...) })
}
