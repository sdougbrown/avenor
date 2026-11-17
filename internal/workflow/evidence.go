package workflow

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/sdougbrown/avenor/internal/durablefile"
)

// StagedEvidence describes one artifact copied into an immutable evidence
// directory. OriginalPath is the caller's original source path; StoredPath is
// the path of the stored copy RELATIVE TO the instance directory
// (evidence/<id>/<storedName>).
type StagedEvidence struct {
	EvidenceID   EvidenceID
	OriginalPath string
	StoredPath   string
	Size         int64
	SHA256       string
}

// evidenceDir returns the immutable evidence directory for one evidence ID
// under an instance directory. It lives under the instance dir inside its own
// evidence namespace, so it never collides with workflow.json/events.ndjson/
// nodes.
func evidenceDir(instanceDir string, id EvidenceID) string {
	return filepath.Join(instanceDir, "evidence", string(id))
}

// validateStoredPath rejects a requested stored artifact path unless it is a
// safe relative path: non-empty, not absolute, no backslash or NUL byte, and
// every "/"-separated component is a safe component (no "", ".", "..", path
// separators, or NUL).
func validateStoredPath(name string) error {
	if name == "" {
		return errors.New("evidence: stored path cannot be empty")
	}
	if filepath.IsAbs(name) {
		return fmt.Errorf("evidence: stored path must be relative, got: %q", name)
	}
	if strings.ContainsAny(name, "\\\x00") {
		return fmt.Errorf("evidence: stored path contains invalid character, got: %q", name)
	}
	for _, part := range strings.Split(name, "/") {
		if !safeComponent(part) {
			return fmt.Errorf("evidence: component %q is unsafe in stored path %q", part, name)
		}
	}
	return nil
}

// sha256File returns the lowercase hex SHA-256 digest of a file's contents.
func sha256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// copyStream copies src to destPath with O_CREATE|O_EXCL so it never
// overwrites an existing file, and fsyncs the result.
func copyStream(src io.Reader, destPath string) error {
	out, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, StateFilePerm)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, src); err != nil {
		// on failure, remove the partial file this call created
		out.Close()
		os.Remove(destPath)
		return err
	}
	if err := out.Sync(); err != nil {
		// on failure, remove the partial file this call created
		out.Close()
		os.Remove(destPath)
		return err
	}
	return nil
}

// stageInto stages one source file as evidence.
func stageInto(srcPath, destDir, storedName string, required bool, expectedSHA string, syncRoot string, fsync func(string) error) (int64, string, error) {
	info, err := os.Stat(srcPath)
	if err != nil {
		return 0, "", err
	}
	if !info.Mode().IsRegular() {
		return 0, "", errors.New("evidence: source is not a regular file")
	}
	if required && info.Size() == 0 {
		return 0, "", errors.New("evidence: required file is empty")
	}

	digest, err := sha256File(srcPath)
	if err != nil {
		return 0, "", err
	}
	if expectedSHA != "" && !strings.EqualFold(digest, expectedSHA) {
		return 0, "", fmt.Errorf("evidence: sha256 mismatch for %q", storedName)
	}

	in, err := os.Open(srcPath)
	if err != nil {
		return 0, "", err
	}
	defer in.Close()
	return stageStream(in, digest, destDir, storedName, required, expectedSHA, syncRoot, fsync)
}

// stageStream is the core primitive for staging evidence. It copies src into the
// immutable evidence directory, refusing to overwrite a different file of the same
// name. It then re-verifies the bytes that landed on disk.
func stageStream(src io.Reader, digest, destDir, storedName string, required bool, expectedSHA string, syncRoot string, fsync func(string) error) (int64, string, error) {
	destPath := filepath.Join(destDir, storedName)
	// Create the destination parent chain (the evidence dir plus any validated
	// nested subdirectories) so a nested stored path stages correctly. Keep the
	// directories this call created. Each of their entries has to be synced too,
	// or a crash can lose the whole capture even though its bytes were durable.
	created, err := mkdirAllTracked(filepath.Dir(destPath))
	if err != nil {
		return 0, "", err
	}
	if fsync == nil {
		fsync = durablefile.FsyncDir
	}

	// Immutability: never overwrite an already-staged file. If the exact
	// destination already exists, dedupe when its content matches the source,
	// otherwise fail.
	if dstat, err := os.Stat(destPath); err == nil {
		existing, err := sha256File(destPath)
		if err != nil {
			return 0, "", err
		}
		if existing == digest {
			return dstat.Size(), existing, nil // dedupe: reuse the staged file
		}
		return 0, "", fmt.Errorf("evidence: stored path %q already exists with different content", storedName)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return 0, "", err
	}

	// Always a byte copy, never a hard link: the source usually lives in a
	// worker's working directory, and a later rewrite of it in place must not
	// change evidence already staged.
	if err := copyStream(src, destPath); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return 0, "", fmt.Errorf("evidence: stored path %q already exists", storedName)
		}
		return 0, "", err
	}

	// Hash the bytes actually on disk after writing (not just the source) and
	// verify any declared hash, so the recorded digest is a verified property
	// of the staged bytes even if the source changed during the write.
	storedDigest, err := sha256File(destPath)
	if err != nil {
		return 0, "", err
	}
	if expectedSHA != "" && !strings.EqualFold(storedDigest, expectedSHA) {
		os.Remove(destPath)
		return 0, "", fmt.Errorf("evidence: sha256 mismatch for %q", storedName)
	}
	storedInfo, err := os.Stat(destPath)
	if err != nil {
		return 0, "", err
	}
	if required && storedInfo.Size() == 0 {
		os.Remove(destPath)
		return 0, "", errors.New("evidence: required file is empty")
	}
	// The file's bytes, then every directory entry staging created. That means the
	// leaf's entry in its parent, each created directory, and the first
	// pre-existing ancestor that holds the shallowest created one.
	if err := fsyncEvidenceDirs(fsync, syncRoot, destDir, created); err != nil {
		return 0, "", err
	}
	return storedInfo.Size(), storedDigest, nil
}

// mkdirAllTracked creates dir and any missing parents at the state directory
// mode. It returns the directories it created from shallowest to deepest, or
// nil when dir already existed.
func mkdirAllTracked(dir string) ([]string, error) {
	var created []string
	for cur := dir; ; cur = filepath.Dir(cur) {
		if _, err := os.Stat(cur); err == nil {
			break
		} else if !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
		created = append([]string{cur}, created...)
		if parent := filepath.Dir(cur); parent == cur {
			break
		}
	}
	if len(created) == 0 {
		return nil, nil
	}
	if err := os.MkdirAll(dir, StateDirPerm); err != nil {
		return nil, err
	}
	return created, nil
}

// fsyncEvidenceDirs syncs the staged file's directory and its ancestors through
// the instances directory, deepest first.
//
// Ancestors are synced on every staging, even when this call did not create
// them. An earlier attempt that failed before its own sync can leave an unsynced
// entry behind. A crash would then lose the evidence directory it created, even
// though a later, synced file lives inside it.
func fsyncEvidenceDirs(fsync func(string) error, syncRoot, leafDir string, created []string) error {
	dirs := make([]string, 0, len(created)+4)
	add := func(dir string) {
		for _, existing := range dirs {
			if existing == dir {
				return
			}
		}
		dirs = append(dirs, dir)
	}
	for i := len(created) - 1; i >= 0; i-- {
		add(created[i])
	}
	for dir := leafDir; ; dir = filepath.Dir(dir) {
		add(dir)
		if syncRoot == "" || dir == syncRoot {
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir || !strings.HasPrefix(dir, syncRoot+string(filepath.Separator)) {
			break
		}
	}
	// Instance creation does not sync its parent. Sync it on every staging so
	// a crash cannot lose the instance directory behind the evidence.
	if syncRoot != "" {
		if parent := filepath.Dir(syncRoot); parent != syncRoot {
			add(parent)
		}
	}
	for _, dir := range dirs {
		if err := fsync(dir); err != nil {
			return err
		}
	}
	return nil
}

// StageEvidence — public Store method (no workflow state transition).
func (s *Store) StageEvidence(workflowID WorkflowID, srcPath, storedName string, required bool, expectedSHA256 string) (StagedEvidence, error) {
	evID, size, digest, err := s.stageIntoFreshDir(workflowID, storedName, func(dir string) (int64, string, error) {
		return stageInto(srcPath, dir, storedName, required, expectedSHA256, s.instanceDir(workflowID), s.evidenceFsync)
	})
	if err != nil {
		// stageIntoFreshDir already removed the directory it minted.
		return StagedEvidence{}, err
	}

	return StagedEvidence{
		EvidenceID:   evID,
		OriginalPath: srcPath,
		StoredPath:   filepath.Join("evidence", string(evID), storedName),
		Size:         size,
		SHA256:       digest,
	}, nil
}

// stageIntoFreshDir stages under a fresh evidence ID. On failure, it attempts
// to remove that ID's directory without touching shared ancestors.
func (s *Store) stageIntoFreshDir(workflowID WorkflowID, storedName string, stage func(dir string) (int64, string, error)) (EvidenceID, int64, string, error) {
	evID, dir, err := s.prepareEvidenceDir(workflowID, storedName)
	if err != nil {
		return "", 0, "", err
	}
	size, digest, err := stage(dir)
	if err != nil {
		if discardErr := s.DiscardEvidence(workflowID, evID); discardErr != nil {
			// The staging error decides what the caller must do, so it keeps its
			// identity. The copy this attempt could not remove is announced with
			// it instead of being dropped silently: it holds the captured content
			// and no recorded fact names it.
			return "", 0, "", errors.Join(err, fmt.Errorf("evidence: discard orphaned evidence %s: %w", evID, discardErr))
		}
		return "", 0, "", err
	}
	return evID, size, digest, nil
}

// prepareEvidenceDir validates one staging request and returns a fresh evidence
// ID with its directory.
func (s *Store) prepareEvidenceDir(workflowID WorkflowID, storedName string) (EvidenceID, string, error) {
	if !safeComponent(string(workflowID)) {
		return "", "", errors.New("invalid workflow id")
	}
	if err := validateStoredPath(storedName); err != nil {
		return "", "", err
	}
	evID := NewEvidenceID()
	return evID, evidenceDir(s.instanceDir(workflowID), evID), nil
}

// DiscardEvidence removes one staged evidence directory, for every caller that
// holds a copy no committed record names: a gate command that failed to land, a
// completion whose staging failed part-way. It removes that evidence ID's own
// directory only, never the shared evidence, instance, or instances directories.
// Discarding an absent evidence ID is a no-op.
func (s *Store) DiscardEvidence(workflowID WorkflowID, id EvidenceID) error {
	if !safeComponent(string(workflowID)) || !safeComponent(string(id)) {
		return errors.New("invalid evidence id")
	}
	if fault := s.discardFaultFunc(); fault != nil {
		if faultErr := fault(id); faultErr != nil {
			return faultErr
		}
	}
	dir := evidenceDir(s.instanceDir(workflowID), id)
	if _, err := os.Stat(dir); errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return os.RemoveAll(dir)
}
