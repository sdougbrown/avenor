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
)

// evidenceLink is the hard-link primitive used by the stager. It is a
// variable so tests can force link failure (e.g. cross-device) deterministically.
var evidenceLink = os.Link

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

// copyFile copies srcPath to destPath with O_CREATE|O_EXCL so it never
// overwrites an existing file, and fsyncs the result.
func copyFile(srcPath, destPath string) error {
	in, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
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

// stageInto is the core primitive for staging evidence.
func stageInto(srcPath, destDir, storedName string, required bool, expectedSHA string) (int64, string, error) {
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

	destPath := filepath.Join(destDir, storedName)
	// Create the destination parent chain (the evidence dir plus any validated
	// nested subdirectories) so a nested stored path stages correctly.
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return 0, "", err
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

	// Same-filesystem hard link first (an explicit optimization: it shares the
	// source inode, so the source must be treated as immutable after staging);
	// fall back to a real byte copy when linking fails (e.g. cross-device).
	if err := evidenceLink(srcPath, destPath); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return 0, "", fmt.Errorf("evidence: stored path %q already exists", storedName)
		}
		if err := copyFile(srcPath, destPath); err != nil {
			return 0, "", err
		}
	}

	// Hash the bytes actually on disk after writing (not just the source) and
	// verify any declared hash, so the recorded digest is a verified property
	// of the staged bytes even if the source changed during the write.
	storedDigest, err := sha256File(destPath)
	if err != nil {
		return 0, "", err
	}
	if expectedSHA != "" && !strings.EqualFold(storedDigest, expectedSHA) {
		return 0, "", fmt.Errorf("evidence: sha256 mismatch for %q", storedName)
	}
	storedInfo, err := os.Stat(destPath)
	if err != nil {
		return 0, "", err
	}
	if err := fsyncDir(destDir); err != nil {
		return 0, "", err
	}
	return storedInfo.Size(), storedDigest, nil
}

// StageEvidence — public Store method (no workflow state transition).
func (s *Store) StageEvidence(workflowID WorkflowID, srcPath, storedName string, required bool, expectedSHA256 string) (StagedEvidence, error) {
	if !safeComponent(string(workflowID)) {
		return StagedEvidence{}, errors.New("invalid workflow id")
	}
	if err := validateStoredPath(storedName); err != nil {
		return StagedEvidence{}, err
	}
	evID := NewEvidenceID()
	dir := evidenceDir(s.instanceDir(workflowID), evID)

	size, digest, err := stageInto(srcPath, dir, storedName, required, expectedSHA256)
	if err != nil {
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
