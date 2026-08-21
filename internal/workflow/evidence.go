package workflow

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
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